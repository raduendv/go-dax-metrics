package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

func main() {
	flags := getFlags()
	appConfig := getAppConfig(flags)

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()
	_ = sigCtx

	awsCfg := getAwsConfig()

	cw := getCloudwatch(awsCfg)
	daxSvc := getDaxSvc(awsCfg)

	// cluster setup and tear down
	clusterName := fmt.Sprintf("radu-cluster-%s", time.Now().Format("2006-01-02-15-04-05"))
	parameterGroup := ""
	switch appConfig.TrafficConfig.CacheHitPercentage {
	case 0:
		parameterGroup = noTtl
	case 50:
		parameterGroup = lowTtl
	default:
		parameterGroup = normalTtl
	}
	clusterEndpoint := daxSvc.CreateCluster(clusterName, parameterGroup)
	if clusterEndpoint == "" {
		panic("empty cluster endpoint")
	}
	defer daxSvc.DeleteCluster(clusterName)
	//clusterName := "radu-cluster-2025-06-08-21-35-11"
	//clusterEndpoint := "dax://radu-cluster-2025-06-08-21-35-11.cykcls.dax-clusters.eu-west-1.amazonaws.com"
	fmt.Println("New cluster available at:", clusterEndpoint)

	daxClient, err := getDaxClient(awsCfg, clusterEndpoint, appConfig)
	if err != nil {
		panic(err)
	}

	if !isWriteOp(flags.op) {
		loadDataForRead(sigCtx, daxClient, appConfig.Table, appConfig.TrafficConfig.ItemSizes[flags.op])
	}

	go func() {
		<-time.After(time.Minute * 120)
		log.Println("Time limit reached!")
		sigCancel()
	}()

	metricChan := make(chan types.MetricDatum, 500_000)

	go runMetricCollector(sigCtx, cw, metricChan, flags)

	if appConfig.TestConfig.Reboot > 0 {
		log.Printf("Will reboot random node once every %d ms", appConfig.TestConfig.Reboot)

		go func() {
			ticker := time.NewTicker(time.Millisecond * time.Duration(appConfig.TestConfig.Reboot))
			for {
				select {
				case <-sigCtx.Done():
					return
				case <-ticker.C:
					log.Println("Rebooting random node")
					daxSvc.RebootRandomNode(clusterName)
					log.Println("Reboot command sent")
				}
			}
		}()
	}

	run(sigCtx, cw, metricChan, daxClient, appConfig, clusterName, flags.op)
}

func collectMetricWorker(ctx context.Context, cw *cloudwatch.Client, metricChan chan types.MetricDatum, f *flags) {
	metricData := make([]types.MetricDatum, 0, 500)

	for shouldExit := false; !shouldExit; {
		shouldSend := false

		select {
		case <-ctx.Done():
			shouldExit = true
			log.Println("[CANCEL] Forcing send of:", len(metricData))
		case <-time.After(time.Second * 10):
			shouldSend = true
			log.Println("[TIMEOUT] Forcing send of:", len(metricData))
			break
		case md, ok := <-metricChan:
			if ok {
				md.Dimensions = []types.Dimension{
					{
						Name:  aws.String("test"),
						Value: aws.String(f.test),
					},
					{
						Name:  aws.String("op"),
						Value: aws.String(f.op),
					},
				}
				metricData = append(metricData, md)
			}
		}

		shouldSend = shouldSend || len(metricData) == 500 || shouldExit

		if shouldSend && len(metricData) > 0 {
			_, _ = cw.PutMetricData(context.Background(), &cloudwatch.PutMetricDataInput{
				Namespace:  aws.String("github.com/aws/aws-dax-go-v2"),
				MetricData: metricData,
			})
			metricData = make([]types.MetricDatum, 0, 500)
		}
	}
}

func runMetricCollector(ctx context.Context, cw *cloudwatch.Client, metricChan chan types.MetricDatum, f *flags) {
	var cancelFuncs []context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			for c := range cancelFuncs {
				cancelFuncs[c]()
			}
			return
		case <-time.After(time.Second):
		}

		currentQueueSize := len(metricChan)
		actual := len(cancelFuncs)
		expected := currentQueueSize / 1000
		if actual < 500 && currentQueueSize > 1000 {
			toStart := min(500-actual, expected, 500)

			// might be redundant, but let's be extra safe
			if toStart+actual > 500 {
				toStart = 500 - actual
			}

			log.Printf("Starting %d metric workers", toStart)
			for range toStart {
				nCtx, nCancel := context.WithCancel(context.Background())
				cancelFuncs = append(cancelFuncs, nCancel)

				go collectMetricWorker(nCtx, cw, metricChan, f)
			}
		} else if actual > 1 && currentQueueSize < 1000 {
			cancelFuncs[0]()
			cancelFuncs = cancelFuncs[1:]
		}
	}
}

func run(ctx context.Context, cw *cloudwatch.Client, metricChan chan types.MetricDatum, client *dax.Dax, appConfig *AppConfig, clusterName, op string) {
	var cancelFuncs []context.CancelFunc
	ticker := time.NewTicker(time.Minute)
	throttleChan := make(chan bool)
	loadBias := 1.0

	tableName := appConfig.Table

	maxIncrease := 64
	if isWriteOp(op) {
		maxIncrease = 16
	}

	var worker workerFn
	switch op {
	case "GetItem":
		worker = workerGetItem
	case "BatchGetItem":
		worker = workerBatchGetItem
	case "Query":
		worker = workerQuery
	case "PutItem":
		worker = workerPutItem
	case "UpdateItem":
		worker = workerUpdateItem
	case "BatchWriteItem":
		worker = workerBatchWriteItem
	default:
		worker = workerWrite
	}

	for range 16 {
		nCtx, nCancel := context.WithCancel(context.Background())
		cancelFuncs = append(cancelFuncs, nCancel)

		go worker(nCtx, metricChan, client, tableName, appConfig, throttleChan)
	}

	lastBiasChange := time.Now().Unix() - 3600

	for {
		select {
		case <-throttleChan:
			if time.Now().Unix()-lastBiasChange > 5 {
				loadBias *= 1.1
				// allow loadBias change once per minute
				lastBiasChange = time.Now().Unix()
				log.Println("Throttle detected, increased load loadBias to:", loadBias)
				if len(cancelFuncs) > 0 {
					cancelFuncs[0]()
					cancelFuncs = cancelFuncs[1:]
				}
			}
			continue
		case <-ctx.Done():
			for c := range cancelFuncs {
				cancelFuncs[c]()
			}

			fmt.Println("CTRL+C pressed")
			return

		case <-ticker.C:
			//
		}

		if time.Now().Unix()-lastBiasChange > 60 {
			if loadBias > 1.0 {
				loadBias /= 1.1
			}
			if loadBias < 1.0 {
				loadBias = 1.0
			}
		}

		stat := getLastMinuteStats(cw, clusterName)
		avg := *stat.Datapoints[0].Average
		log.Printf("Average load %08f", avg)

		avg = avg * loadBias
		if loadBias > 1.0 {
			log.Printf("Load with loadBias: %08f", avg)
		}

		l := len(cancelFuncs)
		a := avg / float64(l)

		if avg < 80 {
			log.Printf("Num go routines: %d", len(cancelFuncs))
			log.Printf("Average load per goroutine: %08f", a)

			if l >= 1000 {
				continue
			}

			s := int((80 - avg) / a)

			if s > maxIncrease {
				s = maxIncrease
			}

			log.Printf("Will start %d goroutine(s)", s)
			for range s {
				nCtx, nCancel := context.WithCancel(context.Background())
				cancelFuncs = append(cancelFuncs, nCancel)

				<-time.After(time.Millisecond)
				go worker(nCtx, metricChan, client, tableName, appConfig, throttleChan)
			}
		} else {
			if len(cancelFuncs) == 0 {
				panic("Load over 80 with zero goroutines!")
			}

			s := int((avg - 80) / a)
			if s == 0 {
				s = int(avg - 80)
			}

			log.Printf("Will stop %d goroutine(s)", s)
			for range s {
				if len(cancelFuncs) > 0 {
					cancelFuncs[0]()
					cancelFuncs = cancelFuncs[1:]
				}
			}
		}
	}
}
