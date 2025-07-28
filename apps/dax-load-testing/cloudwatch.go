package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

func getCloudwatch(cfg *aws.Config) *cloudwatch.Client {
	return cloudwatch.NewFromConfig(*cfg)
}

type CloudWatchSender struct {
	client      *cloudwatch.Client
	buffer      chan cwtypes.MetricDatum
	cancelFuncs []context.CancelFunc
	cancel      context.CancelFunc
}

func (c *CloudWatchSender) Send(md cwtypes.MetricDatum) {
	if c.buffer == nil {
		panic("buffer is nil?")
	}

	c.buffer <- md
}

func (c *CloudWatchSender) Start() {
	if c.cancel != nil {
		panic("already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	// start an initial set of workers
	for range 128 {
		nCtx, nCancel := context.WithCancel(context.Background())
		c.cancelFuncs = append(c.cancelFuncs, nCancel)

		go collectMetricWorker(nCtx, c.client, c.buffer)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				for i := range c.cancelFuncs {
					c.cancelFuncs[i]()
				}
				c.cancelFuncs = []context.CancelFunc{}
				return
			case <-time.After(time.Minute * time.Duration(Flags.App.CloudWatch.PushFrequencyMinutes)):
				break
			}

			currentQueueSize := len(c.buffer)
			actual := len(c.cancelFuncs)
			expected := currentQueueSize / 1000

			if actual < 500 && currentQueueSize > 1000 {
				toStart := min(500-actual, expected, 500)

				if toStart+actual > 500 {
					toStart = 500 - actual
				}

				log.Printf("Starting %d metric workers", toStart)
				for range toStart {
					nCtx, nCancel := context.WithCancel(context.Background())
					c.cancelFuncs = append(c.cancelFuncs, nCancel)

					go collectMetricWorker(nCtx, c.client, c.buffer)
				}
			} else if actual > 1 && currentQueueSize < 1000 {
				c.cancelFuncs[0]()
				c.cancelFuncs = c.cancelFuncs[1:]
			}
		}
	}()
}

func collectMetricWorker(ctx context.Context, cw *cloudwatch.Client, metricChan chan cwtypes.MetricDatum) {
	metricData := make([]cwtypes.MetricDatum, 0, 500)

	for shouldExit := false; !shouldExit; {
		shouldSend := false

		select {
		case <-ctx.Done():
			shouldExit = true
			//log.Println("[CANCEL] Forcing send of:", len(metricData))
		case <-time.After(time.Second * 10):
			shouldSend = true
			//log.Println("[TIMEOUT] Forcing send of:", len(metricData))
			break
		case md, ok := <-metricChan:
			if ok {
				metricData = append(metricData, md)
			}
		}

		shouldSend = shouldSend || len(metricData) == 500 || shouldExit

		if shouldSend && len(metricData) > 0 {
			_, _ = cw.PutMetricData(context.Background(), &cloudwatch.PutMetricDataInput{
				Namespace:  aws.String(Flags.App.CloudWatch.Namespace),
				MetricData: metricData,
			})
			metricData = make([]cwtypes.MetricDatum, 0, 500)
		}
	}
}

func (c *CloudWatchSender) Stop() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}

func NewCloudWatchSender(client *cloudwatch.Client) *CloudWatchSender {
	return &CloudWatchSender{
		client: client,
		buffer: make(chan cwtypes.MetricDatum, 1024*1024),
	}
}
