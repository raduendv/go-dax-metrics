package helper

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

type Manager struct {
	Endpoint      string
	Table         string
	TestConfig    TestConfig
	ClientConfig  ClientConfig
	TrafficConfig TrafficConfig
	Operation     string
	WaitGroup     *sync.WaitGroup
}

func NewManager(endpoint, table string, testConfig TestConfig, clientConfig ClientConfig, trafficConfig TrafficConfig, operation string) *Manager {
	m := &Manager{
		Endpoint:      endpoint,
		Table:         table,
		TestConfig:    testConfig,
		ClientConfig:  clientConfig,
		TrafficConfig: trafficConfig,
		Operation:     operation,
		WaitGroup:     &sync.WaitGroup{},
	}

	return m
}

func (m *Manager) getClientAndProvider() (*dax.Dax, *AsyncMeterProvider) {
	region := GetEnvVar("REGION", "eu-west-1")
	cfg := GetAWSConfig(region)
	if cfg == nil {
		panic("Unable to get aws.Config")
	}

	daxCfg := dax.NewConfig(*cfg, m.Endpoint)
	// populate dax config
	daxCfg.SkipHostnameVerification = true
	daxCfg.MaxPendingConnectionsPerHost = int(m.ClientConfig.MaxPendingConnections)
	daxCfg.ReadRetries = int(m.ClientConfig.ReadRetries)
	daxCfg.WriteRetries = int(m.ClientConfig.WriteRetries)
	daxCfg.RequestTimeout = time.Millisecond * time.Duration(m.ClientConfig.RequestTimeout)
	daxCfg.DialContext = (&net.Dialer{
		Timeout:   time.Second * time.Duration(m.ClientConfig.ConnectionTimeout),
		KeepAlive: time.Minute,
	}).DialContext
	daxCfg.LogLevel = 0 // utils.LogDebugWithRequestRetries

	cw := cloudwatch.NewFromConfig(*cfg)
	mp := NewAsyncMeterProvider(int(m.ClientConfig.MaxConcurrency)*1000, m.WaitGroup, cw)
	daxCfg.MeterProvider = mp

	// ConnectionTimeout     int64 `json:"ConnectionTimeout"`
	// RequestTimeout        int64 `json:"RequestTimeout"`
	// ReadRetries           int64 `json:"ReadRetries"`
	// WriteRetries          int64 `json:"WriteRetries"`
	// MaxConcurrency        int64 `json:"MaxConcurrency"`
	// MaxPendingConnections int64 `json:"MaxPendingConnections"`

	daxCfg.ClientHealthCheckInterval = time.Second / 2
	daxCfg.ClusterUpdateInterval = time.Second / 2
	daxCfg.RouteManagerEnabled = true

	daxClient, err := dax.New(daxCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to get dax config: %v", err))
	}

	return daxClient, mp
}

func (m *Manager) runGetItem(dx *dax.Dax) {
	wg := &sync.WaitGroup{}

	for range 48 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1024 {
				for c := range 1024 {
					item, err := dx.GetItem(
						context.Background(),
						&dynamodb.GetItemInput{
							TableName: pointer(m.Table),
							Key: map[string]types.AttributeValue{
								"pk": &types.AttributeValueMemberN{
									Value: "251",
								},
								"sk": &types.AttributeValueMemberN{
									Value: fmt.Sprintf("%d", c%50),
								},
							},
						},
					)
					if err != nil {
						fmt.Printf("GetItem() error: %v\n", err)

						continue
					}

					_ = item
				}
			}
		}()

		rng := rand.Int64N(1024)
		<-time.After(time.Millisecond * time.Duration(rng))
	}

	wg.Wait()
}
func (m *Manager) runPutItem(dx *dax.Dax) {
	for c := range 1024 {
		item, err := dx.PutItem(
			context.Background(),
			&dynamodb.PutItemInput{
				TableName: pointer(m.Table),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c/50+1558),
					},
					"sk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c%50),
					},
					"a1": &types.AttributeValueMemberS{
						Value: strings.Repeat("1", 256),
					},
					"a2": &types.AttributeValueMemberS{
						Value: strings.Repeat("2", 256),
					},
					"a3": &types.AttributeValueMemberS{
						Value: strings.Repeat("3", 256),
					},
					"a4": &types.AttributeValueMemberS{
						Value: strings.Repeat("4", 256),
					},
					"a5": &types.AttributeValueMemberS{
						Value: strings.Repeat("5", 256),
					},
					"a6": &types.AttributeValueMemberS{
						Value: strings.Repeat("6", 256),
					},
				},
			},
		)
		if err != nil {
			fmt.Printf("PutItem() error: %v", err)

			continue
		}

		_ = item
	}
}
func (m *Manager) runUpdateItem(dx *dax.Dax) {
	for c := range 4 {
		item, err := dx.UpdateItem(
			context.Background(),
			&dynamodb.UpdateItemInput{
				TableName: pointer(m.Table),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c/50+1558),
					},
					"sk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c%50),
					},
				},
				UpdateExpression: aws.String("SET a6 = :newA7"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":newA7": &types.AttributeValueMemberS{
						Value: strings.Repeat("7", 256),
					},
				},
				ReturnValues: types.ReturnValueNone,
			},
		)
		if err != nil {
			fmt.Printf("UpdateItem() error: %v\n", err)

			continue
		}

		_ = item
	}
}
func (m *Manager) runBatchGetItem(dx *dax.Dax) {
	var requests []map[string]types.AttributeValue
	for c := range 1024 {
		requests = append(requests, map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", c/50+1558),
			},
			"sk": &types.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", c%50),
			},
		})
	}

	for len(requests) > 0 {
		var toSend []map[string]types.AttributeValue
		if len(requests) > 25 {
			toSend = requests[0:25]
		} else {
			toSend = requests[0:]
		}

		items, err := dx.BatchGetItem(
			context.Background(),
			&dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					m.Table: {
						Keys: toSend,
					},
				},
			},
		)
		if err != nil {
			fmt.Printf("BatchGetItem() error: %v\n", err)

			continue
		}

		_ = items

		requests = requests[len(toSend):]
	}
}

func (m *Manager) runBatchWriteItem(dx *dax.Dax) {
	var requests []types.WriteRequest
	for c := range 32 {
		requests = append(requests, types.WriteRequest{
			DeleteRequest: nil,
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c/50+1558),
					},
					"sk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c%50),
					},
					"a1": &types.AttributeValueMemberS{
						Value: strings.Repeat("1", 256),
					},
					"a2": &types.AttributeValueMemberS{
						Value: strings.Repeat("2", 256),
					},
					"a3": &types.AttributeValueMemberS{
						Value: strings.Repeat("3", 256),
					},
					"a4": &types.AttributeValueMemberS{
						Value: strings.Repeat("4", 256),
					},
					"a5": &types.AttributeValueMemberS{
						Value: strings.Repeat("5", 256),
					},
					"a6": &types.AttributeValueMemberS{
						Value: strings.Repeat("6", 256),
					},
				},
			},
		})
	}

	for len(requests) > 0 {
		var toSend []types.WriteRequest
		if len(requests) > 25 {
			toSend = requests[0:25]
		} else {
			toSend = requests[0:]
		}

		items, err := dx.BatchWriteItem(
			context.Background(),
			&dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					m.Table: toSend,
				},
				ReturnConsumedCapacity:      types.ReturnConsumedCapacityNone,
				ReturnItemCollectionMetrics: types.ReturnItemCollectionMetricsNone,
			},
		)
		if err != nil {
			fmt.Printf("BatchWriteItem() error: %v\n", err)

			continue
		}

		_ = items

		requests = requests[len(toSend):]
	}
}

func (m *Manager) runTransactGetItems(dx *dax.Dax) {
	var requests []types.TransactGetItem
	for c := range 1024 {
		requests = append(requests, types.TransactGetItem{
			Get: &types.Get{
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c/50+1558),
					},
					"sk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c%50),
					},
				},
				TableName: pointer(m.Table),
			},
		})
	}

	for len(requests) > 0 {
		var toSend []types.TransactGetItem
		if len(requests) > 25 {
			toSend = requests[0:25]
		} else {
			toSend = requests[0:]
		}

		items, err := dx.TransactGetItems(
			context.Background(),
			&dynamodb.TransactGetItemsInput{
				TransactItems:          toSend,
				ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
			},
		)
		if err != nil {
			fmt.Printf("TransactGetItems() error: %v\n", err)

			continue
		}

		_ = items

		requests = requests[len(toSend):]
	}
}
func (m *Manager) runTransactWriteItems(dx *dax.Dax) {
	var requests []types.TransactWriteItem
	for c := range 1024 {
		requests = append(requests, types.TransactWriteItem{
			Put: &types.Put{
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c/50+1558),
					},
					"sk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c%50),
					},
					"a1": &types.AttributeValueMemberS{
						Value: strings.Repeat("1", 256),
					},
					"a2": &types.AttributeValueMemberS{
						Value: strings.Repeat("2", 256),
					},
					"a3": &types.AttributeValueMemberS{
						Value: strings.Repeat("3", 256),
					},
					"a4": &types.AttributeValueMemberS{
						Value: strings.Repeat("4", 256),
					},
					"a5": &types.AttributeValueMemberS{
						Value: strings.Repeat("5", 256),
					},
					"a6": &types.AttributeValueMemberS{
						Value: strings.Repeat("7", 256),
					},
				},
				TableName: pointer(m.Table),
			},
		})
	}

	for len(requests) > 0 {
		var toSend []types.TransactWriteItem
		if len(requests) > 25 {
			toSend = requests[0:25]
		} else {
			toSend = requests[0:]
		}

		items, err := dx.TransactWriteItems(
			context.Background(),
			&dynamodb.TransactWriteItemsInput{
				TransactItems:          toSend,
				ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
			},
		)
		if err != nil {
			fmt.Printf("TransactWriteItems() error: %v\n", err)

			continue
		}

		_ = items

		requests = requests[len(toSend):]
	}
}
func (m *Manager) runQuery(dx *dax.Dax) {
	for c := 0; c < 1024; c += 50 {
		var exclusiveStartKey map[string]types.AttributeValue

		for {
			res, err := dx.Query(
				context.Background(),
				&dynamodb.QueryInput{
					TableName:              pointer(m.Table),
					ExclusiveStartKey:      exclusiveStartKey,
					KeyConditionExpression: pointer("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", c/50+1558),
						},
					},
					Limit: pointer[int32](25),
				},
			)
			if err != nil {
				fmt.Printf("Query() error: %v\n", err)

				continue
			}

			exclusiveStartKey = res.LastEvaluatedKey

			if len(exclusiveStartKey) == 0 {
				break
			}
		}
	}
}
func (m *Manager) runScan(dx *dax.Dax) {
	for c := 0; c < 1024; c += 50 {
		var exclusiveStartKey map[string]types.AttributeValue

		for {
			res, err := dx.Scan(
				context.Background(),
				&dynamodb.ScanInput{
					TableName:         pointer(m.Table),
					ExclusiveStartKey: exclusiveStartKey,
					FilterExpression:  pointer("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", c/50+1558),
						},
					},
					Limit: pointer[int32](25),
				},
			)
			if err != nil {
				fmt.Printf("Query() error: %v\n", err)

				continue
			}

			exclusiveStartKey = res.LastEvaluatedKey

			if len(exclusiveStartKey) == 0 {
				break
			}
		}
	}
}
func (m *Manager) runDeleteItem(dx *dax.Dax) {
	for c := range 1024 {
		item, err := dx.DeleteItem(
			context.Background(),
			&dynamodb.DeleteItemInput{
				TableName: pointer(m.Table),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c/50+1558),
					},
					"sk": &types.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", c%50),
					},
				},
			},
		)
		if err != nil {
			fmt.Printf("DeleteItem() error: %v\n", err)

			continue
		}

		_ = item
	}
}

func (m *Manager) Run() {
	fmt.Printf("Running test %s for operation %s\n", m.TestConfig.Name, m.Operation)
	defer fmt.Printf("Done running test %s for operation %s\nHave a nice day!\n", m.TestConfig.Name, m.Operation)

	daxClient, mp := m.getClientAndProvider()

	defer func(mp *AsyncMeterProvider, dx *dax.Dax) {
		fmt.Println("Closing dax client")
		dx.Close()
		fmt.Println("Closed dax client")

		for len(mp.MainChannel) > 50 {
			mp.push()
		}
		fmt.Println("Remaining unsent:", len(mp.MainChannel))
	}(mp, daxClient)

	runners := []func(*Manager, *dax.Dax){
		(*Manager).runPutItem,
		(*Manager).runGetItem,
		(*Manager).runUpdateItem,
		(*Manager).runBatchGetItem,
		(*Manager).runBatchWriteItem,
		(*Manager).runTransactGetItems,
		(*Manager).runTransactWriteItems,
		(*Manager).runQuery,
		(*Manager).runScan,
		(*Manager).runDeleteItem,
	}

	activeRunners := int64(0)
	go func() {
		canRun := true
		ar := atomic.LoadInt64(&activeRunners)

		for canRun || (!canRun && ar > 0) {
			<-time.After(time.Second)

			atomic.StoreInt64(&ar, atomic.LoadInt64(&activeRunners))

			if canRun && ar > 0 {
				canRun = false
			}

			fmt.Println("Active runners:", ar)
		}
	}()
	rwg := &sync.WaitGroup{}
	for idx, runner := range runners {
		switch idx {
		case 0:
			runner(m, daxClient)

		case len(runners) - 1:
			rwg.Wait() // wait before delete
			runner(m, daxClient)

		default:
			<-time.After(time.Second * 5)
			rwg.Add(1)
			go func(id int) {
				fmt.Println("Starting runner:", id)
				atomic.AddInt64(&activeRunners, 1)
				defer rwg.Done()
				runner(m, daxClient)
				atomic.AddInt64(&activeRunners, -1)
				fmt.Println("Ending runner:", id)
			}(idx)
		}
	}

	m.WaitGroup.Wait()
}
