package main

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	awsCfg := getAwsConfig()

	if Flags.AWS.DynamoDB.CreateTable {
		// create table
	}

	defaultDaxClient, err := getDefaultDaxClient(awsCfg)
	if err != nil {
		panic(err)
	}

	defaultDynamoDBClient := dynamodb.NewFromConfig(*awsCfg)
	cw := getCloudwatch(awsCfg)

	cwSender := NewCloudWatchSender(cw)
	cwSender.Start()
	defer cwSender.Stop()

	c := &DataLoader{
		daxClient:      defaultDaxClient,
		dynamoDBClient: defaultDynamoDBClient,

		randomGetItemMiss:      NewRandom[int](2_000, 60_000),
		randomBatchGetItemMiss: NewRandom[int](1_001, 100_000),
		randomQueryMiss:        NewRandom[int](501, 80_000),
		randomGetItemHit:       NewRandom[int](101, 1),
		randomBatchGetItemHit:  NewRandom[int](101, 400),
		randomQueryHit:         NewRandom[int](101, 200),

		metricsService: Metrics{
			cwSender: cwSender,
		},
	}

	if Flags.AWS.DynamoDB.LoadData {
		//set reprocessing enabled true
		if Flags.App.WithCacheMiss {
			log.Print("Start inserting data for cache miss!")
			c.loadSampleData(50001, 60000, 100, 1024, false)
			c.loadSampleData(60001, 70000, 100, 1024, false)
			c.loadSampleData(70001, 79999, 100, 1024, false)
			c.loadSampleData(80000, 81000, 200, 1024, false)
			c.loadSampleData(100000, 101000, 100, 1024*5, false)
		} else {
			log.Print("Start inserting data for cache hit!")
			c.loadSampleData(1, 100, 100, 1024, true)
			c.loadSampleData(200, 300, 200, 1024, true)
			c.loadSampleData(400, 500, 1, 1024*5, true)
		}
		//set reprocessing enabled false
	}

	if Flags.App.StartTraffic {
		if Flags.App.WriteTest {
			log.Printf("Starting traffic generation with ramp-up adjustment - Cache hit %s default config", ternary(Flags.App.WithCacheMiss, "50%", "100%"))
			c.trafficRampUp(*awsCfg, false)
			//		dynamoDbLoadData.setClientAggressive(false);
			//		dynamoDbLoadData.trafficRampUp();
			log.Printf("Finished traffic generation with ramp-up adjustment - Cache hit %s default config", ternary(Flags.App.WithCacheMiss, "50%", "100%"))
			//		Thread.sleep(5*60*1000);
			time.Sleep(time.Minute * 5)
			log.Printf("Starting traffic generation with ramp-up adjustment - Cache hit %s aggressive config", ternary(Flags.App.WithCacheMiss, "50%", "100%"))
			c.trafficRampUp(*awsCfg, true)
			//		dynamoDbLoadData.setClientAggressive(true);
			//		dynamoDbLoadData.trafficRampUp();
			//		dynamoDbLoadData.setClientAggressive(false);
			log.Printf("Finished traffic generation with ramp-up adjustment - Cache hit %s aggressive config", ternary(Flags.App.WithCacheMiss, "50%", "100%"))
			//		Thread.sleep(5*60*1000);
			time.Sleep(time.Minute * 5)
			log.Printf("Starting traffic generation with ramp-up adjustment - Cache hit %s default config - node restarting every 10 minute", ternary(Flags.App.WithCacheMiss, "50%", "100%"))
			c.trafficRampUp(*awsCfg, false)
			//		setRestartingClusterNode(true);
			//		dynamoDbLoadData.setClientAggressive(false);
			//		dynamoDbLoadData.trafficRampUp();
			//		setRestartingClusterNode(false);
			log.Printf("Finished traffic generation with ramp-up adjustment - Cache hit %s default config - node restarting every 10 minute", ternary(Flags.App.WithCacheMiss, "50%", "100%"))
			//		Thread.sleep(5*60*1000);
			time.Sleep(time.Minute * 5)
		} else {
			log.Print("Starting write test with ramp-up adjustment - default config")
			//		dynamoDbLoadData.setClientAggressive(false);
			//		dynamoDbLoadData.trafficRampUp();
			c.trafficRampUp(*awsCfg, false)
			log.Print("Finished write test with ramp-up adjustment - default config")
			//		Thread.sleep(5*60*1000);
			time.Sleep(time.Minute * 5)
			log.Print("Starting write test with ramp-up adjustment - aggressive config")
			//		dynamoDbLoadData.setClientAggressive(true);
			//		dynamoDbLoadData.trafficRampUp();
			//		dynamoDbLoadData.setClientAggressive(false);
			c.trafficRampUp(*awsCfg, true)
			log.Print("Finished write test with ramp-up adjustment - aggressive config")
			//		Thread.sleep(5*60*1000);
			time.Sleep(time.Minute * 5)
			log.Print("Starting write test with ramp-up adjustment default config - node restarting every 10 minute")
			//		setRestartingClusterNode(true);
			//		dynamoDbLoadData.setClientAggressive(false);
			//		dynamoDbLoadData.trafficRampUp();
			//		setRestartingClusterNode(false);
			c.trafficRampUp(*awsCfg, false)
			log.Print("Finished write test with ramp-up adjustment default config - node restarting every 10 minute")
			//		Thread.sleep(5*60*1000);
			time.Sleep(time.Minute * 5)
		}
	}
}
