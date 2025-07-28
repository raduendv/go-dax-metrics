package main

import "flag"

var Flags flags

/**
app:
  number_of_clients: 1
  number_of_threads_per_client: 100
  initial_rps_per_thread: 0.1
  final_rps_per_thread: 10
  rps_ramping_factor: 0.001
  rps_ramping_interval_ms: 50
  dax-cluster-name: iulian-cluster
  daxClusterRebootNodes:
    enabled: false
    initial-delay-ms: 1000
    fixed-delay-ms: 600000
  cloudwatch:
    namespace: LoadTestApplication
    push-frequency-minutes: 1
  start-traffic: true
  with-cache-miss: false
  write-test: false
  test-duration-minutes: 30
*/

/**
app:
  number_of_clients: 100
  number_of_threads_per_client: 100
  initial_rps_per_thread: 0.1
  final_rps_per_thread: 10
  rps_ramping_factor: 0.001
  rps_ramping_interval_ms: 50
  daxClusterRebootNodes:
    enabled: false
    durationFrequency: 10m
*/

func init() {
	flag.UintVar(&Flags.App.NumberOfClients, "app.number_of_clients", 1, "")
	flag.UintVar(&Flags.App.NumberOfThreadsPerClient, "app.number_of_threads_per_client", 100, "")
	flag.Float64Var(&Flags.App.InitialRPSPerThread, "app.initial_rps_per_thread", 0.1, "")
	flag.Float64Var(&Flags.App.FinalRPSPerThread, "app.final_rps_per_thread", 10, "")
	flag.Float64Var(&Flags.App.RPSRampingFactor, "app.rps_ramping_factor", 0.001, "")
	flag.UintVar(&Flags.App.RPSRampingIntervalMS, "app.rps_ramping_interval_ms", 50, "")
	flag.StringVar(&Flags.App.DaxClusterName, "app.dax-cluster-name", "radu-cluster", "")
	//
	flag.BoolVar(&Flags.App.DaxClusterRebootNodes.Enabled, "app.daxClusterRebootNodes.enabled", false, "")
	flag.UintVar(&Flags.App.DaxClusterRebootNodes.InitialDelayMS, "app.daxClusterRebootNodes.initial-delay-ms", 1000, "")
	flag.UintVar(&Flags.App.DaxClusterRebootNodes.FixedDelayMS, "app.daxClusterRebootNodes.fixed-delay-ms", 600000, "")
	flag.StringVar(&Flags.App.CloudWatch.Namespace, "app.cloudwatch.namespace", "go-dax-load-test", "")
	flag.UintVar(&Flags.App.CloudWatch.PushFrequencyMinutes, "app.cloudwatch.push-frequency-minutes", 1, "")
	flag.BoolVar(&Flags.App.StartTraffic, "app.start-traffic", true, "")
	flag.BoolVar(&Flags.App.WithCacheMiss, "app.with-cache-miss", false, "")
	flag.BoolVar(&Flags.App.WriteTest, "app.write-test", false, "")
	flag.UintVar(&Flags.App.TestDurationMinutes, "app.test-duration-minutes", 30, "")
	// aws
	flag.StringVar(&Flags.AWS.DynamoDB.TableName, "aws.dynamodb.table-name", "RADU-DAX-Performance", "")
	flag.BoolVar(&Flags.AWS.DynamoDB.CreateTable, "aws.dynamodb.create-table", false, "")
	flag.BoolVar(&Flags.AWS.DynamoDB.LoadData, "aws.dynamodb.load-data", false, "")
	flag.StringVar(&Flags.AWS.DAX.Endpoint, "aws.dynamodb.endpoint", "dax://radu-cluster.cykcls.dax-clusters.eu-west-1.amazonaws.com", "")
	flag.StringVar(&Flags.AWS.DAX.Region, "aws.dynamodb.region", "eu-west-1", "")

	flag.UintVar(&Flags.AWS.DAX.MaxPendingConnectionAcquires, "aws.dax.maxPendingConnectionAcquires", 10, "")
	flag.UintVar(&Flags.AWS.DAX.ConnectionTtlMillis, "aws.dax.connectionTtlMillis", 1000, "")
	flag.UintVar(&Flags.AWS.DAX.RequestTimeoutMillis, "aws.dax.requestTimeoutMillis", 60000, "")
	flag.UintVar(&Flags.AWS.DAX.MaxConcurrency, "aws.dax.maxConcurrency", 1000, "")
	flag.UintVar(&Flags.AWS.DAX.WriteRetries, "aws.dax.writeRetries", 2, "")
	flag.UintVar(&Flags.AWS.DAX.ReadRetries, "aws.dax.readRetries", 2, "")
	flag.UintVar(&Flags.AWS.DAX.ReadRetriesAggressive, "aws.dax.readRetriesAggressive", 3, "")
	flag.UintVar(&Flags.AWS.DAX.WriteRetriesAggressive, "aws.dax.writeRetriesAggressive", 3, "")
	flag.UintVar(&Flags.AWS.DAX.RequestTimeoutAggressiveMillis, "aws.dax.requestTimeoutAggressiveMillis", 100, "")
	flag.Parse()
}

type flags struct {
	App struct {
		NumberOfClients          uint    `yaml:"number_of_clients"`
		NumberOfThreadsPerClient uint    `yaml:"number_of_threads_per_client"`
		InitialRPSPerThread      float64 `yaml:"initial_rps_per_thread"`
		FinalRPSPerThread        float64 `yaml:"final_rps_per_thread"`
		RPSRampingFactor         float64 `yaml:"rps_ramping_factor"`
		RPSRampingIntervalMS     uint    `yaml:"rps_ramping_interval_ms"`

		DaxClusterName        string `yaml:"dax-cluster-name"`
		DaxClusterRebootNodes struct {
			Enabled        bool `yaml:"enabled"`
			InitialDelayMS uint `yaml:"initial-delay-ms"`
			FixedDelayMS   uint `yaml:"fixed-delay-ms"`
		} `yaml:"daxClusterRebootNodes"`

		CloudWatch struct {
			Namespace            string `yaml:"namespace"`
			PushFrequencyMinutes uint   `yaml:"push-frequency-minutes"`
		} `yaml:"cloudwatch"`

		StartTraffic        bool `yaml:"start-traffic"`
		WithCacheMiss       bool `yaml:"with-cache-miss"`
		WriteTest           bool `yaml:"write-test"`
		TestDurationMinutes uint `yaml:"test-duration-minutes"`
	} `yaml:"app"`

	AWS struct {
		DynamoDB struct {
			TableName   string `yaml:"table-name"`
			CreateTable bool   `yaml:"create-table"`
			LoadData    bool   `yaml:"load-data"`
		} `yaml:"dynamodb"`
		DAX struct {
			Endpoint                       string `yaml:"endpoint"`
			Region                         string `yaml:"region"`
			MaxPendingConnectionAcquires   uint   `yaml:"maxPendingConnectionAcquires"`
			ConnectionTtlMillis            uint   `yaml:"connectionTtlMillis"`
			RequestTimeoutMillis           uint   `yaml:"requestTimeoutMillis"`
			MaxConcurrency                 uint   `yaml:"maxConcurrency"`
			WriteRetries                   uint   `yaml:"writeRetries"`
			ReadRetries                    uint   `yaml:"readRetries"`
			ReadRetriesAggressive          uint   `yaml:"readRetriesAggressive"`
			WriteRetriesAggressive         uint   `yaml:"writeRetriesAggressive"`
			RequestTimeoutAggressiveMillis uint   `yaml:"requestTimeoutAggressiveMillis"`
		} `yaml:"dax"`
	} `yaml:"aws"'`
}
