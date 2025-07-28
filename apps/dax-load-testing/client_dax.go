package main

import (
	"net"
	"time"

	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func getDaxClient(cfg *aws.Config, aggressive bool) (*dax.Dax, error) {
	if cfg == nil {
		panic("Unable to get aws.Config")
	}

	daxCfg := dax.NewConfig(*cfg, Flags.AWS.DAX.Endpoint)
	// populate dax config
	daxCfg.SkipHostnameVerification = true
	daxCfg.MaxPendingConnectionsPerHost = int(Flags.AWS.DAX.MaxPendingConnectionAcquires)
	daxCfg.ReadRetries = ternary(aggressive, int(Flags.AWS.DAX.ReadRetriesAggressive), int(Flags.AWS.DAX.ReadRetries))
	daxCfg.WriteRetries = ternary(aggressive, int(Flags.AWS.DAX.WriteRetriesAggressive), int(Flags.AWS.DAX.WriteRetries))
	daxCfg.RequestTimeout = time.Millisecond * time.Duration(
		ternary(
			aggressive,
			Flags.AWS.DAX.RequestTimeoutAggressiveMillis,
			Flags.AWS.DAX.RequestTimeoutMillis,
		),
	)
	daxCfg.DialContext = (&net.Dialer{
		Timeout:   time.Millisecond * time.Duration(Flags.AWS.DAX.ConnectionTtlMillis),
		KeepAlive: time.Minute,
	}).DialContext
	daxCfg.LogLevel = 0 // utils.LogDebugWithRequestRetries

	// healthcheck yo!
	//daxCfg.ClientHealthCheckInterval = time.Second * 5
	//daxCfg.ClusterUpdateInterval = time.Second * 5
	//daxCfg.RouteManagerEnabled = true

	client, err := dax.New(daxCfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getDefaultDaxClient(cfg *aws.Config) (*dax.Dax, error) {
	if cfg == nil {
		panic("Unable to get aws.Config")
	}

	daxCfg := dax.NewConfig(*cfg, Flags.AWS.DAX.Endpoint)
	// populate dax config
	////daxCfg.SkipHostnameVerification = true
	////daxCfg.MaxPendingConnectionsPerHost = int(appConfig.ClientConfig.MaxPendingConnections)
	////daxCfg.ReadRetries = int(appConfig.ClientConfig.ReadRetries)
	////daxCfg.WriteRetries = int(appConfig.ClientConfig.WriteRetries)
	//daxCfg.RequestTimeout = time.Millisecond * time.Duration(appConfig.ClientConfig.RequestTimeout)
	//daxCfg.DialContext = (&net.Dialer{
	//	Timeout:   time.Millisecond * time.Duration(appConfig.ClientConfig.ConnectionTimeout),
	//	KeepAlive: time.Minute,
	//}).DialContext
	daxCfg.LogLevel = 0 // utils.LogDebugWithRequestRetries

	// healthcheck yo!
	//daxCfg.ClientHealthCheckInterval = time.Second * 5
	//daxCfg.ClusterUpdateInterval = time.Second * 5
	//daxCfg.RouteManagerEnabled = true

	client, err := dax.New(daxCfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}
