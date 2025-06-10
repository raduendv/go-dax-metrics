package main

import (
	"net"
	"time"
	
	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

func getDaxClient(cfg *aws.Config, endpoint string, appConfig *AppConfig, metricChan chan types.MetricDatum) (*dax.Dax, error) {
	if cfg == nil {
		panic("Unable to get aws.Config")
	}

	daxCfg := dax.NewConfig(*cfg, endpoint)
	// populate dax config
	daxCfg.SkipHostnameVerification = true
	daxCfg.MaxPendingConnectionsPerHost = int(appConfig.ClientConfig.MaxPendingConnections)
	daxCfg.ReadRetries = int(appConfig.ClientConfig.ReadRetries)
	daxCfg.WriteRetries = int(appConfig.ClientConfig.WriteRetries)
	daxCfg.RequestTimeout = time.Millisecond * time.Duration(appConfig.ClientConfig.RequestTimeout)
	daxCfg.DialContext = (&net.Dialer{
		Timeout:   time.Millisecond * time.Duration(appConfig.ClientConfig.ConnectionTimeout),
		KeepAlive: time.Minute,
	}).DialContext
	daxCfg.LogLevel = 0 // utils.LogDebugWithRequestRetries

	// healthcheck yo!
	daxCfg.ClientHealthCheckInterval = time.Second * 5
	daxCfg.ClusterUpdateInterval = time.Second * 5
	daxCfg.RouteManagerEnabled = true

	daxCfg.MeterProvider = &meterProvider{
		ch: metricChan,
	}

	client, err := dax.New(daxCfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}
