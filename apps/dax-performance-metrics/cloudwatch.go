package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"time"
)

func getCloudwatch(cfg *aws.Config) *cloudwatch.Client {
	return cloudwatch.NewFromConfig(*cfg)
}

func getLastMinuteStats(cw *cloudwatch.Client, clusterName string) *cloudwatch.GetMetricStatisticsOutput {
	stat, err := cw.GetMetricStatistics(context.Background(), lastMinuteStatisticInput(clusterName))
	if err != nil {
		panic(err)
	}

	if stat == nil {
		panic("stat is nil")
	}

	if len(stat.Datapoints) != 1 {
		panic(fmt.Sprintf("wrong length for datapoints: %d", len(stat.Datapoints)))
	}

	return stat
}

func lastMinuteStatisticInput(clusterName string) *cloudwatch.GetMetricStatisticsInput {
	now := time.Now()

	return &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/DAX"),
		MetricName: aws.String("CPUUtilization"),
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("ClusterId"),
				Value: aws.String(clusterName),
			},
		},
		Period:    aws.Int32(60),
		EndTime:   aws.Time(now),
		StartTime: aws.Time(now.Add(-time.Minute)),
		Statistics: []types.Statistic{
			types.StatisticAverage,
		},
	}
}
