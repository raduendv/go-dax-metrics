package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"

	"go-dax-metrics/pkg/helper"
)

// maps in go have no guarantee of orders so we ensure we always have the same order for stats
func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, _ := range m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

func main() {
	appCfg := helper.GetAppConfig()

	region := helper.GetEnvVar("REGION", "eu-west-1")

	cfg := helper.GetAWSConfig(region)
	if cfg == nil {
		panic("config is nil")
	}

	cw := helper.GetCloudwatchClient(cfg)
	if cw == nil {
		panic("cannot get cloudwatch")
	}

	widgets := []any{}
	start := "-PT72H"

	stats := map[string]string{
		//"p50": "50th Percentile (Median) - Typical request time",
		//"p75": "75th Percentile - Upper mid-range performance",
		//"p90": "90th Percentile - 10% of requests are slower than this",
		//"p95": "95th Percentile - Helps identify bottlenecks",
		//"p99": "99th Percentile - Detects high-latency spikes",
		//"p99.9": "99.9th Percentile - Identifies ultra-rare slowest requests",

		//"tm95": "Trimmed Mean 95 - Avg of fastest 95%, removes slowest 5%",
		//"tm99": "Trimmed Mean 99 - Avg of fastest 99%, removes outliers",

		"Minimum": "Minimum - Fastest observed request",
		"Maximum": "Maximum - Slowest observed request",
		"Average": "Average - Mean response time (may be skewed by outliers)",
		//"IQM":     "Interquartile Mean - Average of middle 50% of data, removes extremes",
	}
	_ = stats

	y := 0
	for _, test := range appCfg.TestConfigs {
		opHeight := 2
		op := map[string]any{
			"type":   "text",
			"x":      0,        // 0–23
			"y":      y,        // >= 0
			"width":  24,       // 1-24
			"height": opHeight, // 1–1000
			"properties": map[string]any{
				"markdown":   fmt.Sprintf("# %s", test.Name),
				"background": "transparent",
			},
		}
		y += opHeight
		widgets = append(widgets, op)

		for _, api := range appCfg.TrafficConfigs[test.TrafficConfig].APIs {
			infos := []map[string]any{
				{
					"type":   "text",
					"x":      0,        // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Connection Timeout: %d ms",
							appCfg.ClientConfigs[test.ClientConfig].ConnectionTimeout,
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      2,        // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Request Timeout: %d ms",
							appCfg.ClientConfigs[test.ClientConfig].RequestTimeout,
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      4,        // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Read Retries: %d",
							appCfg.ClientConfigs[test.ClientConfig].ReadRetries,
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      6,        // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Write Retries: %d",
							appCfg.ClientConfigs[test.ClientConfig].WriteRetries,
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      8,        // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Max Concurrency: %d",
							appCfg.ClientConfigs[test.ClientConfig].MaxConcurrency,
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      10,       // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Max Pending Connections: %d",
							appCfg.ClientConfigs[test.ClientConfig].MaxPendingConnections,
						),
						"background": "transparent",
					},
				},
				// spaced
				{
					"type":   "text",
					"x":      14,       // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Item Size: %v",
							appCfg.TrafficConfigs[test.TrafficConfig].ItemSizes[api],
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      16,       // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown":   "Number of attributes: 6",
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      18,       // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"API: %v",
							api,
						),
						"background": "transparent",
					},
				},
				{
					"type":   "text",
					"x":      20,       // 0–23
					"y":      y,        // >= 0
					"width":  2,        // 1-24
					"height": opHeight, // 1–1000
					"properties": map[string]any{
						"markdown": fmt.Sprintf(
							"Cache Hit Percentage: %v",
							appCfg.TrafficConfigs[test.TrafficConfig].CacheHitPercentage,
						),
						"background": "transparent",
					},
				},
			}
			for _, i := range infos {
				widgets = append(widgets, i)
			}
			y += opHeight

			metrics := []string{
				fmt.Sprintf("dax.op.%s.success", api),
				fmt.Sprintf("dax.op.%s.failure", api),
				fmt.Sprintf("dax.op.%s.latency_us", api),
			}

			idx := 0
			for stat, statLabel := range stats {
				sttMetrics := []any{}
				for _, metric := range metrics {
					sttMetrics = append(
						sttMetrics,
						[]any{
							"go-dax-metrics",
							metric,
						},
					)
				}
				mtr := map[string]any{
					"type":   "metric",
					"x":      idx,
					"y":      y,
					"width":  8,
					"height": 6,
					"properties": map[string]any{
						"markdown":                 fmt.Sprintf("#### %s", api),
						"metrics":                  sttMetrics,
						"sparkline":                true,
						"view":                     "timeSeries",
						"region":                   "eu-west-1",
						"period":                   60,
						"liveData":                 true,
						"singleValueFullPrecision": false,
						"stat":                     stat,
						"title":                    statLabel,
					},
				}

				widgets = append(widgets, mtr)
				idx += 8
			}
			y += opHeight
		}
	}
	if len(widgets) > 500 {
		fmt.Println(len(widgets))
		panic("too many widgets")
	}

	dashboard := map[string]any{
		"periodOverride": "inherit",
		"widgets":        widgets,
		"start":          start,
	}

	fmt.Println(len(widgets))
	jsn, err := json.Marshal(dashboard)
	if err != nil {
		panic(err)
	}

	_, err = cw.PutDashboard(context.Background(), &cloudwatch.PutDashboardInput{
		DashboardName: aws.String("go-dax-metrics"),
		DashboardBody: aws.String(string(jsn)),
	})

	if err != nil {
		panic(err.Error())
	} else {
		fmt.Println("Dashboard updated")
	}
}

func main2() {
	namespace := "go-sdk-metrics"
	operations := map[string]string{
		"GetItemLargeItemCanaryRunOftenWholeOperation": "Large Items",
		"GetItemSmallItemCanaryRunOftenWholeOperation": "Small Items",
	}
	stages := map[string]string{
		"initial": "Initial Request",
		"regular": "Regular Request",
	}
	metrics := map[string]string{
		"client.http.do_request_duration":                "HTTP Request Duration",
		"client.call.duration":                           "Total SDK Call Duration",
		"client.call.serialization_duration":             "Request Serialization Duration",
		"client.http.connections.dns_lookup_duration":    "DNS Lookup Duration",
		"client.http.connections.acquire_duration":       "Connection Acquisition Duration",
		"client.http.time_to_first_byte":                 "Time to First Byte (TTFB)",
		"client.call.auth.resolve_identity_duration":     "Auth Identity Resolution Duration",
		"client.call.resolve_endpoint_duration":          "Endpoint Resolution Duration",
		"client.call.auth.signing_duration":              "Auth Signing Duration",
		"client.call.deserialization_duration":           "Response Deserialization Duration",
		"client.call.attempt_duration":                   "Single Attempt Duration",
		"client.http.connections.tls_handshake_duration": "TLS Handshake Duration",
		"dynamodb.get_item":                              "DynamoDB GetItem Latency",
	}
	stats := map[string]string{
		"p50": "50th Percentile (Median) - Typical request time",
		"p75": "75th Percentile - Upper mid-range performance",
		"p90": "90th Percentile - 10% of requests are slower than this",
		"p95": "95th Percentile - Helps identify bottlenecks",
		"p99": "99th Percentile - Detects high-latency spikes",
		//"p99.9": "99.9th Percentile - Identifies ultra-rare slowest requests",

		//"tm95": "Trimmed Mean 95 - Avg of fastest 95%, removes slowest 5%",
		//"tm99": "Trimmed Mean 99 - Avg of fastest 99%, removes outliers",

		"Minimum": "Minimum - Fastest observed request",
		"Maximum": "Maximum - Slowest observed request",
		"Average": "Average - Mean response time (may be skewed by outliers)",
		//"IQM":     "Interquartile Mean - Average of middle 50% of data, removes extremes",
	}
	start := "-PT72H"

	fmt.Println("CloudWatchDashboard v0.1.0")
	fmt.Println(len(operations), operations)
	fmt.Println(len(stages), stages)
	fmt.Println(len(metrics), metrics)
	fmt.Println(len(stats), stats)
	fmt.Println(start)
	fmt.Println(len(operations) * len(stages) * len(metrics) * len(stats))

	region := helper.GetEnvVar("REGION", "eu-west-1")

	cfg := helper.GetAWSConfig(region)
	if cfg == nil {
		panic("config is nil")
	}

	cw := helper.GetCloudwatchClient(cfg)
	if cw == nil {
		panic("cannot get cloudwatch")
	}

	widgets := []any{}

	y := 0

	for _, operation := range keys(operations) {
		operationLabel := operations[operation]
		fmt.Printf("Building operation %s [%s]\n", operationLabel, operation)

		opHeight := 2
		op := map[string]any{
			"type":   "text",
			"x":      0,        // 0–23
			"y":      y,        // >= 0
			"width":  24,       // 1-24
			"height": opHeight, // 1–1000
			"properties": map[string]any{
				"markdown":   fmt.Sprintf("# %s", operationLabel),
				"background": "transparent",
			},
		}
		y += opHeight
		widgets = append(widgets, op)

		for _, stage := range keys(stages) {
			stageLabel := stages[stage]
			//fmt.Printf("Building stage %s [%s]\n", stageLabel, stage)
			//
			//stgHeight := 1
			//stg := map[string]any{
			//	"type":   "text",
			//	"x":      0,
			//	"y":      y,
			//	"width":  24,
			//	"height": stgHeight,
			//	"properties": map[string]any{
			//		"markdown":   fmt.Sprintf("## %s", stageLabel),
			//		"background": "transparent",
			//	},
			//}
			//
			//y += stgHeight
			//widgets = append(widgets, stg)

			for _, metric := range keys(metrics) {
				metricLabel := metrics[metric]
				fmt.Printf("Building metric %s [%s]\n", metricLabel, metric)

				mtrHeight := 1
				mtr := map[string]any{
					"type":   "text",
					"x":      0,
					"y":      y,
					"width":  24,
					"height": mtrHeight,
					"properties": map[string]any{
						"markdown":   fmt.Sprintf("### %s %s", stageLabel, metricLabel),
						"background": "transparent",
					},
				}

				y += mtrHeight
				widgets = append(widgets, mtr)

				x := 0
				sttWidth := 6
				for statIdx, stat := range keys(stats) {
					statLabel := stats[stat]

					sttHeight := 5

					sttMetrics := []any{
						[]any{
							namespace,
							metric,
							"stage",
							stage,
							"operation",
							operation,
						},
					}
					stt := map[string]any{
						"type":   "metric",
						"x":      x,
						"y":      y,
						"width":  sttWidth,
						"height": sttHeight,
						"properties": map[string]any{
							"markdown":                 fmt.Sprintf("#### %s", statLabel),
							"metrics":                  sttMetrics,
							"sparkline":                true,
							"view":                     "timeSeries",
							"region":                   "eu-west-1",
							"period":                   60,
							"liveData":                 true,
							"singleValueFullPrecision": false,
							"stat":                     stat,
							"title":                    statLabel,
						},
					}

					x += sttWidth
					if x >= 24-sttWidth || statIdx == len(keys(stats))-1 {
						y += sttHeight
					}
					x %= 24
					widgets = append(widgets, stt)
				}
			}
		}
	}

	dashboard := map[string]any{
		"periodOverride": "inherit",
		"widgets":        widgets,
		"start":          start,
	}

	if len(widgets) > 500 {
		panic("too many widgets")
	}

	jsn, err := json.Marshal(dashboard)
	if err != nil {
		panic(err)
	}

	_, err = cw.PutDashboard(context.Background(), &cloudwatch.PutDashboardInput{
		DashboardName: aws.String("go-sdk-metrics"),
		DashboardBody: aws.String(string(jsn)),
	})

	if err != nil {
		panic(err.Error())
	} else {
		fmt.Println("Dashboard updated")
	}
}
