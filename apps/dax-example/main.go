package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/metrics"
)

func main() {
	region := "eu-west-1"
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		panic(err)
	}

	cw := cloudwatch.NewFromConfig(cfg)
	if cw == nil {
		panic("unable to create cloudwatch client")
	}

	daxCfg := dax.NewConfig(cfg, "dax://radu-cluster.cykcls.dax-clusters.eu-west-1.amazonaws.com")
	// enable route manager health check (will generate metrics if enabled)
	daxCfg.RouteManagerEnabled = true
	daxCfg.MeterProvider = &MyMeterProvider{cw: *cw}

	dax, err := dax.New(daxCfg)
	if err != nil {
		panic(err)
	}

	_, _ = dax.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("RADU-DAX-Performance"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberN{Value: "251"},
			"sk": &ddbtypes.AttributeValueMemberN{Value: "1"},
		},
	})
}

type MyMeterProvider struct {
	cw cloudwatch.Client

	meters map[string]metrics.Meter
}

func (m *MyMeterProvider) Meter(scope string, _ ...metrics.MeterOption) metrics.Meter {
	if m.meters == nil {
		m.meters = make(map[string]metrics.Meter)
	}

	mtr := m.meters[scope]
	if mtr != nil {
		return mtr
	}

	mtr = &MyMeter{scope: scope, cw: m.cw}
	m.meters[scope] = mtr

	return mtr
}

type MyMeter struct {
	scope string
	cw    cloudwatch.Client

	counters   map[string]metrics.Int64Counter
	histograms map[string]metrics.Int64Histogram
	gauges     map[string]metrics.Int64Gauge
}

func (m *MyMeter) Int64Counter(name string, _ ...metrics.InstrumentOption) (metrics.Int64Counter, error) {
	if m.counters == nil {
		m.counters = make(map[string]metrics.Int64Counter)
	}

	c := m.counters[name]
	if c != nil {
		return c, nil
	}

	c = &MyInstrument{scope: m.scope, name: name, cw: m.cw}
	m.counters[name] = c

	return c, nil
}

func (m *MyMeter) Int64UpDownCounter(_ string, _ ...metrics.InstrumentOption) (metrics.Int64UpDownCounter, error) {
	panic("not used")
}

func (m *MyMeter) Int64Gauge(name string, _ ...metrics.InstrumentOption) (metrics.Int64Gauge, error) {
	if m.gauges == nil {
		m.gauges = make(map[string]metrics.Int64Gauge)
	}

	g := m.gauges[name]
	if g != nil {
		return g, nil
	}

	g = &MyInstrument{scope: m.scope, name: name, cw: m.cw}
	m.gauges[name] = g

	return g, nil
}

func (m *MyMeter) Int64Histogram(name string, _ ...metrics.InstrumentOption) (metrics.Int64Histogram, error) {
	if m.histograms == nil {
		m.histograms = make(map[string]metrics.Int64Histogram)
	}

	h := m.histograms[name]
	if h != nil {
		return h, nil
	}

	h = &MyInstrument{scope: m.scope, name: name, cw: m.cw}
	m.histograms[name] = h

	return h, nil
}

func (m *MyMeter) Int64AsyncCounter(_ string, _ metrics.Int64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *MyMeter) Int64AsyncUpDownCounter(_ string, _ metrics.Int64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *MyMeter) Int64AsyncGauge(_ string, _ metrics.Int64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *MyMeter) Float64Counter(_ string, _ ...metrics.InstrumentOption) (metrics.Float64Counter, error) {
	panic("not used")
}

func (m *MyMeter) Float64UpDownCounter(_ string, _ ...metrics.InstrumentOption) (metrics.Float64UpDownCounter, error) {
	panic("not used")
}

func (m *MyMeter) Float64Gauge(_ string, _ ...metrics.InstrumentOption) (metrics.Float64Gauge, error) {
	panic("not used")
}

func (m *MyMeter) Float64Histogram(_ string, _ ...metrics.InstrumentOption) (metrics.Float64Histogram, error) {
	panic("not used")
}

func (m *MyMeter) Float64AsyncCounter(name string, callback metrics.Float64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *MyMeter) Float64AsyncUpDownCounter(name string, callback metrics.Float64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *MyMeter) Float64AsyncGauge(name string, callback metrics.Float64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

type MyInstrument struct {
	scope string
	name  string
	cw    cloudwatch.Client
}

// Sample - gauge
func (m *MyInstrument) Sample(ctx context.Context, i int64, _ ...metrics.RecordMetricOption) {
	res, err := m.cw.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(m.scope),
		MetricData: []types.MetricDatum{
			{
				MetricName: aws.String(m.name),
				Values:     []float64{float64(i)},
			},
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("[%s][%s][%d] %#+v\n", m.scope, m.name, i, res.ResultMetadata)
}

// Record - histogram
func (m *MyInstrument) Record(ctx context.Context, i int64, option ...metrics.RecordMetricOption) {
	res, err := m.cw.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(m.scope),
		MetricData: []types.MetricDatum{
			{
				MetricName: aws.String(m.name),
				Values:     []float64{float64(i)},
			},
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("[%s][%s][%d] %#+v\n", m.scope, m.name, i, res.ResultMetadata)
}

// Add - Counter
func (m *MyInstrument) Add(ctx context.Context, i int64, option ...metrics.RecordMetricOption) {
	res, err := m.cw.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(m.scope),
		MetricData: []types.MetricDatum{
			{
				MetricName: aws.String(m.name),
				Values:     []float64{float64(i)},
			},
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("[%s][%s][%d] %#+v\n", m.scope, m.name, i, res.ResultMetadata)
}
