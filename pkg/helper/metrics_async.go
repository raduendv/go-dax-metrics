package helper

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/smithy-go/metrics"
)

var _ metrics.MeterProvider = (*AsyncMeterProvider)(nil)
var _ metrics.Meter = (*AsyncMeter)(nil)
var _ metrics.Int64Counter = (*AsyncInstrument)(nil)
var _ metrics.Int64Histogram = (*AsyncInstrument)(nil)
var _ metrics.Int64Gauge = (*AsyncInstrument)(nil)

const sendIntervalInSeconds = 10

// NewAsyncMeterProvider NewAsyncMeterProvider
// @see - https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
// @see - https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
// PutMetricData request - 1MB for HTTP POST requests.
// PutMetricData can handle 500 transactions per second (TPS), which is
// the maximum number of operation requests that you can make per second without being throttled.
// PutMetricData can handle 1,000 metrics per request.
func NewAsyncMeterProvider(
	numberOfChannels int,
	wg *sync.WaitGroup,
	cw *cloudwatch.Client,
) *AsyncMeterProvider {
	amp := &AsyncMeterProvider{
		MainChannel:      make(chan types.MetricDatum, numberOfChannels),
		Meters:           map[string]metrics.Meter{},
		WaitGroup:        wg,
		CloudWatch:       cw,
		CloudWatchSender: NewCloudwatchSender(wg, cw),
	}

	//for range numberOfChannels {
	//	amp.MainChannel <- make(chan types.MetricDatum, 1000)
	//}

	return amp
}

type AsyncMeterProvider struct {
	MainChannel      chan types.MetricDatum
	Meters           map[string]metrics.Meter
	Mutex            sync.RWMutex
	WaitGroup        *sync.WaitGroup
	CloudWatch       *cloudwatch.Client
	CloudWatchSender *CloudWatchSender

	currentWorkers int64
}

func (amp *AsyncMeterProvider) returnChannel(ch chan types.MetricDatum) {
	if ch == nil {
		return
	}

	//fmt.Printf("Channel returned with %d messages\n", len(ch))
	amp.WaitGroup.Add(1)
	go func() {
		//out := make([]types.MetricDatum, 0, len(ch))
		//lch := len(ch)

		defer func() {
			amp.WaitGroup.Done()
			//amp.MainChannel <- ch

			//if len(out) == 0 {
			//	return
			//}
			//
			//fmt.Println("Sending", len(out), "metrics")
			//_, err := amp.CloudWatch.PutMetricData(context.Background(), &cloudwatch.PutMetricDataInput{
			//	Namespace:  pointer("go-dax-metrics"),
			//	MetricData: out,
			//})
			//if err != nil {
			//	panic(err)
			//}
			//fmt.Println("Sent", len(out), "metrics")
			//if lch != len(out) {
			//	fmt.Println("WARNING!!!")
			//}
		}()

		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				amp.CloudWatchSender.Queue(v)
				//out = append(out, v)
			default:
				return
			}
		}
	}()
}

func (amp *AsyncMeterProvider) Meter(scope string, fns ...metrics.MeterOption) metrics.Meter {
	amp.Mutex.Lock()
	defer amp.Mutex.Unlock()

	//fmt.Printf("Meter(%s)\n", scope)

	out, ok := amp.Meters[scope]
	if ok {
		return out
	}

	options := metrics.MeterOptions{}
	for _, fn := range fns {
		fn(&options)
	}

	out = &AsyncMeter{
		Scope:    scope,
		Provider: amp,

		Options: options,

		Counters:   map[string]metrics.Int64Counter{},
		Histograms: map[string]metrics.Int64Histogram{},
		Gauges:     map[string]metrics.Int64Gauge{},
	}

	amp.Meters[scope] = out

	return out
}

func (amp *AsyncMeterProvider) queue(d types.MetricDatum) {
	amp.MainChannel <- d

	l := atomic.LoadInt64(&amp.currentWorkers)
	if int64(len(amp.MainChannel)/1000) > l && l < 500 {
		amp.WaitGroup.Add(1)
		atomic.AddInt64(&amp.currentWorkers, 1)

		go func() {
			defer atomic.AddInt64(&amp.currentWorkers, -1)
			defer amp.WaitGroup.Done()

			amp.push()
		}()
	}
}

func (amp *AsyncMeterProvider) push() {
	fmt.Println("Current queue size", len(amp.MainChannel))
	fmt.Println("Current worker pool", amp.currentWorkers)

	pmdi := &cloudwatch.PutMetricDataInput{
		Namespace: pointer("go-dax-metrics"),
	}

	names := map[string]int{}

	run := true
	for len(pmdi.MetricData) < 1000 && run {
		select {
		case m, ok := <-amp.MainChannel:
			if !ok {
				return
			}
			n := names[*m.MetricName]
			n++
			names[*m.MetricName] = n
			pmdi.MetricData = append(pmdi.MetricData, m)
		case <-time.After(time.Second):
			// when not data has been read for a while, send what we have
			if atomic.LoadInt64(&amp.currentWorkers) > 1 {
				run = false
				break
			}
			//cs.Queue(pmdi.MetricData...)
			for _, d := range pmdi.MetricData {
				amp.MainChannel <- d
			}
			if atomic.LoadInt64(&amp.currentWorkers) != 0 {
				return
			}
		}
	}

	if len(pmdi.MetricData) == 0 {
		return
	}

	fmt.Println("Sending", len(pmdi.MetricData))
	_, err := amp.CloudWatch.PutMetricData(context.Background(), pmdi)
	if err != nil {
		panic(err)
	}
	fmt.Println("Sent", len(pmdi.MetricData))

	//fmt.Printf("[%s] Sent %d metric(s).\n", time.Now().Format(time.Stamp), len(pmdi.MetricData))
	//fmt.Printf("[%s] Remaining: %d.\n", time.Now().Format(time.Stamp), len(amp.MainChannel))

	// must have a minimum of 50 more messages to continue
	if len(amp.MainChannel) > 50 && len(amp.MainChannel) < 1000 && atomic.LoadInt64(&amp.currentWorkers) == 1 {
		amp.push()
	}
}

type AsyncMeter struct {
	Scope    string
	Provider *AsyncMeterProvider

	Options metrics.MeterOptions

	Counters      map[string]metrics.Int64Counter
	CountersMutex sync.RWMutex

	Histograms      map[string]metrics.Int64Histogram
	HistogramsMutex sync.RWMutex

	Gauges      map[string]metrics.Int64Gauge
	GaugesMutex sync.RWMutex
}

// integer/synchronous
func (am *AsyncMeter) Int64Counter(name string, fns ...metrics.InstrumentOption) (metrics.Int64Counter, error) {
	am.CountersMutex.Lock()
	defer am.CountersMutex.Unlock()

	out, ok := am.Counters[name]
	if ok {
		return out, nil
	}

	am.Counters[name] = NewAsyncInstrument(
		am,
		name,
		MessageTypeCount,
		fns...,
	)

	return am.Counters[name], nil
}

func (am *AsyncMeter) Int64UpDownCounter(_ string, _ ...metrics.InstrumentOption) (metrics.Int64UpDownCounter, error) {
	return nil, nil
}

func (am *AsyncMeter) Int64Gauge(name string, fns ...metrics.InstrumentOption) (metrics.Int64Gauge, error) {
	am.GaugesMutex.Lock()
	defer am.GaugesMutex.Unlock()

	//fmt.Printf("Int64Gauge(%s)\n", name)

	out, ok := am.Gauges[name]
	if ok {
		return out, nil
	}

	am.Gauges[name] = NewAsyncInstrument(
		am,
		name,
		MessageTypeGauge,
		fns...,
	)

	return am.Gauges[name], nil
}
func (am *AsyncMeter) Int64Histogram(name string, fns ...metrics.InstrumentOption) (metrics.Int64Histogram, error) {
	am.HistogramsMutex.Lock()
	defer am.HistogramsMutex.Unlock()

	//fmt.Printf("Int64Histogram(%s)\n", name)

	out, ok := am.Histograms[name]
	if ok {
		return out, nil
	}

	am.Histograms[name] = NewAsyncInstrument(
		am,
		name,
		MessageTypeHistogram,
		fns...,
	)

	return am.Histograms[name], nil
}

// integer/asynchronous
func (am *AsyncMeter) Int64AsyncCounter(_ string, _ metrics.Int64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	return nil, nil
}
func (am *AsyncMeter) Int64AsyncUpDownCounter(_ string, _ metrics.Int64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	return nil, nil
}
func (am *AsyncMeter) Int64AsyncGauge(_ string, _ metrics.Int64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	return nil, nil
}

// floating-point/synchronous
func (am *AsyncMeter) Float64Counter(_ string, _ ...metrics.InstrumentOption) (metrics.Float64Counter, error) {
	return nil, nil
}
func (am *AsyncMeter) Float64UpDownCounter(_ string, _ ...metrics.InstrumentOption) (metrics.Float64UpDownCounter, error) {
	return nil, nil
}
func (am *AsyncMeter) Float64Gauge(_ string, _ ...metrics.InstrumentOption) (metrics.Float64Gauge, error) {
	return nil, nil
}
func (am *AsyncMeter) Float64Histogram(_ string, _ ...metrics.InstrumentOption) (metrics.Float64Histogram, error) {
	return nil, nil
}

func (am *AsyncMeter) Float64AsyncCounter(_ string, _ metrics.Float64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	return nil, nil
}
func (am *AsyncMeter) Float64AsyncUpDownCounter(_ string, _ metrics.Float64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	return nil, nil
}
func (am *AsyncMeter) Float64AsyncGauge(_ string, _ metrics.Float64Callback, _ ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	return nil, nil
}

type AsyncInstrument struct {
	Name           string
	Meter          *AsyncMeter
	CurrentChannel chan types.MetricDatum

	Type MessageType

	Options metrics.InstrumentOptions

	Values      map[int64][]int64
	ValuesMutex sync.RWMutex

	LastSend int64
}

func NewAsyncInstrument(
	meter *AsyncMeter,
	name string,
	msgType MessageType,
	fns ...metrics.InstrumentOption,
) *AsyncInstrument {
	options := metrics.InstrumentOptions{}

	for _, fn := range fns {
		fn(&options)
	}

	return &AsyncInstrument{
		Name:     name,
		Meter:    meter,
		Options:  options,
		Values:   map[int64][]int64{},
		Type:     msgType,
		LastSend: 0,
	}
}

// Add counter - must lock/unlock to properly add value of counter
func (ai *AsyncInstrument) Add(_ context.Context, v int64, _ ...metrics.RecordMetricOption) {
	d := types.MetricDatum{
		MetricName: pointer(ai.Name),
		Value:      pointer(float64(v)),
		Timestamp:  pointer(time.Now()),
	}

	ai.Meter.Provider.queue(d)
}

// Sample gauge - send data right away
func (ai *AsyncInstrument) Sample(_ context.Context, v int64, _ ...metrics.RecordMetricOption) {
	d := types.MetricDatum{
		MetricName: pointer(ai.Name),
		Value:      pointer(float64(v)),
		Timestamp:  pointer(time.Now()),
	}

	ai.Meter.Provider.queue(d)
}

// Record histogram - gather data for aggregation before sending
func (ai *AsyncInstrument) Record(_ context.Context, v int64, _ ...metrics.RecordMetricOption) {
	d := types.MetricDatum{
		MetricName: pointer(ai.Name),
		Value:      pointer(float64(v)),
		Timestamp:  pointer(time.Now()),
	}

	ai.Meter.Provider.queue(d)
}
