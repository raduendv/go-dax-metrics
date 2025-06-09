package main

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/smithy-go/metrics"
)

type meterProvider struct {
	meters map[string]metrics.Meter
	mtx    sync.RWMutex
	ch     chan types.MetricDatum
}

func (m *meterProvider) Meter(scope string, opts ...metrics.MeterOption) metrics.Meter {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.meters == nil {
		m.meters = map[string]metrics.Meter{}
	}

	mtr, ok := m.meters[scope]
	if ok {
		return mtr
	}

	mtr = &meter{
		scope: scope,
		ch:    m.ch,
	}

	m.meters[scope] = mtr

	return mtr
}

type meter struct {
	scope      string
	mtx        sync.RWMutex
	ch         chan types.MetricDatum
	counters   map[string]metrics.Int64Counter
	histograms map[string]metrics.Int64Histogram
	gauges     map[string]metrics.Int64Gauge
}

func (m *meter) Int64Counter(name string, fns ...metrics.InstrumentOption) (metrics.Int64Counter, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.counters == nil {
		m.counters = map[string]metrics.Int64Counter{}
	}

	c, ok := m.counters[name]
	if ok {
		return c, nil
	}

	opts := metrics.InstrumentOptions{}

	for _, fn := range fns {
		fn(&opts)
	}

	c = &instrument{
		name:    name,
		ch:      m.ch,
		options: opts,
	}

	m.counters[name] = c

	return c, nil
}

func (m *meter) Int64UpDownCounter(name string, opts ...metrics.InstrumentOption) (metrics.Int64UpDownCounter, error) {
	panic("not used")
}

func (m *meter) Int64Gauge(name string, fns ...metrics.InstrumentOption) (metrics.Int64Gauge, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.gauges == nil {
		m.gauges = map[string]metrics.Int64Gauge{}
	}

	g, ok := m.gauges[name]
	if ok {
		return g, nil
	}

	opts := metrics.InstrumentOptions{}

	for _, fn := range fns {
		fn(&opts)
	}

	g = &instrument{
		name:    name,
		ch:      m.ch,
		options: opts,
	}

	m.gauges[name] = g

	return g, nil
}

func (m *meter) Int64Histogram(name string, fns ...metrics.InstrumentOption) (metrics.Int64Histogram, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.histograms == nil {
		m.histograms = map[string]metrics.Int64Histogram{}
	}

	h, ok := m.histograms[name]
	if ok {
		return h, nil
	}

	opts := metrics.InstrumentOptions{}

	for _, fn := range fns {
		fn(&opts)
	}

	h = &instrument{
		name:    name,
		ch:      m.ch,
		options: opts,
	}

	m.histograms[name] = h

	return h, nil
}

func (m *meter) Int64AsyncCounter(name string, callback metrics.Int64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *meter) Int64AsyncUpDownCounter(name string, callback metrics.Int64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *meter) Int64AsyncGauge(name string, callback metrics.Int64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *meter) Float64Counter(name string, opts ...metrics.InstrumentOption) (metrics.Float64Counter, error) {
	panic("not used")
}

func (m *meter) Float64UpDownCounter(name string, opts ...metrics.InstrumentOption) (metrics.Float64UpDownCounter, error) {
	panic("not used")
}

func (m *meter) Float64Gauge(name string, opts ...metrics.InstrumentOption) (metrics.Float64Gauge, error) {
	panic("not used")
}

func (m *meter) Float64Histogram(name string, opts ...metrics.InstrumentOption) (metrics.Float64Histogram, error) {
	panic("not used")
}

func (m *meter) Float64AsyncCounter(name string, callback metrics.Float64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *meter) Float64AsyncUpDownCounter(name string, callback metrics.Float64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

func (m *meter) Float64AsyncGauge(name string, callback metrics.Float64Callback, opts ...metrics.InstrumentOption) (metrics.AsyncInstrument, error) {
	panic("not used")
}

type instrument struct {
	name    string
	ch      chan types.MetricDatum
	options metrics.InstrumentOptions
}

func (i *instrument) Sample(_ context.Context, v int64, _ ...metrics.RecordMetricOption) {
	i.ch <- types.MetricDatum{
		MetricName: aws.String(i.name),
		Value:      aws.Float64(float64(v)),
		Timestamp:  aws.Time(time.Now()),
		Unit:       types.StandardUnitCount,
	}
}

func (i *instrument) Record(_ context.Context, v int64, _ ...metrics.RecordMetricOption) {
	i.ch <- types.MetricDatum{
		MetricName: aws.String(i.name),
		Value:      aws.Float64(float64(v)),
		Timestamp:  aws.Time(time.Now()),
		Unit:       types.StandardUnitMicroseconds,
	}
}

func (i *instrument) Add(_ context.Context, v int64, _ ...metrics.RecordMetricOption) {
	i.ch <- types.MetricDatum{
		MetricName: aws.String(i.name),
		Value:      aws.Float64(float64(v)),
		Timestamp:  aws.Time(time.Now()),
		Unit:       types.StandardUnitCount,
	}
}
