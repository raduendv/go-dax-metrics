package helper

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type MessageType string

const (
	MessageTypeCount     MessageType = "count"
	MessageTypeHistogram MessageType = "histogram"
	MessageTypeGauge     MessageType = "gauge"
)

type Metric struct {
	Type     MessageType
	Name     string
	Values   map[int64][]int64
	UnitType string
}

func statisticSet(values []int64) *types.StatisticSet {
	mx, mn, sum, crt := float64(0), float64(0), float64(0), float64(0)

	for _, v := range values {
		crt = float64(v)

		mx = max(mx, crt)
		mn = min(mn, crt)
		sum += crt
	}

	return &types.StatisticSet{
		Maximum:     pointer(mx),
		Minimum:     pointer(mn),
		SampleCount: pointer(float64(len(values))),
		Sum:         pointer(sum),
	}
}

func (m *Metric) ToData() []types.MetricDatum {
	// return &types.MetricDatum{
	// 	MetricName:        (*string)(nil),
	// 	Counts:            []float64{},
	// 	Dimensions:        []types.Dimension{},
	// 	StatisticValues:   (*types.StatisticSet)(nil),
	// 	StorageResolution: (*int32)(nil),
	// 	Timestamp:         pointer(time.Now()),
	// 	Unit:              types.StandardUnitMicroseconds,
	// 	Value:             (*float64)(nil),
	// 	Values:            []float64{},
	// }
	out := make([]types.MetricDatum, 0, len(m.Values))
	for ts, values := range m.Values {
		t := time.Unix(ts, 0)
		datum := types.MetricDatum{
			MetricName: pointer(m.Name),
			Timestamp:  pointer(t),
			Unit:       types.StandardUnitCount,
		}

		switch m.Type {
		case MessageTypeGauge:
			datum.Value = pointer(float64(values[0]))
			break
		case MessageTypeCount, MessageTypeHistogram:
			datum.Unit = types.StandardUnitMicroseconds
			datum.StatisticValues = statisticSet(values)
			break
		}

		out = append(out, datum)
	}

	return out
}
