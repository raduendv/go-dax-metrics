package helper

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"sync"
	"sync/atomic"
	"time"
)

type CloudWatchSender struct {
	Client         *cloudwatch.Client
	Buffer         chan types.MetricDatum
	WaitGroup      *sync.WaitGroup
	CurrentWorkers int64
}

func (cs *CloudWatchSender) Queue(mds ...types.MetricDatum) {
	for _, md := range mds {
		cs.Buffer <- md
	}

	l := atomic.LoadInt64(&cs.CurrentWorkers)
	if int64(len(cs.Buffer)/1000) > l && l < 500 {
		cs.WaitGroup.Add(1)
		atomic.AddInt64(&cs.CurrentWorkers, 1)
		fmt.Println("Active workers", cs.CurrentWorkers, "/", 500)

		go func() {
			defer atomic.AddInt64(&cs.CurrentWorkers, -1)
			defer cs.WaitGroup.Done()

			cs.Push()

			<-time.After(time.Second)
		}()
	}
}

func (cs *CloudWatchSender) Push() {
	//fmt.Println("Current queue size", len(cs.Buffer))
	//fmt.Println("Current worker pool", cs.CurrentWorkers)

	pmdi := &cloudwatch.PutMetricDataInput{
		Namespace: pointer("go-dax-metrics"),
	}

	run := true
	for len(pmdi.MetricData) < 1000 && run {
		select {
		case m, ok := <-cs.Buffer:
			if !ok {
				return
			}
			pmdi.MetricData = append(pmdi.MetricData, m)
		case <-time.After(time.Second):
			// when not data has been read for a while, send what we have
			if atomic.LoadInt64(&cs.CurrentWorkers) > 1 {
				run = false
				break
			}
			cs.Queue(pmdi.MetricData...)
			if atomic.LoadInt64(&cs.CurrentWorkers) != 0 {
				return
			}
		}
	}

	if len(pmdi.MetricData) == 0 {
		return
	}

	_, err := cs.Client.PutMetricData(context.Background(), pmdi)
	if err != nil {
		panic(err)
	}

	//fmt.Printf("[%s] Sent %d metric(s).\n", time.Now().Format(time.Stamp), len(pmdi.MetricData))
	//fmt.Printf("[%s] Remaining: %d.\n", time.Now().Format(time.Stamp), len(cs.Buffer))

	// must have a minimum of 50 more messages to continue
	if len(cs.Buffer) > 50 && len(cs.Buffer) < 1000 && atomic.LoadInt64(&cs.CurrentWorkers) == 1 {
		cs.Push()
	}
}

func NewCloudwatchSender(wg *sync.WaitGroup, client *cloudwatch.Client) *CloudWatchSender {
	return &CloudWatchSender{
		WaitGroup: wg,
		Client:    client,
		Buffer:    make(chan types.MetricDatum, 1024*1024),
	}
}
