package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func (dl *DataLoader) trafficRampUp(cfg aws.Config, aggressive bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-time.After(time.Minute * time.Duration(Flags.App.TestDurationMinutes))
		log.Print("Cancelling due to timeout")
		cancel()
	}()

	clients := []*dax.Dax{}
	for range rangeClosed(1, Flags.App.NumberOfClients) {
		client, err := getDaxClient(&cfg, aggressive)
		if err != nil {
			panic(err)
		}
		clients = append(clients, client)

		for range Flags.App.NumberOfThreadsPerClient {
			atomic.AddInt64(&dl.activeGoroutines, 1)

			go func() {
				dl.submitTrafficTask(ctx, client, aggressive)
				atomic.AddInt64(&dl.activeGoroutines, -1)
			}()
		}
	}

	for atomic.LoadInt64(&dl.activeGoroutines) > 0 {
		time.Sleep(time.Second)
		log.Printf("Active goroutines: %d", atomic.LoadInt64(&dl.activeGoroutines))
	}

	log.Print("closing all clients")
	for _, client := range clients {
		_ = client.Close()
	}
	log.Print("closed all clients")
}

func (dl *DataLoader) submitTrafficTask(ctx context.Context, client *dax.Dax, aggressive bool) {
	rpsPerThread := Flags.App.InitialRPSPerThread // INITIAL_RPS_PER_THREAD;
	sleepInterval := int(1_000.0 / Flags.App.InitialRPSPerThread)
	r := NewRandom[int](100, 0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			//
		}

		err := dl.executeTrafficCycle(ctx, client, sleepInterval, r.Next(), aggressive)

		if rpsPerThread < Flags.App.FinalRPSPerThread {
			rpsPerThread += Flags.App.RPSRampingFactor
			sleepInterval = int(1_000.0 / rpsPerThread)
		}

		if err != nil {
			log.Printf("Error in %s cycle: %v", ternary(Flags.App.WriteTest, "writeTest traffic", "traffic"), err)
			time.Sleep(time.Second)
		}
	}
}

func (dl *DataLoader) executeTrafficCycle(ctx context.Context, client *dax.Dax, sleepInterval, rnd int, aggressive bool) error {
	var worker workerFn
	if Flags.App.WriteTest {
		if rnd < 75 {
			worker = (*DataLoader).putItem
		} else if rnd < 90 {
			worker = (*DataLoader).updateItem
		} else {
			worker = (*DataLoader).batchWriteItem
		}
	} else {
		if rnd < 75 {
			worker = (*DataLoader).getItem
		} else if rnd < 90 {
			worker = (*DataLoader).query
		} else {
			worker = (*DataLoader).batchGetItem
		}
	}

	if worker == nil {
		return nil
	}

	go func() {
		_ = worker(dl, ctx, client, time.Millisecond*ternary[time.Duration](aggressive, 150, 60_000))
	}()
	time.Sleep(time.Millisecond * time.Duration(sleepInterval))

	return nil
}
