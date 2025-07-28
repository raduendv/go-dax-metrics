package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-dax-go-v2/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type workerFn func(dl *DataLoader, ctx context.Context, dax *dax.Dax, duration time.Duration) error

func (dl *DataLoader) getItem(ctx context.Context, dax *dax.Dax, duration time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, duration)
	defer cancel()
	go func() {
		<-time.After(duration)
		cancel()
	}()

	var pk int
	if Flags.App.WithCacheMiss {
		pk = dl.randomGetItemMiss.Next()
	} else {
		pk = dl.randomGetItemHit.Next()
	}

	sk := dl.keyCounter.NextSK(pk)

	dl.metricsService.incrementThroughput("GetItem")
	dl.metricsService.recordLatency("GetItem", func() error {
		_, err := dax.GetItem(ctx, &dynamodb.GetItemInput{
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", pk),
				},
				"sk": &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", sk),
				},
			},
			TableName: aws.String(Flags.AWS.DynamoDB.TableName),
		})

		dl.metricsService.incrementStatusCounter("GetItem", ternary(err != nil, 400, 200))

		return err
	})

	return nil
}

func (dl *DataLoader) putItem(ctx context.Context, dax *dax.Dax, duration time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, duration)
	defer cancel()
	go func() {
		<-time.After(duration)
		cancel()
	}()

	pk := int(dl.firstPkPutItemKey.Add(1) - 1)
	sk := dl.keyCounterPutItem.NextSK(pk)

	item := MakeItem(
		uint(pk),
		uint(sk),
		4096,
		6,
		false,
	)

	dl.metricsService.incrementThroughput("PutItem")

	dl.metricsService.recordLatency("PutItem", func() error {
		_, err := dax.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      item,
			TableName: aws.String(Flags.AWS.DynamoDB.TableName),
		})

		dl.metricsService.incrementStatusCounter("PutItem", ternary(err != nil, 400, 200))

		return err
	})

	return nil
}

func (dl *DataLoader) updateItem(ctx context.Context, dax *dax.Dax, duration time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, duration)
	defer cancel()
	go func() {
		<-time.After(duration)
		cancel()
	}()

	//String pk = String.valueOf(firstPkPutItemKey.get()),sk = String.valueOf(1);
	pk := int(dl.firstPkPutItemKey.Add(1) - 1)
	sk := dl.keyCounterPutItem.NextSK(pk)

	item := MakeItem(
		uint(pk),
		uint(sk),
		4096,
		6,
		false,
	)
	key := map[string]types.AttributeValue{
		FieldPK: item[FieldPK],
		FieldSK: item[FieldSK],
	}
	delete(item, FieldPK)
	delete(item, FieldSK)

	dl.metricsService.incrementThroughput("PutItem")

	dl.metricsService.recordLatency("PutItem", func() error {
		_, err := dax.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:        aws.String(Flags.AWS.DynamoDB.TableName),
			Key:              key,
			UpdateExpression: aws.String("SET a1 = :val1, a2 = :val2, a3 = :val3, a4 = :val4, a5 = :val5, a6 = :val6"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				"val1": item["a1"],
				"val2": item["a2"],
				"val3": item["a3"],
				"val4": item["a4"],
				"val5": item["a5"],
				"val6": item["a6"],
			},
			ReturnValues: types.ReturnValueAllNew,
		})

		dl.metricsService.incrementStatusCounter("PutItem", ternary(err != nil, 400, 200))

		return err
	})

	return nil
}

func (dl *DataLoader) batchWriteItem(ctx context.Context, dax *dax.Dax, duration time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, duration)
	defer cancel()
	go func() {
		<-time.After(duration)
		cancel()
	}()

	dl.metricsService.incrementThroughput("BatchWriteItem")

	pk := int(dl.firstPkBatchWriteItemKey.Add(1) - 1)

	batch := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			Flags.AWS.DynamoDB.TableName: {},
		},
	}
	for sk := range rangeClosed(1, 25) {
		batch.RequestItems[Flags.AWS.DynamoDB.TableName] = append(
			batch.RequestItems[Flags.AWS.DynamoDB.TableName],
			types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: MakeItem(
						uint(pk),
						uint(sk),
						20*1024,
						6,
						false,
					),
				},
			},
		)
	}

	dl.metricsService.recordLatency("BatchWriteItem", func() error {
		_, err := dax.BatchWriteItem(ctx, batch)

		dl.metricsService.incrementStatusCounter("BatchWriteItem", ternary(err != nil, 400, 200))

		return err
	})

	return nil
}

func (dl *DataLoader) query(ctx context.Context, dax *dax.Dax, duration time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, duration)
	defer cancel()
	go func() {
		<-time.After(duration)
		cancel()
	}()

	var pk int
	if Flags.App.WithCacheMiss {
		pk = dl.randomQueryMiss.Next()
	} else {
		pk = dl.randomQueryHit.Next()
	}

	dl.metricsService.incrementThroughput("Query")
	dl.metricsService.recordLatency("Query", func() error {
		_, err := dax.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(Flags.AWS.DynamoDB.TableName),
			KeyConditionExpression: aws.String("pk = :pkval and sk between :skval1 and :skval2"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pkval": &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", pk),
				},
				":skval1": &types.AttributeValueMemberN{
					Value: "1",
				},
				":skval2": &types.AttributeValueMemberN{
					Value: "200",
				},
			},
		})

		dl.metricsService.incrementStatusCounter("Query", ternary(err != nil, 400, 200))

		if err != nil {
			log.Printf("QueryError: %v - %d", err, pk)
			panic("asd")
		}

		return err
	})

	return nil
}

func (dl *DataLoader) batchGetItem(ctx context.Context, dax *dax.Dax, duration time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, duration)
	defer cancel()
	go func() {
		<-time.After(duration)
		cancel()
	}()

	keys := []map[string]types.AttributeValue{}

	for range rangeClosed(1, 25) {
		var pk int
		if Flags.App.WithCacheMiss {
			pk = dl.randomBatchGetItemMiss.Next()
		} else {
			pk = dl.randomBatchGetItemHit.Next()
		}

		sk := dl.keyCounter.NextSK(pk)

		keys = append(
			keys,
			map[string]types.AttributeValue{
				FieldPK: &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", pk),
				},
				FieldSK: &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", sk),
				},
			},
		)
	}

	batch := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			Flags.AWS.DynamoDB.TableName: types.KeysAndAttributes{
				Keys:            keys,
				AttributesToGet: []string{"a1", "a2", "a3", "a4", "a5", "a6"},
			},
		},
	}

	dl.metricsService.incrementThroughput("BatchGetItem")
	dl.metricsService.recordLatency("BatchGetItem", func() error {
		_, err := dax.BatchGetItem(ctx, batch)

		dl.metricsService.incrementStatusCounter("BatchGetItem", ternary(err != nil, 400, 200))

		return err
	})
	return nil
}
