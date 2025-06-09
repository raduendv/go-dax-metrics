package helper

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// GetEnvVar call with REGION -> will try to find REGION or AWS_REGION then return default if none exist
func GetEnvVar(name, dflt string) string {
	out := os.Getenv(name)
	if out == "" {
		os.Getenv(fmt.Sprintf("AWS_%s", name))
	}
	if out == "" {
		return dflt
	}

	return out
}

func GetAWSConfig(region string) *aws.Config {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)

	if err != nil {
		panic(err)
	}

	return &cfg
}

func GetDynamoDBClient(cfg *aws.Config, optFns ...func(*dynamodb.Options)) *dynamodb.Client {
	return dynamodb.NewFromConfig(*cfg, optFns...)
}

func GetItem(client *dynamodb.Client, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return client.GetItem(context.Background(), input)
}

func GetCloudwatchClient(cfg *aws.Config, optFns ...func(*cloudwatch.Options)) *cloudwatch.Client {
	return cloudwatch.NewFromConfig(*cfg, optFns...)
}

func StartLambda(handler any) {
	lambda.Start(handler)
}
