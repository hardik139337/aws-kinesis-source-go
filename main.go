package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	kinesissdk "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"
	"github.com/numaproj-contrib/aws-sqs-source-go/pkg/kinesis"
	"github.com/numaproj/numaflow-go/pkg/sourcer"
)

var (
	kinesisStream AWSKinesis
)

type AWSKinesis struct {
	stream          string
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

func init() {
	e := godotenv.Load()
	if e != nil {
		fmt.Print(e)
	}
	kinesisStream = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
}

func main() {

	s := session.New(&aws.Config{
		Region:      aws.String(kinesisStream.region),
		Endpoint:    aws.String(kinesisStream.endpoint),
		Credentials: credentials.NewStaticCredentials(kinesisStream.accessKeyID, kinesisStream.secretAccessKey, kinesisStream.sessionToken),
	})
	kc := kinesissdk.New(s)
	streamName := aws.String(kinesisStream.stream)

	awsKinesisSource := kinesis.NewAWSKinesisSource(kc, streamName)
	err := sourcer.NewServer(awsKinesisSource).Start(context.Background())
	if err != nil {
		log.Panic("Failed to start source server : ", err)
	}
}
