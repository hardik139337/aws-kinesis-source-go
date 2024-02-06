package kinesis

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	sourcesdk "github.com/numaproj/numaflow-go/pkg/sourcer"
)

type AWSKinesisSource struct {
	streamName    *string
	shardIterator *string
	kinesisClient *kinesis.Kinesis
}

func NewAWSKinesisSource(client *kinesis.Kinesis, streamName *string) *AWSKinesisSource {
	return &AWSKinesisSource{
		kinesisClient: client,
		streamName:    streamName,
	}
}

func (s *AWSKinesisSource) Read(_ context.Context, readRequest sourcesdk.ReadRequest, messageCh chan<- sourcesdk.Message) {

	if s.shardIterator == nil {
		streams, err := s.kinesisClient.DescribeStream(&kinesis.DescribeStreamInput{StreamName: s.streamName})
		if err != nil {
			log.Panic(err)
		}
		out, err := s.kinesisClient.GetShardIterator(&kinesis.GetShardIteratorInput{
			StreamName:        s.streamName,
			ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
			ShardIteratorType: aws.String("TRIM_HORIZON"),
		})
		if err != nil {
			log.Printf("error getting shard iterator: %s", err)
			return
		}
		s.shardIterator = out.ShardIterator
	}

	shardIterator := s.shardIterator
	var a *string
	for {

		records, err := s.kinesisClient.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})

		if err != nil {
			log.Printf("error getting records: %s", err)
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if len(records.Records) > 0 {
			for _, record := range records.Records {
				messageCh <- sourcesdk.NewMessage(
					record.Data,
					sourcesdk.NewOffsetWithDefaultPartitionId([]byte(*record.SequenceNumber)),
					*record.ApproximateArrivalTimestamp,
				)
				log.Printf("Received message: %s", string(record.Data))
			}
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator || err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}

		time.Sleep(1000 * time.Millisecond)

		s.shardIterator = records.NextShardIterator
	}

}

func (s *AWSKinesisSource) Ack(_ context.Context, _ sourcesdk.AckRequest) {
	// Kinesis does not require explicit acknowledgement of records. The consumer's position in the stream is determined by the sequence number.
}

func (s *AWSKinesisSource) Partitions(_ context.Context) []int32 {
	return sourcesdk.DefaultPartitions()
}

func (s *AWSKinesisSource) Pending(ctx context.Context) int64 {
	// Kinesis does not provide a way to get the number of pending messages.

	return 0
}
