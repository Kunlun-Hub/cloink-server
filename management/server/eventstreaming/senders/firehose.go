package senders

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
	"github.com/netbirdio/netbird/management/server/types"
)

const firehoseMaxBatch = 500

// firehoseBatchAPI is the subset of the Firehose client used by the sender.
type firehoseBatchAPI interface {
	PutRecordBatch(ctx context.Context, params *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

// FirehoseSender streams events to an Amazon Data Firehose delivery stream.
// Config: access_key, secret_key, stream_name, region.
type FirehoseSender struct {
	newClient func(region string, creds aws.CredentialsProvider) firehoseBatchAPI
}

// NewFirehoseSender creates a sender using the real AWS SDK Firehose client.
func NewFirehoseSender() *FirehoseSender {
	return &FirehoseSender{
		newClient: func(region string, creds aws.CredentialsProvider) firehoseBatchAPI {
			return firehose.New(firehose.Options{
				Region:      region,
				Credentials: creds,
			})
		},
	}
}

func (s *FirehoseSender) Platform() string {
	return types.EventStreamingPlatformFirehose
}

// Push sends events in batches of up to 500 records. A batch is accepted only
// when every record is accepted; per-record failures are returned as an error
// so the forwarder does not advance the cursor past failed records.
func (s *FirehoseSender) Push(ctx context.Context, config map[string]string, events []eventstreaming.StreamEvent) error {
	streamName := strings.TrimSpace(config["stream_name"])
	region := strings.TrimSpace(config["region"])
	if streamName == "" {
		return fmt.Errorf("firehose: stream_name is not configured")
	}
	if region == "" {
		return fmt.Errorf("firehose: region is not configured")
	}
	if strings.TrimSpace(config["access_key"]) == "" || strings.TrimSpace(config["secret_key"]) == "" {
		return fmt.Errorf("firehose: access_key/secret_key are not configured")
	}

	client := s.newClient(region, awsCreds(config))
	for start := 0; start < len(events); start += firehoseMaxBatch {
		end := start + firehoseMaxBatch
		if end > len(events) {
			end = len(events)
		}
		if err := s.pushBatch(ctx, client, streamName, events[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *FirehoseSender) pushBatch(ctx context.Context, client firehoseBatchAPI, streamName string, events []eventstreaming.StreamEvent) error {
	records := make([]firehosetypes.Record, 0, len(events))
	for _, e := range events {
		line, err := json.Marshal(map[string]any{
			"id":           e.ID,
			"timestamp":    e.Timestamp.UTC().Format(time.RFC3339),
			"message":      e.Message,
			"initiator_id": e.InitiatorID,
			"target_id":    e.TargetID,
			"kind":         e.Kind,
			"meta":         e.Meta,
		})
		if err != nil {
			return fmt.Errorf("firehose: marshal event: %w", err)
		}
		line = append(line, '\n')
		records = append(records, firehosetypes.Record{Data: line})
	}

	out, err := client.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(streamName),
		Records:            records,
	})
	if err != nil {
		return fmt.Errorf("firehose: PutRecordBatch %s: %w", streamName, err)
	}
	if out.FailedPutCount != nil && *out.FailedPutCount > 0 {
		failed := make([]int, 0, *out.FailedPutCount)
		for i, r := range out.RequestResponses {
			if r.ErrorCode != nil && *r.ErrorCode != "" {
				failed = append(failed, i)
			}
		}
		return fmt.Errorf("firehose: PutRecordBatch %s: %d/%d records failed (indexes %v)",
			streamName, *out.FailedPutCount, len(records), failed)
	}
	return nil
}
