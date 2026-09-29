package senders

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	s3PrefixDateFormat = "2006/01/02"
)

// s3PutObjectAPI is the subset of the S3 client used by the sender.
type s3PutObjectAPI interface {
	PutObject(ctx context.Context, params *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

// S3Sender appends events as a JSONL object to the configured bucket.
// Config: access_key, secret_key, bucket_name, region, optional prefix.
type S3Sender struct {
	newClient func(region string, creds aws.CredentialsProvider) s3PutObjectAPI
}

// NewS3Sender creates a sender using the real AWS SDK S3 client.
func NewS3Sender() *S3Sender {
	return &S3Sender{
		newClient: func(region string, creds aws.CredentialsProvider) s3PutObjectAPI {
			return awss3.New(awss3.Options{
				Region:      region,
				Credentials: creds,
			})
		},
	}
}

func (s *S3Sender) Platform() string {
	return types.EventStreamingPlatformS3
}

func awsCreds(config map[string]string) aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider(
		strings.TrimSpace(config["access_key"]),
		strings.TrimSpace(config["secret_key"]),
		"",
	)
}

// Push writes one JSONL object per batch. The key is
// <prefix/>YYYY/MM/DD/cloink-events-<unixnano>.jsonl so batches never collide.
func (s *S3Sender) Push(ctx context.Context, config map[string]string, events []eventstreaming.StreamEvent) error {
	bucket := strings.TrimSpace(config["bucket_name"])
	region := strings.TrimSpace(config["region"])
	if bucket == "" {
		return fmt.Errorf("s3: bucket_name is not configured")
	}
	if region == "" {
		return fmt.Errorf("s3: region is not configured")
	}
	if strings.TrimSpace(config["access_key"]) == "" || strings.TrimSpace(config["secret_key"]) == "" {
		return fmt.Errorf("s3: access_key/secret_key are not configured")
	}

	var buf bytes.Buffer
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
			return fmt.Errorf("s3: marshal event: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	prefix := strings.Trim(strings.TrimSpace(config["prefix"]), "/")
	now := time.Now().UTC()
	key := fmt.Sprintf("%s/cloink-events-%d.jsonl", now.Format(s3PrefixDateFormat), now.UnixNano())
	if prefix != "" {
		key = prefix + "/" + key
	}

	client := s.newClient(region, awsCreds(config))
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/x-ndjson"),
	})
	if err != nil {
		return fmt.Errorf("s3: PutObject %s/%s: %w", bucket, key, err)
	}
	return nil
}
