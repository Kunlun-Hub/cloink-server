package senders

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
)

type fakeS3Client struct {
	lastBucket string
	lastKey    string
	lastBody   []byte
	err        error
}

func (f *fakeS3Client) PutObject(_ context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	f.lastBucket = aws.ToString(params.Bucket)
	f.lastKey = aws.ToString(params.Key)
	body, _ := io.ReadAll(params.Body)
	f.lastBody = body
	return &awss3.PutObjectOutput{}, f.err
}

func TestS3PushWritesJSONL(t *testing.T) {
	fake := &fakeS3Client{}
	sender := &S3Sender{
		newClient: func(string, aws.CredentialsProvider) s3PutObjectAPI { return fake },
	}

	err := sender.Push(context.Background(), map[string]string{
		"access_key":  "ak",
		"secret_key":  "sk",
		"bucket_name": "my-bucket",
		"region":      "us-east-1",
		"prefix":      "cloink/events",
	}, []eventstreaming.StreamEvent{
		{ID: "1", Timestamp: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), Message: "a", Kind: "activity"},
		{ID: "2", Timestamp: time.Date(2026, 9, 28, 12, 0, 1, 0, time.UTC), Message: "b", Kind: "flow"},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", fake.lastBucket)
	assert.True(t, strings.HasPrefix(fake.lastKey, "cloink/events/"))
	assert.True(t, strings.HasSuffix(fake.lastKey, ".jsonl"))

	lines := bytes.Split(bytes.TrimSpace(fake.lastBody), []byte("\n"))
	require.Len(t, lines, 2)
	assert.Contains(t, string(lines[0]), `"id":"1"`)
	assert.Contains(t, string(lines[1]), `"kind":"flow"`)
}

func TestS3PushRequiresConfig(t *testing.T) {
	sender := NewS3Sender()
	base := map[string]string{"access_key": "ak", "secret_key": "sk", "bucket_name": "b", "region": "r"}
	for _, mutate := range []func(map[string]string){
		func(m map[string]string) { delete(m, "bucket_name") },
		func(m map[string]string) { delete(m, "region") },
		func(m map[string]string) { delete(m, "access_key") },
	} {
		cfg := map[string]string{}
		for k, v := range base {
			cfg[k] = v
		}
		mutate(cfg)
		require.Error(t, sender.Push(context.Background(), cfg, nil))
	}
}

func TestS3PushPropagatesError(t *testing.T) {
	fake := &fakeS3Client{err: errors.New("access denied")}
	sender := &S3Sender{
		newClient: func(string, aws.CredentialsProvider) s3PutObjectAPI { return fake },
	}
	err := sender.Push(context.Background(), map[string]string{
		"access_key": "ak", "secret_key": "sk", "bucket_name": "b", "region": "r",
	}, []eventstreaming.StreamEvent{{ID: "1"}})
	require.Error(t, err)
}

type fakeFirehoseClient struct {
	calls       int
	lastStream  string
	lastRecords int
	failIndexes map[int]bool
	err         error
}

func (f *fakeFirehoseClient) PutRecordBatch(_ context.Context, params *firehose.PutRecordBatchInput, _ ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	f.calls++
	f.lastStream = aws.ToString(params.DeliveryStreamName)
	f.lastRecords = len(params.Records)
	if f.err != nil {
		return nil, f.err
	}
	var failed int32
	responses := make([]firehosetypes.PutRecordBatchResponseEntry, len(params.Records))
	for i := range params.Records {
		if f.failIndexes[i] {
			failed++
			responses[i] = firehosetypes.PutRecordBatchResponseEntry{ErrorCode: aws.String("ServiceUnavailable")}
		}
	}
	return &firehose.PutRecordBatchOutput{
		FailedPutCount:   aws.Int32(failed),
		RequestResponses: responses,
	}, nil
}

func TestFirehosePushBatching(t *testing.T) {
	fake := &fakeFirehoseClient{}
	sender := &FirehoseSender{
		newClient: func(string, aws.CredentialsProvider) firehoseBatchAPI { return fake },
	}

	events := make([]eventstreaming.StreamEvent, 0, 1200)
	for i := 0; i < 1200; i++ {
		events = append(events, eventstreaming.StreamEvent{ID: string(rune('0' + i%10)), Message: "m"})
	}
	err := sender.Push(context.Background(), map[string]string{
		"access_key": "ak", "secret_key": "sk", "stream_name": "my-stream", "region": "us-east-1",
	}, events)
	require.NoError(t, err)
	assert.Equal(t, 3, fake.calls) // 500 + 500 + 200
	assert.Equal(t, "my-stream", fake.lastStream)
	assert.Equal(t, 200, fake.lastRecords)
}

func TestFirehosePushPartialFailure(t *testing.T) {
	fake := &fakeFirehoseClient{failIndexes: map[int]bool{3: true}}
	sender := &FirehoseSender{
		newClient: func(string, aws.CredentialsProvider) firehoseBatchAPI { return fake },
	}
	err := sender.Push(context.Background(), map[string]string{
		"access_key": "ak", "secret_key": "sk", "stream_name": "s", "region": "r",
	}, []eventstreaming.StreamEvent{{ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1/4 records failed")
}

func TestFirehosePushRequiresConfig(t *testing.T) {
	sender := NewFirehoseSender()
	require.Error(t, sender.Push(context.Background(), map[string]string{"region": "r"}, nil))
	require.Error(t, sender.Push(context.Background(), map[string]string{"stream_name": "s"}, nil))
}
