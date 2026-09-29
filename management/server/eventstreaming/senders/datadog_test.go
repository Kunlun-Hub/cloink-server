package senders

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
)

func TestDatadogPush(t *testing.T) {
	var gotBody []byte
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("DD-API-KEY")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sender := NewDatadogSender()
	err := sender.Push(context.Background(), map[string]string{
		"api_key": "dd-key-123",
		"api_url": srv.URL,
	}, []eventstreaming.StreamEvent{
		{
			ID:        "7",
			Timestamp: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC),
			Message:   "peer added",
			Kind:      eventstreaming.StreamEventKindActivity,
			Meta:      map[string]any{"k": "v"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "dd-key-123", gotKey)

	var logs []map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &logs))
	require.Len(t, logs, 1)
	assert.Equal(t, "peer added", logs[0]["message"])
	assert.Equal(t, "cloink", logs[0]["ddsource"])
	assert.Equal(t, "kind:activity", logs[0]["ddtags"])
	assert.Equal(t, float64(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC).UnixMilli()), logs[0]["timestamp"])
}

func TestDatadogPushRejectsBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	sender := NewDatadogSender()
	err := sender.Push(context.Background(), map[string]string{
		"api_key": "bad",
		"api_url": srv.URL,
	}, []eventstreaming.StreamEvent{{ID: "1", Message: "x"}})
	require.Error(t, err)
}

func TestDatadogPushRequiresConfig(t *testing.T) {
	sender := NewDatadogSender()
	require.Error(t, sender.Push(context.Background(), map[string]string{"api_url": "http://x"}, nil))
	require.Error(t, sender.Push(context.Background(), map[string]string{"api_key": "x"}, nil))
}
