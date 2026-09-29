package senders

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
)

func testEvents() []eventstreaming.StreamEvent {
	return []eventstreaming.StreamEvent{
		{
			ID:          "42",
			Timestamp:   time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC),
			Message:     `peer "x" added`,
			InitiatorID: "user-1",
			TargetID:    "peer-1",
			Meta:        map[string]any{"k": "v"},
			Kind:        eventstreaming.StreamEventKindActivity,
		},
	}
}

func TestGenericHTTPDefaultTemplate(t *testing.T) {
	var gotBody []byte
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewGenericHTTPSender()
	err := sender.Push(context.Background(), map[string]string{
		"url":     srv.URL,
		"headers": `{"Authorization":"Bearer tok","X-Custom":"1"}`,
	}, testEvents())
	require.NoError(t, err)

	assert.Equal(t, "Bearer tok", gotHeaders.Get("Authorization"))
	assert.Equal(t, "1", gotHeaders.Get("X-Custom"))
	assert.Equal(t, "application/json", gotHeaders.Get("Content-Type"))

	var batch []map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &batch))
	require.Len(t, batch, 1)
	assert.Equal(t, "42", batch[0]["id"])
	assert.Equal(t, "2026-09-28T12:00:00Z", batch[0]["timestamp"])
	assert.Equal(t, `peer "x" added`, batch[0]["message"])
	assert.Equal(t, "user-1", batch[0]["initiator_id"])
	// meta renders escaped for safe embedding inside JSON quotes
	var meta map[string]any
	require.NoError(t, json.Unmarshal([]byte(batch[0]["meta"].(string)), &meta))
	assert.Equal(t, map[string]any{"k": "v"}, meta)
}

func TestGenericHTTPCustomTemplate(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewGenericHTTPSender()
	err := sender.Push(context.Background(), map[string]string{
		"url":           srv.URL,
		"body_template": `{"event_id":"{{.ID}}","text":"{{.Message}}","when":"{{.Timestamp}}"}`,
	}, testEvents())
	require.NoError(t, err)

	var batch []map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &batch))
	require.Len(t, batch, 1)
	assert.Equal(t, "42", batch[0]["event_id"])
	assert.Equal(t, `peer "x" added`, batch[0]["text"])
}

// TestGenericHTTPDashboardPlaceholder uses the exact placeholder template from
// the dashboard's GenericHTTPModal; its output must be valid JSON.
func TestGenericHTTPDashboardPlaceholder(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	placeholder := `{
  "id": "{{.ID}}",
  "timestamp": "{{.Timestamp.Format "2006-01-02T15:04:05.999Z07:00"}}",
  "message": "{{.Message}}",
  "initiator_id": "{{.InitiatorID}}",
  "target_id": "{{.TargetID}}",
  "meta": "{{.Meta}}"
}`
	sender := NewGenericHTTPSender()
	err := sender.Push(context.Background(), map[string]string{
		"url":           srv.URL,
		"body_template": placeholder,
	}, testEvents())
	require.NoError(t, err)

	var batch []map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &batch))
	require.Len(t, batch, 1)
	assert.Equal(t, "42", batch[0]["id"])
	assert.Equal(t, `peer "x" added`, batch[0]["message"])
	assert.Contains(t, batch[0]["timestamp"], "2026-09-28T12:00:00")
}

func TestGenericHTTPTimestampFormatMethod(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewGenericHTTPSender()
	err := sender.Push(context.Background(), map[string]string{
		"url":           srv.URL,
		"body_template": `{"ts":"{{.Timestamp.Format "2006-01-02"}}"}`,
	}, testEvents())
	require.NoError(t, err)

	var batch []map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &batch))
	require.Len(t, batch, 1)
	assert.Equal(t, "2026-09-28", batch[0]["ts"])
}

func TestGenericHTTPServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sender := NewGenericHTTPSender()
	err := sender.Push(context.Background(), map[string]string{"url": srv.URL}, testEvents())
	require.Error(t, err)
}

func TestGenericHTTPBadHeaders(t *testing.T) {
	sender := NewGenericHTTPSender()
	err := sender.Push(context.Background(), map[string]string{
		"url":     "http://localhost:1",
		"headers": `not-json`,
	}, testEvents())
	require.Error(t, err)
}

func TestGenericHTTPConcurrentPush(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewGenericHTTPSender()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sender.Push(context.Background(), map[string]string{"url": srv.URL}, testEvents())
		}()
	}
	wg.Wait()
}
