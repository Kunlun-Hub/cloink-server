package senders

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	datadogTimeout = 15 * time.Second
	datadogSource  = "cloink"
)

// DatadogSender pushes events to the Datadog Logs HTTP intake (v2).
// Config: api_key (Datadog API key), api_url (region intake URL, e.g.
// https://http-intake.logs.datadoghq.eu/api/v2/logs).
type DatadogSender struct {
	client *http.Client
}

// NewDatadogSender creates a sender with a default HTTP client.
func NewDatadogSender() *DatadogSender {
	return &DatadogSender{
		client: &http.Client{Timeout: datadogTimeout},
	}
}

func (s *DatadogSender) Platform() string {
	return types.EventStreamingPlatformDatadog
}

// Push sends the batch as a JSON array to the intake endpoint.
func (s *DatadogSender) Push(ctx context.Context, config map[string]string, events []eventstreaming.StreamEvent) error {
	apiKey := strings.TrimSpace(config["api_key"])
	apiURL := strings.TrimSpace(config["api_url"])
	if apiKey == "" {
		return fmt.Errorf("datadog: api_key is not configured")
	}
	if apiURL == "" {
		return fmt.Errorf("datadog: api_url is not configured")
	}

	logs := make([]map[string]any, 0, len(events))
	for _, e := range events {
		logs = append(logs, map[string]any{
			"message":      e.Message,
			"ddsource":     datadogSource,
			"ddtags":       fmt.Sprintf("kind:%s", e.Kind),
			"timestamp":    e.Timestamp.UnixMilli(),
			"service":      datadogSource,
			"id":           e.ID,
			"initiator_id": e.InitiatorID,
			"target_id":    e.TargetID,
			"meta":         e.Meta,
		})
	}
	body, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("datadog: marshal batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("datadog: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("datadog: POST %s: %w", apiURL, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBody))
	// The v2 intake returns 202 Accepted on success.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("datadog: POST %s returned status %d", apiURL, resp.StatusCode)
	}
	return nil
}
