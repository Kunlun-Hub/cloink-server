package senders

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/netbirdio/netbird/management/server/eventstreaming"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	genericHTTPTimeout = 15 * time.Second
	maxResponseBody    = 64 << 10
)

// defaultBodyTemplate renders one JSON object per event. The field names match
// the dashboard's generic-HTTP placeholder template; Message and Meta render
// escaped so the quoted embedding is always valid JSON.
const defaultBodyTemplate = `{"id":"{{.ID}}","timestamp":"{{.Timestamp}}","message":"{{.Message}}","initiator_id":"{{.InitiatorID}}","target_id":"{{.TargetID}}","kind":"{{.Kind}}","meta":"{{.Meta}}"}`

// GenericHTTPSender POSTs events to a user-provided webhook URL.
type GenericHTTPSender struct {
	client *http.Client
}

// NewGenericHTTPSender creates a sender with a default HTTP client.
func NewGenericHTTPSender() *GenericHTTPSender {
	return &GenericHTTPSender{
		client: &http.Client{Timeout: genericHTTPTimeout},
	}
}

func (s *GenericHTTPSender) Platform() string {
	return types.EventStreamingPlatformGenericHTTP
}

// Push renders each event with the configured (or default) body template and
// POSTs the batch as a JSON array.
func (s *GenericHTTPSender) Push(ctx context.Context, config map[string]string, events []eventstreaming.StreamEvent) error {
	targetURL := strings.TrimSpace(config["url"])
	if targetURL == "" {
		return fmt.Errorf("generic_http: url is not configured")
	}

	tmpl, err := buildBodyTemplate(config["body_template"])
	if err != nil {
		return err
	}

	headers := map[string]string{}
	if raw := strings.TrimSpace(config["headers"]); raw != "" {
		// The dashboard sends headers as a JSON-serialized string.
		if err := json.Unmarshal([]byte(raw), &headers); err != nil {
			return fmt.Errorf("generic_http: invalid headers JSON: %w", err)
		}
	}

	rendered := make([]json.RawMessage, 0, len(events))
	for i, e := range events {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, toTemplateEvent(e)); err != nil {
			return fmt.Errorf("generic_http: render body template for event %d: %w", i, err)
		}
		out := bytes.TrimSpace(buf.Bytes())
		if !json.Valid(out) {
			snippet := out
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			return fmt.Errorf("generic_http: body template for event %d produced invalid JSON: %.200q", i, snippet)
		}
		rendered = append(rendered, json.RawMessage(out))
	}
	body, err := json.Marshal(rendered)
	if err != nil {
		return fmt.Errorf("generic_http: marshal batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("generic_http: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if strings.EqualFold(k, "Content-Length") || strings.EqualFold(k, "Host") {
			continue
		}
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("generic_http: POST %s: %w", targetURL, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("generic_http: POST %s returned status %d", targetURL, resp.StatusCode)
	}
	return nil
}

func buildBodyTemplate(custom string) (*template.Template, error) {
	src := strings.TrimSpace(custom)
	if src == "" {
		src = defaultBodyTemplate
	}
	// Fields render escaped for safe embedding inside JSON quotes, matching
	// the dashboard's placeholder contract; no helper funcs needed.
	return template.New("body").Parse(src)
}

// templateEvent exposes StreamEvent fields under the exact names the dashboard
// body_template references: {{.ID}} {{.Timestamp}} {{.Message}} {{.InitiatorID}}
// {{.TargetID}} {{.Meta}}.
//
// The dashboard's placeholder template embeds every field inside JSON quotes
// (e.g. "meta": "{{.Meta}}"), so Message and Meta render escaped for safe
// embedding in a quoted JSON string. {{.Timestamp}} renders as RFC3339;
// .Timestamp.Format works too since it embeds time.Time.
type templateEvent struct {
	ID          string
	Timestamp   eventTime
	Message     jsonString
	InitiatorID string
	TargetID    string
	Kind        string
	Meta        jsonMap
}

// eventTime renders as RFC3339 in templates while still exposing time.Time's
// methods (e.g. .Format) for custom templates.
type eventTime struct {
	time.Time
}

func (t eventTime) String() string {
	return t.Time.UTC().Format(time.RFC3339)
}

// jsonString renders with JSON string escaping (quotes, backslashes, control
// characters) but without surrounding quotes, so "{{.Message}}" is always
// valid JSON.
type jsonString string

func (s jsonString) String() string {
	b, err := json.Marshal(string(s))
	if err != nil {
		return ""
	}
	// strip the surrounding quotes added by json.Marshal
	return string(b[1 : len(b)-1])
}

type jsonMap map[string]any

func (m jsonMap) String() string {
	if m == nil {
		return "null"
	}
	b, err := json.Marshal(map[string]any(m))
	if err != nil {
		return "null"
	}
	// escape the JSON object for embedding inside a quoted JSON string
	return jsonString(b).String()
}

func toTemplateEvent(e eventstreaming.StreamEvent) templateEvent {
	return templateEvent{
		ID:          e.ID,
		Timestamp:   eventTime{Time: e.Timestamp},
		Message:     jsonString(e.Message),
		InitiatorID: e.InitiatorID,
		TargetID:    e.TargetID,
		Kind:        e.Kind,
		Meta:        jsonMap(e.Meta),
	}
}
