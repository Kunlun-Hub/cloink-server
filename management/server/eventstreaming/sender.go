package eventstreaming

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/activity"
)

// StreamEvent is the normalized event handed to senders.
type StreamEvent struct {
	ID          string
	Timestamp   time.Time
	Message     string
	InitiatorID string
	TargetID    string
	Meta        map[string]any
	Kind        string // "activity" | "flow"
}

const (
	StreamEventKindActivity = "activity"
	StreamEventKindFlow     = "flow"
)

// Sender pushes a batch of events to one streaming destination.
// Implementations must be safe for concurrent use.
type Sender interface {
	// Platform returns the platform this sender handles (e.g. "datadog").
	Platform() string
	// Push delivers a batch of events. It returns nil only when the batch is
	// durably accepted; any error triggers a retry with the cursor unchanged.
	Push(ctx context.Context, config map[string]string, events []StreamEvent) error
}

// streamEventFromActivity converts an activity event to the normalized form.
// Field names ID/Timestamp/Message/InitiatorID/TargetID/Meta match the
// variables the dashboard's generic-HTTP body_template references
// ({{.ID}} {{.Timestamp}} {{.Message}} {{.InitiatorID}} {{.TargetID}} {{.Meta}}).
func streamEventFromActivity(e *activity.Event) StreamEvent {
	meta := make(map[string]any, len(e.Meta)+2)
	for k, v := range e.Meta {
		meta[k] = v
	}
	return StreamEvent{
		ID:          strconv.FormatUint(e.ID, 10),
		Timestamp:   e.Timestamp,
		Message:     e.Activity.Message(),
		InitiatorID: e.InitiatorID,
		TargetID:    e.TargetID,
		Meta:        meta,
		Kind:        StreamEventKindActivity,
	}
}

// streamEventFromFlow converts a network traffic flow event to the normalized form.
func streamEventFromFlow(e *networktraffic.Event) StreamEvent {
	meta := map[string]any{
		"event_type":       e.EventType,
		"direction":        e.Direction,
		"protocol":         e.Protocol,
		"connection_type":  e.ConnectionType,
		"reporter_id":      e.ReporterID,
		"user_id":          e.UserID,
		"source_id":        e.SourceID,
		"source_type":      e.SourceType,
		"source_name":      e.SourceName,
		"source_address":   e.SourceAddress,
		"destination_id":   e.DestinationID,
		"destination_type": e.DestinationType,
		"destination_name": e.DestinationName,
		"address":          e.DestinationAddress,
		"policy_id":        e.PolicyID,
		"policy_name":      e.PolicyName,
		"rx_bytes":         e.RxBytes,
		"tx_bytes":         e.TxBytes,
		"rx_packets":       e.RxPackets,
		"tx_packets":       e.TxPackets,
	}
	message := fmt.Sprintf("flow %s %s %s -> %s (%d rx / %d tx bytes)",
		e.EventType, e.Direction, e.SourceAddress, e.DestinationAddress, e.RxBytes, e.TxBytes)
	return StreamEvent{
		ID:          e.ID,
		Timestamp:   e.Timestamp,
		Message:     message,
		InitiatorID: e.UserID,
		TargetID:    e.DestinationID,
		Meta:        meta,
		Kind:        StreamEventKindFlow,
	}
}
