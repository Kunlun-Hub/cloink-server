package networktraffic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventToAPIResponseUsesPersistedEventID(t *testing.T) {
	now := time.Now().UTC()
	event := &Event{
		ID:          "event-id",
		FlowID:      "shared-flow-id",
		Timestamp:   now,
		WindowStart: now,
		WindowEnd:   now,
	}

	response := event.ToAPIResponse()
	require.Equal(t, "event-id", response.Id)
	require.Equal(t, "shared-flow-id", response.FlowId)
}
