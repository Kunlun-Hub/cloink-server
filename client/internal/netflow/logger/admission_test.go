package logger

import (
	"net/netip"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/netbirdio/netbird/client/internal/netflow/types"
)

func TestAdmissionStats(t *testing.T) {
	l := New(nil, netip.Prefix{}, netip.Prefix{})
	event := types.EventFields{FlowID: uuid.New()}

	l.StoreEvent(event)

	l.mux.Lock()
	l.rcvChan = make(chan *types.EventFields, 1)
	l.enabled.Store(true)
	l.mux.Unlock()
	l.StoreEvent(event)
	l.StoreEvent(event)

	l.mux.Lock()
	l.enabled.Store(false)
	l.mux.Unlock()
	l.StoreEvent(event)

	assert.Equal(t, Stats{
		Accepted:         1,
		Disabled:         2,
		CaptureQueueFull: 1,
		Closing:          0,
		ShutdownTimeout:  0,
	}, l.Stats())
}
