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
	event := types.EventFields{
		FlowID:   uuid.New(),
		SourceIP: netip.MustParseAddr("100.64.0.1"),
		DestIP:   netip.MustParseAddr("100.64.0.2"),
	}

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

func TestStoreEventFiltersSystemLocalAddresses(t *testing.T) {
	l := New(nil, netip.Prefix{}, netip.Prefix{})
	l.Enable()
	t.Cleanup(l.Close)

	tests := []struct {
		name        string
		source      string
		destination string
	}{
		{name: "IPv4 multicast destination", source: "100.80.165.252", destination: "224.0.0.252"},
		{name: "IPv6 multicast destination", source: "fdaf:6fa3::1", destination: "ff02::16"},
		{name: "IPv4 link local source", source: "169.254.1.10", destination: "100.80.165.252"},
		{name: "IPv6 loopback source", source: "::1", destination: "fdaf:6fa3::1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l.StoreEvent(types.EventFields{
				FlowID:   uuid.New(),
				SourceIP: netip.MustParseAddr(test.source),
				DestIP:   netip.MustParseAddr(test.destination),
			})
		})
	}

	stats := l.Stats()
	assert.Equal(t, uint64(len(tests)), stats.Filtered)
	assert.Zero(t, stats.Accepted)
	assert.Empty(t, l.GetEvents())

	l.StoreEvent(types.EventFields{
		FlowID:   uuid.New(),
		SourceIP: netip.MustParseAddr("100.80.165.252"),
		DestIP:   netip.MustParseAddr("100.80.165.253"),
	})
	assert.Equal(t, uint64(1), l.Stats().Accepted)
}

func TestShouldStoreExitNodeFollowsCollectionSetting(t *testing.T) {
	l := New(nil, netip.MustParsePrefix("100.64.0.0/10"), netip.Prefix{})
	event := &types.EventFields{
		Protocol:  types.TCP,
		SourceIP:  netip.MustParseAddr("100.80.0.1"),
		DestIP:    netip.MustParseAddr("8.8.8.8"),
		DestPort:  443,
		Direction: types.Egress,
	}

	l.UpdateConfig(false, false)
	assert.False(t, l.shouldStore(event, true))
	l.UpdateConfig(false, true)
	assert.True(t, l.shouldStore(event, true))
}
