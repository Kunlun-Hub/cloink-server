//go:build linux && !android

package conntrack

import (
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nfct "github.com/ti-mo/conntrack"
	"github.com/ti-mo/netfilter"

	nftypes "github.com/netbirdio/netbird/client/internal/netflow/types"
	nbnet "github.com/netbirdio/netbird/client/net"
)

type mockListener struct {
	errChan  chan error
	closed   atomic.Bool
	closedCh chan struct{}
}

func newMockListener() *mockListener {
	return &mockListener{
		errChan:  make(chan error, 1),
		closedCh: make(chan struct{}),
	}
}

func (m *mockListener) Listen(evChan chan<- nfct.Event, _ uint8, _ []netfilter.NetlinkGroup) (chan error, error) {
	return m.errChan, nil
}

func (m *mockListener) Close() error {
	if m.closed.CompareAndSwap(false, true) {
		close(m.closedCh)
	}
	return nil
}

type recordingFlowLogger struct {
	nftypes.FlowLogger
	events []nftypes.EventFields
}

func (l *recordingFlowLogger) StoreEvent(event nftypes.EventFields) {
	l.events = append(l.events, event)
}

func TestHandleEventCounters(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mark      uint32
		rxPackets uint64
		txPackets uint64
		rxBytes   uint64
		txBytes   uint64
	}{
		{name: "ingress", mark: nbnet.DataPlaneMarkIn, rxPackets: 11, txPackets: 22, rxBytes: 1100, txBytes: 2200},
		{name: "egress", mark: nbnet.DataPlaneMarkOut, rxPackets: 22, txPackets: 11, rxBytes: 2200, txBytes: 1100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := &recordingFlowLogger{}
			tracker := New(logger, nil)
			flow := nfct.NewFlow(uint8(nftypes.TCP), 0,
				netip.MustParseAddr("100.64.0.1"), netip.MustParseAddr("100.64.0.2"),
				1234, 443, 0, tc.mark)
			flow.ID = 42
			flow.CountersOrig = nfct.Counter{Packets: 11, Bytes: 1100}
			flow.CountersReply = nfct.Counter{Packets: 22, Bytes: 2200}

			tracker.handleEvent(nfct.Event{Type: nfct.EventNew, Flow: &flow})
			tracker.handleEvent(nfct.Event{Type: nfct.EventDestroy, Flow: &flow})

			require.Len(t, logger.events, 2, "new and destroy should be emitted")
			start, end := logger.events[0], logger.events[1]
			assert.Equal(t, start.FlowID, end.FlowID, "lifecycle should preserve flow ID")
			assert.Equal(t, nftypes.TypeStart, start.Type, "new should emit start")
			assert.Zero(t, start.RxPackets, "start RX packets should be zero")
			assert.Zero(t, start.TxPackets, "start TX packets should be zero")
			assert.Zero(t, start.RxBytes, "start RX bytes should be zero")
			assert.Zero(t, start.TxBytes, "start TX bytes should be zero")
			assert.Equal(t, nftypes.TypeEnd, end.Type, "destroy should emit end")
			assert.Equal(t, tc.rxPackets, end.RxPackets, "destroy RX packets should map by direction")
			assert.Equal(t, tc.txPackets, end.TxPackets, "destroy TX packets should map by direction")
			assert.Equal(t, tc.rxBytes, end.RxBytes, "destroy RX bytes should map by direction")
			assert.Equal(t, tc.txBytes, end.TxBytes, "destroy TX bytes should map by direction")
			assert.Equal(t, flow.TupleOrig.IP.SourceAddress, end.SourceIP, "source should remain original tuple")
			assert.Equal(t, flow.TupleOrig.IP.DestinationAddress, end.DestIP, "destination should remain original tuple")
			assert.Equal(t, flow.TupleOrig.Proto.SourcePort, end.SourcePort, "source port should remain original tuple")
			assert.Equal(t, flow.TupleOrig.Proto.DestinationPort, end.DestPort, "destination port should remain original tuple")
		})
	}
}

func TestHandleEventIgnoresUpdate(t *testing.T) {
	logger := &recordingFlowLogger{}
	tracker := New(logger, nil)
	flow := nfct.NewFlow(uint8(nftypes.UDP), 0,
		netip.MustParseAddr("100.64.0.1"), netip.MustParseAddr("100.64.0.2"),
		1234, 53, 0, nbnet.DataPlaneMarkOut)

	tracker.handleEvent(nfct.Event{Type: nfct.EventUpdate, Flow: &flow})

	assert.Empty(t, logger.events, "cumulative update snapshots should be ignored")
}

func TestReconnectAfterError(t *testing.T) {
	first := newMockListener()
	second := newMockListener()
	third := newMockListener()
	listeners := []*mockListener{first, second, third}
	callCount := atomic.Int32{}

	ct := New(nil, nil, WithDialer(func() (listener, error) {
		n := int(callCount.Add(1)) - 1
		return listeners[n], nil
	}))

	err := ct.Start(false)
	require.NoError(t, err)

	// Inject an error on the first listener.
	first.errChan <- assert.AnError

	// Wait for reconnect to complete.
	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 15*time.Second, 100*time.Millisecond, "reconnect should dial a new connection")

	// The first connection must have been closed.
	select {
	case <-first.closedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("first connection was not closed")
	}

	// Verify the receiver is still running by injecting and handling a second error.
	second.errChan <- assert.AnError

	require.Eventually(t, func() bool {
		return callCount.Load() >= 3
	}, 15*time.Second, 100*time.Millisecond, "second reconnect should succeed")

	ct.Stop()
}

func TestStopDuringReconnectBackoff(t *testing.T) {
	mock := newMockListener()

	ct := New(nil, nil, WithDialer(func() (listener, error) {
		return mock, nil
	}))

	err := ct.Start(false)
	require.NoError(t, err)

	// Trigger an error so the receiver enters reconnect.
	mock.errChan <- assert.AnError

	// Wait for the error handler to close the old listener before calling Stop.
	select {
	case <-mock.closedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect to start")
	}

	// Stop while reconnecting.
	ct.Stop()

	ct.mux.Lock()
	assert.False(t, ct.started, "started should be false after Stop")
	assert.Nil(t, ct.conn, "conn should be nil after Stop")
	ct.mux.Unlock()
}

func TestStopRaceWithReconnectDial(t *testing.T) {
	first := newMockListener()
	dialStarted := make(chan struct{})
	dialProceed := make(chan struct{})
	second := newMockListener()
	callCount := atomic.Int32{}

	ct := New(nil, nil, WithDialer(func() (listener, error) {
		n := callCount.Add(1)
		if n == 1 {
			return first, nil
		}
		// Second dial: signal that we're in progress, wait for test to call Stop.
		close(dialStarted)
		<-dialProceed
		return second, nil
	}))

	err := ct.Start(false)
	require.NoError(t, err)

	// Trigger error to enter reconnect.
	first.errChan <- assert.AnError

	// Wait for reconnect's second dial to begin.
	select {
	case <-dialStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for reconnect dial")
	}

	// Stop while dial is in progress (conn is nil at this point).
	ct.Stop()

	// Let the dial complete. reconnect should detect started==false and close the new conn.
	close(dialProceed)

	// The second connection should be closed (not leaked).
	select {
	case <-second.closedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("second connection was leaked after Stop")
	}

	ct.mux.Lock()
	assert.False(t, ct.started)
	assert.Nil(t, ct.conn)
	ct.mux.Unlock()
}

func TestCloseRaceWithReconnectDial(t *testing.T) {
	first := newMockListener()
	dialStarted := make(chan struct{})
	dialProceed := make(chan struct{})
	second := newMockListener()
	callCount := atomic.Int32{}

	ct := New(nil, nil, WithDialer(func() (listener, error) {
		n := callCount.Add(1)
		if n == 1 {
			return first, nil
		}
		close(dialStarted)
		<-dialProceed
		return second, nil
	}))

	err := ct.Start(false)
	require.NoError(t, err)

	first.errChan <- assert.AnError

	select {
	case <-dialStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for reconnect dial")
	}

	// Close while dial is in progress (conn is nil).
	require.NoError(t, ct.Close())

	close(dialProceed)

	// The second connection should be closed (not leaked).
	select {
	case <-second.closedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("second connection was leaked after Close")
	}

	ct.mux.Lock()
	assert.False(t, ct.started)
	assert.Nil(t, ct.conn)
	ct.mux.Unlock()
}

func TestStartIsIdempotent(t *testing.T) {
	mock := newMockListener()
	callCount := atomic.Int32{}

	ct := New(nil, nil, WithDialer(func() (listener, error) {
		callCount.Add(1)
		return mock, nil
	}))

	err := ct.Start(false)
	require.NoError(t, err)

	// Second Start should be a no-op.
	err = ct.Start(false)
	require.NoError(t, err)

	assert.Equal(t, int32(1), callCount.Load(), "dial should only be called once")

	ct.Stop()
}
