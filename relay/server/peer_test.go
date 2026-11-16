package server

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/netbirdio/netbird/relay/metrics"
	"github.com/netbirdio/netbird/relay/server/store"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
	"github.com/netbirdio/netbird/shared/relay/messages"
)

// fakeConn implements listener.Conn for peer send-queue tests.
type fakeConn struct {
	mu           sync.Mutex
	writes       [][]byte
	started      bool
	writeStarted chan struct{}
	blockWrite   chan struct{}
	writeErr     error
	closed       atomic.Bool
}

func newFakeConn() *fakeConn {
	return &fakeConn{writeStarted: make(chan struct{})}
}

func (f *fakeConn) Read(ctx context.Context, b []byte) (int, error) {
	<-ctx.Done()
	return 0, net.ErrClosed
}

func (f *fakeConn) Write(ctx context.Context, b []byte) (int, error) {
	f.mu.Lock()
	if !f.started {
		f.started = true
		close(f.writeStarted)
	}
	f.mu.Unlock()

	if f.blockWrite != nil {
		select {
		case <-f.blockWrite:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}

	cp := make([]byte, len(b))
	copy(cp, b)
	f.mu.Lock()
	f.writes = append(f.writes, cp)
	f.mu.Unlock()
	return len(b), nil
}

func (f *fakeConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }

func (f *fakeConn) Protocol() string { return "fake" }

func (f *fakeConn) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *fakeConn) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.writes)
}

func newTestPeer(t *testing.T, conn *fakeConn, queueSize int) *Peer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m, err := metrics.NewMetrics(ctx, otel.Meter("test"))
	require.NoError(t, err)
	return NewPeer(m, messages.HashID("test-peer"), messages.ProtocolV1, conn, store.NewStore(), store.NewPeerNotifier(), queueSize)
}

func TestPeer_SendQueueSizeDefault(t *testing.T) {
	assert.Equal(t, 1024, newTestPeer(t, newFakeConn(), 0).SendQueueCap())
	assert.Equal(t, 1024, newTestPeer(t, newFakeConn(), -1).SendQueueCap())
	assert.Equal(t, 8, newTestPeer(t, newFakeConn(), 8).SendQueueCap())
}

func TestPeer_EnqueueTransportDropsWhenFull(t *testing.T) {
	p := newTestPeer(t, newFakeConn(), 2)

	// The writer is not running, so the queue fills and the rest must drop
	// without blocking.
	for i := 0; i < 5; i++ {
		p.EnqueueTransport([]byte{byte(i)})
	}
	assert.Equal(t, 2, p.SendQueueLen())
	assert.Equal(t, uint64(3), p.DroppedPackets())
}

// droppedPacketsMetricValue reads an int64 sum counter through a ManualReader.
func droppedPacketsMetricValue(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s is not an int64 sum", name)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

func TestPeer_DroppedPacketsFlushedToMetrics(t *testing.T) {
	// Full-queue drops must show up on the OTel counter after the metrics
	// flush interval, not just on the peer's atomic counter.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := metrics.NewMetrics(ctx, provider.Meter("test"))
	require.NoError(t, err)

	p := NewPeer(m, messages.HashID("test-peer"), messages.ProtocolV1, newFakeConn(), store.NewStore(), store.NewPeerNotifier(), 2)

	// The writer is not running, so the queue fills and the rest must drop
	// without blocking.
	for i := 0; i < 5; i++ {
		p.EnqueueTransport([]byte{byte(i)})
	}
	assert.Equal(t, uint64(3), p.DroppedPackets(), "atomic drop counter must grow on a full queue")

	require.Eventually(t, func() bool {
		return droppedPacketsMetricValue(t, ctx, reader, "relay_transport_dropped_packets_total") == 3
	}, 5*time.Second, 50*time.Millisecond, "dropped packets were not flushed to the OTel counter")
}

func TestPeer_EnqueueTransportCopiesBytes(t *testing.T) {
	conn := newFakeConn()
	p := newTestPeer(t, conn, 4)

	msg := []byte("original")
	p.EnqueueTransport(msg)
	// Simulate the read loop reusing its buffer on the next read.
	for i := range msg {
		msg[i] = 'x'
	}

	p.writerWg.Add(1)
	go p.writeLoop()
	require.Eventually(t, func() bool { return conn.writeCount() == 1 }, 3*time.Second, 10*time.Millisecond)
	p.ctxCancel()
	p.writerWg.Wait()

	assert.Equal(t, []byte("original"), conn.writes[0])
}

func TestPeer_WriteLoopDeliversInOrder(t *testing.T) {
	conn := newFakeConn()
	p := newTestPeer(t, conn, 64)

	p.writerWg.Add(1)
	go p.writeLoop()

	const n = 50
	for i := 0; i < n; i++ {
		p.EnqueueTransport([]byte{byte(i)})
	}
	require.Eventually(t, func() bool { return conn.writeCount() == n }, 3*time.Second, 10*time.Millisecond)
	p.ctxCancel()
	p.writerWg.Wait()

	for i := 0; i < n; i++ {
		assert.Equal(t, []byte{byte(i)}, conn.writes[i], "message %d out of order", i)
	}
	assert.Equal(t, uint64(0), p.DroppedPackets())
}

func TestPeer_SlowPeerDoesNotBlockEnqueue(t *testing.T) {
	conn := newFakeConn()
	conn.blockWrite = make(chan struct{})
	p := newTestPeer(t, conn, 64)

	p.writerWg.Add(1)
	go p.writeLoop()

	// Wait until the writer is stuck inside Write, simulating a slow peer.
	p.EnqueueTransport([]byte("first"))
	select {
	case <-conn.writeStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not start writing")
	}

	// Fill and overflow the queue while the writer is stuck: enqueueing must
	// never block the caller, so the slow peer cannot stall a read loop.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			p.EnqueueTransport([]byte{byte(i)})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("EnqueueTransport blocked while the writer was stuck on a slow peer")
	}

	close(conn.blockWrite)
	p.ctxCancel()
	p.writerWg.Wait()
	assert.Greater(t, p.DroppedPackets(), uint64(0), "expected drops with a stuck writer and a bounded queue")
}
func TestPeer_DropsFramesItCannotHandle(t *testing.T) {
	secondAuth, err := messages.MarshalAuthMsg(messages.HashID("alice"), []byte("token"))
	require.NoError(t, err)

	cases := []struct {
		name  string
		frame []byte
	}{
		{"unknown message type", []byte{byte(messages.ProtocolV1), 200, 0xDE, 0xAD}},
		{"version above the selected one", []byte{2, byte(messages.MsgTypeHealthCheck)}},
		{"version zero", []byte{0, byte(messages.MsgTypeHealthCheck)}},
		{"header shorter than two bytes", []byte{byte(messages.ProtocolV1)}},
		{"reserved hello type", []byte{byte(messages.ProtocolV1), byte(messages.MsgTypeHello)}},
		{"server to client peers online type", []byte{byte(messages.ProtocolV1), byte(messages.MsgTypePeersOnline)}},
		{"auth after authentication", secondAuth},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			relay := newTestRelay(t, &allow.Auth{})
			alice, _ := acceptPeer(t, relay, "alice")
			_, bobID := acceptPeer(t, relay, "bob")

			alice.deliver(t, tc.frame)
			subscribe, err := messages.MarshalSubPeerStateMsg([]messages.PeerID{bobID})
			require.NoError(t, err)
			alice.deliver(t, subscribe[0])

			// The subscription answer proves the loop survived the dropped frame before it.
			frames := alice.waitFrames(t, 2)
			assert.False(t, alice.isClosed(), "a dropped frame must not close the connection")
			online := frames[1]
			assert.Equal(t, byte(messages.ProtocolV1), online[0], "notifications must carry the connection version")
			assert.Equal(t, byte(messages.MsgTypePeersOnline), online[1], "the subscription must be answered")
			ids, err := messages.UnmarshalPeersOnlineMsg(online)
			require.NoError(t, err)
			assert.Equal(t, []messages.PeerID{bobID}, ids, "the online peer must be reported")
		})
	}
}

func TestPeer_ClosesOnClientCloseMessage(t *testing.T) {
	relay := newTestRelay(t, &allow.Auth{})
	conn, peerID := acceptPeer(t, relay, "alice")

	conn.deliver(t, messages.MarshalCloseMsg())

	conn.waitClosed(t)
	assert.Len(t, conn.frames(), 1, "only the auth response may be written")
	assert.Eventually(t, func() bool {
		_, ok := relay.store.Peer(peerID)
		return !ok
	}, testWaitTimeout, 5*time.Millisecond, "the peer must be removed from the store")
}

func TestPeer_ForwardsTransportWithSenderAndDestinationVersion(t *testing.T) {
	relay := newTestRelay(t, &allow.Auth{})
	alice, aliceID := acceptPeer(t, relay, "alice")
	bob, bobID := acceptPeer(t, relay, "bob")

	payload := []byte("hello bob")
	transport, err := messages.MarshalTransportMsg(bobID, payload)
	require.NoError(t, err)
	alice.deliver(t, transport)

	frames := bob.waitFrames(t, 2)
	forwarded := frames[1]
	assert.Equal(t, byte(messages.ProtocolV1), forwarded[0], "the forwarded frame must carry the destination's version")
	assert.Equal(t, byte(messages.MsgTypeTransport), forwarded[1], "the forwarded frame must stay a transport message")
	senderID, gotPayload, err := messages.UnmarshalTransportMsg(forwarded)
	require.NoError(t, err)
	assert.Equal(t, aliceID, *senderID, "the peer ID must be rewritten to the sender")
	assert.Equal(t, payload, gotPayload, "the payload must be preserved")
	assert.False(t, alice.isClosed(), "the sender must stay connected")
}

func TestPeer_GracefulCloseCarriesConnectionVersion(t *testing.T) {
	relay := newTestRelay(t, &allow.Auth{})
	conn, _ := acceptPeer(t, relay, "alice")

	relay.Shutdown(context.Background())

	frames := conn.waitFrames(t, 2)
	assert.Equal(t, messages.NewEncoder(messages.ProtocolV1).CloseMsg(), frames[1], "the close message must carry the connection version")
	assert.True(t, conn.isClosed(), "the connection must be closed after the close message")
}
