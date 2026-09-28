package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/netbirdio/netbird/relay/metrics"
	"github.com/netbirdio/netbird/relay/server/listener"
	"github.com/netbirdio/netbird/relay/server/store"
	"github.com/netbirdio/netbird/shared/relay/messages"
)

// discardConn is a listener.Conn that counts writes without storing them, so
// throughput benchmarks are not dominated by recording every message.
type discardConn struct {
	*fakeConn
	writes atomic.Int64
}

func (d *discardConn) Write(ctx context.Context, b []byte) (int, error) {
	d.writes.Add(1)
	return len(b), nil
}

func newBenchPeer(b *testing.B, conn listener.Conn, queueSize int) *Peer {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	m, err := metrics.NewMetrics(ctx, otel.Meter("bench"))
	if err != nil {
		b.Fatal(err)
	}
	return NewPeer(m, messages.HashID("bench-peer"), conn, store.NewStore(), store.NewPeerNotifier(), queueSize)
}

// startBenchWriter runs the peer's writer goroutine for the benchmark and
// stops it on cleanup.
func startBenchWriter(b *testing.B, p *Peer) {
	b.Helper()
	p.writerWg.Add(1)
	go p.writeLoop()
	b.Cleanup(func() {
		p.ctxCancel()
		p.writerWg.Wait()
	})
}

// waitForPeerDrain blocks until every enqueued message has either been written
// out or counted as dropped.
func waitForPeerDrain(b *testing.B, p *Peer, conn *discardConn, total int) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if conn.writes.Load()+int64(p.DroppedPackets()) >= int64(total) {
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("peer did not drain %d messages (delivered=%d dropped=%d)",
				total, conn.writes.Load(), p.DroppedPackets())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// BenchmarkPeer_EnqueueDequeue measures the enqueue/dequeue pipeline: the
// writer goroutine drains to a discard conn while the benchmark enqueues.
func BenchmarkPeer_EnqueueDequeue(b *testing.B) {
	conn := &discardConn{fakeConn: newFakeConn()}
	p := newBenchPeer(b, conn, 8192)
	startBenchWriter(b, p)

	msg := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.EnqueueTransport(msg)
	}
	b.StopTimer()
	waitForPeerDrain(b, p, conn, b.N)
	b.ReportMetric(float64(conn.writes.Load())/float64(b.N)*100, "delivered_pct")
}

// BenchmarkPeer_EnqueueFullQueueDrops measures the drop path: with the writer
// stopped and the queue pre-filled, every enqueue must drop without blocking.
func BenchmarkPeer_EnqueueFullQueueDrops(b *testing.B) {
	const queueSize = 64
	p := newBenchPeer(b, newFakeConn(), queueSize)

	msg := make([]byte, 128)
	for i := 0; i < queueSize; i++ {
		p.EnqueueTransport(msg)
	}
	if got := p.SendQueueLen(); got != queueSize {
		b.Fatalf("expected a full queue of %d, got %d", queueSize, got)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.EnqueueTransport(msg)
	}
	b.StopTimer()

	if got := p.DroppedPackets(); got != uint64(b.N) {
		b.Fatalf("expected %d drops, got %d", b.N, got)
	}
}

// BenchmarkPeer_EnqueueParallel measures enqueue throughput under concurrent
// producers, the shape of many read loops feeding one peer's queue.
func BenchmarkPeer_EnqueueParallel(b *testing.B) {
	conn := &discardConn{fakeConn: newFakeConn()}
	p := newBenchPeer(b, conn, 8192)
	startBenchWriter(b, p)

	msg := make([]byte, 128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.EnqueueTransport(msg)
		}
	})
	b.StopTimer()
	waitForPeerDrain(b, p, conn, b.N)
}
