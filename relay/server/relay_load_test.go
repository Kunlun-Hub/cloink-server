package server

// This file is an in-process load harness for the relay data plane. It does
// not touch the real network: every peer gets a fake listener.Conn whose
// Read side is fed by an in-memory channel (standing in for the client) and
// whose Write side records deliveries (standing in for the network).
//
// Topology: N peers in a full mesh share one store. Each peer's sender
// goroutine fires MsgTypeTransport packets at random other peers. Packets
// travel the production path end to end:
//
//	sender goroutine -> peer A's read loop -> store routing ->
//	peer B's bounded send queue (patch 01) -> B's writeLoop -> B's fake conn
//
// Every payload carries an 8-byte send timestamp, so the receiving conn can
// measure end-to-end latency. A configurable per-write delay on the receiving
// conns simulates slow clients and creates the backpressure that makes the
// bounded queues drop packets, which is what the report compares across
// queue depths (Config.SendQueueSize).

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/netbirdio/netbird/relay/metrics"
	"github.com/netbirdio/netbird/relay/server/listener"
	"github.com/netbirdio/netbird/relay/server/store"
	"github.com/netbirdio/netbird/shared/relay/messages"
)

// Load-harness knobs. Defaults are CI friendly; -short scales heavy
// combinations down automatically (see loadConfigFor).
var (
	loadPeers      = flag.Int("load.peers", 8, "number of fake peers in the full mesh")
	loadMsgs       = flag.Int("load.msgs", 1500, "transport packets each peer sends")
	loadPktSize    = flag.Int("load.pktsize", 256, "payload bytes per transport packet (includes 8-byte send timestamp)")
	loadBurst      = flag.Int("load.burst", 0, "packets per burst; 0 means continuous firehose")
	loadBurstGap   = flag.Duration("load.burstgap", time.Millisecond, "pause between bursts when load.burst > 0")
	loadRate       = flag.Int("load.rate", 0, "max packets/sec per sender; 0 means unlimited")
	loadWriteDelay = flag.Duration("load.writedelay", 5*time.Microsecond, "artificial per-write latency on receiving conns, simulates slow clients and creates queue backpressure")
	loadQueues     = flag.String("load.queues", "128,1024,8192", "comma-separated Config.SendQueueSize values to compare")
	loadSeed       = flag.Int64("load.seed", 42, "random seed for destination selection")
)

type loadConfig struct {
	peers      int
	msgs       int
	pktSize    int
	burst      int
	burstGap   time.Duration
	rate       int
	writeDelay time.Duration
	seed       int64
}

// loadConfigFor resolves the effective config. In -short mode a heavy
// peer*msgs combination is scaled down so the test stays CI friendly.
func loadConfigFor(t *testing.T) loadConfig {
	t.Helper()
	cfg := loadConfig{
		peers:      *loadPeers,
		msgs:       *loadMsgs,
		pktSize:    *loadPktSize,
		burst:      *loadBurst,
		burstGap:   *loadBurstGap,
		rate:       *loadRate,
		writeDelay: *loadWriteDelay,
		seed:       *loadSeed,
	}
	if cfg.peers < 2 {
		t.Fatalf("load.peers must be >= 2, got %d", cfg.peers)
	}
	if cfg.pktSize < 8 {
		t.Fatalf("load.pktsize must be >= 8 (carries a timestamp), got %d", cfg.pktSize)
	}
	if cfg.msgs < 1 {
		t.Fatalf("load.msgs must be >= 1, got %d", cfg.msgs)
	}
	const shortBudget = 16000 // max packets per queue-depth run in -short mode
	if testing.Short() && cfg.peers*cfg.msgs > shortBudget {
		cfg.msgs = shortBudget / cfg.peers
		t.Logf("short mode: scaling down to %d msgs/peer (peers=%d) to stay within budget", cfg.msgs, cfg.peers)
	}
	// In long mode with untouched defaults, run a heavier combination.
	if !testing.Short() && cfg.peers == 8 && cfg.msgs == 1500 {
		cfg.peers, cfg.msgs = 24, 6000
		t.Logf("long mode: using peers=%d msgs/peer=%d (pass -load.peers/-load.msgs to override)", cfg.peers, cfg.msgs)
	}
	return cfg
}

func parseQueueDepths(t *testing.T, s string) []int {
	t.Helper()
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		q, err := strconv.Atoi(part)
		if err != nil || q <= 0 {
			t.Fatalf("invalid queue depth %q in -load.queues", part)
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		t.Fatalf("no queue depths in -load.queues")
	}
	return out
}

// calibrateTimerDelay measures what a per-write delay of d actually costs on
// this host. Sub-millisecond sleeps are far coarser than requested on some
// virtualized hosts; the report states the effective value so the drain
// throttle the queues push against is honest. The comparison across queue
// depths stays valid because the throttle is identical for every depth.
func calibrateTimerDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	ctx := context.Background()
	const n = 100
	start := time.Now()
	for i := 0; i < n; i++ {
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	return time.Since(start) / n
}

// connStats collects per-connection delivery latencies. Each connection is
// written by a single writer goroutine, so its mutex is effectively
// uncontended; sharding avoids a global lock on the hot write path.
type connStats struct {
	mu   sync.Mutex
	lats []int64 // end-to-end latencies in nanoseconds
}

// loadStats aggregates deliveries across all receiving conns of one run.
type loadStats struct {
	sent      atomic.Int64
	delivered atomic.Int64
	other     atomic.Int64 // non-transport writes (e.g. healthcheck)
	parseErr  atomic.Int64

	mu     sync.Mutex // guards shards, appended once per run at build time
	shards []*connStats
}

func (s *loadStats) addShard(c *connStats) {
	s.mu.Lock()
	s.shards = append(s.shards, c)
	s.mu.Unlock()
}

// recordWrite is called by each receiving fake conn for every Write.
func (c *connStats) recordWrite(s *loadStats, b []byte) {
	if len(b) < 2 || b[1] != byte(messages.MsgTypeTransport) {
		s.other.Add(1)
		return
	}
	_, payload, err := messages.UnmarshalTransportMsg(b)
	if err != nil || len(payload) < 8 {
		s.parseErr.Add(1)
		return
	}
	sentNano := int64(binary.BigEndian.Uint64(payload[:8]))
	s.delivered.Add(1)
	c.mu.Lock()
	c.lats = append(c.lats, time.Now().UnixNano()-sentNano)
	c.mu.Unlock()
}

// percentile returns the nearest-rank percentile over all shards.
func (s *loadStats) percentile(p float64) time.Duration {
	s.mu.Lock()
	shards := make([]*connStats, len(s.shards))
	copy(shards, s.shards)
	s.mu.Unlock()

	var all []int64
	for _, sh := range shards {
		sh.mu.Lock()
		all = append(all, sh.lats...)
		sh.mu.Unlock()
	}
	n := len(all)
	if n == 0 {
		return 0
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	// nearest-rank: rank = ceil(p/100 * n), 1-based
	rank := int(p/100*float64(n) + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return time.Duration(all[rank-1])
}

// loadConn is an in-process listener.Conn. Reads come from an in-memory
// channel (the "client" sending packets); writes record deliveries and can
// be artificially slowed to simulate a slow client draining the queue.
type loadConn struct {
	inbound    chan []byte
	closeCh    chan struct{}
	closed     atomic.Bool
	writeDelay time.Duration
	stats      *loadStats
	cstats     *connStats
}

var _ listener.Conn = (*loadConn)(nil)

func newLoadConn(inboundBuf int, writeDelay time.Duration, stats *loadStats) *loadConn {
	cs := &connStats{}
	stats.addShard(cs)
	return &loadConn{
		inbound:    make(chan []byte, inboundBuf),
		closeCh:    make(chan struct{}),
		writeDelay: writeDelay,
		stats:      stats,
		cstats:     cs,
	}
}

func (c *loadConn) Read(ctx context.Context, b []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, net.ErrClosed
	case <-c.closeCh:
		return 0, net.ErrClosed
	case msg := <-c.inbound:
		return copy(b, msg), nil
	}
}

func (c *loadConn) Write(ctx context.Context, b []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	if c.writeDelay > 0 {
		timer := time.NewTimer(c.writeDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-c.closeCh:
			timer.Stop()
			return 0, net.ErrClosed
		case <-timer.C:
		}
	}
	c.cstats.recordWrite(c.stats, b)
	return len(b), nil
}

func (c *loadConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }
func (c *loadConn) Protocol() string     { return "load-fake" }

func (c *loadConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeCh)
	}
	return nil
}

type loadPeer struct {
	peer *Peer
	conn *loadConn
	id   messages.PeerID
}

type loadResult struct {
	queueSize int
	sent      int64
	delivered int64
	dropped   int64
	p50       time.Duration
	p95       time.Duration
	p99       time.Duration
	elapsed   time.Duration
	maxQueue  int
}

func (r loadResult) dropPct() float64 {
	if r.sent == 0 {
		return 0
	}
	return 100 * float64(r.dropped) / float64(r.sent)
}

func (r loadResult) throughput() float64 {
	if r.elapsed <= 0 {
		return 0
	}
	return float64(r.delivered) / r.elapsed.Seconds()
}

// runLoadMesh builds one full mesh with the given queue depth, fires the
// configured traffic through the real peer read-loop/routing/queue path and
// returns the measured result.
func runLoadMesh(t *testing.T, cfg loadConfig, queueSize int) loadResult {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, err := metrics.NewMetrics(ctx, otel.Meter("load"))
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewStore()
	notifier := store.NewPeerNotifier()
	stats := &loadStats{}

	peers := make([]*loadPeer, cfg.peers)
	ids := make([]messages.PeerID, cfg.peers)
	for i := 0; i < cfg.peers; i++ {
		id := messages.HashID(fmt.Sprintf("load-peer-%d", i))
		conn := newLoadConn(cfg.msgs, cfg.writeDelay, stats)
		p := NewPeer(m, id, conn, st, notifier, queueSize)
		st.AddPeer(p)
		peers[i] = &loadPeer{peer: p, conn: conn, id: id}
		ids[i] = id
	}

	var workWg sync.WaitGroup
	for _, lp := range peers {
		workWg.Add(1)
		go func(p *Peer) {
			defer workWg.Done()
			p.Work()
		}(lp.peer)
	}

	// Sample the deepest queue occupancy across peers while traffic flows.
	var maxQueue atomic.Int64
	stopSample := make(chan struct{})
	var sampleWg sync.WaitGroup
	sampleWg.Add(1)
	go func() {
		defer sampleWg.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSample:
				return
			case <-ticker.C:
				deep := 0
				for _, lp := range peers {
					if l := lp.peer.SendQueueLen(); l > deep {
						deep = l
					}
				}
				for {
					cur := maxQueue.Load()
					if int64(deep) <= cur || maxQueue.CompareAndSwap(cur, int64(deep)) {
						break
					}
				}
			}
		}
	}()

	// Sender goroutines: each peer fires cfg.msgs transport packets at
	// random other peers, stamping the send time into the payload.
	var sendWg sync.WaitGroup
	start := time.Now()
	for i, lp := range peers {
		sendWg.Add(1)
		go func(i int, lp *loadPeer) {
			defer sendWg.Done()
			rng := rand.New(rand.NewSource(cfg.seed + int64(i)))
			var pace <-chan time.Time
			var ticker *time.Ticker
			if cfg.rate > 0 {
				ticker = time.NewTicker(time.Second / time.Duration(cfg.rate))
				defer ticker.Stop()
				pace = ticker.C
			}
			for j := 0; j < cfg.msgs; j++ {
				dst := rng.Intn(cfg.peers - 1)
				if dst >= i {
					dst++
				}
				payload := make([]byte, cfg.pktSize)
				binary.BigEndian.PutUint64(payload, uint64(time.Now().UnixNano()))
				msg, err := messages.MarshalTransportMsg(ids[dst], payload)
				if err != nil {
					t.Errorf("marshal transport msg: %v", err)
					return
				}
				// The inbound channel is sized for the whole allocation, so
				// a sender never blocks here: backpressure is expected only
				// inside the relay's bounded send queues.
				lp.conn.inbound <- msg
				stats.sent.Add(1)
				if cfg.burst > 0 && (j+1)%cfg.burst == 0 {
					time.Sleep(cfg.burstGap)
				}
				if pace != nil {
					<-pace
				}
			}
		}(i, lp)
	}
	sendWg.Wait()

	// Wait until every sent packet has either been delivered or dropped by
	// a full queue; anything else would be stuck in a queue.
	totalDropped := func() int64 {
		var d int64
		for _, lp := range peers {
			d += int64(lp.peer.DroppedPackets())
		}
		return d
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if stats.delivered.Load()+totalDropped() >= stats.sent.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("traffic did not settle: sent=%d delivered=%d dropped=%d",
				stats.sent.Load(), stats.delivered.Load(), totalDropped())
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)

	close(stopSample)
	sampleWg.Wait()

	for _, lp := range peers {
		_ = lp.conn.Close()
	}
	done := make(chan struct{})
	go func() {
		workWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("peers did not shut down")
	}

	if stats.parseErr.Load() > 0 {
		t.Errorf("failed to parse %d delivered transport messages", stats.parseErr.Load())
	}

	return loadResult{
		queueSize: queueSize,
		sent:      stats.sent.Load(),
		delivered: stats.delivered.Load(),
		dropped:   totalDropped(),
		p50:       stats.percentile(50),
		p95:       stats.percentile(95),
		p99:       stats.percentile(99),
		elapsed:   elapsed,
		maxQueue:  int(maxQueue.Load()),
	}
}

func formatDuration(d time.Duration) string {
	switch {
	case d <= 0:
		return "-"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1000)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

// TestRelayDataPlaneLoad runs the in-process load harness once per queue
// depth and prints a comparative report. Use -short (default for CI) for the
// light combination; run without -short for the heavy one. All knobs are
// adjustable via -load.* flags; run with -v to see the report.
func TestRelayDataPlaneLoad(t *testing.T) {
	cfg := loadConfigFor(t)
	queueDepths := parseQueueDepths(t, *loadQueues)

	effectiveDelay := calibrateTimerDelay(cfg.writeDelay)
	t.Logf("load config: peers=%d msgs/peer=%d payload=%dB burst=%d burstgap=%v rate=%d/s writedelay=%v (effective %v on this host) seed=%d",
		cfg.peers, cfg.msgs, cfg.pktSize, cfg.burst, cfg.burstGap, cfg.rate, cfg.writeDelay, effectiveDelay, cfg.seed)

	var results []loadResult
	for _, q := range queueDepths {
		q := q
		t.Run(fmt.Sprintf("queue=%d", q), func(t *testing.T) {
			res := runLoadMesh(t, cfg, q)
			t.Logf("queue=%d: sent=%d delivered=%d dropped=%d (%.2f%%) p50=%s p95=%s p99=%s throughput=%.0f msgs/s maxQueue=%d/%d",
				res.queueSize, res.sent, res.delivered, res.dropped, res.dropPct(),
				formatDuration(res.p50), formatDuration(res.p95), formatDuration(res.p99),
				res.throughput(), res.maxQueue, res.queueSize)
			results = append(results, res)
		})
	}

	t.Log("=== relay data-plane load report ===")
	t.Logf("config: peers=%d msgs/peer=%d payload=%dB writedelay=%v (effective %v; drop%% = relay-side bounded-queue drops; latency includes queueing behind the throttled drain)",
		cfg.peers, cfg.msgs, cfg.pktSize, cfg.writeDelay, effectiveDelay)
	t.Log("| queue |    sent | delivered | dropped |  drop% |     p50 |     p95 |     p99 |  msgs/s | maxQ |")
	t.Log("|-------|---------|-----------|---------|--------|---------|---------|---------|---------|------|")
	for _, r := range results {
		t.Logf("| %5d | %7d | %9d | %7d | %6.2f | %7s | %7s | %7s | %7.0f | %4d |",
			r.queueSize, r.sent, r.delivered, r.dropped, r.dropPct(),
			formatDuration(r.p50), formatDuration(r.p95), formatDuration(r.p99),
			r.throughput(), r.maxQueue)
	}
}
