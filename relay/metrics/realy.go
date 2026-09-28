package metrics

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	idleTimeout = 30 * time.Second

	// metricsFlushInterval is how often atomically accumulated data-path
	// counters are flushed to OTel. Recording per packet would serialize the
	// data path on the SDK; the flushed deltas are equivalent for monotonic
	// counters.
	metricsFlushInterval = time.Second
)

type Metrics struct {
	metric.Meter

	TransferBytesSent  metric.Int64Counter
	TransferBytesRecv  metric.Int64Counter
	AuthenticationTime metric.Float64Histogram
	PeerStoreTime      metric.Float64Histogram
	peerReconnections  metric.Int64Counter
	transportDropped   metric.Int64Counter
	peers              metric.Int64UpDownCounter
	peerActivityChan   chan string
	peerLastActive     map[string]time.Time
	mutexActivity      sync.Mutex
	ctx                context.Context

	// bytesSent, bytesRecv and droppedPackets accumulate data-path counts
	// atomically; a single goroutine flushes them to OTel every
	// metricsFlushInterval.
	bytesSent      atomic.Int64
	bytesRecv      atomic.Int64
	droppedPackets atomic.Int64
}

func NewMetrics(ctx context.Context, meter metric.Meter) (*Metrics, error) {
	bytesSent, err := meter.Int64Counter("relay_transfer_sent_bytes_total",
		metric.WithDescription("Total number of bytes sent to peers"),
	)
	if err != nil {
		return nil, err
	}

	bytesRecv, err := meter.Int64Counter("relay_transfer_received_bytes_total",
		metric.WithDescription("Total number of bytes received from peers"),
	)
	if err != nil {
		return nil, err
	}

	peers, err := meter.Int64UpDownCounter("relay_peers",
		metric.WithDescription("Number of connected peers"),
	)
	if err != nil {
		return nil, err
	}

	peersActive, err := meter.Int64ObservableGauge("relay_peers_active",
		metric.WithDescription("Number of active connected peers"),
	)
	if err != nil {
		return nil, err
	}

	peersIdle, err := meter.Int64ObservableGauge("relay_peers_idle",
		metric.WithDescription("Number of idle connected peers"),
	)
	if err != nil {
		return nil, err
	}

	authTime, err := meter.Float64Histogram("relay_peer_authentication_time_milliseconds",
		metric.WithExplicitBucketBoundaries(getStandardBucketBoundaries()...),
		metric.WithDescription("Time taken to authenticate a peer"),
	)
	if err != nil {
		return nil, err
	}

	peerStoreTime, err := meter.Float64Histogram("relay_peer_store_time_milliseconds",
		metric.WithExplicitBucketBoundaries(getStandardBucketBoundaries()...),
		metric.WithDescription("Time taken to store a new peer connection"),
	)
	if err != nil {
		return nil, err
	}

	peerReconnections, err := meter.Int64Counter("relay_peer_reconnections_total",
		metric.WithDescription("Total number of times peers have reconnected and closed old connections"),
	)
	if err != nil {
		return nil, err
	}

	transportDropped, err := meter.Int64Counter("relay_transport_dropped_packets_total",
		metric.WithDescription("Total number of transport packets dropped because a peer send queue was full"),
	)
	if err != nil {
		return nil, err
	}

	m := &Metrics{
		Meter:              meter,
		TransferBytesSent:  bytesSent,
		TransferBytesRecv:  bytesRecv,
		AuthenticationTime: authTime,
		PeerStoreTime:      peerStoreTime,
		peers:              peers,
		peerReconnections:  peerReconnections,
		transportDropped:   transportDropped,

		ctx:              ctx,
		peerActivityChan: make(chan string, 10),
		peerLastActive:   make(map[string]time.Time),
	}

	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			active, idle := m.calculateActiveIdleConnections()
			o.ObserveInt64(peersActive, active)
			o.ObserveInt64(peersIdle, idle)
			return nil
		},
		peersActive, peersIdle,
	)
	if err != nil {
		return nil, err
	}

	go m.readPeerActivity()
	go m.flushCounters()

	// Export the batched data-path counters with an explicit zero so the
	// series exist from boot. Otherwise Prometheus sees "no data" instead of
	// 0 until the first packet is forwarded or dropped, which breaks
	// dashboards, alerts, and the patch-presence self check.
	m.TransferBytesSent.Add(m.ctx, 0)
	m.TransferBytesRecv.Add(m.ctx, 0)
	m.transportDropped.Add(m.ctx, 0)
	return m, nil
}

// PeerConnected increments the number of connected peers and increments number of idle connections
func (m *Metrics) PeerConnected(id, transport string) {
	m.peers.Add(m.ctx, 1, metric.WithAttributes(attribute.String("transport", transport)))
	m.mutexActivity.Lock()
	defer m.mutexActivity.Unlock()

	m.peerLastActive[id] = time.Time{}
}

// RecordAuthenticationTime measures the time taken for peer authentication
func (m *Metrics) RecordAuthenticationTime(duration time.Duration) {
	m.AuthenticationTime.Record(m.ctx, float64(duration.Nanoseconds())/1e6)
}

// RecordPeerStoreTime measures the time to store the peer in map
func (m *Metrics) RecordPeerStoreTime(duration time.Duration) {
	m.PeerStoreTime.Record(m.ctx, float64(duration.Nanoseconds())/1e6)
}

// PeerDisconnected decrements the number of connected peers and decrements number of idle or active connections
func (m *Metrics) PeerDisconnected(id, transport string) {
	m.peers.Add(m.ctx, -1, metric.WithAttributes(attribute.String("transport", transport)))
	m.mutexActivity.Lock()
	defer m.mutexActivity.Unlock()

	delete(m.peerLastActive, id)
}

func (m *Metrics) RecordPeerReconnection() {
	m.peerReconnections.Add(m.ctx, 1)
}

// AddBytesSent accumulates sent bytes atomically. The total is flushed to OTel
// every metricsFlushInterval by a single goroutine.
func (m *Metrics) AddBytesSent(n int64) {
	m.bytesSent.Add(n)
}

// AddBytesRecv accumulates received bytes atomically. The total is flushed to OTel
// every metricsFlushInterval by a single goroutine.
func (m *Metrics) AddBytesRecv(n int64) {
	m.bytesRecv.Add(n)
}

// RecordDroppedTransportPacket accumulates a dropped transport packet
// atomically. The total is flushed to OTel every metricsFlushInterval by a
// single goroutine instead of recording per packet on the data path.
func (m *Metrics) RecordDroppedTransportPacket() {
	m.droppedPackets.Add(1)
}

// flushCounters moves the atomically accumulated data-path deltas into the
// OTel counters once per metricsFlushInterval.
func (m *Metrics) flushCounters() {
	ticker := time.NewTicker(metricsFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.flushCountersOnce()
			return
		case <-ticker.C:
			m.flushCountersOnce()
		}
	}
}

func (m *Metrics) flushCountersOnce() {
	if n := m.bytesSent.Swap(0); n > 0 {
		m.TransferBytesSent.Add(m.ctx, n)
	}
	if n := m.bytesRecv.Swap(0); n > 0 {
		m.TransferBytesRecv.Add(m.ctx, n)
	}
	if n := m.droppedPackets.Swap(0); n > 0 {
		m.transportDropped.Add(m.ctx, n)
	}
}

// PeerActivity increases the active connections
func (m *Metrics) PeerActivity(peerID string) {
	select {
	case m.peerActivityChan <- peerID:
	default:
		log.Tracef("peer activity channel is full, dropping activity metrics for peer %s", peerID)
	}
}

func (m *Metrics) calculateActiveIdleConnections() (int64, int64) {
	active, idle := int64(0), int64(0)
	m.mutexActivity.Lock()
	defer m.mutexActivity.Unlock()

	for _, lastActive := range m.peerLastActive {
		if time.Since(lastActive) > idleTimeout {
			idle++
		} else {
			active++
		}
	}
	return active, idle
}

func (m *Metrics) readPeerActivity() {
	for {
		select {
		case peerID := <-m.peerActivityChan:
			m.mutexActivity.Lock()
			m.peerLastActive[peerID] = time.Now()
			m.mutexActivity.Unlock()
		case <-m.ctx.Done():
			return
		}
	}
}

func getStandardBucketBoundaries() []float64 {
	return []float64{
		0.1,
		0.5,
		1,
		5,
		10,
		50,
		100,
		500,
		1000,
		5000,
		10000,
	}
}
