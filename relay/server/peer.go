package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/relay/metrics"
	"github.com/netbirdio/netbird/relay/server/listener"
	"github.com/netbirdio/netbird/relay/server/store"
	"github.com/netbirdio/netbird/shared/relay/healthcheck"
	"github.com/netbirdio/netbird/shared/relay/messages"
)

const (
	bufferSize = messages.MaxMessageSize

	// defaultSendQueueSize bounds the per-peer async transport send queue when
	// no explicit size is configured. It absorbs bursts toward a slow peer
	// without stalling the sender's read loop.
	defaultSendQueueSize = 1024

	// activitySampleInterval caps how often a peer reports activity metrics.
	activitySampleInterval = time.Second

	errCloseConn = "failed to close connection to peer: %s"
)

// Peer represents a peer connection
type Peer struct {
	metrics  *metrics.Metrics
	log      *log.Entry
	id       messages.PeerID
	conn     listener.Conn
	connMu   sync.RWMutex
	store    *store.Store
	notifier *store.PeerNotifier

	ctx       context.Context
	ctxCancel context.CancelFunc

	// sendQueue carries outbound transport messages to the writer goroutine.
	// Only MsgTypeTransport packets go through the queue; control messages use
	// the synchronous Write path because they are low-frequency and must not
	// be dropped. A full queue drops packets instead of blocking the read loop.
	sendQueue chan []byte
	writerWg  sync.WaitGroup

	// droppedPackets counts transport messages discarded because sendQueue was full.
	droppedPackets atomic.Uint64

	// lastActivityUnixNano throttles PeerActivity reports to activitySampleInterval per peer.
	lastActivityUnixNano atomic.Int64

	peersListener *store.Listener

	// between the online peer collection step and the notification sending should not be sent offline notifications from another thread
	notificationMutex sync.Mutex
}

// NewPeer creates a new Peer instance and prepare custom logging.
// sendQueueSize bounds the async transport send queue; values <= 0 select defaultSendQueueSize.
func NewPeer(metrics *metrics.Metrics, id messages.PeerID, conn listener.Conn, store *store.Store, notifier *store.PeerNotifier, sendQueueSize int) *Peer {
	if sendQueueSize <= 0 {
		sendQueueSize = defaultSendQueueSize
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		metrics:   metrics,
		log:       log.WithField("peer_id", id.String()),
		id:        id,
		conn:      conn,
		store:     store,
		notifier:  notifier,
		ctx:       ctx,
		ctxCancel: cancel,
		sendQueue: make(chan []byte, sendQueueSize),
	}

	return p
}

// Work reads data from the connection
// It manages the protocol (healthcheck, transport, close). Read the message and determine the message type and handle
// the message accordingly.
func (p *Peer) Work() {
	p.peersListener = p.notifier.NewListener(p.sendPeersOnline, p.sendPeersWentOffline)

	// Drain outbound transport messages without blocking this read loop.
	p.writerWg.Add(1)
	go p.writeLoop()

	defer func() {
		p.ctxCancel()
		p.notifier.RemoveListener(p.peersListener)

		if err := p.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			p.log.Errorf(errCloseConn, err)
		}
		// The close above unblocks the writer's in-flight write; wait for it
		// to exit. Anything still queued is discarded.
		p.writerWg.Wait()
	}()

	ctx := p.ctx

	hc := healthcheck.NewSender(p.log)
	go hc.StartHealthCheck(ctx)
	go p.handleHealthcheckEvents(ctx, hc)

	buf := make([]byte, bufferSize)
	for {
		n, err := p.conn.Read(ctx, buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				p.log.Errorf("failed to read message: %s", err)
			}
			return
		}

		if n == 0 {
			p.log.Errorf("received empty message")
			return
		}

		msg := buf[:n]

		_, err = messages.ValidateVersion(msg)
		if err != nil {
			p.log.Warnf("failed to validate protocol version: %s", err)
			return
		}

		msgType, err := messages.DetermineClientMessageType(msg)
		if err != nil {
			p.log.Errorf("failed to determine message type: %s", err)
			return
		}

		p.handleMsgType(ctx, msgType, hc, n, msg)
	}
}

func (p *Peer) ID() messages.PeerID {
	return p.id
}

func (p *Peer) handleMsgType(ctx context.Context, msgType messages.MsgType, hc *healthcheck.Sender, n int, msg []byte) {
	switch msgType {
	case messages.MsgTypeHealthCheck:
		hc.OnHCResponse()
	case messages.MsgTypeTransport:
		p.metrics.AddBytesRecv(int64(n))
		p.recordActivity()
		p.handleTransportMsg(msg)
	case messages.MsgTypeClose:
		p.log.Infof("peer exited gracefully")
		if err := p.conn.Close(); err != nil {
			log.Errorf(errCloseConn, err)
		}
	case messages.MsgTypeSubscribePeerState:
		p.handleSubscribePeerState(msg)
	case messages.MsgTypeUnsubscribePeerState:
		p.handleUnsubscribePeerState(msg)
	default:
		p.log.Warnf("received unexpected message type: %s", msgType)
	}
}

// Write writes data to the connection
func (p *Peer) Write(ctx context.Context, b []byte) (int, error) {
	p.connMu.RLock()
	defer p.connMu.RUnlock()
	return p.conn.Write(ctx, b)
}

// EnqueueTransport queues a transport message for asynchronous delivery by the
// peer's writer goroutine. The bytes are copied because the caller reuses its
// read buffer. It never blocks: when the queue is full the message is dropped
// and counted instead of stalling the sender's read loop.
func (p *Peer) EnqueueTransport(msg []byte) {
	buf := make([]byte, len(msg))
	copy(buf, msg)

	select {
	case p.sendQueue <- buf:
	default:
		p.droppedPackets.Add(1)
		p.metrics.RecordDroppedTransportPacket()
		p.log.Debugf("send queue full, dropping transport packet")
	}
}

// writeLoop drains sendQueue with synchronous writes until the peer is closed
// or a write fails. It is the only goroutine writing transport messages, so a
// slow peer backs up its own queue instead of stalling other peers' read loops.
func (p *Peer) writeLoop() {
	defer p.writerWg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg := <-p.sendQueue:
			n, err := p.Write(p.ctx, msg)
			if err != nil {
				p.log.Errorf("failed to write transport message: %s", err)
				// Break the connection so the read loop tears the peer down
				// instead of leaving queued messages undeliverable.
				_ = p.conn.Close()
				return
			}
			p.metrics.AddBytesSent(int64(n))
		}
	}
}

// recordActivity reports peer activity at most once per activitySampleInterval.
// Low-rate peers are unaffected: a packet arriving after a quiet interval is always reported.
func (p *Peer) recordActivity() {
	now := time.Now().UnixNano()
	last := p.lastActivityUnixNano.Load()
	if now-last < int64(activitySampleInterval) {
		return
	}
	if p.lastActivityUnixNano.CompareAndSwap(last, now) {
		p.metrics.PeerActivity(p.String())
	}
}

// DroppedPackets returns the number of transport messages dropped because the send queue was full.
func (p *Peer) DroppedPackets() uint64 {
	return p.droppedPackets.Load()
}

// SendQueueLen returns the current number of queued outbound transport messages.
func (p *Peer) SendQueueLen() int {
	return len(p.sendQueue)
}

// SendQueueCap returns the capacity of the outbound transport send queue.
func (p *Peer) SendQueueCap() int {
	return cap(p.sendQueue)
}

// CloseGracefully closes the connection with the peer gracefully. Send a close message to the client and close the
// connection.
func (p *Peer) CloseGracefully(ctx context.Context) {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	err := p.writeWithTimeout(ctx, messages.MarshalCloseMsg())
	if err != nil {
		p.log.Errorf("failed to send close message to peer: %s", p.String())
	}

	p.ctxCancel()
	if err := p.conn.Close(); err != nil {
		p.log.Errorf(errCloseConn, err)
	}
}

func (p *Peer) Close() {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	p.ctxCancel()
	if err := p.conn.Close(); err != nil {
		p.log.Errorf(errCloseConn, err)
	}
}

// String returns the peer ID
func (p *Peer) String() string {
	return p.id.String()
}

func (p *Peer) writeWithTimeout(ctx context.Context, buf []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := p.conn.Write(ctx, buf)
	return err
}

func (p *Peer) handleHealthcheckEvents(ctx context.Context, hc *healthcheck.Sender) {
	for {
		select {
		case <-hc.HealthCheck:
			_, err := p.Write(ctx, messages.MarshalHealthcheck())
			if err != nil {
				p.log.Errorf("failed to send healthcheck message: %s", err)
				return
			}
		case <-hc.Timeout:
			p.log.Errorf("peer healthcheck timeout")
			err := p.conn.Close()
			if err != nil {
				p.log.Errorf("failed to close connection to peer: %s", err)
			}
			p.log.Info("peer connection closed due healthcheck timeout")
			return
		case <-ctx.Done():
			return
		}
	}
}

func (p *Peer) handleTransportMsg(msg []byte) {
	peerID, err := messages.UnmarshalTransportID(msg)
	if err != nil {
		p.log.Errorf("failed to unmarshal transport message: %s", err)
		return
	}

	item, ok := p.store.Peer(*peerID)
	if !ok {
		p.log.Debugf("peer not found: %s", peerID)
		return
	}
	dp := item.(*Peer)

	err = messages.UpdateTransportMsg(msg, p.id)
	if err != nil {
		p.log.Errorf("failed to update transport message: %s", err)
		return
	}

	// Hand the packet to the destination peer's send queue. This never blocks
	// the read loop: a full queue drops the packet and counts it.
	dp.EnqueueTransport(msg)
}

func (p *Peer) handleSubscribePeerState(msg []byte) {
	peerIDs, err := messages.UnmarshalSubPeerStateMsg(msg)
	if err != nil {
		p.log.Errorf("failed to unmarshal open connection message: %s", err)
		return
	}

	p.log.Debugf("received subscription message for %d peers", len(peerIDs))

	// collect online peers to response back to the caller
	p.notificationMutex.Lock()
	defer p.notificationMutex.Unlock()

	onlinePeers := p.store.GetOnlinePeersAndRegisterInterest(peerIDs, p.peersListener)
	if len(onlinePeers) == 0 {
		return
	}

	p.log.Debugf("response with %d online peers", len(onlinePeers))
	p.sendPeersOnline(onlinePeers)
}

func (p *Peer) handleUnsubscribePeerState(msg []byte) {
	peerIDs, err := messages.UnmarshalUnsubPeerStateMsg(msg)
	if err != nil {
		p.log.Errorf("failed to unmarshal open connection message: %s", err)
		return
	}

	p.peersListener.RemoveInterestedPeer(peerIDs)
}

func (p *Peer) sendPeersOnline(peers []messages.PeerID) {
	msgs, err := messages.MarshalPeersOnline(peers)
	if err != nil {
		p.log.Errorf("failed to marshal peer location message: %s", err)
		return
	}

	for n, msg := range msgs {
		if _, err := p.Write(p.ctx, msg); err != nil {
			p.log.Errorf("failed to write %d. peers offline message: %s", n, err)
		}
	}
}

func (p *Peer) sendPeersWentOffline(peers []messages.PeerID) {
	p.notificationMutex.Lock()
	defer p.notificationMutex.Unlock()

	msgs, err := messages.MarshalPeersWentOffline(peers)
	if err != nil {
		p.log.Errorf("failed to marshal peer location message: %s", err)
		return
	}

	for n, msg := range msgs {
		if _, err := p.Write(p.ctx, msg); err != nil {
			p.log.Errorf("failed to write %d. peers offline message: %s", n, err)
		}
	}
}
