package logger

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/client/internal/netflow/store"
	"github.com/netbirdio/netbird/client/internal/netflow/types"
	"github.com/netbirdio/netbird/client/internal/peer"
	"github.com/netbirdio/netbird/dns"
	"github.com/netbirdio/netbird/shared/netiputil"
)

const (
	captureQueueSize = 100
	shutdownTimeout  = time.Second
)

type admissionStats struct {
	accepted        atomic.Uint64
	filtered        atomic.Uint64
	disabled        atomic.Uint64
	full            atomic.Uint64
	closing         atomic.Uint64
	shutdownTimeout atomic.Uint64
}

type Stats struct {
	Accepted         uint64
	Filtered         uint64
	Disabled         uint64
	CaptureQueueFull uint64
	Closing          uint64
	ShutdownTimeout  uint64
}

type Logger struct {
	mux                sync.Mutex
	enabled            atomic.Bool
	rcvChan            chan *types.EventFields
	stopChan           chan struct{}
	receiverDone       chan struct{}
	statusRecorder     *peer.Status
	wgIfaceNet         netip.Prefix
	wgIfaceNetV6       netip.Prefix
	dnsCollection      atomic.Bool
	exitNodeCollection atomic.Bool
	stats              admissionStats
	Store              types.AggregatingStore
}

func New(statusRecorder *peer.Status, wgIfaceIPNet, wgIfaceIPNetV6 netip.Prefix) *Logger {
	return &Logger{
		statusRecorder: statusRecorder,
		wgIfaceNet:     wgIfaceIPNet,
		wgIfaceNetV6:   wgIfaceIPNetV6,
		Store:          store.NewAggregatingMemoryStore(),
	}
}

func (l *Logger) StoreEvent(flowEvent types.EventFields) {
	if !l.enabled.Load() {
		l.stats.disabled.Add(1)
		return
	}
	if !isDNSFlow(&flowEvent) &&
		(netiputil.IsSystemLocalAddress(flowEvent.SourceIP) || netiputil.IsSystemLocalAddress(flowEvent.DestIP)) {
		l.stats.filtered.Add(1)
		return
	}

	l.mux.Lock()
	defer l.mux.Unlock()
	if !l.enabled.Load() {
		l.stats.closing.Add(1)
		return
	}

	select {
	case l.rcvChan <- &flowEvent:
		l.stats.accepted.Add(1)
	default:
		l.stats.full.Add(1)
	}
}

func (l *Logger) Enable() {
	l.mux.Lock()
	defer l.mux.Unlock()
	if l.enabled.Load() {
		return
	}

	l.rcvChan = make(chan *types.EventFields, captureQueueSize)
	l.stopChan = make(chan struct{})
	l.receiverDone = make(chan struct{})
	l.enabled.Store(true)
	go l.startReceiver(l.rcvChan, l.stopChan, l.receiverDone)
}

func (l *Logger) startReceiver(c <-chan *types.EventFields, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case eventFields := <-c:
			l.storeEvent(eventFields)
		case <-stop:
			for {
				select {
				case eventFields := <-c:
					l.storeEvent(eventFields)
				default:
					log.Info("flow Memory store receiver stopped")
					return
				}
			}
		}
	}
}

func (l *Logger) storeEvent(eventFields *types.EventFields) {
	id := uuid.New()
	event := types.Event{
		ID:          id,
		EventFields: *eventFields,
		Timestamp:   time.Now().UTC(),
	}

	var isSrcExitNode bool
	var isDestExitNode bool

	if !l.isOverlayIP(event.SourceIP) {
		event.SourceResourceID, isSrcExitNode = l.statusRecorder.CheckRoutes(event.SourceIP)
	}

	if !l.isOverlayIP(event.DestIP) {
		event.DestResourceID, isDestExitNode = l.statusRecorder.CheckRoutes(event.DestIP)
	}

	if l.shouldStore(eventFields, isSrcExitNode || isDestExitNode) {
		l.Store.StoreEvent(&event)
	}
}

func (l *Logger) Close() {
	l.mux.Lock()
	if !l.enabled.Load() {
		l.Store.Close()
		l.mux.Unlock()
		return
	}

	l.enabled.Store(false)
	close(l.stopChan)
	done := l.receiverDone
	l.mux.Unlock()

	select {
	case <-done:
		l.Store.Close()
	case <-time.After(shutdownTimeout):
		l.stats.shutdownTimeout.Add(1)
		log.Warn("timed out stopping flow Memory store receiver")
	}
}

func (l *Logger) Stats() Stats {
	return Stats{
		Accepted:         l.stats.accepted.Load(),
		Filtered:         l.stats.filtered.Load(),
		Disabled:         l.stats.disabled.Load(),
		CaptureQueueFull: l.stats.full.Load(),
		Closing:          l.stats.closing.Load(),
		ShutdownTimeout:  l.stats.shutdownTimeout.Load(),
	}
}

func (l *Logger) ResetAggregationWindow() types.FlowEventAggregator {
	return l.Store.ResetAggregationWindow()
}

func (l *Logger) GetEvents() []*types.Event {
	return l.Store.GetEvents()
}

func (l *Logger) DeleteEvents(ids []uuid.UUID) {
	l.Store.DeleteEvents(ids)
}

func (l *Logger) UpdateConfig(dnsCollection, exitNodeCollection bool) {
	l.dnsCollection.Store(dnsCollection)
	l.exitNodeCollection.Store(exitNodeCollection)
}

func (l *Logger) isOverlayIP(ip netip.Addr) bool {
	return l.wgIfaceNet.Contains(ip) || (l.wgIfaceNetV6.IsValid() && l.wgIfaceNetV6.Contains(ip))
}

func (l *Logger) shouldStore(event *types.EventFields, isExitNode bool) bool {
	if isDNSFlow(event) {
		return l.dnsCollection.Load()
	}
	if isExitNode {
		return l.exitNodeCollection.Load()
	}
	isP2P := l.isOverlayIP(event.SourceIP) && l.isOverlayIP(event.DestIP)
	hasPublishedResource := len(event.SourceResourceID) > 0 || len(event.DestResourceID) > 0
	return isP2P || hasPublishedResource
}

func isDNSFlow(event *types.EventFields) bool {
	return event.Protocol == types.UDP &&
		(event.DestPort == 53 || event.DestPort == dns.ForwarderClientPort || event.DestPort == dns.ForwarderServerPort)
}
