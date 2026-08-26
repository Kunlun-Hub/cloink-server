package store

import (
	"maps"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/netbirdio/netbird/client/internal/netflow/types"
)

func NewMemoryStore() *Memory {
	return &Memory{
		events: make(map[uuid.UUID]*types.Event),
	}
}

type Memory struct {
	mux    sync.Mutex
	events map[uuid.UUID]*types.Event
}

type IncompleteReason uint8

const (
	IncompleteRetryQueueFull IncompleteReason = iota + 1
	IncompleteOutboxOpen
	IncompleteOutboxWrite
	IncompleteOutboxDelete
	IncompleteOutboxCorrupt
	IncompleteOutboxUnavailable
)

type BoundedMemory struct {
	Memory
	maxEvents  int
	maxBytes   int
	bytes      int
	persist    func(*types.Event) (int, error)
	remove     func(uuid.UUID) error
	sizes      map[uuid.UUID]int
	incomplete [6]atomic.Uint64
}

func NewBoundedMemoryStore(maxEvents, maxBytes int) *BoundedMemory {
	return &BoundedMemory{
		Memory:    Memory{events: make(map[uuid.UUID]*types.Event)},
		maxEvents: maxEvents,
		maxBytes:  maxBytes,
		sizes:     make(map[uuid.UUID]int),
	}
}

func (m *BoundedMemory) MarkIncomplete(reason IncompleteReason) {
	if reason >= IncompleteRetryQueueFull && int(reason) <= len(m.incomplete) {
		m.incomplete[reason-1].Add(1)
	}
}

func (m *BoundedMemory) IncompleteCount(reason IncompleteReason) uint64 {
	if reason < IncompleteRetryQueueFull || int(reason) > len(m.incomplete) {
		return 0
	}
	return m.incomplete[reason-1].Load()
}

func (m *BoundedMemory) TryStoreEvent(event *types.Event) bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	if _, ok := m.events[event.ID]; ok {
		return true
	}

	size := eventSize(event)
	if len(m.events) >= m.maxEvents || m.bytes+size > m.maxBytes {
		m.MarkIncomplete(IncompleteRetryQueueFull)
		return false
	}
	if m.persist != nil {
		var err error
		size, err = m.persist(event)
		if err != nil {
			m.MarkIncomplete(IncompleteOutboxWrite)
			return false
		}
		if m.bytes+size > m.maxBytes {
			m.MarkIncomplete(IncompleteRetryQueueFull)
			if err := m.remove(event.ID); err != nil {
				m.MarkIncomplete(IncompleteOutboxDelete)
			}
			return false
		}
	}
	m.events[event.ID] = event
	m.sizes[event.ID] = size
	m.bytes += size
	return true
}

func (m *BoundedMemory) StoreEvent(event *types.Event) {
	m.TryStoreEvent(event)
}

func (m *BoundedMemory) GetEvents() []*types.Event {
	m.mux.Lock()
	defer m.mux.Unlock()
	events := make([]*types.Event, 0, len(m.events))
	for _, event := range m.events {
		events = append(events, event)
	}
	slices.SortFunc(events, func(a, b *types.Event) int {
		if result := a.Timestamp.Compare(b.Timestamp); result != 0 {
			return result
		}
		return slices.Compare(a.ID[:], b.ID[:])
	})
	return events
}

func (m *BoundedMemory) DeleteEvents(ids []uuid.UUID) {
	m.mux.Lock()
	defer m.mux.Unlock()
	for _, id := range ids {
		if _, ok := m.events[id]; !ok {
			continue
		}
		if m.remove != nil && m.remove(id) != nil {
			m.MarkIncomplete(IncompleteOutboxDelete)
			continue
		}
		m.bytes -= m.sizes[id]
		delete(m.sizes, id)
		delete(m.events, id)
	}
}

func (m *BoundedMemory) Close() {
	// Unacknowledged events intentionally survive flow disable/re-enable. The
	// durable outbox owns terminal cleanup once it replaces this memory store.
}

// ponytail: this is a conservative memory budget, not heap accounting; replace it
// with encoded outbox bytes when the durable outbox is implemented.
func eventSize(event *types.Event) int {
	return 768 + len(event.RuleID) + len(event.SourceResourceID) + len(event.DestResourceID)
}

type AggregatingMemory struct {
	Memory
	WindowStart time.Time
	WindowEnd   time.Time
	nowFunc     func() time.Time
}

func (m *Memory) StoreEvent(event *types.Event) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.events[event.ID] = event
}

func (m *Memory) Close() {
	m.mux.Lock()
	defer m.mux.Unlock()
	clear(m.events)
}

func (m *Memory) GetEvents() []*types.Event {
	m.mux.Lock()
	defer m.mux.Unlock()
	events := make([]*types.Event, 0, len(m.events))
	for _, event := range m.events {
		events = append(events, event)
	}
	return events
}

func (m *Memory) DeleteEvents(ids []uuid.UUID) {
	m.mux.Lock()
	defer m.mux.Unlock()
	for _, id := range ids {
		delete(m.events, id)
	}
}

func NewAggregatingMemoryStore() *AggregatingMemory {
	return NewAggregatingMemoryStoreWithTimeFunc(defaultNowFunc)
}

// used in tests when deterministic (less random) time intervals are required
func NewAggregatingMemoryStoreWithTimeFunc(nowFunc func() time.Time) *AggregatingMemory {
	return &AggregatingMemory{WindowStart: nowFunc(), Memory: Memory{events: make(map[uuid.UUID]*types.Event)}, nowFunc: nowFunc}
}

func (am *AggregatingMemory) ResetAggregationWindow() types.FlowEventAggregator {
	am.mux.Lock()
	defer am.mux.Unlock()

	now := am.nowFunc()
	toret := AggregatingMemory{WindowStart: am.WindowStart, WindowEnd: now, Memory: Memory{events: am.events}}

	am.events = make(map[uuid.UUID]*types.Event)
	am.WindowStart = now

	return &toret
}

type aggregationKey struct {
	srcAddr        netip.Addr
	destAddr       netip.Addr
	destPort       uint16
	direction      int
	protocol       uint8
	icmpType       uint8
	kind           types.Type
	ruleID         string
	sourceResource string
	destResource   string
	unique         uuid.UUID
}

func aggregationKeyFor(event *types.Event) aggregationKey {
	key := aggregationKey{
		srcAddr:        event.SourceIP,
		destAddr:       event.DestIP,
		destPort:       event.DestPort,
		direction:      int(event.Direction),
		protocol:       uint8(event.Protocol),
		icmpType:       event.ICMPType,
		ruleID:         string(event.RuleID),
		sourceResource: string(event.SourceResourceID),
		destResource:   string(event.DestResourceID),
	}
	switch event.Protocol {
	case types.ICMP, types.ICMPv6, types.UDP, types.TCP:
		if event.Type == types.TypeDrop {
			key.kind = types.TypeDrop
		} else {
			key.kind = types.TypeStart
		}
	default:
		key.kind = event.Type
		key.unique = event.ID
	}
	return key
}

type flowAttribution struct {
	ruleID         []byte
	sourceResource []byte
	destResource   []byte
}

func eventWithFlowAttribution(event *types.Event, attributionByFlow map[uuid.UUID]flowAttribution) *types.Event {
	if event.FlowID == uuid.Nil {
		return event
	}
	attribution, ok := attributionByFlow[event.FlowID]
	if !ok {
		return event
	}

	resolved := event.Clone()
	if len(resolved.RuleID) == 0 {
		resolved.RuleID = slices.Clone(attribution.ruleID)
	}
	if len(resolved.SourceResourceID) == 0 {
		resolved.SourceResourceID = slices.Clone(attribution.sourceResource)
	}
	if len(resolved.DestResourceID) == 0 {
		resolved.DestResourceID = slices.Clone(attribution.destResource)
	}
	return resolved
}

func (am *AggregatingMemory) GetAggregatedEvents() []*types.Event {
	am.mux.Lock()
	defer am.mux.Unlock()

	attributionByFlow := make(map[uuid.UUID]flowAttribution)
	for _, event := range am.events {
		if event.FlowID == uuid.Nil {
			continue
		}
		attribution := attributionByFlow[event.FlowID]
		if len(event.RuleID) != 0 {
			attribution.ruleID = slices.Clone(event.RuleID)
		}
		if len(event.SourceResourceID) != 0 {
			attribution.sourceResource = slices.Clone(event.SourceResourceID)
		}
		if len(event.DestResourceID) != 0 {
			attribution.destResource = slices.Clone(event.DestResourceID)
		}
		attributionByFlow[event.FlowID] = attribution
	}

	aggregated := make(map[aggregationKey]*types.Event)
	for _, v := range am.events {
		v = eventWithFlowAttribution(v, attributionByFlow)
		lookupKey := aggregationKeyFor(v)
		if _, ok := aggregated[lookupKey]; !ok {
			event := v.Clone()

			switch event.Type {
			case types.TypeStart:
				event.NumOfStarts += 1
			case types.TypeDrop:
				event.NumOfDrops += 1
			case types.TypeEnd:
				event.NumOfEnds += 1
			}
			event.Type = types.TypeUnknown

			// Please note that ICMPCode field isn't propagated by the manager (see flow/proto/flow.pb.go, FlowFields struct)
			// so the field value in an icmp event in the "aggregated" doesn't matter

			event.WindowStart = am.WindowStart
			event.WindowEnd = am.WindowEnd

			aggregated[lookupKey] = event
			continue
		}

		aggregatedEvent := aggregated[lookupKey]

		aggregatedEvent.RxBytes += v.RxBytes
		aggregatedEvent.RxPackets += v.RxPackets
		aggregatedEvent.TxBytes += v.TxBytes
		aggregatedEvent.TxPackets += v.TxPackets
		switch v.Type {
		case types.TypeStart:
			aggregatedEvent.NumOfStarts += 1
		case types.TypeDrop:
			aggregatedEvent.NumOfDrops += 1
		case types.TypeEnd:
			aggregatedEvent.NumOfEnds += 1
		}
		if aggregatedEvent.Timestamp.Compare(v.Timestamp) > 0 {
			aggregatedEvent.Timestamp = v.Timestamp
			aggregatedEvent.ID = v.ID
			aggregatedEvent.SourcePort = v.SourcePort
		}
		if len(aggregatedEvent.RuleID) == 0 && len(v.RuleID) != 0 {
			aggregatedEvent.RuleID = slices.Clone(v.RuleID)
		}
		if len(aggregatedEvent.SourceResourceID) == 0 && len(v.SourceResourceID) != 0 {
			aggregatedEvent.SourceResourceID = slices.Clone(v.SourceResourceID)
		}
		if len(aggregatedEvent.DestResourceID) == 0 && len(v.DestResourceID) != 0 {
			aggregatedEvent.DestResourceID = slices.Clone(v.DestResourceID)
		}
	}

	return slices.Collect(maps.Values(aggregated)) // could return an iterator instead here
}

func defaultNowFunc() time.Time {
	return time.Now()
}
