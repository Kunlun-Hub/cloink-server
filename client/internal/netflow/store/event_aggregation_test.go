package store

import (
	"math/rand"
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/netbirdio/netbird/client/internal/netflow/types"
	"github.com/stretchr/testify/assert"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func TestFlowAggregation(t *testing.T) {
	var protocols = []types.Protocol{types.ICMP, types.ICMPv6, types.TCP, types.UDP}
	var tests = []struct {
		description string
		addresses   [][]netip.Addr
		dstPort     uint16
		eventTypes  []types.Type
	}{
		{
			description: "start and stop",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeStart, types.TypeEnd},
		},
		{
			description: "start only",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeStart},
		},
		{
			description: "drop only",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeDrop},
		}}

	for _, protocol := range protocols {
		for _, tt := range tests {
			t.Run(tt.description+" "+protocol.String(), func(t *testing.T) {
				store := NewAggregatingMemoryStore()
				store.WindowEnd = time.Now().Add(5 * time.Second)

				allExpected := make([]*types.Event, 0)

				for _, srcAndDst := range tt.addresses {
					inEvents, expected := generateEvents(srcAndDst[0], srcAndDst[1], tt.dstPort, tt.eventTypes, protocol, types.Ingress, 0, store.WindowStart, store.WindowEnd)
					for _, e := range inEvents {
						store.StoreEvent(e)
					}
					allExpected = append(allExpected, expected)
				}

				events := store.GetAggregatedEvents()
				assert.ElementsMatch(t, events, allExpected)
			})
		}
	}
}

func TestFlowAggregationAddsReportedCounters(t *testing.T) {
	store := NewAggregatingMemoryStore()
	store.WindowEnd = store.WindowStart.Add(time.Second)
	flowID := uuid.New()
	for _, event := range []*types.Event{
		{
			ID: uuid.New(), EventFields: types.EventFields{
				FlowID: flowID, Type: types.TypeStart, Protocol: types.TCP,
				Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"),
				DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443,
			},
		},
		{
			ID: uuid.New(), EventFields: types.EventFields{
				FlowID: flowID, Type: types.TypeEnd, Protocol: types.TCP,
				Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"),
				DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443,
				RxPackets: 3, TxPackets: 4, RxBytes: 300, TxBytes: 400,
			},
		},
	} {
		store.StoreEvent(event)
	}

	events := store.GetAggregatedEvents()
	assert.Len(t, events, 1)
	assert.Equal(t, uint64(300), events[0].RxBytes)
	assert.Equal(t, uint64(400), events[0].TxBytes)
	assert.Equal(t, uint64(3), events[0].RxPackets)
	assert.Equal(t, uint64(4), events[0].TxPackets)
	assert.Equal(t, uint64(1), events[0].NumOfStarts)
	assert.Equal(t, uint64(1), events[0].NumOfEnds)
	assert.Equal(t, types.TypeUnknown, events[0].Type)
}

func TestFlowAggregationPreservesLifecycleIdentity(t *testing.T) {
	store := NewAggregatingMemoryStore()
	flowIDs := []uuid.UUID{uuid.New(), uuid.New()}
	for i, flowID := range flowIDs {
		for _, eventType := range []types.Type{types.TypeStart, types.TypeEnd} {
			fields := types.EventFields{
				FlowID: flowID, Type: eventType, Protocol: types.TCP,
				Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"),
				DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443,
			}
			if eventType == types.TypeStart {
				fields.RuleID = []byte{byte(i + 1)}
				fields.SourceResourceID = []byte{byte(i + 3)}
				fields.DestResourceID = []byte{byte(i + 5)}
			}
			store.StoreEvent(&types.Event{ID: uuid.New(), EventFields: fields})
		}
	}

	events := store.GetAggregatedEvents()
	assert.Len(t, events, 2)
	for _, event := range events {
		i := 0
		if event.FlowID == flowIDs[1] {
			i = 1
		}
		assert.Equal(t, flowIDs[i], event.FlowID)
		assert.Equal(t, []byte{byte(i + 1)}, event.RuleID)
		assert.Equal(t, []byte{byte(i + 3)}, event.SourceResourceID)
		assert.Equal(t, []byte{byte(i + 5)}, event.DestResourceID)
		assert.Equal(t, uint64(1), event.NumOfStarts)
		assert.Equal(t, uint64(1), event.NumOfEnds)
	}
}

func TestFlowAggregationCombinesMatchingFlowSignatures(t *testing.T) {
	store := NewAggregatingMemoryStore()
	flowIDs := []uuid.UUID{uuid.New(), uuid.New()}
	for _, flowID := range flowIDs {
		for _, eventType := range []types.Type{types.TypeStart, types.TypeEnd} {
			fields := types.EventFields{
				FlowID: flowID, Type: eventType, Protocol: types.TCP,
				Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"),
				DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443,
			}
			if eventType == types.TypeStart {
				fields.RuleID = []byte("rule")
				fields.DestResourceID = []byte("resource")
			}
			store.StoreEvent(&types.Event{ID: uuid.New(), EventFields: fields})
		}
	}

	events := store.GetAggregatedEvents()
	assert.Len(t, events, 1)
	assert.Equal(t, uint64(2), events[0].NumOfStarts)
	assert.Equal(t, uint64(2), events[0].NumOfEnds)
	assert.Equal(t, []byte("rule"), events[0].RuleID)
	assert.Equal(t, []byte("resource"), events[0].DestResourceID)
}

func TestFlowAggregationSeparatesTargetsAndBlockedTraffic(t *testing.T) {
	store := NewAggregatingMemoryStore()
	for _, event := range []*types.Event{
		{ID: uuid.New(), EventFields: types.EventFields{
			FlowID: uuid.New(), Type: types.TypeStart, Protocol: types.TCP, Direction: types.Egress,
			SourceIP: netip.MustParseAddr("100.64.0.1"), DestIP: netip.MustParseAddr("100.64.0.2"), DestPort: 443,
		}},
		{ID: uuid.New(), EventFields: types.EventFields{
			FlowID: uuid.New(), Type: types.TypeStart, Protocol: types.TCP, Direction: types.Egress,
			SourceIP: netip.MustParseAddr("100.64.0.1"), DestIP: netip.MustParseAddr("100.64.0.3"), DestPort: 443,
		}},
		{ID: uuid.New(), EventFields: types.EventFields{
			FlowID: uuid.New(), Type: types.TypeDrop, Protocol: types.TCP, Direction: types.Egress,
			SourceIP: netip.MustParseAddr("100.64.0.1"), DestIP: netip.MustParseAddr("100.64.0.2"), DestPort: 443,
		}},
	} {
		store.StoreEvent(event)
	}

	events := store.GetAggregatedEvents()
	assert.Len(t, events, 3)
	for _, event := range events {
		assert.False(t, event.NumOfStarts > 0 && event.NumOfDrops > 0)
	}
}

func TestDropAggregationPreservesAttributionWithoutPacketCardinality(t *testing.T) {
	store := NewAggregatingMemoryStore()
	for i := 0; i < 100; i++ {
		store.StoreEvent(&types.Event{ID: uuid.New(), EventFields: types.EventFields{
			FlowID: uuid.New(), Type: types.TypeDrop, Protocol: types.TCP,
			RuleID: []byte("rule"), SourceResourceID: []byte("source"), DestResourceID: []byte("dest"),
			Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"), SourcePort: uint16(i),
			DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443, RxPackets: 1,
		}})
	}
	store.StoreEvent(&types.Event{ID: uuid.New(), EventFields: types.EventFields{
		FlowID: uuid.New(), Type: types.TypeDrop, Protocol: types.TCP,
		RuleID: []byte("other-rule"), SourceResourceID: []byte("source"), DestResourceID: []byte("dest"),
		Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"),
		DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443, RxPackets: 1,
	}})
	store.StoreEvent(&types.Event{ID: uuid.New(), EventFields: types.EventFields{
		FlowID: uuid.New(), Type: types.TypeStart, Protocol: types.TCP,
		Direction: types.Ingress, SourceIP: netip.MustParseAddr("1.1.1.1"),
		DestIP: netip.MustParseAddr("2.2.2.2"), DestPort: 443,
	}})

	events := store.GetAggregatedEvents()
	assert.Len(t, events, 3)
	for _, event := range events {
		switch string(event.RuleID) {
		case "rule":
			assert.Equal(t, uint64(100), event.NumOfDrops)
			assert.Equal(t, uint64(100), event.RxPackets)
		case "other-rule":
			assert.Equal(t, uint64(1), event.NumOfDrops)
		default:
			assert.Equal(t, uint64(1), event.NumOfStarts)
			assert.Zero(t, event.NumOfDrops)
		}
	}
}

func TestIcmpEventAggregation(t *testing.T) {
	var protocols = []types.Protocol{types.ICMP, types.ICMPv6}
	var icmpTypes = []uint8{1, 2, 3}

	var tests = []struct {
		description string
		addresses   [][]netip.Addr
		eventTypes  []types.Type
	}{
		{
			description: "start and stop",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}},
			eventTypes:  []types.Type{types.TypeStart, types.TypeEnd},
		},
		{
			description: "start only",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}},
			eventTypes:  []types.Type{types.TypeStart},
		},
		{
			description: "drop only",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}},
			eventTypes:  []types.Type{types.TypeDrop},
		}}

	for _, protocol := range protocols {
		for _, tt := range tests {
			t.Run(tt.description+" "+protocol.String(), func(t *testing.T) {
				store := NewAggregatingMemoryStore()
				store.WindowEnd = time.Now().Add(5 * time.Second)

				allExpected := make([]*types.Event, 0)
				for _, icmpType := range icmpTypes {
					events, expected := generateEvents(tt.addresses[0][0], tt.addresses[0][1], 0, tt.eventTypes, protocol, types.Ingress, icmpType, store.WindowStart, store.WindowEnd)
					for _, e := range events {
						store.StoreEvent(e)
					}
					allExpected = append(allExpected, expected)
				}
				aggregatedEvents := store.GetAggregatedEvents()
				assert.Len(t, aggregatedEvents, len(allExpected))
				assert.ElementsMatch(t, aggregatedEvents, allExpected)
			})
		}
	}
}

func TestFlowAggregationOfUnknownProtocols(t *testing.T) {
	var tests = []struct {
		description string
		addresses   [][]netip.Addr
		dstPort     uint16
		eventTypes  []types.Type
	}{
		{
			description: "start and stop",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeStart, types.TypeEnd},
		},
		{
			description: "start and drop",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeStart, types.TypeDrop},
		},
		{
			description: "start only",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeStart},
		},
		{
			description: "drop only",
			addresses:   [][]netip.Addr{{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2.2.2.2")}, {netip.MustParseAddr("3.3.3.3"), netip.MustParseAddr("2.2.2.2")}},
			dstPort:     uint16(random.Uint32() >> 16),
			eventTypes:  []types.Type{types.TypeDrop},
		}}

	for _, tt := range tests {
		t.Run(tt.description+" "+types.ProtocolUnknown.String(), func(t *testing.T) {
			store := NewAggregatingMemoryStore()
			store.WindowEnd = time.Now().Add(5 * time.Second)

			allExpected := make([]*types.Event, 0)

			for _, srcAndDst := range tt.addresses {
				inEvents, expected := generateEventsForUnknownProtocol(srcAndDst[0], srcAndDst[1], tt.dstPort, tt.eventTypes, types.ProtocolUnknown, types.Ingress, store.WindowStart, store.WindowEnd)
				for _, e := range inEvents {
					store.StoreEvent(e)
				}
				allExpected = append(allExpected, expected...)
			}

			events := store.GetAggregatedEvents()
			assert.ElementsMatch(t, events, allExpected)
		})
	}
}

func TestResetAggregationWindow(t *testing.T) {
	now := time.Now()
	nowFunc := func() time.Time { return now }
	store := NewAggregatingMemoryStoreWithTimeFunc(nowFunc)
	store.StoreEvent(&types.Event{
		ID:        uuid.New(),
		Timestamp: time.Now(),
		EventFields: types.EventFields{
			FlowID:           uuid.New(),
			Type:             types.TypeStart,
			Protocol:         types.TCP,
			RuleID:           []byte("rule-id-1"),
			Direction:        types.Ingress,
			SourceIP:         netip.MustParseAddr("1.1.1.1"),
			SourcePort:       1234,
			DestIP:           netip.MustParseAddr("2.2.2.2"),
			DestPort:         5678,
			SourceResourceID: []byte("source-resource-id"),
			DestResourceID:   []byte("dest-resource-id"),
			RxPackets:        random.Uint64(),
			TxPackets:        random.Uint64(),
			RxBytes:          random.Uint64(),
			TxBytes:          random.Uint64(),
		},
	})

	now = now.Add(1 * time.Second)
	reset := store.ResetAggregationWindow()
	previousEvents, ok := reset.(*AggregatingMemory)
	assert.True(t, ok)
	assert.NotEqual(t, previousEvents.WindowStart, store.WindowStart)
	assert.Equal(t, previousEvents.WindowEnd, store.WindowStart)
	assert.NotEmpty(t, previousEvents.events)
	assert.Empty(t, store.events)
}

func generateEvents(srcIp, dstIp netip.Addr, dstPort uint16, eventTypes []types.Type, protocol types.Protocol,
	direction types.Direction, icmpType uint8, windowStart, windowEnd time.Time) ([]*types.Event, *types.Event) {
	var rxPackets, txPackets, rxBytes, txBytes uint64
	inEvents := make([]*types.Event, 0)
	ts := time.Now()
	flowId := uuid.New()
	srcPort := uint16(random.Uint32() >> 16)

	for idx, eventType := range eventTypes {
		e := &types.Event{
			ID:        uuid.New(),
			Timestamp: ts.Add(time.Duration(idx) * time.Second),
			EventFields: types.EventFields{
				FlowID:           flowId,
				Type:             eventType,
				Protocol:         protocol,
				RuleID:           []byte("rule-id-1"),
				Direction:        direction,
				SourceIP:         srcIp,
				SourcePort:       srcPort,
				DestIP:           dstIp,
				DestPort:         dstPort,
				SourceResourceID: []byte("source-resource-id"),
				DestResourceID:   []byte("dest-resource-id"),
				RxPackets:        random.Uint64(),
				TxPackets:        random.Uint64(),
				RxBytes:          random.Uint64(),
				TxBytes:          random.Uint64(),
			}}
		rxBytes += e.RxBytes
		txBytes += e.TxBytes
		rxPackets += e.RxPackets
		txPackets += e.TxPackets
		inEvents = append(inEvents, e)
		if protocol == types.ICMP || protocol == types.ICMPv6 {
			e.ICMPType = icmpType
		}
	}

	var start, end, drop uint64
	for _, eventType := range eventTypes {
		switch eventType {
		case types.TypeStart:
			start += 1
		case types.TypeDrop:
			drop += 1
		case types.TypeEnd:
			end += 1
		}
	}
	aggregatedEvent := &types.Event{
		ID:          inEvents[0].ID,
		Timestamp:   inEvents[0].Timestamp,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		EventFields: types.EventFields{
			FlowID:           flowId,
			Type:             types.TypeUnknown,
			Protocol:         inEvents[0].Protocol,
			RuleID:           []byte("rule-id-1"),
			Direction:        inEvents[0].Direction,
			SourceIP:         srcIp,
			SourcePort:       srcPort,
			DestIP:           dstIp,
			DestPort:         dstPort,
			SourceResourceID: []byte("source-resource-id"),
			DestResourceID:   []byte("dest-resource-id"),
			RxPackets:        rxPackets,
			TxPackets:        txPackets,
			RxBytes:          rxBytes,
			TxBytes:          txBytes,
			NumOfStarts:      start,
			NumOfEnds:        end,
			NumOfDrops:       drop,
		}}
	if protocol == types.ICMP || protocol == types.ICMPv6 {
		aggregatedEvent.ICMPType = icmpType
	}

	return inEvents, aggregatedEvent
}

func generateEventsForUnknownProtocol(srcIp, dstIp netip.Addr, dstPort uint16, eventTypes []types.Type, protocol types.Protocol,
	direction types.Direction, windowStart, windowEnd time.Time) ([]*types.Event, []*types.Event) {
	inEvents := make([]*types.Event, 0)
	expectedEvents := make([]*types.Event, 0)

	ts := time.Now()
	flowId := uuid.New()
	srcPort := uint16(random.Uint32() >> 16)

	for idx, eventType := range eventTypes {
		e := &types.Event{
			ID:        uuid.New(),
			Timestamp: ts.Add(time.Duration(idx) * time.Second),
			EventFields: types.EventFields{
				FlowID:           flowId,
				Type:             eventType,
				Protocol:         protocol,
				RuleID:           []byte("rule-id-1"),
				Direction:        direction,
				SourceIP:         srcIp,
				SourcePort:       srcPort,
				DestIP:           dstIp,
				DestPort:         dstPort,
				SourceResourceID: []byte("source-resource-id"),
				DestResourceID:   []byte("dest-resource-id"),
				RxPackets:        random.Uint64(),
				TxPackets:        random.Uint64(),
				RxBytes:          random.Uint64(),
				TxBytes:          random.Uint64(),
			}}
		inEvents = append(inEvents, e)

		var start, end, drop uint64
		switch eventType {
		case types.TypeStart:
			start = 1
		case types.TypeDrop:
			drop = 1
		case types.TypeEnd:
			end = 1
		}

		expectedEvents = append(expectedEvents, &types.Event{
			ID:          e.ID,
			Timestamp:   e.Timestamp,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			EventFields: types.EventFields{
				FlowID:           flowId,
				Type:             types.TypeUnknown,
				Protocol:         e.Protocol,
				RuleID:           []byte("rule-id-1"),
				Direction:        e.Direction,
				SourceIP:         srcIp,
				SourcePort:       srcPort,
				DestIP:           dstIp,
				DestPort:         dstPort,
				SourceResourceID: []byte("source-resource-id"),
				DestResourceID:   []byte("dest-resource-id"),
				RxPackets:        e.RxPackets,
				TxPackets:        e.TxPackets,
				RxBytes:          e.RxBytes,
				TxBytes:          e.TxBytes,
				NumOfStarts:      start,
				NumOfEnds:        end,
				NumOfDrops:       drop,
			}})
	}

	return inEvents, expectedEvents
}
