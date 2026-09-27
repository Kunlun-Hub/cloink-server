package logger_test

import (
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/netbirdio/netbird/client/internal/netflow/logger"
	"github.com/netbirdio/netbird/client/internal/netflow/types"
	"github.com/netbirdio/netbird/client/internal/peer"
)

func TestStoreKeepsPublishedResourceTraffic(t *testing.T) {
	recorder := peer.NewRecorder("")
	recorder.AddLocalPeerStateRoute("10.202.16.131/32", "resource-1")

	l := logger.New(recorder, netip.MustParsePrefix("100.64.0.0/10"), netip.Prefix{})
	l.Enable()
	t.Cleanup(l.Close)

	flowID := uuid.New()
	l.StoreEvent(types.EventFields{
		FlowID:    flowID,
		Protocol:  types.TCP,
		SourceIP:  netip.MustParseAddr("100.80.0.1"),
		DestIP:    netip.MustParseAddr("10.202.16.131"),
		DestPort:  80,
		Direction: types.Egress,
	})

	var events []*types.Event
	for range 100 {
		if events = l.GetEvents(); len(events) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(events) != 1 || events[0].FlowID != flowID {
		t.Fatalf("expected published resource flow to be retained, got %+v", events)
	}
	if got := string(events[0].DestResourceID); got != "resource-1" {
		t.Fatalf("expected resolved dest resource ID, got %q", got)
	}
}

func TestStore(t *testing.T) {
	logger := logger.New(nil, netip.MustParsePrefix("100.64.0.0/10"), netip.Prefix{})
	logger.Enable()

	event := types.EventFields{
		FlowID:    uuid.New(),
		Type:      types.TypeStart,
		Direction: types.Ingress,
		Protocol:  6,
		SourceIP:  netip.MustParseAddr("100.64.0.1"),
		DestIP:    netip.MustParseAddr("100.64.0.2"),
	}

	wait := func() { time.Sleep(time.Millisecond) }
	wait()
	logger.StoreEvent(event)
	wait()

	allEvents := logger.GetEvents()
	matched := false
	for _, e := range allEvents {
		if e.EventFields.FlowID == event.FlowID {
			matched = true
		}
	}
	if !matched {
		t.Errorf("didn't match any event")
	}

	// test disable
	logger.Close()
	wait()
	logger.StoreEvent(event)
	wait()
	allEvents = logger.GetEvents()
	if len(allEvents) != 0 {
		t.Errorf("expected 0 events, got %d", len(allEvents))
	}

	// test re-enable
	logger.Enable()
	wait()
	logger.StoreEvent(event)
	wait()

	allEvents = logger.GetEvents()
	matched = false
	for _, e := range allEvents {
		if e.EventFields.FlowID == event.FlowID {
			matched = true
		}
	}
	if !matched {
		t.Errorf("didn't match any event")
	}
}

func TestStoreKeepsOnlyRequestedTrafficClasses(t *testing.T) {
	l := logger.New(nil, netip.MustParsePrefix("100.64.0.0/10"), netip.Prefix{})
	l.Enable()
	t.Cleanup(l.Close)

	l.StoreEvent(types.EventFields{
		FlowID:    uuid.New(),
		Protocol:  types.TCP,
		SourceIP:  netip.MustParseAddr("100.80.0.1"),
		DestIP:    netip.MustParseAddr("8.8.8.8"),
		DestPort:  443,
		Direction: types.Egress,
	})
	time.Sleep(time.Millisecond)
	if events := l.GetEvents(); len(events) != 0 {
		t.Fatalf("expected direct internet flow to be filtered, got %d events", len(events))
	}

	l.UpdateConfig(true, false)
	dnsFlowID := uuid.New()
	l.StoreEvent(types.EventFields{
		FlowID:    dnsFlowID,
		Protocol:  types.UDP,
		SourceIP:  netip.MustParseAddr("127.0.0.1"),
		DestIP:    netip.MustParseAddr("127.0.0.1"),
		DestPort:  53,
		Direction: types.Egress,
	})
	time.Sleep(time.Millisecond)
	events := l.GetEvents()
	if len(events) != 1 || events[0].FlowID != dnsFlowID {
		t.Fatalf("expected enabled DNS flow to be retained, got %+v", events)
	}
}
