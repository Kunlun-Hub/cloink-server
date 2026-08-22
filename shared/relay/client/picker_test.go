package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestServerPickerCancellationDoesNotCooldown(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		sp := ServerPicker{CooldownDuration: time.Minute}
		sp.markServerFailure("rels://cancelled", time.Now(), err)
		if len(sp.cooldowns) != 0 {
			t.Fatalf("%v entered cooldown: %v", err, sp.cooldowns)
		}
	}
}

func TestServerPickerCooldownBacksOffAndResets(t *testing.T) {
	const relayURL = "rels://failed"
	base := time.Second
	now := time.Now()
	sp := ServerPicker{CooldownDuration: base}

	sp.markServerFailure(relayURL, now, errors.New("handshake failed"))
	first := sp.cooldowns[relayURL].Sub(now)
	sp.markServerFailure(relayURL, now, errors.New("handshake failed"))
	second := sp.cooldowns[relayURL].Sub(now)

	if first < base || first >= base+base/5 {
		t.Fatalf("first cooldown outside jitter range: %v", first)
	}
	if second < 2*base || second >= 2*base+2*base/5 {
		t.Fatalf("second cooldown outside backoff range: %v", second)
	}
	sp.clearServerFailure(relayURL)
	if sp.failures[relayURL] != 0 {
		t.Fatalf("failure count was not reset: %v", sp.failures)
	}
}

func TestServerPickerFailureEntersCooldown(t *testing.T) {
	sp := ServerPicker{CooldownDuration: time.Minute}
	sp.markServerFailure("rels://failed", time.Now(), errors.New("handshake failed"))
	if got := sp.availableServerURLs([]string{"rels://failed", "rels://healthy"}, time.Now()); len(got) != 1 || got[0] != "rels://healthy" {
		t.Fatalf("failed connection remained available: %v", got)
	}
}

func TestServerPicker_UnavailableServers(t *testing.T) {
	timeout := 5 * time.Second
	sp := ServerPicker{
		TokenStore:        nil,
		PeerID:            "test",
		ConnectionTimeout: timeout,
	}
	sp.ServerURLs.Store([]string{"rel://dummy1", "rel://dummy2"})

	ctx, cancel := context.WithTimeout(context.Background(), timeout+1)
	defer cancel()

	go func() {
		_, err := sp.PickServer(ctx)
		if err == nil {
			t.Error(err)
		}
		cancel()
	}()

	<-ctx.Done()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("PickServer() took too long to complete")
	}
}
