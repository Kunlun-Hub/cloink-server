package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/shared/relay/auth/hmac"
)

func TestServerPickerCancellationDoesNotCooldown(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		fmt.Errorf("dial aborted: %w", context.Canceled),
	} {
		sp := ServerPicker{CooldownDuration: time.Minute}
		sp.markServerFailure("rels://cancelled", time.Now(), err)
		if len(sp.cooldowns) != 0 {
			t.Fatalf("%v entered cooldown: %v", err, sp.cooldowns)
		}
	}
}

// A relay that is too slow to answer within the connection timeout is as
// useless as a down one, so a (possibly wrapped) context.DeadlineExceeded
// must put it into cooldown and keep it out of the next selection round.
func TestServerPickerDeadlineExceededEntersCooldown(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("handshake timeout: %w", context.DeadlineExceeded),
	} {
		sp := ServerPicker{CooldownDuration: time.Minute}
		now := time.Now()
		sp.markServerFailure("rels://slow", now, err)
		until, ok := sp.cooldowns["rels://slow"]
		if !ok {
			t.Fatalf("%v did not enter cooldown: %v", err, sp.cooldowns)
		}
		if !until.After(now) {
			t.Fatalf("%v cooldown did not extend into the future: %v", err, until)
		}
		got := sp.availableServerURLs(pickerConfig{}, []string{"rels://slow", "rels://healthy"}, now)
		if len(got) != 1 || got[0] != "rels://healthy" {
			t.Fatalf("%v kept the timed-out relay available: %v", err, got)
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
	got := sp.availableServerURLs(pickerConfig{}, []string{"rels://failed", "rels://healthy"}, time.Now())
	if len(got) != 1 || got[0] != "rels://healthy" {
		t.Fatalf("failed connection remained available: %v", got)
	}
}

func TestServerPickerAllCooledDownOrdersByExpiry(t *testing.T) {
	sp := ServerPicker{CooldownDuration: time.Minute}
	now := time.Now()
	// second fails first, so it recovers first and must be retried first
	sp.markServerFailure("rels://second", now.Add(-30*time.Second), errors.New("handshake failed"))
	sp.markServerFailure("rels://first", now, errors.New("handshake failed"))

	got := sp.availableServerURLs(pickerConfig{}, []string{"rels://first", "rels://second"}, now)
	if len(got) != 2 {
		t.Fatalf("expected full fallback list, got: %v", got)
	}
	if got[0] != "rels://second" {
		t.Fatalf("expected earliest-expiring relay first, got: %v", got)
	}
}

func TestServerPickerAllCooledDownKeepsWeightGroups(t *testing.T) {
	sp := ServerPicker{CooldownDuration: time.Minute}
	now := time.Now()
	config := pickerConfig{serverWeights: map[string]int{
		"rels://heavy": 100,
		"rels://light": 10,
	}}
	// the light relay recovers first, but must not jump the weight group
	sp.markServerFailure("rels://light", now.Add(-45*time.Second), errors.New("handshake failed"))
	sp.markServerFailure("rels://heavy", now, errors.New("handshake failed"))

	got := sp.availableServerURLs(config, []string{"rels://heavy", "rels://light"}, now)
	if len(got) != 2 || got[0] != "rels://heavy" {
		t.Fatalf("weight grouping must stay authoritative, got: %v", got)
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

// A blackholed higher-weight Relay must not consume the entire selection
// timeout. The lower-weight Relay is started after the grace period and can
// win while the first attempt remains in flight.
func TestServerPickerHedgedFallbackFromBlackholedHigherWeightRelay(t *testing.T) {
	blackholedURL := startBlackholeListener(t)
	healthyURL := startMigrationRelayServer(t, "picker-hedge")

	picker := &ServerPicker{
		TokenStore:        &hmac.TokenStore{},
		PeerID:            "picker-hedge-peer",
		ConnectionTimeout: 5 * time.Second,
		CooldownDuration:  time.Minute,
	}
	picker.storeConfig(pickerConfig{
		serverURLs: []string{blackholedURL, healthyURL},
		serverWeights: map[string]int{
			blackholedURL: 100,
			healthyURL:    10,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	started := time.Now()
	relayClient, err := picker.PickServer(ctx)
	require.NoError(t, err)
	defer func() { _ = relayClient.Close() }()

	require.Equal(t, healthyURL, relayClient.connectionURL)
	require.Less(t, time.Since(started), 4*time.Second,
		"fallback Relay should be selected after the grace period, not the full timeout")
}

// A manual override must get an immediate attempt even when its previous
// failure is still in cooldown.
func TestServerPickerForcedRelayBypassesCooldown(t *testing.T) {
	const (
		forcedURL   = "rels://forced.example:443"
		fallbackURL = "rels://fallback.example:443"
	)
	picker := &ServerPicker{CooldownDuration: time.Minute}
	picker.markServerFailure(forcedURL, time.Now(), errors.New("previous attempt failed"))

	config := pickerConfig{
		serverURLs:    []string{forcedURL, fallbackURL},
		serverWeights: map[string]int{forcedURL: 100, fallbackURL: 10},
		forcedURL:     forcedURL,
	}
	got := picker.availableServerURLs(config, config.serverURLs, time.Now())

	require.Equal(t, []string{forcedURL, fallbackURL}, got,
		"forced Relay must remain first and bypass cooldown")

	// Even when every fallback is still cooling down, a failed forced attempt
	// must be able to hedge to the rest of the list.
	picker.markServerFailure(fallbackURL, time.Now(), errors.New("fallback failed recently"))
	got = picker.availableServerURLs(config, config.serverURLs, time.Now())
	require.Equal(t, []string{forcedURL, fallbackURL}, got,
		"forced Relay must stay first while cooled-down fallbacks remain available after it")
}

func startBlackholeListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(io.Discard, conn)
			}(conn)
		}
	}()

	return "rel://" + listener.Addr().String()
}
