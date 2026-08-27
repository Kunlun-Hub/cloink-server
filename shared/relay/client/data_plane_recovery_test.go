package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/relay/server"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
)

func TestRelayDataPlaneFailuresDistinctPeersTriggerRecovery(t *testing.T) {
	now := time.Unix(100, 0)
	failures := newRelayDataPlaneFailures()
	failures.now = func() time.Time { return now }

	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.True(t, failures.reportFailure("relay-a", "peer-b"))
}

func TestRelayDataPlaneFailuresRepeatedPeerTriggersRecovery(t *testing.T) {
	failures := newRelayDataPlaneFailures()

	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.True(t, failures.reportFailure("relay-a", "peer-a"))
}

func TestRelayDataPlaneFailuresSuccessClearsPeer(t *testing.T) {
	failures := newRelayDataPlaneFailures()

	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	failures.reportSuccess("relay-a", "peer-a")
	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.True(t, failures.reportFailure("relay-a", "peer-a"))
}

func TestRelayDataPlaneFailuresCooldownPreventsStorm(t *testing.T) {
	now := time.Unix(100, 0)
	failures := newRelayDataPlaneFailures()
	failures.now = func() time.Time { return now }

	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.True(t, failures.reportFailure("relay-a", "peer-b"))
	require.False(t, failures.reportFailure("relay-a", "peer-c"))
	require.False(t, failures.reportFailure("relay-a", "peer-d"))

	now = now.Add(dataPlaneRecoveryCooldown)
	require.True(t, failures.reportFailure("relay-a", "peer-e"))
}

func TestRelayDataPlaneFailuresAreScopedPerRelay(t *testing.T) {
	failures := newRelayDataPlaneFailures()

	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	require.False(t, failures.reportFailure("relay-b", "peer-b"))
	require.True(t, failures.reportFailure("relay-a", "peer-c"))
}

func TestRelayDataPlaneFailuresExpireOutsideWindow(t *testing.T) {
	now := time.Unix(100, 0)
	failures := newRelayDataPlaneFailures()
	failures.now = func() time.Time { return now }

	require.False(t, failures.reportFailure("relay-a", "peer-a"))
	now = now.Add(dataPlaneFailureWindow + time.Second)
	require.False(t, failures.reportFailure("relay-a", "peer-b"), "stale peer must not contribute to a Relay-wide verdict")
}

func TestManagerForcedRelayDoesNotEnterCooldownOnDataPlaneFailure(t *testing.T) {
	manager := NewManager(context.Background(), []string{"rel://relay-a:80"}, "peer-local", iface.DefaultMTU)
	manager.forcedRelayURL = "rel://relay-a:80"
	manager.markDataPlaneServerFailure("rel://relay-a:80")
	require.Empty(t, manager.serverPicker.cooldowns, "forced Relay must be rebuilt without switching servers")

	manager.forcedRelayURL = ""
	manager.markDataPlaneServerFailure("rel://relay-a:80")
	require.Contains(t, manager.serverPicker.cooldowns, "rel://relay-a:80", "automatic selection should avoid a blackholed Relay")
}

func TestManagerDataPlaneFailuresRebuildHomeRelayClient(t *testing.T) {
	address, _ := freeAddr(t)
	srv, err := server.NewServer(server.Config{
		Meter:          otel.Meter("relay-data-plane-recovery-test"),
		ExposedAddress: address,
		TLSSupport:     false,
		AuthValidator:  &allow.Auth{},
	})
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Listen(server.ListenerConfig{Address: address}) }()
	require.NoError(t, waitForServerToStart(errCh))
	t.Cleanup(func() { require.NoError(t, srv.Shutdown(context.Background())) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	manager := NewManager(ctx, []string{"rel://" + address}, "peer-local", iface.DefaultMTU,
		WithRelayServerCooldown(time.Millisecond))
	require.NoError(t, manager.Serve())

	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	relayAddress, _, err := manager.RelayInstanceAddress()
	require.NoError(t, err)

	manager.ReportDataPlaneFailure(relayAddress, "peer-a")
	manager.ReportDataPlaneFailure(relayAddress, "peer-b")

	require.Eventually(t, func() bool {
		manager.relayClientMu.RLock()
		defer manager.relayClientMu.RUnlock()
		return manager.relayClient != nil && manager.relayClient != oldClient && manager.relayClient.Ready()
	}, 5*time.Second, 20*time.Millisecond, "home Relay client was not rebuilt after data-plane failures")
}
