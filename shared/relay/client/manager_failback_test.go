package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
)

// Under the 1-Primary + 1-Standby architecture, while the Primary Relay is
// healthy, it must never migrate to another relay during background probes or
// server list refreshes that still contain the Primary Relay.
func TestManagerDoesNotMigrateHealthyPrimaryOnProbeOrRefresh(t *testing.T) {
	primaryURL := startMigrationRelayServer(t, "primary")
	standbyURL := startMigrationRelayServer(t, "standby")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{primaryURL}, "peer-local", iface.DefaultMTU,
		WithAutoFailback(true),
		WithRelayMigrationGrace(300*time.Millisecond))
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, oldClient)
	require.True(t, oldClient.Ready())

	// Adding a second server via UpdateServerURLsWithWeights must keep the
	// healthy primary untouched and select the new server as standby.
	manager.UpdateServerURLsWithWeights([]string{standbyURL, primaryURL}, map[string]int{standbyURL: 100, primaryURL: 10})
	manager.probeTick()

	require.Equal(t, standbyURL, manager.StandbyURL(), "new healthy relay should become standby")
	manager.relayClientMu.RLock()
	defer manager.relayClientMu.RUnlock()
	assert.Same(t, oldClient, manager.relayClient, "healthy primary relay must not move")
	assert.Equal(t, primaryURL, manager.relayClient.connectionURL)
}
