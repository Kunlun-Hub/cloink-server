package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
)

// The failback ticker only moves the home Relay upward to a strictly
// higher-weight server, and only after that server has been observed healthy
// (outside its failure cooldown) for relayFailbackStableChecks consecutive
// ticks.
func TestManagerAutoFailbackRequiresStableTarget(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "low")
	highURL := startMigrationRelayServer(t, "high")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "peer-local", iface.DefaultMTU,
		WithAutoFailback(true),
		WithRelayMigrationGrace(300*time.Millisecond))
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, oldClient)
	require.True(t, oldClient.Ready())

	// Simulate a recovered higher-priority server that the config already
	// knows about, without triggering the config-change migration path: the
	// failback ticker is what should move the home Relay.
	manager.relayConfigMu.Lock()
	manager.relayWeights[highURL] = 80
	manager.relayWeights[lowURL] = 20
	manager.configuredRelayURLs = []string{highURL, lowURL}
	manager.relayConfigMu.Unlock()
	manager.serverPicker.storeConfig(pickerConfig{
		serverURLs:    []string{highURL, lowURL},
		serverWeights: map[string]int{highURL: 80, lowURL: 20},
	})

	// While the target sits in failure cooldown, ticks must neither
	// accumulate stability nor migrate.
	manager.serverPicker.markServerFailure(highURL, time.Now(), assert.AnError)
	streaks := make(map[string]int)
	manager.failbackTick(streaks)
	manager.failbackTick(streaks)
	require.Empty(t, streaks)
	manager.relayClientMu.RLock()
	require.Same(t, oldClient, manager.relayClient)
	manager.relayClientMu.RUnlock()

	// Once the cooldown clears, a single healthy tick is not enough.
	manager.serverPicker.clearServerFailure(highURL)
	manager.failbackTick(streaks)
	require.Equal(t, 1, streaks[highURL])
	manager.relayClientMu.RLock()
	require.Same(t, oldClient, manager.relayClient, "a single healthy tick must not trigger failback")
	manager.relayClientMu.RUnlock()

	// The second consecutive healthy tick triggers the failback.
	manager.failbackTick(streaks)
	require.Empty(t, streaks, "streaks reset after triggering failback")
	require.Eventually(t, func() bool {
		manager.relayClientMu.RLock()
		defer manager.relayClientMu.RUnlock()
		return manager.relayClient != nil && manager.relayClient != oldClient &&
			manager.relayClient.connectionURL == highURL && manager.relayClient.Ready()
	}, 5*time.Second, 20*time.Millisecond, "home Relay should fail back to the stable higher-priority server")
}

// With auto-failback disabled the manager never evaluates a failback, even
// when a higher-priority server is healthy.
func TestManagerAutoFailbackDisabledByDefault(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "low")
	highURL := startMigrationRelayServer(t, "high")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "peer-local", iface.DefaultMTU,
		WithRelayMigrationGrace(300*time.Millisecond))
	require.NoError(t, manager.Serve())
	require.False(t, manager.autoFailback)

	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, oldClient)

	manager.relayConfigMu.Lock()
	manager.relayWeights[highURL] = 80
	manager.relayWeights[lowURL] = 20
	manager.configuredRelayURLs = []string{highURL, lowURL}
	manager.relayConfigMu.Unlock()

	streaks := make(map[string]int)
	manager.failbackTick(streaks)
	manager.failbackTick(streaks)
	manager.relayClientMu.RLock()
	defer manager.relayClientMu.RUnlock()
	assert.Same(t, oldClient, manager.relayClient, "failback tick must be a no-op when the manager is not running under auto-failback")
}
