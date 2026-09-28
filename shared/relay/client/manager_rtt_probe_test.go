package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
)

func TestTopPriorityGroup(t *testing.T) {
	urls := []string{"rels://a.example", "rels://b.example", "rels://c.example"}
	weights := map[string]int{"rels://a.example": 100, "rels://b.example": 100, "rels://c.example": 10}

	group := topPriorityGroup(urls, weights)
	require.ElementsMatch(t, []string{"rels://a.example", "rels://b.example"}, group,
		"only the max-weight relays belong to the probe group")

	// Missing weights fall back to the default weight, like the picker does.
	group = topPriorityGroup(urls, nil)
	require.ElementsMatch(t, urls, group, "all relays share the default weight")

	require.Empty(t, topPriorityGroup(nil, weights), "no URLs means no group")
}

func TestLightProbeMeasuresTransport(t *testing.T) {
	relayURL := startMigrationRelayServer(t, "light-probe")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{relayURL}, "peer-light-probe", iface.DefaultMTU)
	probeCtx, probeCancel := context.WithTimeout(ctx, relayProbeTimeout)
	defer probeCancel()
	rtt, err := manager.lightProbe(probeCtx, relayURL)
	require.NoError(t, err)
	require.Greater(t, rtt, time.Duration(0), "a transport handshake must take measurable time")
	require.Less(t, rtt, relayProbeTimeout, "the probe must finish inside its timeout")

	// The light probe must not leave a relay session behind: it only performs
	// the transport handshake, never the relay handshake.
	manager.relayClientsMutex.RLock()
	track := manager.relayClients[relayURL]
	manager.relayClientsMutex.RUnlock()
	require.Nil(t, track, "light probe must not register a relay client track")
}

func feedRTT(t *testing.T, manager *Manager, relayURL string, rtt time.Duration) {
	t.Helper()
	for i := 0; i < relayRTTMinSamples; i++ {
		manager.rttCache.set(relayURL, rtt)
	}
}

// The home relay migrates only after the alternative has been significantly
// faster for relayRTTMigrationStreaks consecutive ticks.
func TestManagerRTTMigratesPersistentlySlowHome(t *testing.T) {
	slowURL := startMigrationRelayServer(t, "rtt-slow")
	fastURL := startMigrationRelayServer(t, "rtt-fast")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{slowURL}, "peer-rtt", iface.DefaultMTU,
		WithRelayMigrationGrace(300*time.Millisecond))
	require.NoError(t, manager.Serve())
	require.Equal(t, slowURL, manager.currentRelayURL())

	// Introduce the faster relay as same-weight, like a config update would.
	manager.relayConfigMu.Lock()
	manager.relayWeights[slowURL] = 50
	manager.relayWeights[fastURL] = 50
	manager.configuredRelayURLs = []string{slowURL, fastURL}
	manager.relayConfigMu.Unlock()

	group := topPriorityGroup([]string{slowURL, fastURL},
		map[string]int{slowURL: 50, fastURL: 50})
	require.Len(t, group, 2)

	feedRTT(t, manager, slowURL, 500*time.Millisecond)
	feedRTT(t, manager, fastURL, 100*time.Millisecond)

	streaks := make(map[string]int)
	manager.maybeMigrateOnRTT(group, streaks)
	manager.maybeMigrateOnRTT(group, streaks)
	require.Equal(t, slowURL, manager.currentRelayURL(),
		"fewer than relayRTTMigrationStreaks ticks must not migrate")

	manager.maybeMigrateOnRTT(group, streaks)
	require.Eventually(t, func() bool {
		return manager.currentRelayURL() == fastURL && manager.Ready()
	}, 5*time.Second, 20*time.Millisecond, "home relay must migrate to the persistently faster relay")
}

// A small RTT advantage must never trigger a migration: the win is not worth
// disrupting peer traffic.
func TestManagerRTTDoesNotMigrateOnSmallDifference(t *testing.T) {
	slowURL := startMigrationRelayServer(t, "rtt-small-slow")
	fastURL := startMigrationRelayServer(t, "rtt-small-fast")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{slowURL}, "peer-rtt-small", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	require.Equal(t, slowURL, manager.currentRelayURL())

	manager.relayConfigMu.Lock()
	manager.relayWeights[slowURL] = 50
	manager.relayWeights[fastURL] = 50
	manager.configuredRelayURLs = []string{slowURL, fastURL}
	manager.relayConfigMu.Unlock()

	group := []string{slowURL, fastURL}
	feedRTT(t, manager, slowURL, 100*time.Millisecond)
	feedRTT(t, manager, fastURL, 90*time.Millisecond)

	streaks := make(map[string]int)
	for i := 0; i < relayRTTMigrationStreaks+2; i++ {
		manager.maybeMigrateOnRTT(group, streaks)
	}
	require.Empty(t, streaks, "a small difference must not accumulate a streak")
	require.Equal(t, slowURL, manager.currentRelayURL(), "no migration on a small RTT difference")
}

// Under-sampled or missing RTTs must not trigger anything.
func TestManagerRTTDoesNotMigrateWithoutSamples(t *testing.T) {
	slowURL := startMigrationRelayServer(t, "rtt-nosample-slow")
	fastURL := startMigrationRelayServer(t, "rtt-nosample-fast")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{slowURL}, "peer-rtt-nosample", iface.DefaultMTU)
	require.NoError(t, manager.Serve())

	group := []string{slowURL, fastURL}
	streaks := make(map[string]int)
	manager.maybeMigrateOnRTT(group, streaks)
	require.Empty(t, streaks)
	require.Equal(t, slowURL, manager.currentRelayURL())
}
