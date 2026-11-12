package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
)

func TestBestRTTMigrationCandidateUsesLatencyAwarePercentageThresholds(t *testing.T) {
	homeURL := "rels://home.example"
	candidateURL := "rels://candidate.example"
	manager := NewManager(context.Background(), []string{homeURL, candidateURL}, "peer-rtt-threshold", iface.DefaultMTU)

	require.Equal(t, candidateURL, manager.bestRTTMigrationCandidate(homeURL, []string{candidateURL}, map[string]time.Duration{
		homeURL:      240 * time.Millisecond,
		candidateURL: 30 * time.Millisecond,
	}), "a high-latency candidate below half should be eligible")

	require.Equal(t, candidateURL, manager.bestRTTMigrationCandidate(homeURL, []string{candidateURL}, map[string]time.Duration{
		homeURL:      60 * time.Millisecond,
		candidateURL: 40 * time.Millisecond,
	}), "a low-latency candidate with a 25% improvement should be eligible")

	require.Empty(t, manager.bestRTTMigrationCandidate(homeURL, []string{candidateURL}, map[string]time.Duration{
		homeURL:      150 * time.Millisecond,
		candidateURL: 100 * time.Millisecond,
	}), "a high-latency candidate must beat half of the home RTT")

	require.Empty(t, manager.bestRTTMigrationCandidate(homeURL, []string{candidateURL}, map[string]time.Duration{
		homeURL:      20 * time.Millisecond,
		candidateURL: 15 * time.Millisecond,
	}), "a sub-10ms difference is treated as probe noise")
}

func TestManagerRTTMigrationPreconnectsAndRetiresOldHomeSmoothly(t *testing.T) {
	slowURL := startMigrationRelayServer(t, "rtt-migration-slow")
	fastURL := startMigrationRelayServer(t, "rtt-migration-fast")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{slowURL}, "peer-rtt-migration", iface.DefaultMTU,
		WithRelayMigrationGrace(1500*time.Millisecond))
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, oldClient)

	manager.UpdateServerURLs([]string{slowURL, fastURL})
	require.Eventually(t, func() bool {
		return manager.StandbyURL() == fastURL
	}, 3*time.Second, 20*time.Millisecond, "the configuration refresh probe should select the faster relay as standby")
	// UpdateServerURLs starts a probe round asynchronously. Let that round finish
	// before feeding deterministic samples; otherwise it can legitimately reset
	// the synthetic migration streak midway through this test.
	require.Eventually(t, func() bool {
		if manager.probeMu.TryLock() {
			manager.probeMu.Unlock()
			return true
		}
		return false
	}, 3*time.Second, 20*time.Millisecond, "the configuration refresh probe should finish")
	generation := manager.relayConfigGeneration.Load()
	rtts := map[string]time.Duration{
		slowURL: 320 * time.Millisecond,
		fastURL: 60 * time.Millisecond,
	}
	required := manager.requiredMigrationStreaks(slowURL, rtts[slowURL], rtts[fastURL])

	for i := 0; i < required-1; i++ {
		manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, rtts)
	}
	require.Equal(t, slowURL, manager.currentRelayURL(), "migration must wait for a sustained advantage")
	manager.relayClientMu.RLock()
	require.Same(t, oldClient, manager.relayClient)
	manager.relayClientMu.RUnlock()
	require.Eventually(t, func() bool {
		manager.hotStandbyMu.Lock()
		defer manager.hotStandbyMu.Unlock()
		return manager.hotStandbyClient != nil && manager.hotStandbyClient.Ready()
	}, 3*time.Second, 20*time.Millisecond, "the improved candidate should be preconnected while the streak accumulates")

	manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, rtts)
	require.Equal(t, fastURL, manager.currentRelayURL(), "candidate must be promoted after a successful preflight")
	require.True(t, oldClient.Ready(), "old home Relay must stay alive while existing peer connections drain")

	require.Eventually(t, func() bool { return !oldClient.Ready() }, 3*time.Second, 20*time.Millisecond,
		"old home Relay should retire after the migration grace period")
}

func TestManagerRTTMigrationIgnoresSmallOrTransientAdvantage(t *testing.T) {
	slowURL := startMigrationRelayServer(t, "rtt-small-slow")
	fastURL := startMigrationRelayServer(t, "rtt-small-fast")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{slowURL}, "peer-rtt-small", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	manager.UpdateServerURLs([]string{slowURL, fastURL})
	generation := manager.relayConfigGeneration.Load()

	smallWin := map[string]time.Duration{
		slowURL: 150 * time.Millisecond,
		fastURL: 120 * time.Millisecond,
	}
	for i := 0; i < relayRTTMigrationStreaks+2; i++ {
		manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, smallWin)
	}
	require.Equal(t, slowURL, manager.currentRelayURL(), "a small RTT win must not disrupt peer traffic")

	largeWin := map[string]time.Duration{
		slowURL: 320 * time.Millisecond,
		fastURL: 60 * time.Millisecond,
	}
	required := manager.requiredMigrationStreaks(slowURL, largeWin[slowURL], largeWin[fastURL])
	for i := 0; i < required-1; i++ {
		manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, largeWin)
	}
	manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, smallWin)
	for i := 0; i < required-1; i++ {
		manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, largeWin)
	}
	require.Equal(t, slowURL, manager.currentRelayURL(), "a non-advantageous round must reset the migration streak before it can trigger")
}

func TestManagerRTTMigrationRespectsForcedRelay(t *testing.T) {
	slowURL := startMigrationRelayServer(t, "rtt-forced-slow")
	fastURL := startMigrationRelayServer(t, "rtt-forced-fast")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{slowURL, fastURL}, "peer-rtt-forced", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	// Same-weight relays are ordered by peer ID, so the initial home relay is
	// not deterministic. Force the slow relay and wait for the switch to land
	// before exercising the automatic migration path.
	_, err := manager.SetForcedRelay(slowURL)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return manager.currentRelayURL() == slowURL
	}, 10*time.Second, 50*time.Millisecond, "forced relay should become the home relay")

	generation := manager.relayConfigGeneration.Load()
	rtts := map[string]time.Duration{
		slowURL: 320 * time.Millisecond,
		fastURL: 60 * time.Millisecond,
	}
	for i := 0; i < relayRTTMigrationStreaks+1; i++ {
		manager.maybeMigrateHomeRelayOnRTT(generation, slowURL, []string{slowURL, fastURL}, rtts)
	}

	manager.relayClientMu.RLock()
	defer manager.relayClientMu.RUnlock()
	require.NotNil(t, manager.relayClient)
	require.Equal(t, slowURL, manager.relayClient.connectionURL, "manual forced relay must disable automatic RTT migration")
}
