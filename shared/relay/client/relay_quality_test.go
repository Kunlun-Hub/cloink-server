package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
)

func TestRelayLiveQualityTracksRTTJitterAndLoss(t *testing.T) {
	var now time.Time
	now = time.Unix(100, 0)
	quality := relayLiveQualityTracker{now: func() time.Time { return now }}

	quality.recordSuccess(100 * time.Millisecond)
	now = now.Add(time.Second)
	quality.recordSuccess(140 * time.Millisecond)
	now = now.Add(time.Second)
	quality.recordFailure()

	snapshot := quality.snapshot()
	require.Equal(t, 2, snapshot.Samples)
	require.Greater(t, snapshot.RTT, time.Duration(0))
	require.Equal(t, 40*time.Millisecond, snapshot.Jitter)
	require.Equal(t, 140*time.Millisecond, snapshot.P95)
	require.Equal(t, 33, snapshot.LossPercent)
	require.Equal(t, 1, snapshot.ConsecutiveFailures)
	require.True(t, quality.failedRecently(5*time.Second))

	now = now.Add(time.Second)
	quality.recordSuccess(100 * time.Millisecond)
	snapshot = quality.snapshot()
	require.Zero(t, snapshot.ConsecutiveFailures)
	require.Equal(t, 25, snapshot.LossPercent)
	require.Len(t, quality.samples, 3)
}

func TestRelayProbeQualityPenalisesJitterAndLoss(t *testing.T) {
	cache := newRelayRTTCache()
	cache.set("rel://relay", 40*time.Millisecond)
	cache.set("rel://relay", 100*time.Millisecond)
	cache.recordFailure("rel://relay")

	snapshot, ok := cache.snapshot("rel://relay")
	require.True(t, ok)
	require.Greater(t, snapshot.Jitter, time.Duration(0))
	require.Equal(t, 33, snapshot.LossPercent)

	score, ok := cache.score("rel://relay")
	require.True(t, ok)
	require.Greater(t, score, snapshot.RTT, "jitter and loss should raise the ordering score")

	cache.clear()
	_, ok = cache.snapshot("rel://relay")
	require.False(t, ok)
}

func TestRelayProbeLossUsesRecentOutcomeWindow(t *testing.T) {
	cache := newRelayRTTCache()
	cache.set("rel://relay", 40*time.Millisecond)
	cache.set("rel://relay", 45*time.Millisecond)
	cache.recordFailure("rel://relay")

	snapshot, ok := cache.snapshot("rel://relay")
	require.True(t, ok)
	require.Equal(t, 33, snapshot.LossPercent)

	for range relayQualityOutcomeWindow {
		cache.set("rel://relay", 40*time.Millisecond)
	}
	snapshot, ok = cache.snapshot("rel://relay")
	require.True(t, ok)
	require.Zero(t, snapshot.LossPercent, "old failures must age out of the recent outcome window")
}

func TestRelayCooldownClassificationAndProbeRecovery(t *testing.T) {
	picker := &ServerPicker{CooldownDuration: time.Minute}
	now := time.Now()

	picker.markServerFailure("rel://timeout", now, context.DeadlineExceeded)
	require.Positive(t, picker.cooldownRemaining("rel://timeout"))
	require.Equal(t, relayFailureTransient, picker.failureKinds["rel://timeout"])
	picker.clearTransientFailure("rel://timeout")
	require.Zero(t, picker.cooldownRemaining("rel://timeout"))

	picker.markServerFailure("rel://cert", now, errors.New("tls: failed to verify certificate: x509: certificate has expired"))
	require.Positive(t, picker.cooldownRemaining("rel://cert"))
	require.Equal(t, relayFailureDeterministic, picker.failureKinds["rel://cert"])
	picker.clearTransientFailure("rel://cert")
	require.Positive(t, picker.cooldownRemaining("rel://cert"), "a successful transport probe must not clear an identity failure")
	picker.clearServerFailure("rel://cert")
	require.Zero(t, picker.cooldownRemaining("rel://cert"))
}

func TestRelayProbeClearsTransientCooldownAndBecomesStandby(t *testing.T) {
	homeURL := startMigrationRelayServer(t, "cooldown-home")
	candidateURL := startMigrationRelayServer(t, "cooldown-candidate")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{homeURL, candidateURL}, "peer-cooldown-probe", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	manager.serverPicker.markServerFailure(candidateURL, time.Now(), context.DeadlineExceeded)
	require.Positive(t, manager.serverPicker.cooldownRemaining(candidateURL))

	relays := manager.ProbeRelayServers(ctx)
	for _, relay := range relays {
		if relay.URL == candidateURL {
			require.True(t, relay.Available)
		}
	}
	require.Zero(t, manager.serverPicker.cooldownRemaining(candidateURL), "a successful probe should clear a transient timeout cooldown")
}

func TestRequiredMigrationStreaksShortensForDataPlaneFailureAndLargeGap(t *testing.T) {
	manager := NewManager(context.Background(), []string{"rel://home"}, "peer-streaks", iface.DefaultMTU)
	home := NewClient("rel://home", manager.tokenStore, "peer-streaks", iface.DefaultMTU)
	manager.relayClient = home

	require.Equal(t, relayRTTMigrationStreaks, manager.requiredMigrationStreaks("rel://home", 80*time.Millisecond, 65*time.Millisecond))
	require.Equal(t, 2, manager.requiredMigrationStreaks("rel://home", 320*time.Millisecond, 60*time.Millisecond))

	home.liveQuality.recordFailure()
	require.Equal(t, 1, manager.requiredMigrationStreaks("rel://home", 80*time.Millisecond, 65*time.Millisecond))
}

func TestProactiveSwitchRespectsMinimumResidence(t *testing.T) {
	manager := NewManager(context.Background(), []string{"rel://a", "rel://b"}, "peer-residence", iface.DefaultMTU)
	manager.recordHomeSwitch("rtt-migration")
	require.Positive(t, manager.proactiveSwitchRemaining())

	manager.relayConfigMu.Lock()
	manager.lastHomeSwitchAt = time.Now().Add(-relayProactiveSwitchMinResidence - time.Second)
	manager.relayConfigMu.Unlock()
	require.Zero(t, manager.proactiveSwitchRemaining())

	manager.relayConfigMu.RLock()
	require.Equal(t, "rtt-migration", manager.lastHomeSwitchReason)
	require.Equal(t, 1, manager.homeSwitchCount)
	manager.relayConfigMu.RUnlock()
}

func TestManagerHotStandbyPromotesImmediatelyOnHomeFailure(t *testing.T) {
	homeURL := startMigrationRelayServer(t, "hot-standby-home")
	standbyURL := startMigrationRelayServer(t, "hot-standby-standby")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{homeURL}, "peer-hot-standby", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	manager.UpdateServerURLs([]string{homeURL, standbyURL})

	manager.ensureHotStandby(standbyURL)
	require.Eventually(t, func() bool {
		manager.hotStandbyMu.Lock()
		defer manager.hotStandbyMu.Unlock()
		return manager.hotStandbyClient != nil && manager.hotStandbyClient.Ready()
	}, 3*time.Second, 20*time.Millisecond)

	manager.relayClientMu.RLock()
	home := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, home)
	require.NoError(t, home.Close())

	require.Eventually(t, func() bool {
		return manager.currentRelayURL() == standbyURL && manager.Ready()
	}, 5*time.Second, 20*time.Millisecond, "the preconnected standby should be promoted without a cold dial")
}

func TestManagerHandleNetworkChangeClosesSessionAndInvalidatesRTT(t *testing.T) {
	relayURL := startMigrationRelayServer(t, "network-change")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{relayURL}, "peer-network-change", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, oldClient)
	manager.rttCache.set(relayURL, 20*time.Millisecond)

	manager.HandleNetworkChange("unit-test")

	require.Eventually(t, func() bool { return !oldClient.Ready() }, 2*time.Second, 20*time.Millisecond,
		"the stale home Relay socket must be closed immediately")
	if _, ok := manager.rttCache.snapshot(relayURL); ok {
		t.Fatal("network change must invalidate probe measurements from the old path")
	}
}

func TestManagerStableConnectionResetsTransientFailures(t *testing.T) {
	oldInterval := relayStableResetInterval
	relayStableResetInterval = 50 * time.Millisecond
	t.Cleanup(func() { relayStableResetInterval = oldInterval })

	relayURL := startMigrationRelayServer(t, "stable-reset")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	manager := NewManager(ctx, []string{relayURL}, "peer-stable-reset", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	manager.serverPicker.markServerFailure(relayURL, time.Now(), context.DeadlineExceeded)

	require.Eventually(t, func() bool {
		return manager.serverPicker.cooldownRemaining(relayURL) == 0
	}, time.Second, 10*time.Millisecond, "a stable connection should reset transient cooldown history")
}
