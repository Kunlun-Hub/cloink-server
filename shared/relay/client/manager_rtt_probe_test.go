package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
)

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

// Upon initial connection, the first relay to complete the handshake becomes
// the Primary Relay, and the remaining relays are probed via lightProbe to
// select the Standby Relay.
func TestManagerInitialConnectComputesStandby(t *testing.T) {
	relayA := startMigrationRelayServer(t, "init-a")
	relayB := startMigrationRelayServer(t, "init-b")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{relayA, relayB}, "peer-init-standby", iface.DefaultMTU)
	require.NoError(t, manager.Serve())

	primary := manager.currentRelayURL()
	require.Contains(t, []string{relayA, relayB}, primary)
	expectedStandby := relayB
	if primary == relayB {
		expectedStandby = relayA
	}

	require.Eventually(t, func() bool {
		return manager.StandbyURL() == expectedStandby
	}, 3*time.Second, 20*time.Millisecond, "remaining healthy relay should be selected as standby")
}

// During daily operation, when Primary and Standby are both healthy (and
// Standby has not degraded beyond hysteresis), neither Primary nor Standby moves.
func TestManagerProbeTickKeepsHealthyPrimaryAndStandby(t *testing.T) {
	primaryURL := startMigrationRelayServer(t, "probe-primary")
	standbyURL := startMigrationRelayServer(t, "probe-standby")
	otherURL := startMigrationRelayServer(t, "probe-other")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{primaryURL}, "peer-keep-standby", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	require.Equal(t, primaryURL, manager.currentRelayURL())

	manager.relayConfigMu.Lock()
	manager.configuredRelayURLs = []string{primaryURL, standbyURL, otherURL}
	manager.standbyRelayURL = standbyURL
	manager.relayConfigMu.Unlock()

	// Both standbyURL and otherURL are healthy and within hysteresis band.
	manager.selectStandbyFromProbes(primaryURL, []string{standbyURL, otherURL}, map[string]time.Duration{
		standbyURL: 40 * time.Millisecond,
		otherURL:   30 * time.Millisecond, // within 25ms hysteresis
	})
	require.Equal(t, primaryURL, manager.currentRelayURL(), "primary relay must never move while healthy")
	require.Equal(t, standbyURL, manager.StandbyURL(), "healthy standby within hysteresis must stay unchanged")
}

// When Primary is healthy and Standby becomes unreachable or significantly
// slower, a new Standby is selected by lowest RTT while Primary does not move.
func TestManagerProbeTickReselectsStandbyWhenStandbyFailsOrSlowsWithoutMovingPrimary(t *testing.T) {
	primaryURL := startMigrationRelayServer(t, "reselect-primary")
	standbyURL := startMigrationRelayServer(t, "reselect-standby")
	fastAltURL := startMigrationRelayServer(t, "reselect-fast")
	slowAltURL := startMigrationRelayServer(t, "reselect-slow")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{primaryURL}, "peer-reselect-standby", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	require.Equal(t, primaryURL, manager.currentRelayURL())

	candidates := []string{standbyURL, fastAltURL, slowAltURL}
	manager.relayConfigMu.Lock()
	manager.configuredRelayURLs = append([]string{primaryURL}, candidates...)
	manager.standbyRelayURL = standbyURL
	manager.relayConfigMu.Unlock()

	// Case 1: Standby fails probe -> lowest-RTT healthy alternative becomes new Standby; Primary stays untouched.
	manager.selectStandbyFromProbes(primaryURL, candidates, map[string]time.Duration{
		fastAltURL: 25 * time.Millisecond,
		slowAltURL: 90 * time.Millisecond,
	})
	require.Equal(t, primaryURL, manager.currentRelayURL(), "primary relay must not move when standby fails")
	require.Equal(t, fastAltURL, manager.StandbyURL(), "lowest-RTT healthy candidate must become new standby")

	// Case 2: Current Standby (fastAltURL) degrades significantly -> switch Standby to slowAltURL (now faster); Primary stays untouched.
	manager.selectStandbyFromProbes(primaryURL, candidates, map[string]time.Duration{
		fastAltURL: 250 * time.Millisecond,
		slowAltURL: 40 * time.Millisecond,
	})
	require.Equal(t, primaryURL, manager.currentRelayURL(), "primary relay must not move when standby degrades")
	require.Equal(t, slowAltURL, manager.StandbyURL(), "degraded standby should be replaced by faster healthy candidate")
}

// If an administrator removes a relay while a background probe round is in
// flight, stale probe results must not write the removed relay back as
// standbyRelayURL.
func TestManagerSelectStandbyIgnoresRelayRemovedDuringProbe(t *testing.T) {
	primaryURL := startMigrationRelayServer(t, "race-primary")
	removedFastURL := startMigrationRelayServer(t, "race-removed")
	keptSlowURL := startMigrationRelayServer(t, "race-kept")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{primaryURL}, "peer-race-standby", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	require.Equal(t, primaryURL, manager.currentRelayURL())

	// Simulate configuration updated mid-probe so removedFastURL is no longer in configuredRelayURLs.
	manager.UpdateServerURLs([]string{primaryURL, keptSlowURL})

	manager.selectStandbyFromProbes(primaryURL, []string{removedFastURL, keptSlowURL}, map[string]time.Duration{
		removedFastURL: 10 * time.Millisecond,
		keptSlowURL:    80 * time.Millisecond,
	})
	require.Equal(t, keptSlowURL, manager.StandbyURL(),
		"removed relay must not be selected as standby even if it had the lowest probe RTT")
}
