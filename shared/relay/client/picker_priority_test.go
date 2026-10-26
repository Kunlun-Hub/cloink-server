package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/shared/relay/auth/hmac"
)

func TestServerPickerForcedRelayFallsBackWhenRemoved(t *testing.T) {
	picker := &ServerPicker{}
	picker.ServerWeights.Store(map[string]int{
		"rels://fallback.example": 100,
	})
	picker.setForcedServerURL("rels://removed.example")

	var started []string
	next := picker.startNextPriorityGroup([]string{
		"rels://fallback.example",
	}, 0, func(relayURL string) {
		started = append(started, relayURL)
	})

	require.Equal(t, 1, next)
	require.Equal(t, []string{"rels://fallback.example"}, started)
}

func TestServerPickerForcedRelayRunsAlone(t *testing.T) {
	picker := &ServerPicker{}
	picker.ServerWeights.Store(map[string]int{
		"rels://preferred.example": 100,
		"rels://forced.example":    10,
	})
	picker.setForcedServerURL("rels://forced.example")

	var started []string
	next := picker.startNextPriorityGroup([]string{
		"rels://forced.example",
		"rels://preferred.example",
	}, 0, func(relayURL string) {
		started = append(started, relayURL)
	})

	require.Equal(t, 1, next)
	require.Equal(t, []string{"rels://forced.example"}, started)
}

// A forced Relay that ultimately fails must not prevent automatic fallback.
func TestServerPickerForcedRelayFallsBackAfterFailure(t *testing.T) {
	healthyURL := startMigrationRelayServer(t, "picker-forced-fallback")
	const forcedURL = "rel://127.0.0.1:1"

	picker := &ServerPicker{
		TokenStore:        &hmac.TokenStore{},
		PeerID:            "picker-forced-fallback-peer",
		ConnectionTimeout: 3 * time.Second,
	}
	picker.storeConfig(pickerConfig{
		serverURLs:    []string{forcedURL, healthyURL},
		serverWeights: map[string]int{forcedURL: 100, healthyURL: 10},
		forcedURL:     forcedURL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	relayClient, err := picker.PickServer(ctx)
	require.NoError(t, err)
	defer func() { _ = relayClient.Close() }()
	require.Equal(t, healthyURL, relayClient.connectionURL)
	require.Less(t, time.Since(started), 3*time.Second,
		"forced failure should hedge to the fallback Relay after the grace period")
}

func TestManagerSetForcedRelayClearsCooldown(t *testing.T) {
	const (
		forcedURL   = "rel://forced.example:80"
		fallbackURL = "rel://fallback.example:80"
	)
	manager := NewManager(context.Background(), []string{forcedURL, fallbackURL}, "peer-local", iface.DefaultMTU)
	manager.serverPicker.markServerFailure(forcedURL, time.Now(), context.DeadlineExceeded)

	selected, err := manager.SetForcedRelay(forcedURL)
	require.NoError(t, err)
	require.Equal(t, forcedURL, selected)
	require.NotContains(t, manager.serverPicker.cooldowns, forcedURL,
		"setting a forced Relay must clear its previous failure cooldown")
	require.Equal(t, forcedURL, manager.serverPicker.loadConfig().forcedURL)
}
