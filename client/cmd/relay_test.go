package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/proto"
)

func TestRelayLabelsUsesStandbyInsteadOfPreferred(t *testing.T) {
	labels := relayLabels(&proto.RelayServer{
		Current:   true,
		Preferred: true,
		Forced:    true,
	})

	require.Equal(t, []string{"current", "standby", "forced"}, labels)
}

func TestRelayQualityDisplaysCooldownLiveMetricsAndSwitchReason(t *testing.T) {
	relay := &proto.RelayServer{
		LiveRttMs:           12,
		P95RttMs:            20,
		JitterMs:            3,
		LossPercent:         5,
		Transport:           "ws",
		CooldownRemainingMs: int64(90 * time.Second / time.Millisecond),
		LastSwitchReason:    "rtt-migration",
		LastSwitchAtUnixMs:  time.UnixMilli(1700000000000).UnixMilli(),
		SwitchCount:         4,
	}

	require.Equal(t, "RTT 12ms, p95 20ms, jitter 3ms, loss 5%", relayLiveQualityDisplay(relay))
	require.Equal(t, "1m30s remaining", relayCooldownDisplay(relay))
	require.Contains(t, relaySwitchDisplay(relay), "rtt-migration")
	require.Contains(t, relaySwitchDisplay(relay), "total 4")
}
