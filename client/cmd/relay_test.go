package cmd

import (
	"testing"

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
