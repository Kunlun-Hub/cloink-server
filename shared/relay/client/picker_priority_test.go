package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
