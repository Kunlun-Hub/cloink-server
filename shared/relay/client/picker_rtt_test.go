package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartNextPriorityGroupOrdersByCachedRTT(t *testing.T) {
	picker := &ServerPicker{}
	picker.rttLookup = func(relayURL string) (time.Duration, bool) {
		switch relayURL {
		case "rels://slow.example":
			return 300 * time.Millisecond, true
		case "rels://fast.example":
			return 20 * time.Millisecond, true
		}
		return 0, false
	}
	serverURLs := []string{
		"rels://slow.example",
		"rels://fast.example",
		"rels://cold.example",
		"rels://other.example",
	}
	config := pickerConfig{
		serverURLs: serverURLs,
		serverWeights: map[string]int{
			"rels://slow.example":  100,
			"rels://fast.example":  100,
			"rels://cold.example":  100,
			"rels://other.example": 10,
		},
	}

	var started []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(relayURL string) {
		started = append(started, relayURL)
	})

	require.Equal(t, 3, next, "only the weight-100 group should start")
	require.Equal(t, []string{
		"rels://fast.example",
		"rels://slow.example",
		"rels://cold.example",
	}, started, "cached RTTs sort ascending, uncached relays keep order at the back")
}

func TestStartNextPriorityGroupWithoutRTTLookupKeepsOrder(t *testing.T) {
	picker := &ServerPicker{}
	serverURLs := []string{"rels://a.example", "rels://b.example"}
	config := pickerConfig{
		serverURLs:    serverURLs,
		serverWeights: map[string]int{"rels://a.example": 100, "rels://b.example": 100},
	}

	var started []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(relayURL string) {
		started = append(started, relayURL)
	})

	require.Equal(t, 2, next)
	require.Equal(t, []string{"rels://a.example", "rels://b.example"}, started)
}

func TestRelayRTTCacheRoundTrip(t *testing.T) {
	cache := newRelayRTTCache()
	if _, ok := cache.get("rels://a.example"); ok {
		t.Fatal("expected a miss for an unknown relay")
	}
	cache.set("rels://a.example", 42*time.Millisecond)
	rtt, ok := cache.get("rels://a.example")
	require.True(t, ok)
	require.Equal(t, 42*time.Millisecond, rtt)
}

func TestRelayRTTCacheExpires(t *testing.T) {
	previousTTL := relayRTTCacheTTL
	relayRTTCacheTTL = 50 * time.Millisecond
	t.Cleanup(func() { relayRTTCacheTTL = previousTTL })

	cache := newRelayRTTCache()
	cache.set("rels://a.example", 10*time.Millisecond)
	if _, ok := cache.get("rels://a.example"); !ok {
		t.Fatal("expected a hit before the TTL elapses")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := cache.get("rels://a.example"); ok {
		t.Fatal("expected the entry to expire")
	}
}

func TestStartNextPriorityGroupSortsFullGroupBeyondBatchSize(t *testing.T) {
	// Ten same-weight relays in worst-RTT-first configured order. The first
	// attempt must start the seven lowest-RTT relays of the whole group, and
	// the second attempt the remaining three.
	serverURLs := make([]string, 0, 10)
	rtts := make(map[string]time.Duration, 10)
	weights := make(map[string]int, 10)
	for i := 9; i >= 0; i-- {
		url := "rels://relay" + string(rune('0'+i)) + ".example"
		serverURLs = append(serverURLs, url)
		rtts[url] = time.Duration(i) * 10 * time.Millisecond
		weights[url] = 100
	}
	picker := &ServerPicker{rttLookup: func(url string) (time.Duration, bool) {
		rtt, ok := rtts[url]
		return rtt, ok
	}}
	config := pickerConfig{serverURLs: serverURLs, serverWeights: weights}

	var first []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(url string) {
		first = append(first, url)
	})
	require.Equal(t, 7, next)
	require.Len(t, first, 7)
	for i := 0; i < 7; i++ {
		require.Equal(t, rtts[first[i]], time.Duration(i)*10*time.Millisecond, "batch must follow ascending RTT of the full group")
	}

	var second []string
	next = picker.startNextPriorityGroupWithConfig(config, serverURLs, next, func(url string) {
		second = append(second, url)
	})
	require.Equal(t, 10, next)
	require.Len(t, second, 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, rtts[second[i]], time.Duration(7+i)*10*time.Millisecond)
	}
}

func TestStartNextPriorityGroupSkipsForcedRelayInGroupSort(t *testing.T) {
	// The forced relay is dialed on its own; the following batch sorts the
	// rest of the group by RTT without re-dialing it.
	serverURLs := []string{"rels://forced.example", "rels://slow.example", "rels://fast.example"}
	weights := map[string]int{
		"rels://forced.example": 100,
		"rels://slow.example":   100,
		"rels://fast.example":   100,
	}
	picker := &ServerPicker{rttLookup: func(url string) (time.Duration, bool) {
		switch url {
		case "rels://fast.example":
			return 10 * time.Millisecond, true
		case "rels://slow.example":
			return 90 * time.Millisecond, true
		}
		return 0, false
	}}
	config := pickerConfig{serverURLs: serverURLs, serverWeights: weights, forcedURL: "rels://forced.example"}

	var first []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(url string) {
		first = append(first, url)
	})
	require.Equal(t, []string{"rels://forced.example"}, first)
	require.Equal(t, 1, next)

	var second []string
	next = picker.startNextPriorityGroupWithConfig(config, serverURLs, next, func(url string) {
		second = append(second, url)
	})
	require.Equal(t, []string{"rels://fast.example", "rels://slow.example"}, second)
	require.Equal(t, 3, next)
}
