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
	}

	var started []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(relayURL string) {
		started = append(started, relayURL)
	})

	require.Equal(t, 4, next, "all candidate relays should start in one flat wave")
	require.Equal(t, []string{
		"rels://fast.example",
		"rels://slow.example",
		"rels://cold.example",
		"rels://other.example",
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
	if _, ok := cache.get("rels://a.example"); ok {
		t.Fatal("expected a miss before the minimum sample count is reached")
	}
	cache.set("rels://a.example", 42*time.Millisecond)
	rtt, ok := cache.get("rels://a.example")
	require.True(t, ok)
	require.Equal(t, 42*time.Millisecond, rtt)
}

func TestRelayRTTCacheEWMASmoothing(t *testing.T) {
	cache := newRelayRTTCache()
	cache.set("rels://a.example", 100*time.Millisecond)
	cache.set("rels://a.example", 200*time.Millisecond)
	rtt, ok := cache.get("rels://a.example")
	require.True(t, ok)
	// alpha=0.3: 0.3*200 + 0.7*100 = 130ms
	require.Equal(t, 130*time.Millisecond, rtt)

	cache.set("rels://a.example", 200*time.Millisecond)
	rtt, ok = cache.get("rels://a.example")
	require.True(t, ok)
	// 0.3*200 + 0.7*130 = 151ms
	require.Equal(t, 151*time.Millisecond, rtt)
}

func TestRelayRTTCacheExpires(t *testing.T) {
	previousTTL := relayRTTCacheTTL
	relayRTTCacheTTL = 50 * time.Millisecond
	t.Cleanup(func() { relayRTTCacheTTL = previousTTL })

	cache := newRelayRTTCache()
	cache.set("rels://a.example", 10*time.Millisecond)
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
	// Twenty relays in worst-RTT-first configured order. The first attempt
	// starts the maxConcurrentServers (16) lowest-RTT relays, and the second
	// attempt starts the remaining four.
	const total = 20
	serverURLs := make([]string, 0, total)
	rtts := make(map[string]time.Duration, total)
	for i := total - 1; i >= 0; i-- {
		url := "rels://relay-" + string(rune('a'+i)) + ".example"
		serverURLs = append(serverURLs, url)
		rtts[url] = time.Duration(i) * 30 * time.Millisecond
	}
	picker := &ServerPicker{rttLookup: func(url string) (time.Duration, bool) {
		rtt, ok := rtts[url]
		return rtt, ok
	}}
	config := pickerConfig{serverURLs: serverURLs}

	var first []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(url string) {
		first = append(first, url)
	})
	require.Equal(t, maxConcurrentServers, next)
	require.Len(t, first, maxConcurrentServers)
	for i := 0; i < maxConcurrentServers; i++ {
		require.Equal(t, rtts[first[i]], time.Duration(i)*30*time.Millisecond, "batch must follow ascending RTT of the full group")
	}

	var second []string
	next = picker.startNextPriorityGroupWithConfig(config, serverURLs, next, func(url string) {
		second = append(second, url)
	})
	require.Equal(t, total, next)
	require.Len(t, second, total-maxConcurrentServers)
	for i := 0; i < total-maxConcurrentServers; i++ {
		require.Equal(t, rtts[second[i]], time.Duration(maxConcurrentServers+i)*30*time.Millisecond)
	}
}

func TestStartNextPriorityGroupRTTHysteresis(t *testing.T) {
	// RTTs inside the hysteresis band count as equal, so probe noise does not
	// reorder the group; RTTs outside the band still sort ascending.
	serverURLs := []string{
		"rels://noisy-a.example", // 100ms, first in configured order
		"rels://noisy-b.example", // 110ms, within band of a
		"rels://slow.example",    // 300ms, outside band
	}
	rtts := map[string]time.Duration{
		"rels://noisy-a.example": 100 * time.Millisecond,
		"rels://noisy-b.example": 110 * time.Millisecond,
		"rels://slow.example":    300 * time.Millisecond,
	}
	weights := map[string]int{
		"rels://noisy-a.example": 100,
		"rels://noisy-b.example": 100,
		"rels://slow.example":    100,
	}
	picker := &ServerPicker{rttLookup: func(url string) (time.Duration, bool) {
		rtt, ok := rtts[url]
		return rtt, ok
	}}
	config := pickerConfig{serverURLs: serverURLs, serverWeights: weights}

	var started []string
	next := picker.startNextPriorityGroupWithConfig(config, serverURLs, 0, func(url string) {
		started = append(started, url)
	})
	require.Equal(t, 3, next)
	require.Equal(t, []string{
		"rels://noisy-a.example",
		"rels://noisy-b.example",
		"rels://slow.example",
	}, started, "in-band RTTs keep configured order, out-of-band still sorts last")
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
