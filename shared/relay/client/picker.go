package client

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	auth "github.com/netbirdio/netbird/shared/relay/auth/hmac"
)

const (
	maxConcurrentServers     = 7
	defaultConnectionTimeout = 30 * time.Second
	// relayPriorityFallbackGrace is how long the current priority group gets
	// before the next weight group starts in parallel. A blackholed transport
	// can otherwise consume the full selection timeout without ever giving a
	// lower-priority relay a chance to connect.
	relayPriorityFallbackGrace = 1500 * time.Millisecond
)

// relayRTTHysteresis is the absolute RTT band inside which two relays count
// as equally fast for ordering, so probe noise does not reorder a group.
var relayRTTHysteresis = time.Duration(relayRTTHysteresisMs) * time.Millisecond

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

type connResult struct {
	RelayClient *Client
	Url         string
	Err         error
}

type pickerConfig struct {
	serverURLs    []string
	serverWeights map[string]int
	forcedURL     string
}

type ServerPicker struct {
	TokenStore *auth.TokenStore
	// These fields remain for compatibility with existing callers. Manager
	// updates use config so selection observes one immutable snapshot.
	ServerURLs        atomic.Value
	ServerWeights     atomic.Value
	ForcedServerURL   atomic.Pointer[string]
	config            atomic.Value
	PeerID            string
	MTU               uint16
	ConnectionTimeout time.Duration
	TransportFallback *transportFallback
	NetEvents         NetEvents
	CooldownDuration  time.Duration

	// rttLookup returns the cached dial RTT for a relay URL, set by the
	// Manager. Same-weight servers with a cached RTT are attempted lowest
	// RTT first; servers without a cached RTT keep their configured order.
	rttLookup func(string) (time.Duration, bool)

	cooldownMu sync.Mutex
	cooldowns  map[string]time.Time
	failures   map[string]int
}

func (sp *ServerPicker) PickServer(parentCtx context.Context) (*Client, error) {
	return sp.pickServer(parentCtx, sp.loadConfig())
}

// PickServerFrom limits a selection attempt to the supplied Relay URLs while
// retaining their configured weights, transport fallback and cooldown state.
func (sp *ServerPicker) PickServerFrom(parentCtx context.Context, serverURLs []string) (*Client, error) {
	config := sp.loadConfig()
	config.serverURLs = slices.Clone(serverURLs)
	if config.forcedURL != "" && !slices.Contains(config.serverURLs, config.forcedURL) {
		config.forcedURL = ""
	}
	return sp.pickServer(parentCtx, config)
}

func (sp *ServerPicker) pickServer(parentCtx context.Context, config pickerConfig) (*Client, error) {
	connectionTimeout := sp.ConnectionTimeout
	if connectionTimeout <= 0 {
		connectionTimeout = defaultConnectionTimeout
	}

	ctx, cancel := context.WithTimeout(parentCtx, connectionTimeout)
	defer cancel()

	serverURLs := sp.availableServerURLs(config, config.serverURLs, time.Now())
	totalServers := len(serverURLs)
	if totalServers == 0 {
		return nil, errors.New("failed to connect to any relay server: all attempts failed")
	}

	connResultChan := make(chan connResult, totalServers)
	concurrentLimiter := make(chan struct{}, maxConcurrentServers)
	startedServers := 0
	connectionCancels := make(map[string]context.CancelFunc, totalServers)

	startConnection := func(url string) {
		startedServers++
		connectionCtx, connectionCancel := context.WithCancel(ctx)
		connectionCancels[url] = connectionCancel
		go func(url string) {
			select {
			case concurrentLimiter <- struct{}{}:
				defer func() { <-concurrentLimiter }()
				sp.startConnection(connectionCtx, parentCtx, connResultChan, url)
			case <-connectionCtx.Done():
				// The picker still accounts for every scheduled attempt.
				connResultChan <- connResult{Url: url, Err: connectionCtx.Err()}
			}
		}(url)
	}

	cancelConnectionsExcept := func(selectedURL string) {
		for url, cancelConnection := range connectionCancels {
			if url != selectedURL {
				cancelConnection()
			}
		}
	}

	log.Debugf("pick server from list: %v", serverURLs)
	startedUpTo := sp.startNextPriorityGroupWithConfig(config, serverURLs, 0, startConnection)
	var fallbackTimer *time.Timer
	var fallbackTimerC <-chan time.Time
	stopFallbackTimer := func() {
		if fallbackTimer != nil {
			fallbackTimer.Stop()
			fallbackTimer = nil
		}
		fallbackTimerC = nil
	}
	resetFallbackTimer := func() {
		stopFallbackTimer()
		if startedUpTo < totalServers {
			fallbackTimer = time.NewTimer(relayPriorityFallbackGrace)
			fallbackTimerC = fallbackTimer.C
		}
	}
	defer stopFallbackTimer()
	resetFallbackTimer()

	receivedResults := 0
	for receivedResults < startedServers || startedUpTo < totalServers {
		select {
		case cr := <-connResultChan:
			receivedResults++
			if cr.Err == nil {
				instanceURL, serverIP, err := cr.RelayClient.serverInstanceAddress()
				if err != nil {
					log.Infof("chosen home Relay server: %s, instance address unavailable: %v", cr.Url, err)
				} else {
					log.Infof("chosen home Relay server: %s, instance URL: %s, server IP: %s", cr.Url, instanceURL, serverIP)
				}
				sp.clearServerFailure(cr.Url)
				cancelConnectionsExcept(cr.Url)
				go sp.drainConnResults(connResultChan, receivedResults, startedServers)
				return cr.RelayClient, nil
			}

			log.Tracef("failed to connect to Relay server: %s: %v", cr.Url, cr.Err)
			sp.markServerFailure(cr.Url, time.Now(), cr.Err)
			if receivedResults == startedServers && startedUpTo < totalServers {
				startedUpTo = sp.startNextPriorityGroupWithConfig(config, serverURLs, startedUpTo, startConnection)
				resetFallbackTimer()
			}
		case <-fallbackTimerC:
			// Keep the slower priority group dialing; this is a hedged
			// request, not a cancellation. Whichever relay succeeds first
			// wins and the remaining attempts are cancelled by the success
			// path above.
			startedUpTo = sp.startNextPriorityGroupWithConfig(config, serverURLs, startedUpTo, startConnection)
			resetFallbackTimer()
		case <-ctx.Done():
			cancelConnectionsExcept("")
			stopFallbackTimer()
			// A connection may have completed between the timeout firing and
			// its cancellation. Drain every scheduled result and close any
			// late winner so the failed pick cannot leak a live client.
			go sp.drainConnResults(connResultChan, receivedResults, startedServers)
			return nil, fmt.Errorf("failed to connect to any relay server: %w", ctx.Err())
		}
	}

	stopFallbackTimer()
	cancelConnectionsExcept("")
	return nil, errors.New("failed to connect to any relay server: all attempts failed")
}

func (sp *ServerPicker) availableServerURLs(config pickerConfig, serverURLs []string, now time.Time) []string {
	if sp.CooldownDuration <= 0 || len(serverURLs) == 0 {
		return serverURLs
	}
	sp.cooldownMu.Lock()
	defer sp.cooldownMu.Unlock()

	forcedPresent := config.forcedURL != "" && slices.Contains(serverURLs, config.forcedURL)
	available := make([]string, 0, len(serverURLs))
	if forcedPresent {
		// A manual override always gets a chance immediately, even when its
		// previous attempt put it into cooldown.
		available = append(available, config.forcedURL)
	}
	unavailableOthers := 0
	for _, relayURL := range serverURLs {
		if relayURL == config.forcedURL && forcedPresent {
			delete(sp.cooldowns, relayURL)
			continue
		}
		until, ok := sp.cooldowns[relayURL]
		if !ok || !now.Before(until) {
			delete(sp.cooldowns, relayURL)
			available = append(available, relayURL)
		} else {
			unavailableOthers++
		}
	}
	allNonForcedCoolingDown := forcedPresent && unavailableOthers > 0 && len(available) == 1
	if len(available) == 0 || allNonForcedCoolingDown {
		// When every non-forced relay is cooling down, keep the full fallback
		// list so a failed forced attempt can still try them after its own
		// failure. Without a forced URL this preserves the historical behavior
		// of trying every relay once the cooldown list is exhausted.
		fallback := sp.serverURLsByCooldownExpiryLocked(config, serverURLs)
		if !forcedPresent {
			return fallback
		}
		ordered := make([]string, 0, len(fallback))
		ordered = append(ordered, config.forcedURL)
		for _, relayURL := range fallback {
			if relayURL != config.forcedURL {
				ordered = append(ordered, relayURL)
			}
		}
		return ordered
	}
	return available
}

// serverURLsByCooldownExpiryLocked orders fully cooled-down servers so the one
// recovering first is retried first. Weight grouping stays authoritative: only
// servers of equal weight are reordered against each other.
// Caller holds cooldownMu.
func (sp *ServerPicker) serverURLsByCooldownExpiryLocked(config pickerConfig, serverURLs []string) []string {
	ordered := slices.Clone(serverURLs)
	slices.SortStableFunc(ordered, func(left, right string) int {
		if weightDiff := config.weight(right) - config.weight(left); weightDiff != 0 {
			return weightDiff
		}
		return sp.cooldowns[left].Compare(sp.cooldowns[right])
	})
	return ordered
}

func (sp *ServerPicker) markServerFailure(relayURL string, now time.Time, err error) {
	// Local cancellation is not a relay failure and never triggers cooldown.
	// A deadline-exceeded error is: a relay too slow to answer within the
	// connection timeout is equivalent to a down relay for routing, so it
	// shares the same cooldown and backoff instead of being retried every
	// round. errors.Is matches wrapped %w timeouts too.
	if sp.CooldownDuration <= 0 || relayURL == "" || errors.Is(err, context.Canceled) {
		return
	}
	sp.cooldownMu.Lock()
	if sp.cooldowns == nil {
		sp.cooldowns = make(map[string]time.Time)
	}
	if sp.failures == nil {
		sp.failures = make(map[string]int)
	}
	sp.failures[relayURL]++
	duration := sp.CooldownDuration
	for range min(sp.failures[relayURL]-1, 6) {
		duration = min(duration*2, 5*time.Minute)
	}
	duration += time.Duration(rand.Int64N(max(1, int64(duration/5))))
	sp.cooldowns[relayURL] = now.Add(duration)
	sp.cooldownMu.Unlock()
}

func (sp *ServerPicker) clearServerFailure(relayURL string) {
	sp.cooldownMu.Lock()
	delete(sp.cooldowns, relayURL)
	delete(sp.failures, relayURL)
	sp.cooldownMu.Unlock()
}

func (sp *ServerPicker) startNextPriorityGroup(serverURLs []string, startAt int, startConnection func(string)) int {
	return sp.startNextPriorityGroupWithConfig(sp.loadConfig(), serverURLs, startAt, startConnection)
}

func (sp *ServerPicker) startNextPriorityGroupWithConfig(config pickerConfig, serverURLs []string, startAt int, startConnection func(string)) int {
	if startAt >= len(serverURLs) {
		return startAt
	}
	if config.forcedURL != "" && serverURLs[startAt] == config.forcedURL {
		startConnection(serverURLs[startAt])
		return startAt + 1
	}
	weight := config.weight(serverURLs[startAt])
	// Find the full same-weight group so the lowest-RTT relay of the whole
	// group is attempted first, even when the group is larger than one
	// attempt batch. The scan stops before the forced relay, which is
	// dialed on its own when a batch starts on it.
	groupStart := startAt
	for groupStart > 0 && serverURLs[groupStart-1] != config.forcedURL && config.weight(serverURLs[groupStart-1]) == weight {
		groupStart--
	}
	groupEnd := startAt
	for groupEnd < len(serverURLs) && config.weight(serverURLs[groupEnd]) == weight {
		groupEnd++
	}
	group := slices.Clone(serverURLs[groupStart:groupEnd])
	// Order the group by cached dial RTT so the lowest-latency relay wins the
	// race. Relays without a cached RTT keep their configured relative order
	// at the back. RTTs within the hysteresis band count as equal so probe
	// noise does not reorder the group.
	if sp.rttLookup != nil {
		slices.SortStableFunc(group, func(left, right string) int {
			leftRTT, leftOK := sp.rttLookup(left)
			rightRTT, rightOK := sp.rttLookup(right)
			if leftOK != rightOK {
				if leftOK {
					return -1
				}
				return 1
			}
			if !leftOK {
				return 0
			}
			if absDuration(leftRTT-rightRTT) <= relayRTTHysteresis {
				return 0
			}
			return cmp.Compare(leftRTT, rightRTT)
		})
	}
	// Attempt the next batch of the RTT-ordered group.
	offset := startAt - groupStart
	batch := group[offset:min(offset+maxConcurrentServers, len(group))]
	for _, relayURL := range batch {
		startConnection(relayURL)
	}
	return startAt + len(batch)
}

func (config pickerConfig) weight(relayURL string) int {
	if config.serverWeights[relayURL] <= 0 {
		return defaultRelayWeight
	}
	return config.serverWeights[relayURL]
}

func (sp *ServerPicker) loadConfig() pickerConfig {
	if config, ok := sp.config.Load().(pickerConfig); ok {
		return config
	}
	urls, _ := sp.ServerURLs.Load().([]string)
	weights, _ := sp.ServerWeights.Load().(map[string]int)
	forced := ""
	if value := sp.ForcedServerURL.Load(); value != nil {
		forced = *value
	}
	return pickerConfig{slices.Clone(urls), maps.Clone(weights), forced}
}

func (sp *ServerPicker) relayURLWeight(relayURL string) int {
	return sp.loadConfig().weight(relayURL)
}

func (sp *ServerPicker) setForcedServerURL(relayURL string) {
	config := sp.loadConfig()
	config.forcedURL = relayURL
	sp.storeConfig(config)
}

func (sp *ServerPicker) storeConfig(config pickerConfig) {
	config.serverURLs = slices.Clone(config.serverURLs)
	config.serverWeights = maps.Clone(config.serverWeights)
	sp.config.Store(config)
	// Keep legacy observability fields synchronized for callers outside picker.
	sp.ServerURLs.Store(slices.Clone(config.serverURLs))
	sp.ServerWeights.Store(maps.Clone(config.serverWeights))
	if config.forcedURL == "" {
		sp.ForcedServerURL.Store(nil)
	} else {
		forced := config.forcedURL
		sp.ForcedServerURL.Store(&forced)
	}
}

func (sp *ServerPicker) drainConnResults(resultChan <-chan connResult, receivedResults, startedServers int) {
	for ; receivedResults < startedServers; receivedResults++ {
		cr := <-resultChan
		if cr.Err == nil && cr.RelayClient != nil {
			_ = cr.RelayClient.Close()
		}
	}
}

func (sp *ServerPicker) startConnection(connectCtx, lifecycleCtx context.Context, resultChan chan connResult, url string) {
	log.Infof("try to connecting to relay server: %s", url)
	relayClient := NewClient(url, sp.TokenStore, sp.PeerID, sp.MTU)
	relayClient.SetTransportFallback(sp.TransportFallback)
	relayClient.netEvents = sp.NetEvents
	err := relayClient.connectWithContexts(connectCtx, lifecycleCtx)
	resultChan <- connResult{
		RelayClient: relayClient,
		Url:         url,
		Err:         err,
	}
}

// pickErr combines per-server connection failures into a single error.
func pickErr(errs []error) error {
	if len(errs) == 0 {
		return errors.New("no relay server available")
	}
	return errors.Join(errs...)
}
