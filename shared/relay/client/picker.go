package client

import (
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
)

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
	CooldownDuration  time.Duration

	cooldownMu sync.Mutex
	cooldowns  map[string]time.Time
	failures   map[string]int
}

func (sp *ServerPicker) PickServer(parentCtx context.Context) (*Client, error) {
	ctx, cancel := context.WithTimeout(parentCtx, sp.ConnectionTimeout)
	defer cancel()

	config := sp.loadConfig()
	serverURLs := sp.availableServerURLs(config.serverURLs, time.Now())
	totalServers := len(serverURLs)
	if totalServers == 0 {
		return nil, errors.New("failed to connect to any relay server: all attempts failed")
	}

	connResultChan := make(chan connResult, totalServers)
	concurrentLimiter := make(chan struct{}, maxConcurrentServers)
	startedServers := 0
	connectionCancels := make(map[string]context.CancelFunc, totalServers)

	startConnection := func(url string) {
		concurrentLimiter <- struct{}{}
		startedServers++
		connectionCtx, connectionCancel := context.WithTimeout(parentCtx, sp.ConnectionTimeout)
		connectionCancels[url] = connectionCancel
		go func(url string) {
			defer func() { <-concurrentLimiter }()
			sp.startConnection(connectionCtx, parentCtx, connResultChan, url)
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
	receivedResults := 0
	for receivedResults < startedServers || startedUpTo < totalServers {
		select {
		case cr := <-connResultChan:
			receivedResults++
			if cr.Err == nil {
				log.Infof("chosen home Relay server: %s", cr.Url)
				sp.clearServerFailure(cr.Url)
				cancelConnectionsExcept(cr.Url)
				go sp.drainConnResults(connResultChan, receivedResults, startedServers)
				return cr.RelayClient, nil
			}

			log.Tracef("failed to connect to Relay server: %s: %v", cr.Url, cr.Err)
			sp.markServerFailure(cr.Url, time.Now(), cr.Err)
			if receivedResults == startedServers && startedUpTo < totalServers {
				startedUpTo = sp.startNextPriorityGroupWithConfig(config, serverURLs, startedUpTo, startConnection)
			}
		case <-ctx.Done():
			cancelConnectionsExcept("")
			return nil, fmt.Errorf("failed to connect to any relay server: %w", ctx.Err())
		}
	}

	cancelConnectionsExcept("")
	return nil, errors.New("failed to connect to any relay server: all attempts failed")
}

func (sp *ServerPicker) availableServerURLs(serverURLs []string, now time.Time) []string {
	if sp.CooldownDuration <= 0 || len(serverURLs) == 0 {
		return serverURLs
	}
	sp.cooldownMu.Lock()
	defer sp.cooldownMu.Unlock()
	available := make([]string, 0, len(serverURLs))
	for _, relayURL := range serverURLs {
		until, ok := sp.cooldowns[relayURL]
		if !ok || !now.Before(until) {
			delete(sp.cooldowns, relayURL)
			available = append(available, relayURL)
		}
	}
	if len(available) == 0 {
		return serverURLs
	}
	return available
}

func (sp *ServerPicker) markServerFailure(relayURL string, now time.Time, err error) {
	if sp.CooldownDuration <= 0 || relayURL == "" || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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
	idx := startAt
	for capacity := maxConcurrentServers; idx < len(serverURLs) && config.weight(serverURLs[idx]) == weight && capacity > 0; capacity-- {
		startConnection(serverURLs[idx])
		idx++
	}
	return idx
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
