package client

import (
	"context"
	"errors"
	"fmt"
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

type ServerPicker struct {
	TokenStore        *auth.TokenStore
	ServerURLs        atomic.Value
	ServerWeights     atomic.Value
	ForcedServerURL   atomic.Pointer[string]
	PeerID            string
	MTU               uint16
	ConnectionTimeout time.Duration
	TransportFallback *transportFallback
	CooldownDuration  time.Duration

	cooldownMu sync.Mutex
	cooldowns  map[string]time.Time
}

func (sp *ServerPicker) PickServer(parentCtx context.Context) (*Client, error) {
	ctx, cancel := context.WithTimeout(parentCtx, sp.ConnectionTimeout)
	defer cancel()

	serverURLs := sp.availableServerURLs(sp.ServerURLs.Load().([]string), time.Now())
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
		connectionCtx, connectionCancel := context.WithCancel(parentCtx)
		connectionCancels[url] = connectionCancel
		go func(url string) {
			defer func() { <-concurrentLimiter }()
			sp.startConnection(connectionCtx, connResultChan, url)
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
	startedUpTo := sp.startNextPriorityGroup(serverURLs, 0, startConnection)
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
				startedUpTo = sp.startNextPriorityGroup(serverURLs, startedUpTo, startConnection)
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

func (sp *ServerPicker) markServerFailure(relayURL string, now time.Time, _ error) {
	if sp.CooldownDuration <= 0 || relayURL == "" {
		return
	}
	sp.cooldownMu.Lock()
	if sp.cooldowns == nil {
		sp.cooldowns = make(map[string]time.Time)
	}
	sp.cooldowns[relayURL] = now.Add(sp.CooldownDuration)
	sp.cooldownMu.Unlock()
}

func (sp *ServerPicker) clearServerFailure(relayURL string) {
	sp.cooldownMu.Lock()
	delete(sp.cooldowns, relayURL)
	sp.cooldownMu.Unlock()
}

func (sp *ServerPicker) startNextPriorityGroup(serverURLs []string, startAt int, startConnection func(string)) int {
	if startAt >= len(serverURLs) {
		return startAt
	}
	if forcedURL := sp.ForcedServerURL.Load(); forcedURL != nil && serverURLs[startAt] == *forcedURL {
		startConnection(serverURLs[startAt])
		return startAt + 1
	}
	weight := sp.relayURLWeight(serverURLs[startAt])
	idx := startAt
	for capacity := maxConcurrentServers; idx < len(serverURLs) && sp.relayURLWeight(serverURLs[idx]) == weight && capacity > 0; capacity-- {
		startConnection(serverURLs[idx])
		idx++
	}
	return idx
}

func (sp *ServerPicker) relayURLWeight(relayURL string) int {
	weights, ok := sp.ServerWeights.Load().(map[string]int)
	if !ok || weights[relayURL] <= 0 {
		return defaultRelayWeight
	}
	return weights[relayURL]
}

func (sp *ServerPicker) setForcedServerURL(relayURL string) {
	if relayURL == "" {
		sp.ForcedServerURL.Store(nil)
		return
	}
	sp.ForcedServerURL.Store(&relayURL)
}

func (sp *ServerPicker) drainConnResults(resultChan <-chan connResult, receivedResults, startedServers int) {
	for ; receivedResults < startedServers; receivedResults++ {
		cr := <-resultChan
		if cr.Err == nil && cr.RelayClient != nil {
			_ = cr.RelayClient.Close()
		}
	}
}

func (sp *ServerPicker) startConnection(ctx context.Context, resultChan chan connResult, url string) {
	log.Infof("try to connecting to relay server: %s", url)
	relayClient := NewClient(url, sp.TokenStore, sp.PeerID, sp.MTU)
	relayClient.SetTransportFallback(sp.TransportFallback)
	err := relayClient.Connect(ctx)
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
