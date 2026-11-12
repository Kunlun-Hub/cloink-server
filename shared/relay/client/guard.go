package client

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
)

const (
	defaultMaxBackoffInterval = 60 * time.Second

	// quickReconnectBudget bounds how long a quick reconnect waits for the
	// network before handing the retry over to the ticker.
	quickReconnectBudget = 1500 * time.Millisecond

	// verdictSettleWindow is how long an online verdict must hold before it
	// is trusted: the disconnect often precedes the OS offline flag by a few
	// milliseconds.
	verdictSettleWindow = 200 * time.Millisecond
)

// NetworkWatcher is the availability view the guard gates reconnects on.
type NetworkWatcher interface {
	Wait(ctx context.Context) (bool, error)
	IsOnline() bool
	WaitSettled(ctx context.Context, budget, settleWindow time.Duration) bool
}

// Guard manage the reconnection tries to the Relay server in case of disconnection event.
type Guard struct {
	// OnNewRelayClient is a channel that is used to notify the relay manager about a new relay client instance.
	OnNewRelayClient chan *Client
	OnReconnected    chan struct{}
	serverPicker     *ServerPicker

	// maxBackoffInterval caps the exponential backoff between reconnect
	// attempts.
	maxBackoffInterval time.Duration

	// netWatcher gates reconnect attempts on OS-reported network availability.
	netWatcher NetworkWatcher

	// lastErr is the error from the most recent failed reconnect attempt,
	// surfaced as the home relay status while disconnected.
	lastErr atomic.Pointer[error]

	// reconnecting prevents overlapping reconnect rounds when several
	// disconnect events arrive for the same home relay.
	reconnecting atomic.Bool

	// backoffReset is bumped when a connection has been stable long enough to
	// forget the previous failure streak. A running retry loop checks it before
	// using the next ticker interval.
	backoffReset atomic.Uint64
}

// NewGuard creates a new guard for the relay client. A non-positive
// maxBackoffInterval falls back to defaultMaxBackoffInterval.
func NewGuard(sp *ServerPicker, maxBackoffInterval time.Duration, netWatcher NetworkWatcher) *Guard {
	if maxBackoffInterval <= 0 {
		maxBackoffInterval = defaultMaxBackoffInterval
	}
	g := &Guard{
		OnNewRelayClient:   make(chan *Client, 1),
		OnReconnected:      make(chan struct{}, 1),
		serverPicker:       sp,
		maxBackoffInterval: maxBackoffInterval,
		netWatcher:         netWatcher,
	}
	return g
}

// LastError returns the error from the most recent failed reconnect attempt, or
// nil if reconnection last succeeded.
func (g *Guard) LastError() error {
	if p := g.lastErr.Load(); p != nil {
		return *p
	}
	return nil
}

// StartReconnectTrys is called when the relay client is disconnected from the relay server.
// It attempts to reconnect to the relay server. The function first tries a quick reconnect
// to the same server that was used before, if the server URL is still valid. If the quick
// reconnect fails, it starts a ticker to periodically attempt server picking until it
// succeeds or the context is done.
//
// Parameters:
// - ctx: The context to control the lifecycle of the reconnection attempts.
// - relayClient: The relay client instance that was disconnected.
func (g *Guard) StartReconnectTrys(ctx context.Context, relayClient *Client) {
	if !g.reconnecting.CompareAndSwap(false, true) {
		log.Debug("reconnect attempt already active; ignoring duplicate request")
		return
	}
	defer g.reconnecting.Store(false)

	if g.serverPicker != nil && g.serverPicker.takeHotStandby != nil {
		if hot := g.serverPicker.takeHotStandby(); hot != nil {
			log.Infof("promoting preconnected standby Relay %s immediately", hot.connectionURL)
			g.setLastError(nil)
			g.drainRelayClientChan()
			g.OnNewRelayClient <- hot
			return
		}
	}

	if relayClient != nil && g.hasAlternativeRelay(relayClient.connectionURL) {
		if g.serverPicker.loadConfig().forcedURL != relayClient.connectionURL {
			g.serverPicker.markServerFailure(relayClient.connectionURL, time.Now(), ErrServerDisconnected)
		}
		canTryImmediate := true
		if g.netWatcher != nil && !g.netWatcher.IsOnline() {
			if !g.netWatcher.WaitSettled(ctx, quickReconnectBudget, verdictSettleWindow) || !g.netWatcher.IsOnline() {
				canTryImmediate = false
			}
		}
		if canTryImmediate {
			if err := g.failoverFrom(ctx, relayClient.connectionURL); err == nil {
				return
			} else {
				log.Errorf("immediate standby failover from %s failed: %s", relayClient.connectionURL, err)
				g.setLastError(err)
			}
		}
	} else if ok := g.tryToQuickReconnect(ctx, relayClient); ok {
		if g.isServerURLStillValid(relayClient) {
			g.notifyReconnected()
			return
		}
		_ = relayClient.Close()
	}

	// start a ticker to pick a new server
	ticker := g.exponentTicker(ctx)
	backoffGeneration := g.backoffReset.Load()
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			if generation := g.backoffReset.Load(); generation != backoffGeneration {
				backoffGeneration = generation
				ticker.Stop()
				ticker = g.exponentTicker(ctx)
				log.Infof("relay reconnect backoff reset after a stable connection")
				continue
			}
			// suspend reconnect attempts while the OS reports no usable network
			if g.netWatcher != nil {
				if waited, err := g.netWatcher.Wait(ctx); err != nil {
					return
				} else if waited {
					ticker.Stop()
					ticker = g.exponentTicker(ctx)
					continue
				}
			}
			if err := g.retry(ctx); err != nil {
				log.Errorf("failed to pick new Relay server: %s", err)
				g.setLastError(err)
				continue
			}
			return
		case <-ctx.Done():
			return
		}
	}
}

func (g *Guard) setLastError(err error) {
	g.lastErr.Store(&err)
}

func (g *Guard) tryToQuickReconnect(parentCtx context.Context, rc *Client) bool {
	if rc == nil {
		return false
	}

	if !g.isServerURLStillValid(rc) {
		return false
	}

	if g.netWatcher != nil {
		if ok := g.netWatcher.WaitSettled(parentCtx, quickReconnectBudget, verdictSettleWindow); !ok {
			return false
		}
		// Still offline after the budget: leave the retry to the ticker.
		if !g.netWatcher.IsOnline() {
			return false
		}
	} else {
		if cancelled := waitBeforeRetry(parentCtx); !cancelled {
			return false
		}
	}

	log.Infof("try to reconnect to Relay server: %s", rc.connectionURL)

	if err := rc.Connect(parentCtx); err != nil {
		log.Errorf("failed to reconnect to relay server: %s", err)
		g.setLastError(err)
		return false
	}
	return true
}

func (g *Guard) hasAlternativeRelay(failedURL string) bool {
	if g.serverPicker == nil {
		return false
	}
	for _, u := range g.serverPicker.loadConfig().serverURLs {
		if u != "" && u != failedURL {
			return true
		}
	}
	return false
}

func (g *Guard) failoverFrom(ctx context.Context, failedURL string) error {
	configURLs := g.serverPicker.loadConfig().serverURLs
	standbyURL := ""
	if g.serverPicker.standbyLookup != nil {
		standbyURL = g.serverPicker.standbyLookup()
	}
	if standbyURL == failedURL || !slices.Contains(configURLs, standbyURL) {
		standbyURL = ""
	}
	fallbackURLs := make([]string, 0, len(configURLs))
	for _, u := range configURLs {
		if u != "" && u != failedURL && u != standbyURL {
			fallbackURLs = append(fallbackURLs, u)
		}
	}
	if standbyURL == "" && len(fallbackURLs) == 0 {
		return g.retry(ctx)
	}
	relayClient, err := g.serverPicker.PickStandbyWithFallback(ctx, standbyURL, fallbackURLs)
	if err != nil {
		return err
	}
	g.setLastError(nil)
	g.drainRelayClientChan()
	g.OnNewRelayClient <- relayClient
	return nil
}

func (g *Guard) retry(ctx context.Context) error {
	log.Infof("try to pick up a new Relay server")
	relayClient, err := g.serverPicker.PickServer(ctx)
	if err != nil {
		return err
	}
	g.setLastError(nil)

	// prevent to work with a deprecated Relay client instance
	g.drainRelayClientChan()

	g.OnNewRelayClient <- relayClient
	return nil
}

func (g *Guard) drainRelayClientChan() {
	select {
	case <-g.OnNewRelayClient:
	default:
	}
}

func (g *Guard) isServerURLStillValid(rc *Client) bool {
	for _, url := range g.serverPicker.loadConfig().serverURLs {
		if url == rc.connectionURL {
			return true
		}
	}
	return false
}

func (g *Guard) notifyReconnected() {
	g.setLastError(nil)
	select {
	case g.OnReconnected <- struct{}{}:
	default:
	}
}

func (g *Guard) exponentTicker(ctx context.Context) *backoff.Ticker {
	bo := backoff.WithContext(&backoff.ExponentialBackOff{
		InitialInterval: 2 * time.Second,
		// Spreads the reconnects of every client that lost the same relay server.
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          2,
		MaxInterval:         g.maxBackoffInterval,
		Clock:               backoff.SystemClock,
	}, ctx)

	return backoff.NewTicker(bo)
}

// ResetBackoff tells an in-flight reconnect loop to discard its accumulated
// exponential delay. It is called after the home relay has stayed connected for
// a full stability window, so the next failure starts from the short interval.
func (g *Guard) ResetBackoff() {
	g.backoffReset.Add(1)
}

func waitBeforeRetry(ctx context.Context) bool {
	timer := time.NewTimer(quickReconnectBudget)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
