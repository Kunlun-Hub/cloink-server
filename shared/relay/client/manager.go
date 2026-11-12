package client

import (
	"container/list"
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	relayAuth "github.com/netbirdio/netbird/shared/relay/auth/hmac"
)

var (
	relayCleanupInterval  = 60 * time.Second
	keepUnusedServerTime  = 5 * time.Second
	relayProbeTimeout     = 6 * time.Second
	relayServerCooldown   = 2 * time.Minute
	relayMigrationGrace   = 30 * time.Second
	relayMigrationMinWait = time.Second
	defaultRelayWeight    = 30
	// relayRTTCacheTTL bounds how long a probed dial RTT stays eligible for
	// in-priority-group relay ordering.
	relayRTTCacheTTL = 5 * time.Minute
	// relayRTTProbeInterval is how often the background probe loop refreshes
	// dial RTTs for relays sharing the top priority group.
	relayRTTProbeInterval = 60 * time.Second
	// relayRTTProbeJitter bounds the random jitter added to each probe tick,
	// so a fleet of clients does not probe in lockstep.
	relayRTTProbeJitter = 15 * time.Second
	// relayRTTEWMAAlpha is the weight of a fresh sample in the smoothed RTT.
	relayRTTEWMAAlpha = 0.3
	// relayRTTMinSamples is how many probes a relay needs before its RTT
	// influences picker ordering. A single sample is too noisy to trust.
	relayRTTMinSamples = 2
	// relayRTTHysteresisMs is the absolute RTT band inside which two relays
	// are treated as equally fast, so the picker and standby selection do not
	// switch on probe noise.
	relayRTTHysteresisMs = 25
	// relayRTTMigrationStreaks is how many consecutive probe rounds the home
	// Relay must be significantly slower than a healthy alternative before a
	// preconnected migration is attempted. Persistence avoids moving traffic
	// for a single noisy sample.
	relayRTTMigrationStreaks = 3
	// relayRTTMigrationHighRTT is the boundary between the high- and
	// low-latency migration rules.
	relayRTTMigrationHighRTT = 100 * time.Millisecond
	// relayRTTMigrationNoiseFloor filters probe noise at very low RTTs. It is
	// deliberately much smaller than the old 50ms hard requirement.
	relayRTTMigrationNoiseFloor = 10 * time.Millisecond
	// dataPlaneRecoveryRetryInterval is the initial backoff before retrying a
	// failed home-relay data-plane failover when all candidate relays failed.
	dataPlaneRecoveryRetryInterval = 2 * time.Second
	// relayStableResetInterval is how long a home relay must stay connected
	// before its reconnect backoff and transient failure history are reset.
	relayStableResetInterval = 60 * time.Second
	// relayProactiveSwitchMinResidence prevents RTT-based migration from
	// oscillating between two acceptable relays. Failover after a real
	// disconnect bypasses this guard.
	relayProactiveSwitchMinResidence = 5 * time.Minute

	ErrRelayClientNotConnected = fmt.Errorf("relay client not connected")
)

// EnvAutoFailback is retained for backwards compatibility with callers.
const EnvAutoFailback = "NB_RELAY_AUTO_FAILBACK"

// RelayTrack hold the relay clients for the foreign relay servers.
// With the mutex can ensure we can open new connection in case the relay connection has been established with
// the relay server.
type RelayTrack struct {
	sync.RWMutex
	relayClient *Client
	err         error
	created     time.Time
	// ready is closed once the dial started by openConnVia finishes (relayClient
	// or err is set). Callers reusing a track wait on this instead of the track
	// lock, so the dial never runs under rt.Lock.
	ready chan struct{}
}

func NewRelayTrack() *RelayTrack {
	return &RelayTrack{
		created: time.Now(),
		ready:   make(chan struct{}),
	}
}

type OnServerCloseListener func()

// RelayServerInfo describes a configured relay and its local state.
type RelayServerInfo struct {
	URL       string
	Weight    int
	Preferred bool
	Forced    bool
	Current   bool
	Available bool
	Error     string
	// RTTMs is the last probed dial round-trip time in milliseconds, or zero
	// when no fresh probe result is cached.
	RTTMs       int64
	LiveRTTMs   int64
	P95RTTMs    int64
	JitterMs    int64
	LossPercent int
	Transport   string
	// CooldownRemaining is non-zero while selection currently avoids this
	// relay after a failure.
	CooldownRemaining time.Duration
	LastSwitchReason  string
	LastSwitchAt      time.Time
	SwitchCount       int
}

// relayRTTCache stores dial round-trip times measured by relay probes with a
// TTL, so the server picker can prefer low-latency relays inside a priority
// group. Samples are smoothed with an EWMA to damp single-probe noise.
type relayRTTCache struct {
	mu      sync.RWMutex
	entries map[string]relayRTTEntry
}

type relayRTTEntry struct {
	rtt        time.Duration
	jitter     time.Duration
	lastSample time.Duration
	samples    int
	outcomes   []bool
	expiresAt  time.Time
}

func newRelayRTTCache() *relayRTTCache {
	return &relayRTTCache{entries: make(map[string]relayRTTEntry)}
}

// get returns the smoothed RTT. It reports false when the entry is missing,
// expired, or has fewer than relayRTTMinSamples probes.
func (c *relayRTTCache) get(relayURL string) (time.Duration, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.RLock()
	entry, ok := c.entries[relayURL]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) || entry.samples < relayRTTMinSamples {
		return 0, false
	}
	return entry.rtt, true
}

// latest returns the most recent smoothed RTT even when only a single probe
// sample has been recorded so far.
func (c *relayRTTCache) latest(relayURL string) (time.Duration, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.RLock()
	entry, ok := c.entries[relayURL]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) || entry.samples < 1 {
		return 0, false
	}
	return entry.rtt, true
}

// set folds a fresh probe sample into the entry's EWMA and refreshes its TTL.
func (c *relayRTTCache) set(relayURL string, sample time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	entry, ok := c.entries[relayURL]
	if !ok {
		entry = relayRTTEntry{rtt: sample}
	} else {
		if entry.lastSample > 0 {
			jitter := absDuration(sample - entry.lastSample)
			if entry.jitter == 0 {
				entry.jitter = jitter
			} else {
				entry.jitter = time.Duration(float64(jitter)*relayRTTEWMAAlpha + float64(entry.jitter)*(1-relayRTTEWMAAlpha))
			}
		}
		entry.rtt = time.Duration(float64(sample)*relayRTTEWMAAlpha + float64(entry.rtt)*(1-relayRTTEWMAAlpha))
	}
	entry.lastSample = sample
	entry.samples++
	entry.outcomes = appendRelayProbeOutcome(entry.outcomes, true)
	entry.expiresAt = time.Now().Add(relayRTTCacheTTL)
	c.entries[relayURL] = entry
	c.mu.Unlock()
}

// recordFailure adds a failed probe to the recent-loss window without
// discarding the last successful RTT. Selection still uses cooldown to decide
// whether the relay is currently eligible.
func (c *relayRTTCache) recordFailure(relayURL string) {
	if c == nil || relayURL == "" {
		return
	}
	c.mu.Lock()
	entry := c.entries[relayURL]
	entry.outcomes = appendRelayProbeOutcome(entry.outcomes, false)
	entry.expiresAt = time.Now().Add(relayRTTCacheTTL)
	c.entries[relayURL] = entry
	c.mu.Unlock()
}

func appendRelayProbeOutcome(outcomes []bool, success bool) []bool {
	outcomes = append(outcomes, success)
	if len(outcomes) > relayQualityOutcomeWindow {
		outcomes = outcomes[len(outcomes)-relayQualityOutcomeWindow:]
	}
	return outcomes
}

// snapshot returns the smoothed RTT together with the current jitter and
// loss ratio. It is intentionally separate from get so ordering can penalise
// unstable links while the CLI still displays the raw smoothed RTT.
func (c *relayRTTCache) snapshot(relayURL string) (relayRTTSnapshot, bool) {
	if c == nil {
		return relayRTTSnapshot{}, false
	}
	c.mu.RLock()
	entry, ok := c.entries[relayURL]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) || entry.samples < 1 {
		return relayRTTSnapshot{}, false
	}
	loss := 0
	if len(entry.outcomes) > 0 {
		failures := 0
		for _, success := range entry.outcomes {
			if !success {
				failures++
			}
		}
		loss = failures * 100 / len(entry.outcomes)
	}
	return relayRTTSnapshot{
		RTT:         entry.rtt,
		Jitter:      entry.jitter,
		LossPercent: loss,
		Samples:     entry.samples,
	}, true
}

// score is the ordering value used by the picker. Jitter is weighted more
// heavily than a single RTT sample because a link that alternates between fast
// and slow causes more user-visible stalls than a consistently slower one. A
// link losing at least a quarter of its probes is penalised by half its RTT.
func (c *relayRTTCache) score(relayURL string) (time.Duration, bool) {
	snapshot, ok := c.snapshot(relayURL)
	if !ok || snapshot.Samples < relayRTTMinSamples {
		return 0, false
	}
	score := snapshot.RTT + snapshot.Jitter*2
	if snapshot.LossPercent >= 25 {
		score += snapshot.RTT / 2
	}
	return score, true
}

func (c *relayRTTCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.entries)
	c.mu.Unlock()
}

type relayRTTSnapshot struct {
	RTT         time.Duration
	Jitter      time.Duration
	LossPercent int
	Samples     int
}

// ManagerOption configures a Manager at construction time.
type ManagerOption func(*Manager)

// RelayConnState is the connection state of a single relay server.
type RelayConnState struct {
	// URL is the server's instance address when connected, otherwise the
	// configured server URL.
	URL string
	// Transport is the negotiated transport, empty if not connected.
	Transport string
	// Err is set when the relay is not connected.
	Err error
}

// WithMaxBackoffInterval caps the exponential backoff between reconnect
// attempts to the home relay. A non-positive value keeps the default.
func WithMaxBackoffInterval(d time.Duration) ManagerOption {
	return func(m *Manager) { m.maxBackoffInterval = d }
}

// WithRelayServerCooldown sets how long failed relay servers are skipped.
func WithRelayServerCooldown(d time.Duration) ManagerOption {
	return func(m *Manager) { m.relayServerCooldown = d }
}

// WithAutoFailback enables periodic checks that move the home Relay back to a
// higher-priority server once it has recovered and proven stable. It only ever
// moves upward to a strictly higher weight, never downward. Disabled by
// default.
func WithAutoFailback(enabled bool) ManagerOption {
	return func(m *Manager) { m.autoFailback = enabled }
}

// WithNetEvents injects the OS network event handling.
func WithNetEvents(events NetEvents) ManagerOption {
	return func(m *Manager) { m.netEvents = events }
}

// WithRelayMigrationGrace sets how long an old home Relay client may remain
// alive while peer connections migrate to its replacement.
func WithRelayMigrationGrace(d time.Duration) ManagerOption {
	return func(m *Manager) {
		if d > 0 {
			m.relayMigrationGrace = d
		}
	}
}

// Manager is a manager for the relay client instances. It establishes one persistent connection to the given relay URL
// and automatically reconnect to them in case disconnection.
// The manager also manage temporary relay connection. If a client wants to communicate with a client on a
// different relay server, the manager will establish a new connection to the relay server. The connection with these
// relay servers will be closed if there is no active connection. Periodically the manager will check if there is any
// unused relay connection and close it.
type Manager struct {
	ctx          context.Context
	peerID       string
	running      atomic.Bool
	tokenStore   *relayAuth.TokenStore
	serverPicker *ServerPicker

	relayClient *Client
	// the guard logic can overwrite the relayClient variable, this mutex protect the usage of the variable
	relayClientMu  sync.RWMutex
	reconnectGuard *Guard

	relayClients      map[string]*RelayTrack
	relayClientsMutex sync.RWMutex

	// rttCache holds dial RTTs measured by the relay probes so the server
	// picker can order same-weight relays by latency.
	rttCache *relayRTTCache
	// probeMu serializes background probe rounds. A probe round can end in a
	// preconnected home-relay migration, so overlapping rounds must not race.
	probeMu sync.Mutex
	// rttMigrationMu protects the multi-round RTT migration decision state.
	rttMigrationMu        sync.Mutex
	rttMigrationHomeURL   string
	rttMigrationTargetURL string
	rttMigrationStreak    int

	// A standby session is preconnected as soon as a materially better relay
	// is detected. Promotion then reuses the ready session instead of paying a
	// full dial during failover.
	hotStandbyMu         sync.Mutex
	hotStandbyURL        string
	hotStandbyClient     *Client
	hotStandbyDialing    bool
	hotStandbyCancel     context.CancelFunc
	hotStandbyGeneration uint64

	// stableResetMu guards the timer that clears transient failure history
	// after a home relay has remained connected long enough.
	stableResetMu    sync.Mutex
	stableResetTimer *time.Timer

	networkChangeMu   sync.Mutex
	lastNetworkChange time.Time

	onDisconnectedListeners map[*Client]*list.List
	onReconnectedListenerFn func()
	listenerLock            sync.Mutex

	mtu                   uint16
	maxBackoffInterval    time.Duration
	netEvents             NetEvents
	relayServerCooldown   time.Duration
	switchMu              sync.Mutex
	relayConfigMu         sync.RWMutex
	configuredRelayURLs   []string
	relayWeights          map[string]int
	forcedRelayURL        string
	standbyRelayURL       string
	relayConfigGeneration atomic.Uint64
	relayMigrationGrace   time.Duration
	autoFailback          bool
	lastHomeSwitchAt      time.Time
	lastHomeSwitchReason  string
	homeSwitchCount       int

	cleanupInterval      time.Duration
	keepUnusedServerTime time.Duration

	// transportFallback is shared across home and foreign relay clients so a
	// datagram transport failure is remembered across reconnects.
	transportFallback *transportFallback
	dataPlaneFailures *relayDataPlaneFailures
	dataPlaneRetrying atomic.Bool
}

// NewManager creates a new manager instance.
// The serverURL address can be empty. In this case, the manager will not serve.
func NewManager(ctx context.Context, serverURLs []string, peerID string, mtu uint16, opts ...ManagerOption) *Manager {
	tokenStore := &relayAuth.TokenStore{}
	tf := newTransportFallback()

	m := &Manager{
		ctx:                 ctx,
		peerID:              peerID,
		tokenStore:          tokenStore,
		mtu:                 mtu,
		transportFallback:   tf,
		dataPlaneFailures:   newRelayDataPlaneFailures(),
		relayServerCooldown: relayServerCooldown,
		relayMigrationGrace: relayMigrationGrace,
		serverPicker: &ServerPicker{
			TokenStore:        tokenStore,
			PeerID:            peerID,
			MTU:               mtu,
			ConnectionTimeout: defaultConnectionTimeout,
			TransportFallback: tf,
		},
		relayClients:            make(map[string]*RelayTrack),
		onDisconnectedListeners: make(map[*Client]*list.List),
		cleanupInterval:         relayCleanupInterval,
		keepUnusedServerTime:    keepUnusedServerTime,
		rttCache:                newRelayRTTCache(),
	}
	m.serverPicker.rttLookup = m.rttCache.score
	m.serverPicker.standbyLookup = m.StandbyURL
	m.serverPicker.takeHotStandby = m.takeHotStandby
	for _, opt := range opts {
		opt(m)
	}
	m.serverPicker.NetEvents = m.netEvents
	m.serverPicker.CooldownDuration = m.relayServerCooldown
	m.configuredRelayURLs = slices.Clone(serverURLs)
	m.relayWeights = relayWeightsFromURLs(serverURLs)
	m.serverPicker.storeConfig(pickerConfig{
		serverURLs:    m.effectiveRelayURLsLocked(),
		serverWeights: maps.Clone(m.relayWeights),
	})
	m.relayConfigGeneration.Store(1)
	m.reconnectGuard = NewGuard(m.serverPicker, m.maxBackoffInterval, m.netEvents)
	return m
}

// ReportDataPlaneFailure records a WireGuard handshake timeout on a connection
// using relayAddress. Repeated failures rebuild the underlying Relay client,
// even when its transport still reports connected.
func (m *Manager) ReportDataPlaneFailure(relayAddress, peerKey string) {
	if m.dataPlaneFailures == nil || !m.dataPlaneFailures.reportFailure(relayAddress, peerKey) {
		return
	}
	go m.recoverRelayDataPlane(relayAddress)
}

// ReportDataPlaneSuccess clears a peer's pending Relay data-plane failures.
func (m *Manager) ReportDataPlaneSuccess(relayAddress, peerKey string) {
	if m.dataPlaneFailures != nil {
		m.dataPlaneFailures.reportSuccess(relayAddress, peerKey)
	}
}

func (m *Manager) recoverRelayDataPlane(relayAddress string) {
	if m.tryRecoverRelayDataPlane(relayAddress) {
		return
	}
	m.scheduleDataPlaneRecoveryRetry(relayAddress)
}

func (m *Manager) tryRecoverRelayDataPlane(relayAddress string) bool {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()
	if !m.running.Load() || m.ctx.Err() != nil {
		return true
	}

	m.relayClientMu.RLock()
	home := m.relayClient
	isHome := false
	homeURL := ""
	if home != nil {
		homeURL = home.connectionURL
		instanceURL, err := home.ServerInstanceURL()
		isHome = relayAddress == homeURL || err == nil && relayAddress == instanceURL
	}
	m.relayClientMu.RUnlock()

	if isHome {
		generation := m.relayConfigGeneration.Load()
		log.Warnf("Relay data plane is unresponsive on %s; preparing a replacement home Relay client", homeURL)
		m.markDataPlaneTransportFailure(home)
		m.markDataPlaneServerFailure(homeURL)
		candidateURLs := m.recoveryCandidateURLs(homeURL)
		standbyURL := m.StandbyURL()
		if standbyURL == homeURL || !slices.Contains(candidateURLs, standbyURL) {
			standbyURL = ""
		}
		var candidate *Client
		var err error
		if len(candidateURLs) > 0 {
			// Reuse an already connected standby before paying for another dial.
			// This also prevents two sessions to the same candidate racing to
			// replace each other on the relay server.
			candidate = m.takeHotStandby()
			if candidate != nil && !slices.Contains(candidateURLs, candidate.connectionURL) {
				_ = candidate.Close()
				candidate = nil
			}
			if candidate == nil {
				candidate, err = m.serverPicker.PickStandbyWithFallback(m.ctx, standbyURL, candidateURLs)
			}
		} else {
			// A Relay accepts one session per peer ID, so replacing the only
			// available URL cannot overlap old and new sessions.
			home.SetOnDisconnectListener(nil)
			m.notifyOnDisconnectListeners(home)
			_ = home.Close()
			candidate, err = m.serverPicker.PickServer(m.ctx)
		}
		if err != nil {
			if len(candidateURLs) == 0 {
				log.Errorf("failed to reconnect the only Relay %s: %v", homeURL, err)
				go m.reconnectGuard.StartReconnectTrys(m.ctx, home)
				return true
			}
			log.Errorf("failed to prepare replacement Relay client; keeping %s active and scheduling retry: %v", homeURL, err)
			if m.dataPlaneFailures != nil {
				m.dataPlaneFailures.resetCooldown(relayAddress)
			}
			return false
		}
		if !m.swapHomeRelay(home, candidate, generation) {
			_ = candidate.Close()
			m.relayClientMu.RLock()
			stillSameHome := m.relayClient == home
			m.relayClientMu.RUnlock()
			if stillSameHome && m.dataPlaneFailures != nil {
				m.dataPlaneFailures.resetCooldown(relayAddress)
			}
			return !stillSameHome
		}
		m.recordHomeSwitch("data-plane-recovery")
		log.Infof("replacement home Relay client %s is ready; migrating peer connections from %s", candidate.connectionURL, homeURL)
		m.onServerConnected()
		m.retireHomeRelay(home)
		return true
	}

	m.relayClientsMutex.RLock()
	track := m.relayClients[relayAddress]
	m.relayClientsMutex.RUnlock()
	if track == nil {
		return true
	}
	track.RLock()
	foreign := track.relayClient
	track.RUnlock()
	if foreign != nil {
		log.Warnf("Relay data plane is unresponsive on foreign server %s; rebuilding it", relayAddress)
		_ = foreign.Close()
	}
	return true
}

func (m *Manager) scheduleDataPlaneRecoveryRetry(relayAddress string) {
	if !m.dataPlaneRetrying.CompareAndSwap(false, true) {
		return
	}
	m.relayClientMu.RLock()
	failedHome := m.relayClient
	m.relayClientMu.RUnlock()
	if failedHome == nil {
		m.dataPlaneRetrying.Store(false)
		return
	}
	triggeredAt := time.Now()
	if m.dataPlaneFailures != nil && m.dataPlaneFailures.now != nil {
		triggeredAt = m.dataPlaneFailures.now()
	}

	go func() {
		defer m.dataPlaneRetrying.Store(false)
		delay := dataPlaneRecoveryRetryInterval
		if delay <= 0 {
			delay = 2 * time.Second
		}
		maxDelay := m.maxBackoffInterval
		if maxDelay <= 0 {
			maxDelay = defaultMaxBackoffInterval
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-timer.C:
				if !m.running.Load() || m.ctx.Err() != nil {
					return
				}
				m.relayClientMu.RLock()
				currentHome := m.relayClient
				m.relayClientMu.RUnlock()
				if currentHome == nil || currentHome != failedHome {
					return
				}
				if m.dataPlaneFailures != nil && m.dataPlaneFailures.hasSucceededSince(relayAddress, triggeredAt) {
					return
				}
				if m.tryRecoverRelayDataPlane(relayAddress) {
					return
				}
				delay = min(delay*2, maxDelay)
				timer.Reset(delay)
			}
		}
	}()
}

func (m *Manager) markDataPlaneTransportFailure(relayClient *Client) {
	if relayClient == nil || relayClient.Transport() != "quic" || m.transportFallback == nil {
		return
	}
	if mode := transportModeFromEnv(); !mode.allowsAutomaticFallback() {
		return
	}

	window := m.transportFallback.recordFailure(relayClient.connectionURL)
	log.Warnf("QUIC Relay data plane failed on %s; avoiding QUIC for %s", relayClient.connectionURL, window)
}

func (m *Manager) markDataPlaneServerFailure(relayURL string) {
	m.relayConfigMu.RLock()
	forced := m.forcedRelayURL != ""
	m.relayConfigMu.RUnlock()
	if forced {
		return
	}
	m.serverPicker.markServerFailure(relayURL, time.Now(), fmt.Errorf("relay data plane handshake timeouts"))
}

func (m *Manager) recoveryCandidateURLs(currentURL string) []string {
	m.relayConfigMu.RLock()
	defer m.relayConfigMu.RUnlock()
	result := make([]string, 0, len(m.configuredRelayURLs))
	for _, relayURL := range m.configuredRelayURLs {
		if relayURL != currentURL {
			result = append(result, relayURL)
		}
	}
	return result
}

// Serve starts the manager, attempting to establish a connection with the relay server.
// If the connection fails, it will keep trying to reconnect in the background.
// Additionally, it starts a cleanup loop to remove unused relay connections.
// The manager will automatically reconnect to the relay server in case of disconnection.
func (m *Manager) Serve() error {
	if !m.running.CompareAndSwap(false, true) {
		return fmt.Errorf("manager already serving")
	}
	log.Debugf("starting relay client manager with %v relay servers", m.serverPicker.loadConfig().serverURLs)

	client, err := m.serverPicker.PickServer(m.ctx)
	if err != nil {
		// record the initial failure so status shows the real reason before
		// the guard's first retry tick
		m.reconnectGuard.setLastError(err)
		go m.reconnectGuard.StartReconnectTrys(m.ctx, nil)
	} else {
		m.storeClient(client)
	}

	go m.listenGuardEvent(m.ctx)
	go m.listenNetworkChanges(m.ctx)
	go m.startCleanupLoop()
	// The probe loop periodically probes non-home relays via lightweight
	// transport handshakes to maintain the lowest-latency standby relay.
	go m.startProbeLoop()
	return err
}

// OpenConn opens a connection to the given peer key. If the peer is on the same relay server, the connection will be
// established via the relay server. If the peer is on a different relay server, the manager will establish a new
// connection to the relay server. It returns the relayed connection to the remote peer.
//
// serverIP, when valid and serverAddress is foreign, is used as a dial target if the FQDN-based dial fails.
// Ignored for the local home-server path. TLS verification still uses the FQDN via SNI.
func (m *Manager) OpenConn(ctx context.Context, serverAddress, peerKey string, serverIP netip.Addr) (*Conn, error) {
	m.relayClientMu.RLock()
	homeClient := m.relayClient
	m.relayClientMu.RUnlock()

	if homeClient == nil {
		return nil, ErrRelayClientNotConnected
	}

	foreign, err := m.isForeignServer(homeClient, serverAddress)
	if err != nil {
		return nil, err
	}

	var netConn *Conn
	if !foreign {
		log.Debugf("open peer connection via permanent server: %s", peerKey)
		netConn, err = homeClient.OpenConn(ctx, peerKey)
	} else {
		log.Debugf("open peer connection via foreign server: %s", serverAddress)
		netConn, err = m.openConnVia(ctx, serverAddress, peerKey, serverIP)
	}
	if err != nil {
		return nil, err
	}

	return netConn, err
}

// Ready returns true if the home Relay client is connected to the relay server.
func (m *Manager) Ready() bool {
	m.relayClientMu.RLock()
	defer m.relayClientMu.RUnlock()

	if m.relayClient == nil {
		return false
	}
	return m.relayClient.Ready()
}

func (m *Manager) SetOnReconnectedListener(f func()) {
	m.listenerLock.Lock()
	defer m.listenerLock.Unlock()

	m.onReconnectedListenerFn = f
}

// AddCloseListener adds a listener to the given server instance address. The listener will be called if the connection
// closed.
func (m *Manager) AddCloseListener(serverAddress string, onClosedListener OnServerCloseListener) error {
	m.relayClientMu.RLock()
	defer m.relayClientMu.RUnlock()

	if m.relayClient == nil {
		return ErrRelayClientNotConnected
	}

	foreign, err := m.isForeignServer(m.relayClient, serverAddress)
	if err != nil {
		return err
	}

	var listenerClient *Client
	if foreign {
		m.relayClientsMutex.RLock()
		track := m.relayClients[serverAddress]
		m.relayClientsMutex.RUnlock()
		if track == nil {
			return ErrRelayClientNotConnected
		}
		track.RLock()
		listenerClient = track.relayClient
		track.RUnlock()
	} else {
		listenerClient = m.relayClient
	}
	if listenerClient == nil {
		return ErrRelayClientNotConnected
	}
	m.addListener(listenerClient, onClosedListener)
	return nil
}

// RelayInstanceAddress returns the address and resolved IP of the permanent relay server. It could change if the
// network connection is lost. The address is sent to the target peer to choose the common relay server for the
// communication; the IP is sent alongside so remote peers can dial directly without their own DNS lookup. Both
// values are read under the same lock so they cannot diverge across a reconnection.
func (m *Manager) RelayInstanceAddress() (string, netip.Addr, error) {
	m.relayClientMu.RLock()
	defer m.relayClientMu.RUnlock()

	if m.relayClient == nil {
		return "", netip.Addr{}, ErrRelayClientNotConnected
	}
	return m.relayClient.serverInstanceAddress()
}

// ServerURLs returns the addresses of the relay servers.
func (m *Manager) ServerURLs() []string {
	return slices.Clone(m.serverPicker.loadConfig().serverURLs)
}

// RelayConnectError returns the error from the most recent failed home relay
// reconnect attempt, or nil if the relay last connected successfully.
func (m *Manager) RelayConnectError() error {
	return m.reconnectGuard.LastError()
}

// RelayStates returns the connection state of the home relay and every foreign
// relay the manager currently tracks.
func (m *Manager) RelayStates() []RelayConnState {
	var states []RelayConnState

	m.relayClientMu.RLock()
	home := m.relayClient
	m.relayClientMu.RUnlock()
	if home != nil {
		st := relayConnState(home)
		// The home relay reconnects through the guard, so the real failure
		// reason lives there rather than on the (stale) client.
		if st.Err != nil {
			if gErr := m.reconnectGuard.LastError(); gErr != nil {
				st.Err = gErr
			}
		}
		states = append(states, st)
	}

	// Snapshot the tracks, then query each outside the map lock: a track can be
	// held by an in-progress Connect, and blocking on it must not stall other
	// relay operations.
	m.relayClientsMutex.RLock()
	tracks := make([]*RelayTrack, 0, len(m.relayClients))
	for _, rt := range m.relayClients {
		tracks = append(tracks, rt)
	}
	m.relayClientsMutex.RUnlock()

	// Only connected foreign relays carry state; a failed connect is evicted
	// immediately (openConnVia), so there is no error state to surface.
	for _, rt := range tracks {
		rt.RLock()
		rc := rt.relayClient
		rt.RUnlock()
		if rc != nil {
			states = append(states, relayConnState(rc))
		}
	}

	return states
}

// HasRelayAddress returns true if the manager is serving. With this method can check if the peer can communicate with
// Relay service.
func (m *Manager) HasRelayAddress() bool {
	return len(m.serverPicker.loadConfig().serverURLs) > 0
}

// StandbyURL returns the currently selected standby relay URL, or empty string
// if no standby relay has been selected or it is no longer valid in the
// current configuration.
func (m *Manager) StandbyURL() string {
	currentURL := m.currentRelayURL()
	m.relayConfigMu.RLock()
	defer m.relayConfigMu.RUnlock()
	if m.standbyRelayURL == "" || m.standbyRelayURL == currentURL || !slices.Contains(m.configuredRelayURLs, m.standbyRelayURL) {
		return ""
	}
	return m.standbyRelayURL
}

func (m *Manager) setStandbyURL(relayURL string) {
	m.relayConfigMu.Lock()
	m.standbyRelayURL = relayURL
	m.relayConfigMu.Unlock()
}

func (m *Manager) UpdateServerURLs(serverURLs []string) {
	m.UpdateServerURLsWithWeights(serverURLs, nil)
}

func (m *Manager) UpdateServerURLsWithWeights(serverURLs []string, _ map[string]int) {
	log.Infof("update relay server URLs: %v", serverURLs)
	m.relayConfigMu.Lock()
	previousURLs := m.effectiveRelayURLsLocked()
	previousForcedURL := m.forcedRelayURL
	m.relayWeights = relayWeightsFromURLs(serverURLs)
	m.configuredRelayURLs = slices.Clone(serverURLs)
	if m.forcedRelayURL != "" && !slices.Contains(m.configuredRelayURLs, m.forcedRelayURL) {
		log.Warnf("forced Relay server %s is no longer configured, clearing override", m.forcedRelayURL)
		m.forcedRelayURL = ""
	}
	if m.standbyRelayURL != "" && !slices.Contains(m.configuredRelayURLs, m.standbyRelayURL) {
		m.standbyRelayURL = ""
	}
	effectiveURLs := m.effectiveRelayURLsLocked()
	weights := maps.Clone(m.relayWeights)
	forcedURL := m.forcedRelayURL
	m.relayConfigMu.Unlock()
	configChanged := !slices.Equal(previousURLs, effectiveURLs) || previousForcedURL != forcedURL
	if configChanged {
		m.closeHotStandby("relay configuration changed")
	}

	m.serverPicker.storeConfig(pickerConfig{
		serverURLs:    effectiveURLs,
		serverWeights: weights,
		forcedURL:     forcedURL,
	})
	generation := m.relayConfigGeneration.Load()
	if configChanged {
		generation = m.relayConfigGeneration.Add(1)
	}
	go m.switchHomeRelayIfNeeded(effectiveURLs, generation)
}

func (m *Manager) RelayServers() []RelayServerInfo {
	m.relayConfigMu.RLock()
	configuredURLs := slices.Clone(m.configuredRelayURLs)
	weights := maps.Clone(m.relayWeights)
	forcedURL := m.forcedRelayURL
	standbyURL := m.standbyRelayURL
	lastSwitchAt := m.lastHomeSwitchAt
	lastSwitchReason := m.lastHomeSwitchReason
	switchCount := m.homeSwitchCount
	m.relayConfigMu.RUnlock()

	m.relayClientMu.RLock()
	currentClient := m.relayClient
	currentURL := ""
	if currentClient != nil {
		currentURL = currentClient.connectionURL
	}
	m.relayClientMu.RUnlock()

	result := make([]RelayServerInfo, 0, len(configuredURLs))
	for _, relayURL := range configuredURLs {
		weight := weights[relayURL]
		if weight <= 0 {
			weight = defaultRelayWeight
		}
		info := RelayServerInfo{
			URL:               relayURL,
			Weight:            weight,
			Preferred:         relayURL == standbyURL && relayURL != currentURL,
			Forced:            relayURL == forcedURL,
			Current:           relayURL == currentURL,
			CooldownRemaining: m.serverPicker.cooldownRemaining(relayURL),
		}
		if relayURL == currentURL {
			info.LastSwitchReason = lastSwitchReason
			info.LastSwitchAt = lastSwitchAt
			info.SwitchCount = switchCount
			info.Transport = currentClient.Transport()
			live := currentClient.LiveQuality()
			info.LiveRTTMs = relayRTTMilliseconds(live.RTT)
			info.P95RTTMs = relayRTTMilliseconds(live.P95)
			info.JitterMs = relayRTTMilliseconds(live.Jitter)
			info.LossPercent = live.LossPercent
		}
		if rtt, ok := m.rttCache.latest(relayURL); ok {
			info.RTTMs = relayRTTMilliseconds(rtt)
		}
		result = append(result, info)
	}
	return result
}

func (m *Manager) ProbeRelayServers(ctx context.Context) []RelayServerInfo {
	relays := m.RelayServers()
	currentURL := m.currentRelayURL()
	var wg sync.WaitGroup
	for i := range relays {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if relays[idx].URL == currentURL {
				m.relayClientMu.RLock()
				home := m.relayClient
				m.relayClientMu.RUnlock()
				if home != nil && home.Ready() {
					relays[idx].Available = true
					relays[idx].Error = ""
					relays[idx].Transport = home.Transport()
					live := home.LiveQuality()
					relays[idx].LiveRTTMs = relayRTTMilliseconds(live.RTT)
					relays[idx].P95RTTMs = relayRTTMilliseconds(live.P95)
					relays[idx].JitterMs = relayRTTMilliseconds(live.Jitter)
					relays[idx].LossPercent = live.LossPercent
				} else if err := m.reconnectGuard.LastError(); err != nil {
					relays[idx].Error = err.Error()
				}
				return
			}
			probeCtx, cancel := context.WithTimeout(ctx, relayProbeTimeout)
			defer cancel()
			rtt, err := m.lightProbe(probeCtx, relays[idx].URL)
			if err != nil {
				m.rttCache.recordFailure(relays[idx].URL)
				relays[idx].Error = err.Error()
				relays[idx].CooldownRemaining = m.serverPicker.cooldownRemaining(relays[idx].URL)
				return
			}
			m.rttCache.set(relays[idx].URL, rtt)
			m.serverPicker.clearTransientFailure(relays[idx].URL)
			relays[idx].Available = true
			relays[idx].CooldownRemaining = m.serverPicker.cooldownRemaining(relays[idx].URL)
			if snapshot, ok := m.rttCache.snapshot(relays[idx].URL); ok {
				relays[idx].RTTMs = relayRTTMilliseconds(snapshot.RTT)
				relays[idx].JitterMs = relayRTTMilliseconds(snapshot.Jitter)
				relays[idx].LossPercent = snapshot.LossPercent
			}
		}(i)
	}
	wg.Wait()
	return relays
}

// relayRTTMilliseconds rounds a positive RTT up so a healthy sub-millisecond
// relay is not reported as 0, which is reserved for "no measurement".
func relayRTTMilliseconds(rtt time.Duration) int64 {
	if rtt <= 0 {
		return 0
	}
	return int64((rtt + time.Millisecond - 1) / time.Millisecond)
}

func (m *Manager) SetForcedRelay(identifier string) (string, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "", fmt.Errorf("relay identifier is required")
	}

	m.relayConfigMu.Lock()
	if strings.EqualFold(identifier, "auto") || strings.EqualFold(identifier, "default") || strings.EqualFold(identifier, "clear") {
		m.forcedRelayURL = ""
		effectiveURLs := m.effectiveRelayURLsLocked()
		m.relayConfigMu.Unlock()
		m.serverPicker.storeConfig(pickerConfig{
			serverURLs:    effectiveURLs,
			serverWeights: maps.Clone(m.relayWeights),
			forcedURL:     "",
		})
		m.closeHotStandby("relay override cleared")
		generation := m.relayConfigGeneration.Add(1)
		go m.switchHomeRelayIfNeeded(effectiveURLs, generation)
		return "", nil
	}

	relayURL, err := matchRelayURL(identifier, m.configuredRelayURLs)
	if err != nil {
		m.relayConfigMu.Unlock()
		return "", err
	}
	m.forcedRelayURL = relayURL
	effectiveURLs := m.effectiveRelayURLsLocked()
	m.relayConfigMu.Unlock()
	m.serverPicker.clearServerFailure(relayURL)
	m.closeHotStandby("manual relay override")
	m.serverPicker.storeConfig(pickerConfig{
		serverURLs:    effectiveURLs,
		serverWeights: maps.Clone(m.relayWeights),
		forcedURL:     relayURL,
	})
	generation := m.relayConfigGeneration.Add(1)
	go m.switchHomeRelayIfNeeded(effectiveURLs, generation)
	return relayURL, nil
}

func (m *Manager) effectiveRelayURLsLocked() []string {
	if m.forcedRelayURL == "" {
		return slices.Clone(m.configuredRelayURLs)
	}
	result := []string{m.forcedRelayURL}
	for _, relayURL := range m.configuredRelayURLs {
		if relayURL != m.forcedRelayURL {
			result = append(result, relayURL)
		}
	}
	return result
}

func relayWeightsFromURLs(relayURLs []string) map[string]int {
	weights := make(map[string]int, len(relayURLs))
	for _, relayURL := range relayURLs {
		if relayURL != "" {
			weights[relayURL] = defaultRelayWeight
		}
	}
	return weights
}

func sortRelayURLsByWeight(relayURLs []string, weights map[string]int) []string {
	urls := slices.Clone(relayURLs)
	slices.SortStableFunc(urls, func(left, right string) int {
		leftWeight, rightWeight := weights[left], weights[right]
		if leftWeight <= 0 {
			leftWeight = defaultRelayWeight
		}
		if rightWeight <= 0 {
			rightWeight = defaultRelayWeight
		}
		return rightWeight - leftWeight
	})
	return urls
}

func (m *Manager) currentRelayURL() string {
	m.relayClientMu.RLock()
	defer m.relayClientMu.RUnlock()
	if m.relayClient == nil {
		return ""
	}
	return m.relayClient.connectionURL
}

func matchRelayURL(identifier string, relayURLs []string) (string, error) {
	var matches []string
	for _, relayURL := range relayURLs {
		if relayURL == identifier {
			return relayURL, nil
		}
		parsedURL, _ := url.Parse(relayURL)
		host := parsedURL.Hostname()
		if strings.EqualFold(host, identifier) {
			return relayURL, nil
		}
		if strings.Contains(strings.ToLower(relayURL), strings.ToLower(identifier)) || strings.Contains(strings.ToLower(host), strings.ToLower(identifier)) {
			matches = append(matches, relayURL)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("relay %q was not found in received relay list", identifier)
	}
	return "", fmt.Errorf("relay %q matches multiple relays: %s", identifier, strings.Join(matches, ", "))
}

func (m *Manager) switchHomeRelayIfNeeded(serverURLs []string, generation uint64) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()
	if generation != m.relayConfigGeneration.Load() {
		return
	}

	if len(serverURLs) == 0 {
		m.setStandbyURL("")
		m.relayClientMu.Lock()
		if !m.running.Load() || m.relayClient == nil {
			m.relayClientMu.Unlock()
			return
		}
		oldClient := m.relayClient
		oldURL := oldClient.connectionURL
		oldClient.SetOnDisconnectListener(nil)
		m.relayClient = nil
		m.relayClientMu.Unlock()

		log.Infof("closing home Relay server %s because no Relay servers are configured", oldURL)
		m.notifyOnDisconnectListeners(oldClient)
		if err := oldClient.Close(); err != nil {
			log.Warnf("failed to close previous home Relay server %s: %v", oldURL, err)
		}
		return
	}

	m.relayClientMu.RLock()
	if !m.running.Load() || m.relayClient == nil {
		m.relayClientMu.RUnlock()
		return
	}
	if m.currentRelayStillHighestPriorityLocked(serverURLs) {
		m.relayClientMu.RUnlock()
		if m.StandbyURL() == "" && len(serverURLs) > 1 {
			go m.refreshStandbyRelay()
		}
		return
	}
	oldClient := m.relayClient
	oldURL := oldClient.connectionURL
	m.relayClientMu.RUnlock()

	log.Infof("home Relay %s is no longer target in %v; preparing replacement home Relay before switching", oldURL, serverURLs)
	candidateURLs := m.priorityMigrationCandidateURLs(oldClient, serverURLs)
	if len(candidateURLs) == 0 {
		return
	}
	candidate, foreignTrackPresent := m.foreignRelayCandidate(candidateURLs)
	foreignCandidate := candidate != nil
	if candidate == nil {
		if foreignTrackPresent {
			log.Infof("replacement Relay connection is still being prepared; keeping %s active", oldURL)
			return
		}
		var err error
		standbyURL := m.StandbyURL()
		if standbyURL == oldURL || !slices.Contains(candidateURLs, standbyURL) {
			standbyURL = ""
		}
		candidate, err = m.serverPicker.PickStandbyWithFallback(m.ctx, standbyURL, candidateURLs)
		if err != nil {
			log.Errorf("failed to prepare replacement Relay; keeping %s active: %v", oldURL, err)
			return
		}
	}
	if !m.isValidPriorityMigration(oldClient, candidate, serverURLs) {
		log.Infof("no valid Relay candidate became ready; keeping %s active", oldURL)
		if !foreignCandidate {
			_ = candidate.Close()
		}
		return
	}
	if !m.swapHomeRelay(oldClient, candidate, generation) {
		if !foreignCandidate {
			_ = candidate.Close()
		}
		return
	}
	m.recordHomeSwitch("configuration-change")
	log.Infof("new home Relay %s is ready; migrating peer connections from %s", candidate.connectionURL, oldURL)
	m.onServerConnected()
	m.retireHomeRelay(oldClient)
}

// foreignRelayCandidate returns an existing connection to a candidate Relay so
// home migration does not create a second session with the same peer ID. The
// boolean reports that a matching track exists but its dial has not completed.
func (m *Manager) foreignRelayCandidate(candidateURLs []string) (*Client, bool) {
	for _, relayURL := range candidateURLs {
		m.relayClientsMutex.RLock()
		track := m.relayClients[relayURL]
		m.relayClientsMutex.RUnlock()
		if track == nil {
			continue
		}

		select {
		case <-track.ready:
			track.RLock()
			candidate, err := track.relayClient, track.err
			track.RUnlock()
			if err == nil && candidate != nil && candidate.Ready() {
				return candidate, true
			}
		case <-m.ctx.Done():
			return nil, true
		default:
			return nil, true
		}
	}
	return nil, false
}

func (m *Manager) priorityMigrationCandidateURLs(oldClient *Client, serverURLs []string) []string {
	if oldClient == nil {
		return nil
	}
	oldURL := oldClient.connectionURL
	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	m.relayConfigMu.RUnlock()
	if forcedURL != "" {
		if forcedURL == oldURL {
			return nil
		}
		return []string{forcedURL}
	}
	if !slices.Contains(serverURLs, oldURL) {
		return slices.Clone(serverURLs)
	}
	return nil
}

func (m *Manager) isValidPriorityMigration(oldClient, candidate *Client, serverURLs []string) bool {
	if oldClient == nil || candidate == nil {
		return false
	}
	oldURL, candidateURL := oldClient.connectionURL, candidate.connectionURL
	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	m.relayConfigMu.RUnlock()
	if forcedURL != "" {
		return candidateURL == forcedURL
	}
	if !slices.Contains(serverURLs, oldURL) {
		return slices.Contains(serverURLs, candidateURL)
	}
	return false
}

func (m *Manager) swapHomeRelay(oldClient, candidate *Client, generation uint64) bool {
	if candidate == nil || !candidate.Ready() || generation != m.relayConfigGeneration.Load() {
		return false
	}

	m.relayClientMu.Lock()
	if !m.running.Load() || m.ctx.Err() != nil || m.relayClient != oldClient || generation != m.relayConfigGeneration.Load() {
		m.relayClientMu.Unlock()
		return false
	}
	m.relayClientsMutex.Lock()
	if track := m.relayClients[candidate.connectionURL]; track != nil {
		track.RLock()
		trackedClient := track.relayClient
		track.RUnlock()
		if trackedClient == candidate {
			delete(m.relayClients, candidate.connectionURL)
		}
	}
	oldTrack := NewRelayTrack()
	oldTrack.relayClient = oldClient
	close(oldTrack.ready)
	m.relayClients[oldClient.connectionURL] = oldTrack
	m.relayClientsMutex.Unlock()
	oldClient.SetOnDisconnectListener(func(address string) { m.onServerDisconnectedClient(oldClient, address) })
	m.relayClient = candidate
	candidate.SetOnDisconnectListener(func(address string) { m.onServerDisconnectedClient(candidate, address) })
	m.relayClientMu.Unlock()

	m.relayConfigMu.Lock()
	if m.standbyRelayURL == candidate.connectionURL {
		m.standbyRelayURL = ""
	}
	m.relayConfigMu.Unlock()
	m.scheduleStableReset(candidate)
	go m.refreshStandbyRelay()
	return true
}

func (m *Manager) retireHomeRelay(oldClient *Client) {
	if oldClient == nil {
		return
	}
	go func() {
		minimumWait := time.NewTimer(min(relayMigrationMinWait, m.relayMigrationGrace))
		defer minimumWait.Stop()
		select {
		case <-minimumWait.C:
		case <-m.ctx.Done():
			m.closeRetiredHomeRelay(oldClient)
			return
		}

		deadline := time.NewTimer(max(time.Millisecond, m.relayMigrationGrace-relayMigrationMinWait))
		ticker := time.NewTicker(100 * time.Millisecond)
		defer deadline.Stop()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !oldClient.HasConns() {
					m.closeRetiredHomeRelay(oldClient)
					return
				}
			case <-deadline.C:
				if oldClient.HasConns() {
					log.Infof("Relay migration grace period expired for %s; keeping it as a foreign Relay while peer connections remain", oldClient.connectionURL)
					continue
				}
				m.closeRetiredHomeRelay(oldClient)
				return
			case <-m.ctx.Done():
				m.closeRetiredHomeRelay(oldClient)
				return
			}
		}
	}()
}

func (m *Manager) closeRetiredHomeRelay(relayClient *Client) {
	m.evictForeignRelayClient(relayClient.connectionURL, relayClient)
	relayClient.SetOnDisconnectListener(nil)
	m.dropDisconnectListeners(relayClient)
	_ = relayClient.Close()
}

// Caller holds relayClientMu for the referenced client.
func (m *Manager) currentRelayStillHighestPriorityLocked(serverURLs []string) bool {
	currentURL := m.relayClient.connectionURL
	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	m.relayConfigMu.RUnlock()
	if forcedURL != "" {
		return currentURL == forcedURL
	}
	return slices.Contains(serverURLs, currentURL)
}

// UpdateToken updates the token in the token store.
func (m *Manager) UpdateToken(token *relayAuth.Token) error {
	return m.tokenStore.UpdateToken(token)
}

func (m *Manager) openConnVia(ctx context.Context, serverAddress, peerKey string, serverIP netip.Addr) (*Conn, error) {
	// check if already has a connection to the desired relay server
	m.relayClientsMutex.RLock()
	rt, ok := m.relayClients[serverAddress]
	m.relayClientsMutex.RUnlock()
	if ok {
		return m.openConnOnTrack(ctx, rt, peerKey)
	}

	// if not, establish a new connection but check it again (because changed the lock type) before starting the
	// connection
	m.relayClientsMutex.Lock()
	rt, ok = m.relayClients[serverAddress]
	if ok {
		m.relayClientsMutex.Unlock()
		return m.openConnOnTrack(ctx, rt, peerKey)
	}

	// Publish the track and release the map lock BEFORE dialing, so the dial does
	// not run under rt.Lock (which would block RelayStates and the cleanup loop
	// for the full dial). Concurrent callers find this track and wait on rt.ready.
	rt = NewRelayTrack()
	m.relayClients[serverAddress] = rt
	m.relayClientsMutex.Unlock()

	relayClient := NewClientWithServerIP(serverAddress, serverIP, m.tokenStore, m.peerID, m.mtu)
	relayClient.SetTransportFallback(m.transportFallback)
	relayClient.netEvents = m.netEvents
	err := relayClient.Connect(m.ctx)
	if err != nil {
		rt.Lock()
		rt.err = err
		rt.Unlock()
		close(rt.ready)
		m.relayClientsMutex.Lock()
		delete(m.relayClients, serverAddress)
		m.relayClientsMutex.Unlock()
		return nil, err
	}
	// if connection closed then delete the relay client from the list
	relayClient.SetOnDisconnectListener(func(address string) { m.onServerDisconnectedClient(relayClient, address) })
	rt.Lock()
	rt.relayClient = relayClient
	rt.Unlock()
	close(rt.ready)

	return relayClient.OpenConn(ctx, peerKey)
}

// openConnOnTrack opens a peer connection through an existing relay track,
// waiting for the dial started by another openConnVia call to finish. It waits
// on rt.ready rather than the track lock, so it neither holds nor contends the
// track lock across the dial.
func (m *Manager) openConnOnTrack(ctx context.Context, rt *RelayTrack, peerKey string) (*Conn, error) {
	select {
	case <-rt.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	rt.RLock()
	defer rt.RUnlock()
	if rt.err != nil {
		return nil, rt.err
	}
	if rt.relayClient == nil {
		return nil, ErrRelayClientNotConnected
	}
	return rt.relayClient.OpenConn(ctx, peerKey)
}

func (m *Manager) onServerConnected() {
	m.listenerLock.Lock()
	defer m.listenerLock.Unlock()

	if m.onReconnectedListenerFn == nil {
		return
	}
	go m.onReconnectedListenerFn()
}

// onServerDisconnected finds the client tracked for a relay address and handles its
// disconnect event. It is kept for callers that only know the relay address.
func (m *Manager) onServerDisconnected(serverAddress string) {
	m.relayClientMu.RLock()
	homeClient := m.relayClient
	m.relayClientMu.RUnlock()

	if homeClient != nil && homeClient.connectionURL == serverAddress {
		m.onServerDisconnectedClient(homeClient, serverAddress)
		return
	}

	m.relayClientsMutex.RLock()
	track := m.relayClients[serverAddress]
	m.relayClientsMutex.RUnlock()

	var disconnectedClient *Client
	if track != nil {
		track.RLock()
		disconnectedClient = track.relayClient
		track.RUnlock()
	}
	m.onServerDisconnectedClient(disconnectedClient, serverAddress)
}

// onServerDisconnectedClient handles relay disconnect events. For the home server it
// starts the reconnect guard. For foreign servers it evicts the now-dead client
// from the cache so the next OpenConn builds a fresh one instead of reusing a
// closed client.
func (m *Manager) onServerDisconnectedClient(disconnectedClient *Client, serverAddress string) {
	m.relayClientMu.Lock()
	isHome := m.relayClient == disconnectedClient
	if isHome {
		m.stopStableReset()
		go func(client *Client) {
			m.reconnectGuard.StartReconnectTrys(m.ctx, client)
		}(disconnectedClient)
	}
	m.relayClientMu.Unlock()

	if !isHome {
		m.evictForeignRelayClient(serverAddress, disconnectedClient)
	}
	m.notifyOnDisconnectListeners(disconnectedClient)
}

// evictForeignRelay removes an unusable foreign relay track by address.
func (m *Manager) evictForeignRelay(serverAddress string) {
	m.evictForeignRelayClient(serverAddress, nil)
}

func (m *Manager) evictForeignRelayClient(serverAddress string, disconnectedClient *Client) {
	m.relayClientsMutex.Lock()
	defer m.relayClientsMutex.Unlock()
	rt, ok := m.relayClients[serverAddress]
	if !ok {
		return
	}

	rt.RLock()
	client := rt.relayClient
	rt.RUnlock()
	if client == nil {
		log.Debugf("keeping foreign relay track with a dial in progress: %s", serverAddress)
		return
	}
	if client.Ready() {
		log.Debugf("keeping reconnected foreign relay client: %s", serverAddress)
		return
	}

	delete(m.relayClients, serverAddress)
	log.Debugf("evicted disconnected foreign relay client: %s", serverAddress)
}

func (m *Manager) listenGuardEvent(ctx context.Context) {
	for {
		select {
		case <-m.reconnectGuard.OnReconnected:
			m.onServerConnected()
		case rc := <-m.reconnectGuard.OnNewRelayClient:
			if !m.reconnectGuard.isServerURLStillValid(rc) {
				_ = rc.Close()
				continue
			}
			previousURL := m.currentRelayURL()
			if previousURL != "" && previousURL != rc.connectionURL {
				m.recordHomeSwitch("failover")
			}
			m.storeClient(rc)
			m.onServerConnected()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) storeClient(client *Client) {
	m.relayClientMu.Lock()
	m.relayClient = client
	m.relayClient.SetOnDisconnectListener(func(address string) { m.onServerDisconnectedClient(client, address) })
	m.relayClientMu.Unlock()

	if client != nil {
		m.closeHotStandby("home relay changed")
		m.scheduleStableReset(client)
		m.relayConfigMu.Lock()
		if m.standbyRelayURL == client.connectionURL {
			m.standbyRelayURL = ""
		}
		m.relayConfigMu.Unlock()
		go m.refreshStandbyRelay()
	}
}

func (m *Manager) isForeignServer(homeClient *Client, address string) (bool, error) {
	rAddr, err := homeClient.ServerInstanceURL()
	if err != nil {
		return false, fmt.Errorf("relay client not connected")
	}
	return rAddr != address, nil
}

func (m *Manager) startCleanupLoop() {
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.cleanUpUnusedRelays()
		}
	}
}

// startProbeLoop periodically probes non-home relays via lightweight transport
// handshakes to maintain the lowest-latency healthy standby relay. Like DERP
// probing in Tailscale, each tick is jittered to keep a fleet of clients from
// probing in lockstep.
func (m *Manager) startProbeLoop() {
	timer := time.NewTimer(relayRTTProbeInterval + rand.N(relayRTTProbeJitter))
	defer timer.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			timer.Reset(relayRTTProbeInterval + rand.N(relayRTTProbeJitter))
			m.probeTick()
		}
	}
}

// refreshStandbyRelay immediately probes all non-home relays and selects the
// lowest-latency healthy relay as the standby relay.
func (m *Manager) refreshStandbyRelay() {
	m.probeTick()
}

// probeTick probes the home Relay and every configured alternative with the
// lightweight transport probe, updates standby selection, and migrates the
// home Relay only after a candidate has been significantly faster for several
// consecutive rounds. Migration always preconnects the candidate first.
func (m *Manager) probeTick() {
	if !m.running.Load() || m.ctx.Err() != nil {
		return
	}
	if !m.probeMu.TryLock() {
		return
	}
	defer m.probeMu.Unlock()
	// Skip while a home-relay switch is in flight; the switch holds switchMu,
	// and TryLock keeps the probe loop from blocking on it.
	if !m.switchMu.TryLock() {
		return
	}
	m.switchMu.Unlock()

	generation := m.relayConfigGeneration.Load()
	homeURL := m.currentRelayURL()
	m.relayConfigMu.RLock()
	urls := slices.Clone(m.configuredRelayURLs)
	m.relayConfigMu.RUnlock()

	candidates := make([]string, 0, len(urls))
	probeURLs := make([]string, 0, len(urls)+1)
	if homeURL != "" {
		probeURLs = append(probeURLs, homeURL)
	}
	for _, u := range urls {
		if u != "" && u != homeURL {
			candidates = append(candidates, u)
			probeURLs = append(probeURLs, u)
		}
	}
	if len(candidates) == 0 {
		if generation == m.relayConfigGeneration.Load() && m.currentRelayURL() == homeURL {
			m.setStandbyURL("")
			m.closeHotStandby("no alternative relay configured")
		}
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, relayProbeTimeout)
	defer cancel()

	var (
		wg         sync.WaitGroup
		probedMu   sync.Mutex
		probedRTTs = make(map[string]time.Duration, len(candidates))
	)
	for _, relayURL := range probeURLs {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			rtt, err := m.lightProbe(ctx, url)
			if err != nil {
				m.rttCache.recordFailure(url)
				log.Debugf("relay probe decision: %s unavailable, not eligible for standby/migration: %v", url, err)
				return
			}
			m.rttCache.set(url, rtt)
			m.serverPicker.clearTransientFailure(url)
			probedMu.Lock()
			probedRTTs[url] = rtt
			probedMu.Unlock()
			snapshot, _ := m.rttCache.snapshot(url)
			log.Debugf("relay probe decision: %s healthy, rtt=%s jitter=%s loss=%d%%, eligible for standby/migration", url, snapshot.RTT.Round(time.Millisecond), snapshot.Jitter.Round(time.Millisecond), snapshot.LossPercent)
		}(relayURL)
	}
	wg.Wait()

	// The current home relay's established session is a better signal than a
	// fresh probe. Use its liveness RTT when available, including when the
	// lightweight probe itself failed on a transient path.
	if homeURL != "" {
		if live := m.homeDataPlaneQuality(homeURL); live.Samples > 0 && live.RTT > 0 {
			probedRTTs[homeURL] = live.RTT
			log.Debugf("relay probe decision: using live data-plane RTT %s for home %s (p95=%s jitter=%s loss=%d%%)", live.RTT.Round(time.Millisecond), homeURL, live.P95.Round(time.Millisecond), live.Jitter.Round(time.Millisecond), live.LossPercent)
		}
	}

	if generation != m.relayConfigGeneration.Load() || m.currentRelayURL() != homeURL {
		return
	}
	m.selectStandbyFromProbes(homeURL, candidates, probedRTTs)
	m.maybeMigrateHomeRelayOnRTT(generation, homeURL, candidates, probedRTTs)
}

// maybeMigrateHomeRelayOnRTT tracks how many consecutive probe rounds show a
// materially faster healthy alternative. Once the advantage is persistent it
// asks migrateHomeRelayOnRTT to preconnect and atomically promote the candidate.
func (m *Manager) maybeMigrateHomeRelayOnRTT(generation uint64, homeURL string, candidates []string, probedRTTs map[string]time.Duration) {
	choice := m.migrationChoice(homeURL, candidates, probedRTTs)
	streak := m.advanceRTTMigrationStreak(homeURL, choice.URL)
	if choice.URL == "" {
		log.Debugf("relay migration decision: keeping %s: %s", homeURL, choice.Reason)
		return
	}

	required := m.requiredMigrationStreaks(homeURL, probedRTTs[homeURL], choice.RTT)
	if streak < required {
		// Armed as soon as degradation is visible, well before the migration
		// threshold. A later failover can reuse this ready session immediately.
		m.ensureHotStandby(choice.URL)
		log.Debugf("relay migration decision: candidate=%s streak=%d/%d home_rtt=%s candidate_rtt=%s; waiting for sustained evidence", choice.URL, streak, required, probedRTTs[homeURL].Round(time.Millisecond), choice.RTT.Round(time.Millisecond))
		return
	}
	if remaining := m.proactiveSwitchRemaining(); remaining > 0 {
		m.ensureHotStandby(choice.URL)
		log.Infof("relay migration decision: candidate=%s is eligible but held for %.0fs to prevent oscillation", choice.URL, remaining.Seconds())
		return
	}
	m.resetRTTMigrationStreak()
	m.migrateHomeRelayOnRTT(generation, homeURL, choice.URL)
}

func (m *Manager) bestRTTMigrationCandidate(homeURL string, candidates []string, probedRTTs map[string]time.Duration) string {
	return m.migrationChoice(homeURL, candidates, probedRTTs).URL
}

type relayMigrationChoice struct {
	URL    string
	RTT    time.Duration
	Reason string
}

func (m *Manager) migrationChoice(homeURL string, candidates []string, probedRTTs map[string]time.Duration) relayMigrationChoice {
	homeRTT, ok := probedRTTs[homeURL]
	if homeURL == "" || !ok || homeRTT <= 0 {
		return relayMigrationChoice{Reason: "home relay has no fresh or live RTT measurement"}
	}

	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	configured := slices.Clone(m.configuredRelayURLs)
	m.relayConfigMu.RUnlock()
	if forcedURL != "" {
		return relayMigrationChoice{Reason: "manual relay override is active"}
	}

	bestURL := ""
	bestRTT := time.Duration(0)
	var skipped []string
	for _, candidateURL := range candidates {
		if candidateURL == "" || candidateURL == homeURL {
			continue
		}
		if !slices.Contains(configured, candidateURL) {
			skipped = append(skipped, candidateURL+" (not configured)")
			continue
		}
		if cooling := m.serverPicker.cooldownRemaining(candidateURL); cooling > 0 {
			skipped = append(skipped, fmt.Sprintf("%s (cooldown %s)", candidateURL, cooling.Round(time.Second)))
			continue
		}
		rtt, ok := probedRTTs[candidateURL]
		if !ok || rtt <= 0 {
			skipped = append(skipped, candidateURL+" (probe unavailable)")
			continue
		}
		if bestURL == "" || rtt < bestRTT {
			bestURL = candidateURL
			bestRTT = rtt
		}
	}
	if bestURL == "" || homeRTT-bestRTT < relayRTTMigrationNoiseFloor {
		reason := "no candidate beats the home relay by the noise floor"
		if len(skipped) > 0 {
			reason += ": " + strings.Join(skipped, ", ")
		}
		return relayMigrationChoice{Reason: reason}
	}
	// High-latency links must halve; low-latency links need a 25% improvement.
	// This keeps domestic 20-60ms relay pools eligible while retaining a
	// stricter bar where an RTT win is worth a more disruptive migration.
	maximumCandidateRTT := homeRTT * 3 / 4
	if homeRTT >= relayRTTMigrationHighRTT {
		maximumCandidateRTT = homeRTT / 2
	}
	if bestRTT > maximumCandidateRTT {
		return relayMigrationChoice{Reason: fmt.Sprintf("candidate %s at %s does not clear the improvement threshold for home %s", bestURL, bestRTT.Round(time.Millisecond), homeRTT.Round(time.Millisecond))}
	}
	return relayMigrationChoice{URL: bestURL, RTT: bestRTT}
}

func (m *Manager) homeDataPlaneQuality(homeURL string) relayDataPlaneQuality {
	m.relayClientMu.RLock()
	home := m.relayClient
	m.relayClientMu.RUnlock()
	if home == nil || home.connectionURL != homeURL {
		return relayDataPlaneQuality{}
	}
	return home.LiveQuality()
}

func (m *Manager) requiredMigrationStreaks(homeURL string, homeRTT, candidateRTT time.Duration) int {
	quality := m.homeDataPlaneQuality(homeURL)
	if quality.ConsecutiveFailures > 0 || quality.LossPercent >= 25 || quality.Jitter >= max(time.Millisecond, quality.RTT/2) {
		return 1
	}
	if candidateRTT > 0 && homeRTT >= candidateRTT*3 {
		return 2
	}
	return relayRTTMigrationStreaks
}

func (m *Manager) advanceRTTMigrationStreak(homeURL, targetURL string) int {
	m.rttMigrationMu.Lock()
	defer m.rttMigrationMu.Unlock()
	if targetURL == "" || m.rttMigrationHomeURL != homeURL || m.rttMigrationTargetURL != targetURL {
		m.rttMigrationHomeURL = homeURL
		m.rttMigrationTargetURL = targetURL
		m.rttMigrationStreak = 0
	}
	if targetURL == "" {
		return 0
	}
	m.rttMigrationStreak++
	return m.rttMigrationStreak
}

func (m *Manager) resetRTTMigrationStreak() {
	m.rttMigrationMu.Lock()
	m.rttMigrationHomeURL = ""
	m.rttMigrationTargetURL = ""
	m.rttMigrationStreak = 0
	m.rttMigrationMu.Unlock()
}

// migrateHomeRelayOnRTT preconnects the candidate as a real Relay session.
// Only after it is Ready does the manager atomically swap the home client. The
// previous home remains alive under the migration grace period so existing peer
// connections can drain without a traffic blackhole.
func (m *Manager) migrateHomeRelayOnRTT(generation uint64, homeURL, targetURL string) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()
	if generation != m.relayConfigGeneration.Load() || !m.running.Load() || m.ctx.Err() != nil {
		return
	}

	m.relayClientMu.RLock()
	oldClient := m.relayClient
	m.relayClientMu.RUnlock()
	if oldClient == nil || oldClient.connectionURL != homeURL || !oldClient.Ready() {
		return
	}

	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	targetConfigured := slices.Contains(m.configuredRelayURLs, targetURL)
	m.relayConfigMu.RUnlock()
	if forcedURL != "" || targetURL == "" || targetURL == homeURL || !targetConfigured {
		return
	}
	if remaining := m.proactiveSwitchRemaining(); remaining > 0 {
		log.Infof("RTT migration: candidate %s deferred for %.0fs to prevent relay oscillation", targetURL, remaining.Seconds())
		return
	}

	candidate := m.takeHotStandbyFor(targetURL)
	if candidate == nil {
		// A probe round may have started the session moments before the
		// migration threshold was reached. Reuse that dial rather than opening
		// a second session for the same peer ID.
		candidate = m.waitForHotStandby(targetURL, 2*time.Second)
		if candidate == nil {
			candidate = m.takeHotStandbyFor(targetURL)
		}
	}
	m.hotStandbyMu.Lock()
	stillDialing := m.hotStandbyDialing && m.hotStandbyURL == targetURL
	m.hotStandbyMu.Unlock()
	if candidate == nil && stillDialing {
		log.Debugf("RTT migration: preconnect to %s is still in flight; keeping %s and retrying on the next probe", targetURL, homeURL)
		return
	}
	if candidate != nil {
		log.Infof("RTT migration: using preconnected hot standby %s to replace %s", targetURL, homeURL)
	} else {
		log.Infof("RTT migration: preconnecting candidate Relay %s before replacing %s", targetURL, homeURL)
		var err error
		candidate, err = m.serverPicker.PickServerFromWithCooldownBypass(m.ctx, []string{targetURL})
		if err != nil {
			log.Warnf("RTT migration: candidate Relay %s failed preflight; keeping %s: %v", targetURL, homeURL, err)
			return
		}
	}
	if candidate == nil || candidate.connectionURL != targetURL || !candidate.Ready() {
		if candidate != nil {
			_ = candidate.Close()
		}
		log.Warnf("RTT migration: candidate Relay %s did not become ready; keeping %s", targetURL, homeURL)
		return
	}
	if !m.swapHomeRelay(oldClient, candidate, generation) {
		_ = candidate.Close()
		log.Infof("RTT migration: home Relay changed during preflight; keeping the current relay instead of %s", targetURL)
		return
	}

	m.recordHomeSwitch("rtt-migration")
	log.Infof("RTT migration: home Relay moved from %s to %s after successful preflight", homeURL, targetURL)
	m.onServerConnected()
	m.retireHomeRelay(oldClient)
}

// selectStandbyFromProbes updates standbyRelayURL based on fresh non-home
// probe results:
//   - If the current standby relay succeeded in this probe round and no other
//     healthy candidate is faster by more than relayRTTHysteresis, keep the
//     current standby relay unchanged.
//   - If the current standby relay failed the probe, is in cooldown, or became
//     significantly slower than another healthy candidate, pick the healthy
//     non-home candidate with the lowest RTT as the new standby relay.
func (m *Manager) selectStandbyFromProbes(homeURL string, candidates []string, probedRTTs map[string]time.Duration) {
	liveHomeURL := m.currentRelayURL()
	if homeURL != "" && liveHomeURL != "" && liveHomeURL != homeURL {
		return
	}
	if liveHomeURL == "" {
		liveHomeURL = homeURL
	}

	m.relayConfigMu.Lock()
	bestURL := ""
	bestRTT := time.Duration(0)
	for _, u := range candidates {
		if u == "" || u == homeURL || u == liveHomeURL || !slices.Contains(m.configuredRelayURLs, u) {
			continue
		}
		rtt, ok := probedRTTs[u]
		if !ok {
			continue
		}
		if bestURL == "" || rtt < bestRTT {
			bestURL = u
			bestRTT = rtt
		}
	}

	currentStandby := m.standbyRelayURL
	if currentStandby != "" && currentStandby != homeURL && currentStandby != liveHomeURL &&
		slices.Contains(candidates, currentStandby) &&
		slices.Contains(m.configuredRelayURLs, currentStandby) {
		if standbyRTT, ok := probedRTTs[currentStandby]; ok {
			if bestURL == "" || bestURL == currentStandby || standbyRTT-bestRTT <= relayRTTHysteresis {
				m.relayConfigMu.Unlock()
				return
			}
		}
	}
	changed := m.standbyRelayURL != bestURL
	m.standbyRelayURL = bestURL
	m.relayConfigMu.Unlock()

	if changed {
		if bestURL != "" {
			log.Infof("selected standby Relay server: %s (RTT %s)", bestURL, bestRTT.Round(time.Millisecond))
		} else {
			log.Debug("no healthy standby Relay server available")
		}
	}
}

// lightProbe measures the transport handshake round trip to a relay without
// establishing a relay session, so periodic probing stays cheap for both the
// client and the relay server.
func (m *Manager) lightProbe(ctx context.Context, relayURL string) (time.Duration, error) {
	probeClient := NewClient(relayURL, m.tokenStore, m.peerID+"-rtt-probe", m.mtu)
	probeClient.SetTransportFallback(m.transportFallback)
	return probeClient.ProbeTransport(ctx)
}

// relayServerCoolingDown reports whether the picker's failure cooldown
// currently skips the given Relay server. Expired entries are evicted, mirroring
// the picker's own availability check.
func (m *Manager) relayServerCoolingDown(relayURL string) bool {
	picker := m.serverPicker
	picker.cooldownMu.Lock()
	defer picker.cooldownMu.Unlock()
	until, ok := picker.cooldowns[relayURL]
	if !ok {
		return false
	}
	if !time.Now().Before(until) {
		delete(picker.cooldowns, relayURL)
		return false
	}
	return true
}

func (m *Manager) cleanUpUnusedRelays() {
	m.relayClientsMutex.Lock()
	defer m.relayClientsMutex.Unlock()

	for addr, rt := range m.relayClients {
		rt.Lock()
		// if the connection failed to the server the relay client will be nil
		// but the instance will be kept in the relayClients until the next locking
		if rt.err != nil {
			rt.Unlock()
			continue
		}

		// dial still in progress (openConnVia publishes the track before Connect
		// completes and no longer holds rt.Lock during it), nothing to clean up.
		if rt.relayClient == nil {
			rt.Unlock()
			continue
		}

		if time.Since(rt.created) <= m.keepUnusedServerTime {
			rt.Unlock()
			continue
		}

		if rt.relayClient.HasConns() {
			rt.Unlock()
			continue
		}
		rt.relayClient.SetOnDisconnectListener(nil)
		go func() {
			_ = rt.relayClient.Close()
		}()
		log.Debugf("clean up unused relay server connection: %s", addr)
		delete(m.relayClients, addr)
		rt.Unlock()
	}
}

func (m *Manager) addListener(relayClient *Client, onClosedListener OnServerCloseListener) {
	m.listenerLock.Lock()
	defer m.listenerLock.Unlock()
	l, ok := m.onDisconnectedListeners[relayClient]
	if !ok {
		l = list.New()
	}
	for e := l.Front(); e != nil; e = e.Next() {
		if reflect.ValueOf(e.Value).Pointer() == reflect.ValueOf(onClosedListener).Pointer() {
			return
		}
	}
	l.PushBack(onClosedListener)
	m.onDisconnectedListeners[relayClient] = l
}

func (m *Manager) notifyOnDisconnectListeners(relayClient *Client) {
	m.listenerLock.Lock()
	defer m.listenerLock.Unlock()

	l, ok := m.onDisconnectedListeners[relayClient]
	if !ok {
		return
	}
	for e := l.Front(); e != nil; e = e.Next() {
		go e.Value.(OnServerCloseListener)()
	}
	delete(m.onDisconnectedListeners, relayClient)
}

func (m *Manager) dropDisconnectListeners(relayClient *Client) {
	m.listenerLock.Lock()
	delete(m.onDisconnectedListeners, relayClient)
	m.listenerLock.Unlock()
}

func relayConnState(c *Client) RelayConnState {
	addr, err := c.ServerInstanceURL()
	if err != nil {
		return RelayConnState{URL: c.connectionURL, Err: err}
	}
	return RelayConnState{URL: addr, Transport: c.Transport()}
}
