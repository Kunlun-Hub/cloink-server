package client

import (
	"container/list"
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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
	// are treated as equally fast, so the picker does not reorder on noise.
	relayRTTHysteresisMs = 25
	// relayRTTMigrationStreaks is how many consecutive probe ticks the home
	// relay must be significantly slower than a same-priority alternative
	// before the manager migrates to it.
	relayRTTMigrationStreaks = 3

	ErrRelayClientNotConnected = fmt.Errorf("relay client not connected")
)

// EnvAutoFailback enables the manager's automatic failback to a recovered
// higher-priority Relay server. It is disabled by default.
const EnvAutoFailback = "NB_RELAY_AUTO_FAILBACK"

const (
	// relayAutoFailbackInterval is how often the manager checks whether the
	// home Relay can move back to a higher-priority server that has recovered.
	relayAutoFailbackInterval = 60 * time.Second
	// relayFailbackStableChecks is the number of consecutive healthy checks a
	// higher-priority Relay server needs before the manager fails back to it.
	relayFailbackStableChecks = 2
)

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
	RTTMs int64
}

// relayRTTCache stores dial round-trip times measured by relay probes with a
// TTL, so the server picker can prefer low-latency relays inside a priority
// group. Samples are smoothed with an EWMA to damp single-probe noise.
type relayRTTCache struct {
	mu      sync.RWMutex
	entries map[string]relayRTTEntry
}

type relayRTTEntry struct {
	rtt       time.Duration
	samples   int
	expiresAt time.Time
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
		entry.rtt = time.Duration(float64(sample)*relayRTTEWMAAlpha + float64(entry.rtt)*(1-relayRTTEWMAAlpha))
	}
	entry.samples++
	entry.expiresAt = time.Now().Add(relayRTTCacheTTL)
	c.entries[relayURL] = entry
	c.mu.Unlock()
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
	relayConfigGeneration atomic.Uint64
	relayMigrationGrace   time.Duration
	autoFailback          bool

	cleanupInterval      time.Duration
	keepUnusedServerTime time.Duration

	// transportFallback is shared across home and foreign relay clients so a
	// datagram transport failure is remembered across reconnects.
	transportFallback *transportFallback
	dataPlaneFailures *relayDataPlaneFailures
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
	m.serverPicker.rttLookup = m.rttCache.get
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
	m.switchMu.Lock()
	defer m.switchMu.Unlock()
	if !m.running.Load() || m.ctx.Err() != nil {
		return
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
		var candidate *Client
		var err error
		if len(candidateURLs) > 0 {
			candidate, err = m.serverPicker.PickServerFrom(m.ctx, candidateURLs)
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
			} else {
				log.Errorf("failed to prepare replacement Relay client; keeping %s active: %v", homeURL, err)
			}
			return
		}
		if !m.swapHomeRelay(home, candidate, generation) {
			_ = candidate.Close()
			return
		}
		log.Infof("replacement home Relay client %s is ready; migrating peer connections from %s", candidate.connectionURL, homeURL)
		m.onServerConnected()
		m.retireHomeRelay(home)
		return
	}

	m.relayClientsMutex.RLock()
	track := m.relayClients[relayAddress]
	m.relayClientsMutex.RUnlock()
	if track == nil {
		return
	}
	track.RLock()
	foreign := track.relayClient
	track.RUnlock()
	if foreign != nil {
		log.Warnf("Relay data plane is unresponsive on foreign server %s; rebuilding it", relayAddress)
		_ = foreign.Close()
	}
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
	go m.startCleanupLoop()
	// The failback loop only runs when explicitly enabled, so a disabled
	// manager pays no cost for it.
	if m.autoFailback {
		go m.startFailbackLoop()
	}
	// The probe loop keeps dial RTTs warm so the picker can order relays
	// inside a priority group by latency. It is cheap: one transport
	// handshake per relay per interval, no relay session is established.
	go m.startProbeLoop()
	return err
}

// OpenConn opens a connection to the given peer key. If the peer is on the same relay server, the connection will be
// established via the relay server. If the peer is on a different relay server, the manager will establish a new
// connection to the relay server. It returns back with a net.Conn what represent the remote peer connection.
//
// serverIP, when valid and serverAddress is foreign, is used as a dial target if the FQDN-based dial fails.
// Ignored for the local home-server path. TLS verification still uses the FQDN via SNI.
func (m *Manager) OpenConn(ctx context.Context, serverAddress, peerKey string, serverIP netip.Addr) (net.Conn, error) {
	m.relayClientMu.RLock()
	defer m.relayClientMu.RUnlock()

	if m.relayClient == nil {
		return nil, ErrRelayClientNotConnected
	}

	foreign, err := m.isForeignServer(serverAddress)
	if err != nil {
		return nil, err
	}

	var (
		netConn net.Conn
	)
	if !foreign {
		log.Debugf("open peer connection via permanent server: %s", peerKey)
		netConn, err = m.relayClient.OpenConn(ctx, peerKey)
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

	foreign, err := m.isForeignServer(serverAddress)
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

func (m *Manager) UpdateServerURLs(serverURLs []string) {
	m.UpdateServerURLsWithWeights(serverURLs, nil)
}

func (m *Manager) UpdateServerURLsWithWeights(serverURLs []string, relayWeights map[string]int) {
	log.Infof("update relay server URLs: %v", serverURLs)
	m.relayConfigMu.Lock()
	previousURLs := m.effectiveRelayURLsLocked()
	previousWeights := maps.Clone(m.relayWeights)
	previousForcedURL := m.forcedRelayURL
	m.relayWeights = relayWeightsFromURLs(serverURLs)
	for relayURL, weight := range relayWeights {
		if relayURL != "" && weight > 0 {
			m.relayWeights[relayURL] = weight
		}
	}
	m.configuredRelayURLs = sortRelayURLsByWeight(serverURLs, m.relayWeights)
	if m.forcedRelayURL != "" && !slices.Contains(m.configuredRelayURLs, m.forcedRelayURL) {
		log.Warnf("forced Relay server %s is no longer configured, clearing override", m.forcedRelayURL)
		m.forcedRelayURL = ""
	}
	effectiveURLs := m.effectiveRelayURLsLocked()
	weights := maps.Clone(m.relayWeights)
	forcedURL := m.forcedRelayURL
	m.relayConfigMu.Unlock()
	configChanged := !slices.Equal(previousURLs, effectiveURLs) || !maps.Equal(previousWeights, weights) || previousForcedURL != forcedURL

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
	m.relayConfigMu.RUnlock()
	currentURL := m.currentRelayURL()

	result := make([]RelayServerInfo, 0, len(configuredURLs))
	for _, relayURL := range configuredURLs {
		weight := weights[relayURL]
		if weight <= 0 {
			weight = defaultRelayWeight
		}
		info := RelayServerInfo{
			URL: relayURL, Weight: weight, Forced: relayURL == forcedURL, Current: relayURL == currentURL,
		}
		if rtt, ok := m.rttCache.get(relayURL); ok {
			info.RTTMs = rtt.Milliseconds()
		}
		result = append(result, info)
	}
	return result
}

func (m *Manager) ProbeRelayServers(ctx context.Context) []RelayServerInfo {
	relays := m.RelayServers()
	var wg sync.WaitGroup
	for i := range relays {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, relayProbeTimeout)
			defer cancel()
			// Relay servers allow one session per peer ID. A probe must use an
			// independent identity or it can evict a live home/foreign session.
			probeID := m.peerID + "-relay-probe-" + uuid.NewString()
			probeClient := NewClient(relays[idx].URL, m.tokenStore, probeID, m.mtu)
			probeClient.SetTransportFallback(m.transportFallback)
			probeStarted := time.Now()
			if err := probeClient.Connect(probeCtx); err != nil {
				relays[idx].Error = err.Error()
				return
			}
			rtt := time.Since(probeStarted)
			m.rttCache.set(relays[idx].URL, rtt)
			relays[idx].Available = true
			relays[idx].RTTMs = rtt.Milliseconds()
			if err := probeClient.Close(); err != nil {
				// The probe itself succeeded; a close failure must not
				// overturn the result and mark the relay unavailable.
				log.Debugf("relay probe for %s: close failed after successful probe: %v", relays[idx].URL, err)
			}
		}(i)
	}
	wg.Wait()
	return relays
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
	if !m.running.Load() || m.relayClient == nil || m.currentRelayStillHighestPriorityLocked(serverURLs) {
		m.relayClientMu.RUnlock()
		return
	}
	oldClient := m.relayClient
	oldURL := oldClient.connectionURL
	m.relayClientMu.RUnlock()

	log.Infof("relay priority changed from %s to %s; preparing the new home Relay before switching", oldURL, serverURLs[0])
	candidateURLs := m.priorityMigrationCandidateURLs(oldClient, serverURLs)
	if len(candidateURLs) == 0 {
		return
	}
	candidate, foreignTrackPresent := m.foreignRelayCandidate(candidateURLs)
	foreignCandidate := candidate != nil
	if candidate == nil {
		if foreignTrackPresent {
			log.Infof("higher-priority Relay connection is still being prepared; keeping %s active", oldURL)
			return
		}
		var err error
		candidate, err = m.serverPicker.PickServerFrom(m.ctx, candidateURLs)
		if err != nil {
			log.Errorf("failed to prepare higher-priority Relay; keeping %s active: %v", oldURL, err)
			return
		}
	}
	if !m.isValidPriorityMigration(oldClient, candidate, serverURLs) {
		log.Infof("no better Relay candidate became ready; keeping %s active", oldURL)
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
	oldWeight := m.relayWeights[oldURL]
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
	if oldWeight <= 0 {
		oldWeight = defaultRelayWeight
	}
	result := make([]string, 0, len(serverURLs))
	for _, relayURL := range serverURLs {
		weight := m.serverPicker.relayURLWeight(relayURL)
		if relayURL != oldURL && weight > oldWeight {
			result = append(result, relayURL)
		}
	}
	return result
}

func (m *Manager) isValidPriorityMigration(oldClient, candidate *Client, serverURLs []string) bool {
	if oldClient == nil || candidate == nil {
		return false
	}
	oldURL, candidateURL := oldClient.connectionURL, candidate.connectionURL
	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	oldWeight, candidateWeight := m.relayWeights[oldURL], m.relayWeights[candidateURL]
	m.relayConfigMu.RUnlock()
	if forcedURL != "" {
		return candidateURL == forcedURL
	}
	if !slices.Contains(serverURLs, oldURL) {
		return slices.Contains(serverURLs, candidateURL)
	}
	if oldWeight <= 0 {
		oldWeight = defaultRelayWeight
	}
	if candidateWeight <= 0 {
		candidateWeight = defaultRelayWeight
	}
	return candidateURL != oldURL && candidateWeight > oldWeight
}

func (m *Manager) swapHomeRelay(oldClient, candidate *Client, generation uint64) bool {
	if candidate == nil || !candidate.Ready() || generation != m.relayConfigGeneration.Load() {
		return false
	}

	m.relayClientMu.Lock()
	defer m.relayClientMu.Unlock()
	if !m.running.Load() || m.ctx.Err() != nil || m.relayClient != oldClient || generation != m.relayConfigGeneration.Load() {
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
	oldClient.SetOnDisconnectListener(func(address string) { m.onServerDisconnected(oldClient, address) })
	m.relayClient = candidate
	candidate.SetOnDisconnectListener(func(address string) { m.onServerDisconnected(candidate, address) })
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
	currentWeight, topWeight := m.relayWeights[currentURL], m.relayWeights[serverURLs[0]]
	m.relayConfigMu.RUnlock()
	if forcedURL != "" {
		return currentURL == forcedURL
	}
	if currentWeight <= 0 {
		currentWeight = defaultRelayWeight
	}
	if topWeight <= 0 {
		topWeight = defaultRelayWeight
	}
	return currentWeight == topWeight && slices.Contains(serverURLs, currentURL)
}

// UpdateToken updates the token in the token store.
func (m *Manager) UpdateToken(token *relayAuth.Token) error {
	return m.tokenStore.UpdateToken(token)
}

func (m *Manager) openConnVia(ctx context.Context, serverAddress, peerKey string, serverIP netip.Addr) (net.Conn, error) {
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
	relayClient.SetOnDisconnectListener(func(address string) { m.onServerDisconnected(relayClient, address) })
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
func (m *Manager) openConnOnTrack(ctx context.Context, rt *RelayTrack, peerKey string) (net.Conn, error) {
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

// onServerDisconnected handles relay disconnect events. For the home server it
// starts the reconnect guard. For foreign servers it evicts the now-dead client
// from the cache so the next OpenConn builds a fresh one instead of reusing a
// closed client.
func (m *Manager) onServerDisconnected(disconnectedClient *Client, serverAddress string) {
	m.relayClientMu.Lock()
	isHome := m.relayClient == disconnectedClient
	if isHome {
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

func (m *Manager) evictForeignRelayClient(serverAddress string, disconnectedClient *Client) {
	m.relayClientsMutex.Lock()
	defer m.relayClientsMutex.Unlock()
	track := m.relayClients[serverAddress]
	if track == nil {
		return
	}
	track.RLock()
	trackedClient := track.relayClient
	track.RUnlock()
	if trackedClient == disconnectedClient {
		delete(m.relayClients, serverAddress)
		log.Debugf("evicted disconnected foreign relay client: %s", serverAddress)
	}
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
			m.storeClient(rc)
			m.onServerConnected()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) storeClient(client *Client) {
	m.relayClientMu.Lock()
	defer m.relayClientMu.Unlock()

	m.relayClient = client
	m.relayClient.SetOnDisconnectListener(func(address string) { m.onServerDisconnected(client, address) })
}

func (m *Manager) isForeignServer(address string) (bool, error) {
	rAddr, err := m.relayClient.ServerInstanceURL()
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

// startFailbackLoop periodically checks whether a higher-priority Relay server
// has recovered so the home Relay can fail back to it.
func (m *Manager) startFailbackLoop() {
	log.Infof("Relay auto-failback enabled, checking every %s", relayAutoFailbackInterval)
	ticker := time.NewTicker(relayAutoFailbackInterval)
	defer ticker.Stop()
	// healthyStreaks counts consecutive ticks in which the failback target was
	// observed outside its failure cooldown. It is owned by this loop.
	healthyStreaks := make(map[string]int)
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.failbackTick(healthyStreaks)
		}
	}
}

// startProbeLoop periodically refreshes dial RTTs for the relays sharing the
// top priority group, so the server picker can order them by latency. Like
// DERP probing in Tailscale, each tick is jittered to keep a fleet of clients
// from probing in lockstep.
func (m *Manager) startProbeLoop() {
	// slowStreaks counts consecutive ticks in which the home relay was
	// significantly slower than a same-priority alternative. It is owned by
	// this loop.
	slowStreaks := make(map[string]int)
	timer := time.NewTimer(relayRTTProbeInterval + rand.N(relayRTTProbeJitter))
	defer timer.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			timer.Reset(relayRTTProbeInterval + rand.N(relayRTTProbeJitter))
			m.probeTick(slowStreaks)
		}
	}
}

// probeTick probes the top priority group with the cheap transport probe and,
// when the home relay is persistently much slower than an alternative, hands
// the migration decision to maybeMigrateOnRTT.
func (m *Manager) probeTick(slowStreaks map[string]int) {
	if !m.running.Load() || m.ctx.Err() != nil {
		return
	}
	// Skip while a home-relay switch is in flight; the switch holds switchMu,
	// and TryLock keeps the probe loop from blocking on it.
	if !m.switchMu.TryLock() {
		return
	}
	m.switchMu.Unlock()

	m.relayConfigMu.RLock()
	urls := slices.Clone(m.configuredRelayURLs)
	weights := maps.Clone(m.relayWeights)
	m.relayConfigMu.RUnlock()

	group := topPriorityGroup(urls, weights)
	if len(group) < 2 {
		clear(slowStreaks)
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, relayProbeTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, relayURL := range group {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			rtt, err := m.lightProbe(ctx, url)
			if err != nil {
				log.Debugf("relay RTT probe for %s failed: %v", url, err)
				return
			}
			m.rttCache.set(url, rtt)
			log.Debugf("relay RTT probe for %s: %s", url, rtt.Round(time.Millisecond))
		}(relayURL)
	}
	wg.Wait()

	m.maybeMigrateOnRTT(group, slowStreaks)
}

// lightProbe measures the transport handshake round trip to a relay without
// establishing a relay session, so periodic probing stays cheap for both the
// client and the relay server.
func (m *Manager) lightProbe(ctx context.Context, relayURL string) (time.Duration, error) {
	probeClient := NewClient(relayURL, m.tokenStore, m.peerID+"-rtt-probe", m.mtu)
	probeClient.SetTransportFallback(m.transportFallback)
	return probeClient.ProbeTransport(ctx)
}

// topPriorityGroup returns the relay URLs sharing the highest effective
// weight. Probing and RTT ordering only matter inside one weight group,
// because a relay never outranks a higher-weight one.
func topPriorityGroup(urls []string, weights map[string]int) []string {
	best := -1
	for _, u := range urls {
		if w := relayWeightOrDefault(weights, u); w > best {
			best = w
		}
	}
	if best < 0 {
		return nil
	}
	var group []string
	for _, u := range urls {
		if relayWeightOrDefault(weights, u) == best {
			group = append(group, u)
		}
	}
	return group
}

// maybeMigrateOnRTT migrates the home relay to a same-priority alternative
// that has been significantly faster for relayRTTMigrationStreaks consecutive
// ticks. It never overrides weight priority: both relays are in the top
// priority group. The thresholds are deliberately conservative because a
// migration briefly disrupts peer traffic.
func (m *Manager) maybeMigrateOnRTT(group []string, slowStreaks map[string]int) {
	homeURL := m.currentRelayURL()
	if homeURL == "" || !slices.Contains(group, homeURL) {
		clear(slowStreaks)
		return
	}
	homeRTT, ok := m.rttCache.get(homeURL)
	if !ok {
		clear(slowStreaks)
		return
	}
	bestURL, bestRTT := "", time.Duration(0)
	for _, u := range group {
		if u == homeURL {
			continue
		}
		rtt, ok := m.rttCache.get(u)
		if !ok {
			continue
		}
		if bestURL == "" || rtt < bestRTT {
			bestURL, bestRTT = u, rtt
		}
	}
	// The alternative must be at least twice as fast and 100ms faster in
	// absolute terms; below that the win is not worth a migration.
	if bestURL == "" || homeRTT < 2*bestRTT || homeRTT-bestRTT < 100*time.Millisecond {
		clear(slowStreaks)
		return
	}
	slowStreaks[bestURL]++
	if slowStreaks[bestURL] < relayRTTMigrationStreaks {
		log.Debugf("home relay %s slower than %s (%s vs %s), streak %d/%d",
			homeURL, bestURL, homeRTT.Round(time.Millisecond), bestRTT.Round(time.Millisecond),
			slowStreaks[bestURL], relayRTTMigrationStreaks)
		return
	}
	clear(slowStreaks)
	log.Infof("home relay %s persistently slower than %s (%s vs %s); migrating",
		homeURL, bestURL, homeRTT.Round(time.Millisecond), bestRTT.Round(time.Millisecond))
	m.migrateHomeRelayTo(bestURL)
}

// migrateHomeRelayTo connects to targetURL and swaps it in as the home relay.
// It follows the same swap-then-retire pattern as the data-plane recovery
// path, guarded by the config generation so a concurrent reconfiguration wins.
func (m *Manager) migrateHomeRelayTo(targetURL string) {
	if !m.running.Load() || m.ctx.Err() != nil {
		return
	}
	m.relayClientMu.RLock()
	home := m.relayClient
	m.relayClientMu.RUnlock()
	if home == nil {
		return
	}
	homeURL := home.connectionURL
	if homeURL == targetURL {
		return
	}
	generation := m.relayConfigGeneration.Load()
	candidate, err := m.serverPicker.PickServerFrom(m.ctx, []string{targetURL})
	if err != nil {
		log.Warnf("RTT migration to %s failed to connect; keeping %s: %v", targetURL, homeURL, err)
		return
	}
	if !m.swapHomeRelay(home, candidate, generation) {
		log.Warnf("RTT migration to %s lost the race; keeping %s", targetURL, homeURL)
		_ = candidate.Close()
		return
	}
	log.Infof("RTT migration: home relay moved from %s to %s", homeURL, targetURL)
	m.onServerConnected()
	m.retireHomeRelay(home)
}

// failbackTick evaluates one automatic failback opportunity. It only ever
// moves the home Relay upward to the strictly highest-weight server, and only
// after that server has been observed healthy for relayFailbackStableChecks
// consecutive ticks, which damps flapping around a recovering server. The
// actual move reuses switchHomeRelayIfNeeded so the migration, its validation,
// and the generation guard stay on the single existing path.
func (m *Manager) failbackTick(healthyStreaks map[string]int) {
	if !m.autoFailback || !m.running.Load() || m.ctx.Err() != nil {
		return
	}

	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	weights := maps.Clone(m.relayWeights)
	configuredURLs := slices.Clone(m.configuredRelayURLs)
	m.relayConfigMu.RUnlock()

	// A manual relay override always wins over automatic failback.
	if forcedURL != "" {
		clear(healthyStreaks)
		return
	}

	homeURL := m.currentRelayURL()
	homeWeight := relayWeightOrDefault(weights, homeURL)
	targetURL, targetWeight := highestWeightRelay(configuredURLs, weights)
	if homeURL == "" || targetURL == "" || targetURL == homeURL || targetWeight <= homeWeight {
		clear(healthyStreaks)
		return
	}

	// Drop streaks for servers that are no longer the failback target, so a
	// stale count can never trigger a move to a different server.
	for trackedURL := range healthyStreaks {
		if trackedURL != targetURL {
			delete(healthyStreaks, trackedURL)
		}
	}

	// A server that failed recently sits in the picker's failure cooldown. It
	// must stay out of cooldown for relayFailbackStableChecks consecutive ticks
	// to count as stable enough to fail back to.
	if m.relayServerCoolingDown(targetURL) {
		if _, ok := healthyStreaks[targetURL]; ok {
			log.Debugf("deferring Relay failback to %s: server is in failure cooldown", targetURL)
			delete(healthyStreaks, targetURL)
		}
		return
	}
	healthyStreaks[targetURL]++
	if healthyStreaks[targetURL] < relayFailbackStableChecks {
		log.Debugf("Relay %s healthy for %d of %d failback checks", targetURL, healthyStreaks[targetURL], relayFailbackStableChecks)
		return
	}
	clear(healthyStreaks)

	log.Infof("failing back to higher-priority Relay %s (weight %d) from %s (weight %d): server is stable",
		targetURL, targetWeight, homeURL, homeWeight)
	// The config is unchanged, so the generation is observed, not advanced: a
	// concurrent relay config update invalidates this attempt inside
	// switchHomeRelayIfNeeded.
	generation := m.relayConfigGeneration.Load()
	go m.switchHomeRelayIfNeeded(sortRelayURLsByWeight(configuredURLs, weights), generation)
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

// relayWeightOrDefault returns the configured weight of a Relay server,
// falling back to the default weight for unknown or non-positive values.
func relayWeightOrDefault(weights map[string]int, relayURL string) int {
	if weight := weights[relayURL]; weight > 0 {
		return weight
	}
	return defaultRelayWeight
}

// highestWeightRelay returns the first URL with the highest configured weight.
func highestWeightRelay(relayURLs []string, weights map[string]int) (string, int) {
	bestURL, bestWeight := "", 0
	for _, relayURL := range relayURLs {
		if weight := relayWeightOrDefault(weights, relayURL); weight > bestWeight {
			bestURL, bestWeight = relayURL, weight
		}
	}
	return bestURL, bestWeight
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
