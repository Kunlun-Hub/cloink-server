package client

import (
	"container/list"
	"context"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/client/netstate"
	"github.com/netbirdio/netbird/client/netsweep"
	relayAuth "github.com/netbirdio/netbird/shared/relay/auth/hmac"
)

var (
	relayCleanupInterval = 60 * time.Second
	keepUnusedServerTime = 5 * time.Second
	relayProbeTimeout    = 6 * time.Second
	relayServerCooldown  = 2 * time.Minute
	defaultRelayWeight   = 30

	ErrRelayClientNotConnected = fmt.Errorf("relay client not connected")
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

// WithNetworkState injects the OS network availability state that gates the
// reconnect guard; without it reconnect attempts are not gated.
func WithNetworkState(netState *netstate.State) ManagerOption {
	return func(m *Manager) { m.netState = netState }
}

// WithSweeper injects the network change sweeper.
func WithSweeper(sweeper *netsweep.Sweeper) ManagerOption {
	return func(m *Manager) { m.sweeper = sweeper }
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

	onDisconnectedListeners map[string]*list.List
	onReconnectedListenerFn func()
	listenerLock            sync.Mutex

	mtu                 uint16
	maxBackoffInterval  time.Duration
	netState            *netstate.State
	sweeper             *netsweep.Sweeper
	relayServerCooldown time.Duration
	switchMu            sync.Mutex
	relayConfigMu       sync.RWMutex
	configuredRelayURLs []string
	relayWeights        map[string]int
	forcedRelayURL      string

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
		serverPicker: &ServerPicker{
			TokenStore:        tokenStore,
			PeerID:            peerID,
			MTU:               mtu,
			ConnectionTimeout: defaultConnectionTimeout,
			TransportFallback: tf,
		},
		relayClients:            make(map[string]*RelayTrack),
		onDisconnectedListeners: make(map[string]*list.List),
		cleanupInterval:         relayCleanupInterval,
		keepUnusedServerTime:    keepUnusedServerTime,
	}
	for _, opt := range opts {
		opt(m)
	}
	m.serverPicker.Sweeper = m.sweeper
	m.serverPicker.CooldownDuration = m.relayServerCooldown
	m.configuredRelayURLs = slices.Clone(serverURLs)
	m.relayWeights = relayWeightsFromURLs(serverURLs)
	m.serverPicker.storeConfig(pickerConfig{
		serverURLs:    m.effectiveRelayURLsLocked(),
		serverWeights: maps.Clone(m.relayWeights),
	})
	m.reconnectGuard = NewGuard(m.serverPicker, m.maxBackoffInterval, m.netState)
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

	m.relayClientMu.Lock()
	home := m.relayClient
	isHome := false
	homeURL := ""
	if home != nil {
		homeURL = home.connectionURL
		instanceURL, err := home.ServerInstanceURL()
		isHome = relayAddress == homeURL || err == nil && relayAddress == instanceURL
	}
	if isHome {
		home.SetOnDisconnectListener(nil)
		m.relayClient = nil
	}
	m.relayClientMu.Unlock()

	if isHome {
		log.Warnf("Relay data plane is unresponsive on %s; rebuilding the home Relay client", homeURL)
		m.markDataPlaneTransportFailure(home)
		m.markDataPlaneServerFailure(homeURL)
		m.notifyOnDisconnectListeners(homeURL)
		if err := home.Close(); err != nil {
			log.Warnf("failed to close unresponsive home Relay client %s: %v", homeURL, err)
		}
		client, err := m.serverPicker.PickServer(m.ctx)
		if err != nil {
			log.Errorf("failed to recover Relay data plane: %v", err)
			go m.reconnectGuard.StartReconnectTrys(m.ctx, nil)
			return
		}
		m.storeClient(client)
		m.onServerConnected()
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

	var listenerAddr string
	if foreign {
		listenerAddr = serverAddress
	} else {
		listenerAddr = m.relayClient.connectionURL
	}
	m.addListener(listenerAddr, onClosedListener)
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
	addr, err := m.relayClient.ServerInstanceURL()
	if err != nil {
		return "", netip.Addr{}, err
	}
	return addr, m.relayClient.ConnectedIP(), nil
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

	m.serverPicker.storeConfig(pickerConfig{
		serverURLs:    effectiveURLs,
		serverWeights: weights,
		forcedURL:     forcedURL,
	})
	go m.switchHomeRelayIfNeeded(effectiveURLs)
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
		result = append(result, RelayServerInfo{
			URL: relayURL, Weight: weight, Forced: relayURL == forcedURL, Current: relayURL == currentURL,
		})
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
			probeClient := NewClient(relays[idx].URL, m.tokenStore, m.peerID, m.mtu)
			probeClient.SetTransportFallback(m.transportFallback)
			if err := probeClient.Connect(probeCtx); err != nil {
				relays[idx].Error = err.Error()
				return
			}
			relays[idx].Available = true
			if err := probeClient.Close(); err != nil {
				relays[idx].Available = false
				relays[idx].Error = err.Error()
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
		go m.switchHomeRelayIfNeeded(effectiveURLs)
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
	go m.switchHomeRelayIfNeeded(effectiveURLs)
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

func (m *Manager) switchHomeRelayIfNeeded(serverURLs []string) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

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
		if err := oldClient.Close(); err != nil {
			log.Warnf("failed to close previous home Relay server %s: %v", oldURL, err)
		}
		return
	}

	m.relayClientMu.Lock()
	if !m.running.Load() || m.relayClient == nil || m.currentRelayStillHighestPriorityLocked(serverURLs) {
		m.relayClientMu.Unlock()
		return
	}
	oldClient := m.relayClient
	oldURL := oldClient.connectionURL
	oldClient.SetOnDisconnectListener(nil)
	m.relayClient = nil
	m.relayClientMu.Unlock()

	log.Infof("relay priority changed from %s to %s, switching home Relay server", oldURL, serverURLs[0])
	if err := oldClient.Close(); err != nil {
		log.Warnf("failed to close previous home Relay server %s: %v", oldURL, err)
	}
	newClient, err := m.serverPicker.PickServer(m.ctx)
	if err != nil {
		log.Errorf("failed to switch home Relay server: %v", err)
		go m.reconnectGuard.StartReconnectTrys(m.ctx, nil)
		return
	}
	m.storeClient(newClient)
	m.onServerConnected()
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
	relayClient.sweeper = m.sweeper
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
	relayClient.SetOnDisconnectListener(m.onServerDisconnected)
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
func (m *Manager) onServerDisconnected(serverAddress string) {
	m.relayClientMu.Lock()
	isHome := m.relayClient != nil && serverAddress == m.relayClient.connectionURL
	if isHome {
		go func(client *Client) {
			m.reconnectGuard.StartReconnectTrys(m.ctx, client)
		}(m.relayClient)
	}
	m.relayClientMu.Unlock()

	if !isHome {
		m.evictForeignRelay(serverAddress)
	}

	m.notifyOnDisconnectListeners(serverAddress)
}

func (m *Manager) evictForeignRelay(serverAddress string) {
	m.relayClientsMutex.Lock()
	defer m.relayClientsMutex.Unlock()
	if _, ok := m.relayClients[serverAddress]; ok {
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
	m.relayClient.SetOnDisconnectListener(m.onServerDisconnected)
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

func (m *Manager) addListener(serverAddress string, onClosedListener OnServerCloseListener) {
	m.listenerLock.Lock()
	defer m.listenerLock.Unlock()
	l, ok := m.onDisconnectedListeners[serverAddress]
	if !ok {
		l = list.New()
	}
	for e := l.Front(); e != nil; e = e.Next() {
		if reflect.ValueOf(e.Value).Pointer() == reflect.ValueOf(onClosedListener).Pointer() {
			return
		}
	}
	l.PushBack(onClosedListener)
	m.onDisconnectedListeners[serverAddress] = l
}

func (m *Manager) notifyOnDisconnectListeners(serverAddress string) {
	m.listenerLock.Lock()
	defer m.listenerLock.Unlock()

	l, ok := m.onDisconnectedListeners[serverAddress]
	if !ok {
		return
	}
	for e := l.Front(); e != nil; e = e.Next() {
		go e.Value.(OnServerCloseListener)()
	}
	delete(m.onDisconnectedListeners, serverAddress)
}

func relayConnState(c *Client) RelayConnState {
	addr, err := c.ServerInstanceURL()
	if err != nil {
		return RelayConnState{URL: c.connectionURL, Err: err}
	}
	return RelayConnState{URL: addr, Transport: c.Transport()}
}
