package relays

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"

	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/server/account"
	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/geolocation"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/telemetry"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/relay/healthcheck/peerid"
	relayserver "github.com/netbirdio/netbird/relay/server"
	"github.com/netbirdio/netbird/shared/auth"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
	nbrelay "github.com/netbirdio/netbird/shared/relay"
	"github.com/netbirdio/netbird/shared/relay/messages"
)

const (
	relayProbeTimeout           = 5 * time.Second
	relayHealthMaxBytes         = 64 << 10
	relaySetupTokenNeverExpires = 0
	relaySetupTokenTTL          = 15 * time.Minute
	relayRegistrationTTL        = 2 * time.Minute
	relaySetupTokenVersion      = "v1"
	defaultRelayPriority        = 30
	maxRelayPriority            = 1000
	maxRelayIDLength            = 128
	maxRelayNameLength          = 256
	maxRelayAddressLength       = 2048
	maxRelayManagementURLLength = 2048
	maxRelayVersionLength       = 128
	maxRelayGroups              = 32
	maxRelayGroupNameLength     = 64
)

type Handler struct {
	accountManager account.Manager
	config         *nbconfig.Relay
	geo            geolocation.Geolocation
	configPusher   relayConfigPusher
	permissions    permissions.Manager
	metrics        *telemetry.RelayMetrics
}

type relayConfigPusher interface {
	PushRelayList(ctx context.Context, accountID string, peerIDs []string) int
}

type RelayStatus struct {
	Address           string `json:"address"`
	ID                string `json:"id,omitempty"`
	Name              string `json:"name,omitempty"`
	ObservedID        string `json:"observed_id,omitempty"`
	Registered        bool   `json:"registered,omitempty"`
	Priority          int    `json:"priority"`
	Status            string `json:"status"`
	ConnectedClients  *int   `json:"connected_clients,omitempty"`
	RegisteredClients int    `json:"registered_clients"`
	PublicIP          string `json:"public_ip,omitempty"`
	CountryCode       string `json:"country_code,omitempty"`
	CityName          string `json:"city_name,omitempty"`
	// Groups lists the relay's distribution scope as group names.
	// Empty means the relay is distributed to every peer.
	Groups      []string  `json:"groups,omitempty"`
	LastChecked time.Time `json:"last_checked"`
	Error       string    `json:"error,omitempty"`
}

type relaySetupTokenResponse struct {
	Token           string `json:"token"`
	RelayAuthSecret string `json:"relay_auth_secret"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

type registerRelayRequest struct {
	SetupKey         string `json:"setup_key"`
	ID               string `json:"id"`
	Name             string `json:"name,omitempty"`
	Address          string `json:"address"`
	Priority         int    `json:"priority,omitempty"`
	ManagementURL    string `json:"management_url,omitempty"`
	Version          string `json:"version,omitempty"`
	ConnectedClients *int   `json:"connected_clients,omitempty"`
	// Groups limits relay distribution to peers in at least one of these
	// groups, given as group names. Empty means the relay is distributed to
	// every peer.
	Groups []string `json:"groups,omitempty"`
}

type registerRelayResponse struct {
	Status string `json:"status"`
}

type updateRelayRequest struct {
	Priority int `json:"priority"`
	// Groups replaces the relay's distribution groups when present, given as
	// group names and resolved to group IDs. Nil leaves the current groups
	// unchanged; an empty list restores global distribution.
	Groups *[]string `json:"groups,omitempty"`
}

type applyRelayConfigResponse struct {
	Status      string `json:"status"`
	TargetPeers int    `json:"target_peers"`
}

type healthResponse struct {
	ConnectedPeers *int   `json:"connected_peers,omitempty"`
	RelayID        string `json:"relay_id,omitempty"`
}

type registeredRelay struct {
	ID               string
	Name             string
	Address          string
	Priority         int
	ManagementURL    string
	Version          string
	ConnectedClients *int
	// Groups limits relay distribution to peers in at least one of these
	// groups, stored as group IDs. Empty means the relay is distributed to
	// every peer.
	Groups   []string
	LastSeen time.Time
}

type RelayServerDescriptor struct {
	ID       string
	Name     string
	Address  string
	Priority int
	// ConnectedPeers is the last known number of connected clients, or zero
	// when unknown. It feeds the load-aware effective priority.
	ConnectedPeers int
	// Groups limits relay distribution to peers in at least one of these
	// groups, stored as group IDs. Empty means the relay is distributed to
	// every peer.
	Groups []string
}

type relayRegistry struct {
	mu     sync.RWMutex
	relays map[string]registeredRelay
}

var activeRelayRegistry = &relayRegistry{
	relays: make(map[string]registeredRelay),
}

func ActiveRelayAddresses(config *nbconfig.Relay) []string {
	return relayDescriptorAddresses(ActiveRelayServers(config))
}

func ActiveRelayServers(config *nbconfig.Relay) []RelayServerDescriptor {
	return relayServers(config, nil)
}

func RelayAddressesForAccount(config *nbconfig.Relay, settings *types.Settings) []string {
	return relayDescriptorAddresses(RelayServersForAccount(config, settings))
}

func RelayServersForAccount(config *nbconfig.Relay, settings *types.Settings) []RelayServerDescriptor {
	return relayServers(config, registeredRelaysFromSettings(settings))
}

func relayServers(config *nbconfig.Relay, registeredRelays []registeredRelay) []RelayServerDescriptor {
	var allRelays []RelayServerDescriptor
	seenAll := make(map[string]int)
	addRelay := func(id, name, address string, priority, connectedPeers int, groups []string) {
		if address == "" {
			return
		}
		priority = normalizeRelayPriority(priority)
		if idx, ok := seenAll[address]; ok {
			existing := &allRelays[idx]
			if priority > existing.Priority {
				existing.ID = relayKey(id, address)
				existing.Name = name
				existing.Priority = priority
				existing.ConnectedPeers = connectedPeers
				existing.Groups = slices.Clone(groups)
				return
			}
			if priority == existing.Priority {
				if existing.Name == "" {
					existing.Name = name
				}
				if id != "" && strings.HasPrefix(existing.ID, "relay_") {
					existing.ID = relayKey(id, address)
				}
				// A self-registered duplicate carries fresher load and group
				// data than the static config entry; adopt it when present.
				if connectedPeers > 0 {
					existing.ConnectedPeers = connectedPeers
				}
				if len(groups) > 0 {
					existing.Groups = slices.Clone(groups)
				}
			}
			return
		}
		relay := RelayServerDescriptor{
			ID:             relayKey(id, address),
			Name:           name,
			Address:        address,
			Priority:       priority,
			ConnectedPeers: connectedPeers,
			Groups:         slices.Clone(groups),
		}
		seenAll[address] = len(allRelays)
		allRelays = append(allRelays, relay)
	}

	if config != nil {
		for _, server := range config.GetServers() {
			if server == nil {
				continue
			}
			addRelay(server.ID, server.Name, server.Address, server.Priority, 0, nil)
		}
	}

	for _, relay := range registeredRelays {
		if time.Since(relay.LastSeen) > relayRegistrationTTL {
			continue
		}
		connectedPeers := 0
		if relay.ConnectedClients != nil && *relay.ConnectedClients > 0 {
			connectedPeers = *relay.ConnectedClients
		}
		addRelay(relay.ID, relay.Name, relay.Address, relay.Priority, connectedPeers, relay.Groups)
	}

	sortRelayDescriptorsByPriority(allRelays)
	return allRelays
}

func relayDescriptorAddresses(relays []RelayServerDescriptor) []string {
	addresses := make([]string, 0, len(relays))
	for _, relay := range relays {
		if relay.Address == "" {
			continue
		}
		addresses = append(addresses, relay.Address)
	}
	return addresses
}

func normalizeRelayPriority(priority int) int {
	if priority <= 0 {
		return defaultRelayPriority
	}
	return priority
}

// normalizeRelayGroups trims group names, drops empties and duplicates, and
// enforces the count and length limits.
func normalizeRelayGroups(groups []string) ([]string, error) {
	if len(groups) > maxRelayGroups {
		return nil, fmt.Errorf("relay groups must not exceed %d entries", maxRelayGroups)
	}
	result := make([]string, 0, len(groups))
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		name := strings.TrimSpace(group)
		if name == "" {
			continue
		}
		if strings.ContainsAny(name, "\x00\r\n") {
			return nil, fmt.Errorf("relay group name %q contains invalid characters", name)
		}
		if len(name) > maxRelayGroupNameLength {
			return nil, fmt.Errorf("relay group name %q exceeds %d characters", name, maxRelayGroupNameLength)
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	return result, nil
}

// resolveRelayGroupIDs maps user-supplied group names to group IDs for the
// account. Relay distribution stores group IDs: they are stable across group
// renames, and both distribution paths (sync pull and config push) already
// work with IDs. The API keeps accepting names for usability; unknown names
// are rejected so a typo cannot silently strand a relay.
func (h *Handler) resolveRelayGroupIDs(ctx context.Context, accountID string, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	if h.accountManager == nil {
		return nil, fmt.Errorf("account manager is not available")
	}
	account, err := h.accountManager.GetAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to load account groups: %w", err)
	}
	idsByName := make(map[string][]string)
	for id, group := range account.Groups {
		if group == nil || group.Name == "" {
			continue
		}
		idsByName[group.Name] = append(idsByName[group.Name], id)
	}
	ids := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	var unknown []string
	for _, name := range names {
		matched, ok := idsByName[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		for _, id := range matched {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("unknown groups: %s", strings.Join(unknown, ", "))
	}
	return ids, nil
}

// groupNamesForIDs is the best-effort reverse of resolveRelayGroupIDs, used to
// display stored group IDs as names in API responses. Unresolvable IDs are
// returned as-is so a deleted group stays visible instead of vanishing.
func (h *Handler) groupNamesForIDs(ctx context.Context, accountID string, ids []string) []string {
	if len(ids) == 0 || h.accountManager == nil {
		return nil
	}
	account, err := h.accountManager.GetAccount(ctx, accountID)
	if err != nil {
		return slices.Clone(ids)
	}
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		if group, ok := account.Groups[id]; ok && group != nil && group.Name != "" {
			names = append(names, group.Name)
			continue
		}
		names = append(names, id)
	}
	return names
}

// FilterRelayDescriptorsForGroups returns the relays a peer may receive:
// relays without groups are distributed globally, relays with groups only to
// peers whose group IDs intersect. peerGroupIDs holds the peer's group IDs;
// matching is exact and case-sensitive.
func FilterRelayDescriptorsForGroups(relays []RelayServerDescriptor, peerGroupIDs []string) []RelayServerDescriptor {
	if len(relays) == 0 {
		return nil
	}
	peerGroups := make(map[string]struct{}, len(peerGroupIDs))
	for _, id := range peerGroupIDs {
		peerGroups[id] = struct{}{}
	}
	result := make([]RelayServerDescriptor, 0, len(relays))
	for _, relay := range relays {
		if len(relay.Groups) == 0 {
			result = append(result, relay)
			continue
		}
		for _, group := range relay.Groups {
			if _, ok := peerGroups[group]; ok {
				result = append(result, relay)
				break
			}
		}
	}
	return result
}

func sortRelayDescriptorsByPriority(relays []RelayServerDescriptor) {
	slices.SortStableFunc(relays, func(left, right RelayServerDescriptor) int {
		if left.Priority != right.Priority {
			return right.Priority - left.Priority
		}
		return strings.Compare(relayKey(left.ID, left.Address), relayKey(right.ID, right.Address))
	})
}

func AddEndpoints(accountManager account.Manager, config *nbconfig.Relay, geo geolocation.Geolocation, configPusher relayConfigPusher, permissionsManager permissions.Manager, metrics *telemetry.RelayMetrics, router *mux.Router) {
	handler := &Handler{accountManager: accountManager, config: config, geo: geo, configPusher: configPusher, permissions: permissionsManager, metrics: metrics}
	router.HandleFunc("/relays", handler.getAllRelays).Methods("GET", "OPTIONS")
	router.HandleFunc("/relays/apply", handler.applyRelayConfig).Methods("POST", "OPTIONS")
	router.HandleFunc("/relays/setup-token", handler.createSetupToken).Methods("POST", "OPTIONS")
	router.HandleFunc("/relays/register", handler.registerRelay).Methods("POST", "OPTIONS")
	router.HandleFunc("/relays/{id}", handler.updateRelay).Methods("PUT", "OPTIONS")
	router.HandleFunc("/relays/{id}", handler.deleteRelay).Methods("DELETE", "OPTIONS")
}

func (h *Handler) getAllRelays(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Read)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, relayProbeTimeout)
	defer cancel()

	servers := make([]*nbconfig.RelayServer, 0)
	if h.config != nil {
		servers = h.config.GetServers()
	}

	registeredClients, err := h.registeredClients(ctx, userAuth.AccountId, userAuth.UserId)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}

	relays := make([]RelayStatus, 0, len(servers))
	seen := make(map[string]struct{}, len(servers))
	for _, server := range servers {
		if server == nil || server.Address == "" {
			continue
		}
		relays = append(relays, h.probeRelay(ctx, server, registeredClients))
		seen[relayKey(server.ID, server.Address)] = struct{}{}
	}

	for _, relay := range activeRelayRegistry.list(userAuth.AccountId) {
		key := relayKey(relay.ID, relay.Address)
		if _, ok := seen[key]; ok {
			continue
		}
		status := h.registeredRelayStatus(ctx, relay, registeredClients)
		status.Groups = h.groupNamesForIDs(ctx, userAuth.AccountId, relay.Groups)
		if status.Status == "online" && status.ConnectedClients != nil {
			// Refresh the load snapshot so the effective-priority
			// calculation in the relay config conversion sees live data.
			relay.ConnectedClients = status.ConnectedClients
			activeRelayRegistry.upsert(userAuth.AccountId, relay)
		}
		relays = append(relays, status)
		seen[key] = struct{}{}
	}

	for _, relay := range h.storedRegisteredRelays(ctx, userAuth.AccountId) {
		if _, ok := seen[relayKey(relay.ID, relay.Address)]; ok {
			continue
		}
		status := h.registeredRelayStatus(ctx, relay, registeredClients)
		status.Groups = h.groupNamesForIDs(ctx, userAuth.AccountId, relay.Groups)
		relays = append(relays, status)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(relays); err != nil {
		log.WithContext(r.Context()).Errorf("failed to encode relay status response: %v", err)
	}
}

func (h *Handler) createSetupToken(w http.ResponseWriter, r *http.Request) {
	if h.config == nil || h.config.Secret == "" {
		util.WriteErrorResponse("relay secret is not configured", http.StatusPreconditionFailed, w)
		return
	}

	userAuth, _, ok := h.authorize(w, r, operations.Update)
	if !ok {
		return
	}
	expiresAt := time.Now().Add(relaySetupTokenTTL)
	token, err := signRelaySetupToken(h.config.Secret, expiresAt.Unix(), userAuth.AccountId)
	if err != nil {
		util.WriteErrorResponse("failed to generate relay setup token", http.StatusInternalServerError, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(relaySetupTokenResponse{
		Token:           token,
		RelayAuthSecret: h.config.Secret,
		ExpiresAt:       expiresAt.UTC().Format(time.RFC3339),
	}); err != nil {
		log.WithContext(r.Context()).Errorf("failed to encode relay setup token response: %v", err)
	}
}

func (h *Handler) applyRelayConfig(w http.ResponseWriter, r *http.Request) {
	if h.configPusher == nil {
		util.WriteErrorResponse("relay config pusher is not configured", http.StatusPreconditionFailed, w)
		return
	}

	userAuth, ctx, ok := h.authorize(w, r, operations.Update)
	if !ok {
		return
	}

	if _, err := h.accountManager.GetAccountByID(ctx, userAuth.AccountId, userAuth.UserId); err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}

	targetPeers, err := h.pushRelayListToAccount(ctx, userAuth.AccountId, userAuth.UserId)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(applyRelayConfigResponse{
		Status:      "ok",
		TargetPeers: targetPeers,
	}); err != nil {
		log.WithContext(r.Context()).Errorf("failed to encode relay config apply response: %v", err)
	}
}

func validateRelayRegistration(req registerRelayRequest) error {
	if req.ID == "" {
		return errors.New("relay ID is required")
	}
	if len(req.ID) > maxRelayIDLength || strings.ContainsAny(req.ID, "\x00\r\n") {
		return errors.New("relay ID is invalid")
	}
	if len(req.Name) > maxRelayNameLength || strings.ContainsAny(req.Name, "\x00\r\n") {
		return errors.New("relay name is invalid")
	}
	if len(req.Version) > maxRelayVersionLength || strings.ContainsAny(req.Version, "\x00\r\n") {
		return errors.New("relay version is invalid")
	}
	if len(req.Address) > maxRelayAddressLength {
		return errors.New("relay address is too long")
	}
	if err := validateRelayURL(req.Address); err != nil {
		return err
	}
	if len(req.ManagementURL) > maxRelayManagementURLLength {
		return errors.New("relay management URL is too long")
	}
	if req.ManagementURL != "" {
		managementURL, err := url.ParseRequestURI(req.ManagementURL)
		if err != nil || managementURL.Host == "" || (managementURL.Scheme != "http" && managementURL.Scheme != "https") || managementURL.User != nil {
			return errors.New("relay management URL is invalid")
		}
	}
	if req.ConnectedClients != nil && *req.ConnectedClients < 0 {
		return errors.New("connected clients cannot be negative")
	}
	if req.Priority < 0 || req.Priority > maxRelayPriority {
		return fmt.Errorf("relay priority must be between 0 and %d", maxRelayPriority)
	}
	return nil
}

func validateRelayURL(address string) error {
	if address == "" {
		return errors.New("relay address is required")
	}
	parsed, err := url.ParseRequestURI(address)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
		return errors.New("relay address is invalid")
	}
	if parsed.Scheme != relayserver.SchemeREL && parsed.Scheme != relayserver.SchemeRELS {
		return errors.New("relay address must use rel or rels scheme")
	}
	if port := parsed.Port(); port != "" {
		value, err := strconv.ParseUint(port, 10, 16)
		if err != nil || value == 0 {
			return errors.New("relay address has an invalid port")
		}
	}
	return nil
}

func (h *Handler) registerRelay(w http.ResponseWriter, r *http.Request) {
	result := "error"
	defer func() { h.metrics.CountRegister(r.Context(), result) }()
	if h.config == nil || h.config.Secret == "" {
		util.WriteErrorResponse("relay secret is not configured", http.StatusPreconditionFailed, w)
		return
	}

	var req registerRelayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}

	req.ID = strings.TrimSpace(req.ID)
	req.Name = strings.TrimSpace(req.Name)
	req.Address = strings.TrimSpace(req.Address)
	req.ManagementURL = strings.TrimSpace(req.ManagementURL)
	req.Version = strings.TrimSpace(req.Version)
	if err := validateRelayRegistration(req); err != nil {
		util.WriteErrorResponse(err.Error(), http.StatusBadRequest, w)
		return
	}
	claims, err := parseRelaySetupToken(req.SetupKey, h.config.Secret)
	if err != nil {
		util.WriteErrorResponse("invalid relay setup token", http.StatusUnauthorized, w)
		return
	}
	accountID := claims.AccountID
	if accountID == "" {
		util.WriteErrorResponse("invalid relay setup token", http.StatusUnauthorized, w)
		return
	}

	priority := normalizeRelayPriority(req.Priority)
	storedRelay, stored := h.storedRelay(r.Context(), accountID, req.ID, req.Address)
	if claims.Expired(time.Now()) && !stored {
		util.WriteErrorResponse("invalid relay setup token", http.StatusUnauthorized, w)
		return
	}
	if stored {
		priority = normalizeRelayPriority(storedRelay.Priority)
	}

	groups, err := normalizeRelayGroups(req.Groups)
	if err != nil {
		util.WriteErrorResponse(err.Error(), http.StatusBadRequest, w)
		return
	}
	if req.Groups == nil {
		// The relay agent does not report its distribution scope, so a
		// re-registration must not wipe groups assigned through the API.
		// Stored groups are already group IDs.
		if stored {
			groups = slices.Clone(storedRelay.Groups)
		} else if activeGroups, ok := activeRelayRegistry.groupsFor(accountID, req.ID, req.Address); ok {
			groups = activeGroups
		}
	} else {
		// Resolve the supplied group names to IDs for distribution.
		groups, err = h.resolveRelayGroupIDs(r.Context(), accountID, groups)
		if err != nil {
			util.WriteErrorResponse(err.Error(), http.StatusBadRequest, w)
			return
		}
	}

	relay := registeredRelay{
		ID:               req.ID,
		Name:             req.Name,
		Address:          req.Address,
		Priority:         priority,
		ManagementURL:    req.ManagementURL,
		Version:          req.Version,
		ConnectedClients: req.ConnectedClients,
		Groups:           groups,
		LastSeen:         time.Now(),
	}
	if accountID != "" {
		if err := h.persistRegisteredRelay(r.Context(), accountID, relay); err != nil {
			util.WriteError(r.Context(), err, w)
			return
		}
	}
	activeRelayRegistry.upsert(accountID, relay)
	result = "success"

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(registerRelayResponse{Status: "ok"}); err != nil {
		log.WithContext(r.Context()).Errorf("failed to encode relay registration response: %v", err)
	}
}

func (h *Handler) updateRelay(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Update)
	if !ok {
		return
	}

	var req updateRelayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}

	id := relayIDFromRequest(r)
	if id == "" {
		util.WriteErrorResponse("relay ID is required", http.StatusBadRequest, w)
		return
	}

	priority := normalizeRelayPriority(req.Priority)
	updatedActive := activeRelayRegistry.updatePriority(userAuth.AccountId, id, priority)
	updatedStored := h.updateStoredRelayPriority(ctx, userAuth.AccountId, id, priority)
	if req.Groups != nil {
		groups, err := normalizeRelayGroups(*req.Groups)
		if err != nil {
			util.WriteErrorResponse(err.Error(), http.StatusBadRequest, w)
			return
		}
		groupIDs, err := h.resolveRelayGroupIDs(ctx, userAuth.AccountId, groups)
		if err != nil {
			util.WriteErrorResponse(err.Error(), http.StatusBadRequest, w)
			return
		}
		updatedActive = activeRelayRegistry.updateGroups(userAuth.AccountId, id, groupIDs) || updatedActive
		updatedStored = h.updateStoredRelayGroups(ctx, userAuth.AccountId, id, groupIDs) || updatedStored
	}
	if !updatedActive && !updatedStored {
		util.WriteErrorResponse("relay not found", http.StatusNotFound, w)
		return
	}

	targetPeers := 0
	if h.configPusher != nil {
		var err error
		targetPeers, err = h.pushRelayListToAccount(ctx, userAuth.AccountId, userAuth.UserId)
		if err != nil {
			util.WriteError(r.Context(), err, w)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(applyRelayConfigResponse{
		Status:      "ok",
		TargetPeers: targetPeers,
	}); err != nil {
		log.WithContext(r.Context()).Errorf("failed to encode relay update response: %v", err)
	}
}

func (h *Handler) deleteRelay(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Update)
	if !ok {
		return
	}
	id := relayIDFromRequest(r)
	if id == "" {
		util.WriteErrorResponse("relay ID is required", http.StatusBadRequest, w)
		return
	}

	deletedStored, err := h.deleteStoredRelay(ctx, userAuth.AccountId, id)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	deletedActive := activeRelayRegistry.delete(userAuth.AccountId, id)
	if !deletedActive && !deletedStored {
		util.WriteErrorResponse("relay not found", http.StatusNotFound, w)
		return
	}

	if h.configPusher != nil {
		if _, err := h.pushRelayListToAccount(ctx, userAuth.AccountId, userAuth.UserId); err != nil {
			util.WriteError(r.Context(), err, w)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, operation operations.Operation) (auth.UserAuth, context.Context, bool) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return auth.UserAuth{}, r.Context(), false
	}
	if h.permissions == nil {
		util.WriteError(r.Context(), status.Errorf(status.Internal, "relay permissions are not available"), w)
		return auth.UserAuth{}, r.Context(), false
	}
	allowed, ctx, err := h.permissions.ValidateUserPermissions(r.Context(), userAuth.AccountId, userAuth.UserId, modules.Settings, operation)
	if err != nil {
		util.WriteError(ctx, status.NewPermissionValidationError(err), w)
		return auth.UserAuth{}, ctx, false
	}
	if !allowed {
		util.WriteError(ctx, status.NewPermissionDeniedError(), w)
		return auth.UserAuth{}, ctx, false
	}
	return userAuth, ctx, true
}

func relayIDFromRequest(r *http.Request) string {
	id := strings.TrimSpace(mux.Vars(r)["id"])
	if id == "" {
		return ""
	}
	decoded, err := url.PathUnescape(id)
	if err != nil {
		return id
	}
	return strings.TrimSpace(decoded)
}

func (h *Handler) pushRelayListToAccount(ctx context.Context, accountID, userID string) (int, error) {
	result := "error"
	defer func() { h.metrics.CountConfigPush(ctx, result) }()
	if h.configPusher == nil {
		result = "skipped"
		return 0, nil
	}
	peers, err := h.accountManager.GetPeers(ctx, accountID, userID, "", "")
	if err != nil {
		return 0, err
	}

	peerIDs := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.ProxyMeta.Embedded {
			continue
		}
		peerIDs = append(peerIDs, peer.ID)
	}
	result = "success"
	return h.configPusher.PushRelayList(ctx, accountID, peerIDs), nil
}

func (h *Handler) registeredClients(ctx context.Context, accountID, userID string) (int, error) {
	peers, err := h.accountManager.GetPeers(ctx, accountID, userID, "", "")
	if err != nil {
		return 0, err
	}

	count := 0
	for _, peer := range peers {
		if peer.ProxyMeta.Embedded {
			continue
		}
		count++
	}
	return count, nil
}

func (r *relayRegistry) upsert(accountID string, relay registeredRelay) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.relays[registryKey(accountID, relayKey(relay.ID, relay.Address))] = relay
}

func (r *relayRegistry) delete(accountID, id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := registryKey(accountID, id)
	if _, ok := r.relays[key]; !ok {
		return false
	}
	delete(r.relays, key)
	return true
}

func (r *relayRegistry) updatePriority(accountID, id string, priority int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, relay := range r.relays {
		if !strings.HasPrefix(key, registryKey(accountID, "")) || !matchesRelay(id, relayKey(relay.ID, relay.Address), relay.ID, relay.Address) {
			continue
		}
		relay.Priority = priority
		r.relays[key] = relay
		return true
	}
	return false
}

func (r *relayRegistry) updateGroups(accountID, id string, groups []string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, relay := range r.relays {
		if !strings.HasPrefix(key, registryKey(accountID, "")) || !matchesRelay(id, relayKey(relay.ID, relay.Address), relay.ID, relay.Address) {
			continue
		}
		relay.Groups = slices.Clone(groups)
		r.relays[key] = relay
		return true
	}
	return false
}

func (r *relayRegistry) priorityFor(accountID, id, address string) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	searchID := relayKey(id, address)
	for key, relay := range r.relays {
		if !strings.HasPrefix(key, registryKey(accountID, "")) || !matchesRelayIdentity(searchID, address, relayKey(relay.ID, relay.Address), relay.ID, relay.Address) {
			continue
		}
		return normalizeRelayPriority(relay.Priority), true
	}
	return 0, false
}

func (r *relayRegistry) groupsFor(accountID, id, address string) ([]string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	searchID := relayKey(id, address)
	for key, relay := range r.relays {
		if !strings.HasPrefix(key, registryKey(accountID, "")) || !matchesRelayIdentity(searchID, address, relayKey(relay.ID, relay.Address), relay.ID, relay.Address) {
			continue
		}
		return slices.Clone(relay.Groups), true
	}
	return nil, false
}

func (r *relayRegistry) list(accountID string) []registeredRelay {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]registeredRelay, 0, len(r.relays))
	for key, relay := range r.relays {
		if strings.HasPrefix(key, registryKey(accountID, "")) {
			result = append(result, relay)
		}
	}
	sortRegisteredRelays(result)
	return result
}

func (h *Handler) storedRegisteredRelays(ctx context.Context, accountID string) []registeredRelay {
	if accountID == "" || h.accountManager == nil {
		return nil
	}
	storeManager := h.accountManager.GetStore()
	if storeManager == nil {
		return nil
	}
	settings, err := storeManager.GetAccountSettings(ctx, store.LockingStrengthNone, accountID)
	if err != nil {
		log.WithContext(ctx).Debugf("failed to load stored registered relays for account %s: %v", accountID, err)
		return nil
	}
	return registeredRelaysFromSettings(settings)
}

func registeredRelaysFromSettings(settings *types.Settings) []registeredRelay {
	if settings == nil || settings.Extra == nil || len(settings.Extra.RegisteredRelays) == 0 {
		return nil
	}
	result := make([]registeredRelay, 0, len(settings.Extra.RegisteredRelays))
	for _, relay := range settings.Extra.RegisteredRelays {
		result = append(result, registeredRelay{
			ID:               relay.ID,
			Name:             relay.Name,
			Address:          relay.Address,
			Priority:         relay.Priority,
			ManagementURL:    relay.ManagementURL,
			Version:          relay.Version,
			ConnectedClients: relay.ConnectedClients,
			Groups:           slices.Clone(relay.Groups),
			LastSeen:         relay.LastSeen,
		})
	}
	sortRegisteredRelays(result)
	return result
}

func sortRegisteredRelays(relays []registeredRelay) {
	slices.SortFunc(relays, func(left, right registeredRelay) int {
		return strings.Compare(relayKey(left.ID, left.Address), relayKey(right.ID, right.Address))
	})
}

func (h *Handler) updateStoredRelayPriority(ctx context.Context, accountID, id string, priority int) bool {
	if accountID == "" || h.accountManager == nil {
		return false
	}
	storeManager := h.accountManager.GetStore()
	if storeManager == nil {
		return false
	}

	updated := false
	if err := storeManager.ExecuteInTransaction(ctx, func(transaction store.Store) error {
		settings, err := transaction.GetAccountSettings(ctx, store.LockingStrengthUpdate, accountID)
		if err != nil {
			return err
		}
		if settings == nil || settings.Extra == nil || len(settings.Extra.RegisteredRelays) == 0 {
			return nil
		}

		settings = settings.Copy()
		for key, relay := range settings.Extra.RegisteredRelays {
			if !matchesRelay(id, key, relay.ID, relay.Address) {
				continue
			}
			relay.Priority = priority
			settings.Extra.RegisteredRelays[key] = relay
			updated = true
		}
		if !updated {
			return nil
		}
		return transaction.SaveAccountSettings(ctx, accountID, settings)
	}); err != nil {
		log.WithContext(ctx).Warnf("failed to update stored relay %s priority for account %s: %v", id, accountID, err)
	}
	return updated
}

// storedRelay returns the persisted registration for a relay identity, if any.
func (h *Handler) storedRelay(ctx context.Context, accountID, id, address string) (types.RegisteredRelay, bool) {
	if accountID == "" || h.accountManager == nil {
		return types.RegisteredRelay{}, false
	}
	storeManager := h.accountManager.GetStore()
	if storeManager == nil {
		return types.RegisteredRelay{}, false
	}
	settings, err := storeManager.GetAccountSettings(ctx, store.LockingStrengthNone, accountID)
	if err != nil || settings == nil || settings.Extra == nil {
		return types.RegisteredRelay{}, false
	}
	return findStoredRelay(settings.Extra.RegisteredRelays, id, address)
}

func (h *Handler) updateStoredRelayGroups(ctx context.Context, accountID, id string, groups []string) bool {
	if accountID == "" || h.accountManager == nil {
		return false
	}
	storeManager := h.accountManager.GetStore()
	if storeManager == nil {
		return false
	}

	updated := false
	if err := storeManager.ExecuteInTransaction(ctx, func(transaction store.Store) error {
		settings, err := transaction.GetAccountSettings(ctx, store.LockingStrengthUpdate, accountID)
		if err != nil {
			return err
		}
		if settings == nil || settings.Extra == nil || len(settings.Extra.RegisteredRelays) == 0 {
			return nil
		}

		settings = settings.Copy()
		for key, relay := range settings.Extra.RegisteredRelays {
			if !matchesRelay(id, key, relay.ID, relay.Address) {
				continue
			}
			relay.Groups = slices.Clone(groups)
			settings.Extra.RegisteredRelays[key] = relay
			updated = true
		}
		if !updated {
			return nil
		}
		return transaction.SaveAccountSettings(ctx, accountID, settings)
	}); err != nil {
		log.WithContext(ctx).Warnf("failed to update stored relay %s groups for account %s: %v", id, accountID, err)
	}
	return updated
}

func (h *Handler) persistRegisteredRelay(ctx context.Context, accountID string, relay registeredRelay) error {
	return h.accountManager.GetStore().ExecuteInTransaction(ctx, func(transaction store.Store) error {
		settings, err := transaction.GetAccountSettings(ctx, store.LockingStrengthUpdate, accountID)
		if err != nil {
			return err
		}
		settings = settings.Copy()
		if settings.Extra == nil {
			settings.Extra = &types.ExtraSettings{}
		}
		if settings.Extra.RegisteredRelays == nil {
			settings.Extra.RegisteredRelays = make(map[string]types.RegisteredRelay)
		}
		key := relayKey(relay.ID, relay.Address)
		if existingKey, _, ok := findStoredRelayWithKey(settings.Extra.RegisteredRelays, relay.ID, relay.Address); ok {
			key = existingKey
		}
		if newKey := relayKey(relay.ID, relay.Address); newKey != "" && newKey != key {
			delete(settings.Extra.RegisteredRelays, key)
			key = newKey
		}
		settings.Extra.RegisteredRelays[key] = types.RegisteredRelay{
			ID:               relay.ID,
			Name:             relay.Name,
			Address:          relay.Address,
			Priority:         relay.Priority,
			ManagementURL:    relay.ManagementURL,
			Version:          relay.Version,
			ConnectedClients: relay.ConnectedClients,
			Groups:           slices.Clone(relay.Groups),
			LastSeen:         relay.LastSeen,
		}
		return transaction.SaveAccountSettings(ctx, accountID, settings)
	})
}

func (h *Handler) deleteStoredRelay(ctx context.Context, accountID, id string) (bool, error) {
	deleted := false
	if accountID == "" || h.accountManager == nil || h.accountManager.GetStore() == nil {
		return false, nil
	}
	if err := h.accountManager.GetStore().ExecuteInTransaction(ctx, func(transaction store.Store) error {
		settings, err := transaction.GetAccountSettings(ctx, store.LockingStrengthUpdate, accountID)
		if err != nil {
			return err
		}
		if settings == nil || settings.Extra == nil || len(settings.Extra.RegisteredRelays) == 0 {
			return nil
		}
		key, _, ok := findStoredRelayWithKey(settings.Extra.RegisteredRelays, id, "")
		if !ok {
			return nil
		}
		settings = settings.Copy()
		delete(settings.Extra.RegisteredRelays, key)
		deleted = true
		return transaction.SaveAccountSettings(ctx, accountID, settings)
	}); err != nil {
		log.WithContext(ctx).Warnf("failed to delete stored relay %s for account %s: %v", id, accountID, err)
		return false, err
	}
	return deleted, nil
}

func (h *Handler) registeredRelayStatus(ctx context.Context, r registeredRelay, registeredClients int) RelayStatus {
	status := "offline"
	if time.Since(r.LastSeen) <= relayRegistrationTTL {
		status = "online"
	}
	result := RelayStatus{
		Address:           r.Address,
		ID:                relayKey(r.ID, r.Address),
		Name:              r.Name,
		ObservedID:        r.ID,
		Registered:        true,
		Priority:          normalizeRelayPriority(r.Priority),
		Status:            status,
		ConnectedClients:  r.ConnectedClients,
		RegisteredClients: registeredClients,
		LastChecked:       r.LastSeen,
	}
	h.enrichRelayLocation(&result)
	if status != "online" {
		return result
	}
	probeStarted := time.Now()
	if err := probeRelayWebsocket(ctx, r.Address); err != nil {
		h.metrics.RecordProbe(ctx, "error", time.Since(probeStarted))
		result.Status = "offline"
		result.Error = err.Error()
		result.ConnectedClients = nil
		return result
	}
	h.metrics.RecordProbe(ctx, "success", time.Since(probeStarted))
	if health, err := fetchHealth(ctx, r.Address); err == nil {
		result.ConnectedClients = health.ConnectedPeers
		if health.RelayID != "" {
			result.ObservedID = health.RelayID
		}
	}
	return result
}

func registryKey(accountID, relayID string) string {
	return accountID + "\x00" + relayID
}

func relayKey(id, address string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return id
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(address))
	return "relay_" + base64.RawURLEncoding.EncodeToString(sum[:])[:16]
}

func matchesRelay(searchID, key, id, address string) bool {
	return searchID == key || searchID == id || searchID == address || searchID == relayKey(id, address)
}

func matchesRelayIdentity(searchID, searchAddress, key, id, address string) bool {
	if matchesRelay(searchID, key, id, address) {
		return true
	}
	return searchAddress != "" && strings.TrimSpace(searchAddress) == strings.TrimSpace(address)
}

func findStoredRelay(relays map[string]types.RegisteredRelay, id, address string) (types.RegisteredRelay, bool) {
	_, relay, ok := findStoredRelayWithKey(relays, id, address)
	return relay, ok
}

func findStoredRelayWithKey(relays map[string]types.RegisteredRelay, id, address string) (string, types.RegisteredRelay, bool) {
	searchID := relayKey(id, address)
	for key, relay := range relays {
		if !matchesRelayIdentity(searchID, address, key, relay.ID, relay.Address) {
			continue
		}
		return key, relay, true
	}
	return "", types.RegisteredRelay{}, false
}

func signRelaySetupToken(secret string, expiresAt int64, accountID string) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	payload := fmt.Sprintf("%s:%d:%s:%s", relaySetupTokenVersion, expiresAt, base64.RawURLEncoding.EncodeToString(nonce), accountID)
	sig := relaySetupTokenSignature(secret, payload)
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

type relaySetupTokenClaims struct {
	AccountID string
	ExpiresAt int64
}

func (c relaySetupTokenClaims) Expired(now time.Time) bool {
	return c.ExpiresAt != relaySetupTokenNeverExpires && now.Unix() >= c.ExpiresAt
}

func verifyRelaySetupToken(token, secret string) (string, error) {
	claims, err := parseRelaySetupToken(token, secret)
	if err != nil {
		return "", err
	}
	if claims.Expired(time.Now()) {
		return "", errors.New("relay setup token has expired")
	}
	return claims.AccountID, nil
}

func parseRelaySetupToken(token, secret string) (relaySetupTokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return relaySetupTokenClaims{}, errors.New("invalid token format")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return relaySetupTokenClaims{}, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return relaySetupTokenClaims{}, err
	}
	payload := string(payloadBytes)
	if !hmac.Equal(signature, relaySetupTokenSignature(secret, payload)) {
		return relaySetupTokenClaims{}, errors.New("invalid token signature")
	}
	payloadParts := strings.Split(payload, ":")
	if (len(payloadParts) != 3 && len(payloadParts) != 4) || payloadParts[0] != relaySetupTokenVersion {
		return relaySetupTokenClaims{}, errors.New("invalid token payload")
	}
	expiresAt, err := strconv.ParseInt(payloadParts[1], 10, 64)
	if err != nil {
		return relaySetupTokenClaims{}, err
	}
	claims := relaySetupTokenClaims{ExpiresAt: expiresAt}
	if len(payloadParts) == 4 {
		claims.AccountID = payloadParts[3]
	}
	return claims, nil
}

func relaySetupTokenSignature(secret, payload string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	return mac.Sum(nil)
}

func (h *Handler) probeRelay(ctx context.Context, server *nbconfig.RelayServer, registeredClients int) RelayStatus {
	result := RelayStatus{
		Address:           server.Address,
		ID:                relayKey(server.ID, server.Address),
		Name:              server.Name,
		Priority:          normalizeRelayPriority(server.Priority),
		Status:            "offline",
		RegisteredClients: registeredClients,
		LastChecked:       time.Now(),
	}
	h.enrichRelayLocation(&result)

	probeStarted := time.Now()
	if err := probeRelayWebsocket(ctx, server.Address); err != nil {
		h.metrics.RecordProbe(ctx, "error", time.Since(probeStarted))
		result.Error = err.Error()
		return result
	}

	h.metrics.RecordProbe(ctx, "success", time.Since(probeStarted))
	result.Status = "online"
	if health, err := fetchHealth(ctx, server.Address); err == nil {
		result.ConnectedClients = health.ConnectedPeers
		result.ObservedID = health.RelayID
	}
	return result
}

func (h *Handler) enrichRelayLocation(result *RelayStatus) {
	ip := publicIPFromAddress(result.Address)
	if ip == nil {
		return
	}
	result.PublicIP = ip.String()
	if h.geo == nil {
		return
	}
	location, err := h.geo.Lookup(ip)
	if err != nil {
		log.Debugf("failed to lookup relay location for %s: %v", ip.String(), err)
		return
	}
	result.CountryCode = location.Country.ISOCode
	result.CityName = location.City.Names.En
}

func publicIPFromAddress(address string) net.IP {
	parsed, err := url.Parse(address)
	if err != nil {
		return nil
	}
	host := parsed.Hostname()
	if host == "" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			return ip
		}
	}
	if len(ips) > 0 {
		return ips[0]
	}
	return nil
}

func probeRelayWebsocket(ctx context.Context, address string) error {
	wsURL, err := relayWebsocketURL(address)
	if err != nil {
		return err
	}

	client := &http.Client{CheckRedirect: rejectRelayProbeRedirect}
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPClient: client})
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("connect relay websocket: %w", err)
	}
	defer conn.CloseNow()

	authMsg, err := messages.MarshalAuthMsg(peerid.HealthCheckPeerID, peerid.DummyAuthToken)
	if err != nil {
		return fmt.Errorf("marshal relay health auth: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, authMsg); err != nil {
		return fmt.Errorf("write relay health auth: %w", err)
	}
	return nil
}

func relayWebsocketURL(address string) (string, error) {
	if err := validateRelayURL(address); err != nil {
		return "", err
	}
	parsed, err := url.Parse(address)
	if err != nil {
		return "", fmt.Errorf("parse relay address: %w", err)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("relay address has no host")
	}

	switch parsed.Scheme {
	case relayserver.SchemeRELS, "https", "wss":
		parsed.Scheme = "wss"
	default:
		parsed.Scheme = "ws"
	}
	parsed.Path = nbrelay.WebSocketURLPath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func fetchHealth(ctx context.Context, address string) (*healthResponse, error) {
	healthURL, err := relayHealthURL(address)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{CheckRedirect: rejectRelayProbeRedirect}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("relay health returned %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, relayHealthMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > relayHealthMaxBytes {
		return nil, errors.New("relay health response is too large")
	}
	var health healthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		return nil, err
	}
	return &health, nil
}

func rejectRelayProbeRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

func relayHealthURL(address string) (string, error) {
	if err := validateRelayURL(address); err != nil {
		return "", err
	}
	parsed, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("relay address has no host")
	}

	switch parsed.Scheme {
	case relayserver.SchemeRELS, "https", "wss":
		parsed.Scheme = "https"
	default:
		parsed.Scheme = "http"
	}
	parsed.Path = "/health"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
