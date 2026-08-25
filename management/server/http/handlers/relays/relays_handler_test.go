package relays

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/mock_server"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/auth"
)

type relayConfigPusherMock struct {
	accountID string
	peerIDs   []string
	count     int
}

func (m *relayConfigPusherMock) PushRelayList(_ context.Context, accountID string, peerIDs []string) int {
	m.accountID = accountID
	m.peerIDs = append([]string(nil), peerIDs...)
	return m.count
}

func TestRelayProbeURLsRejectInvalidSchemes(t *testing.T) {
	for _, build := range []func(string) (string, error){relayWebsocketURL, relayHealthURL} {
		_, err := build("http://relay.example.com:443")
		require.EqualError(t, err, "relay address must use rel or rels scheme")
	}
}

func TestFetchHealthRejectsRedirect(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected = true
		_, _ = w.Write([]byte(`{"connected_peers":1}`))
	}))
	defer target.Close()
	server := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	defer server.Close()

	_, err := fetchHealth(context.Background(), "rel://"+strings.TrimPrefix(server.URL, "http://"))

	require.EqualError(t, err, "relay health returned 302 Found")
	require.False(t, redirected)
}

func TestFetchHealthRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, relayHealthMaxBytes+1))
	}))
	defer server.Close()

	_, err := fetchHealth(context.Background(), "rel://"+strings.TrimPrefix(server.URL, "http://"))

	require.EqualError(t, err, "relay health response is too large")
}

func TestValidateRelayRegistration(t *testing.T) {
	negativeClients := -1
	excessivePriority := maxRelayPriority + 1
	tests := []struct {
		name string
		req  registerRelayRequest
		want string
	}{
		{name: "valid TLS", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:443"}},
		{name: "valid non-TLS", req: registerRelayRequest{ID: "relay-1", Address: "rel://10.0.0.2:8080", ManagementURL: "https://management.example.com"}},
		{name: "missing ID", req: registerRelayRequest{Address: "rels://relay.example.com:443"}, want: "relay ID is required"},
		{name: "missing address", req: registerRelayRequest{ID: "relay-1"}, want: "relay address is required"},
		{name: "unsupported scheme", req: registerRelayRequest{ID: "relay-1", Address: "http://relay.example.com:443"}, want: "relay address must use rel or rels scheme"},
		{name: "userinfo", req: registerRelayRequest{ID: "relay-1", Address: "rels://user@relay.example.com:443"}, want: "relay address is invalid"},
		{name: "path", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:443/path"}, want: "relay address is invalid"},
		{name: "query", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:443?x=1"}, want: "relay address is invalid"},
		{name: "zero port", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:0"}, want: "relay address has an invalid port"},
		{name: "invalid management URL", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:443", ManagementURL: "file:///tmp/relay"}, want: "relay management URL is invalid"},
		{name: "negative clients", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:443", ConnectedClients: &negativeClients}, want: "connected clients cannot be negative"},
		{name: "excessive priority", req: registerRelayRequest{ID: "relay-1", Address: "rels://relay.example.com:443", Priority: excessivePriority}, want: "relay priority must be between 0 and 1000"},
		{name: "ID control character", req: registerRelayRequest{ID: "relay\n1", Address: "rels://relay.example.com:443"}, want: "relay ID is invalid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRelayRegistration(test.req)
			if test.want == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.want)
		})
	}
}

func TestApplyRelayConfigPushesAccountPeers(t *testing.T) {
	const accountID, userID = "account-id", "user-id"
	ctrl := gomock.NewController(t)
	permissionsManager := permissions.NewMockManager(ctrl)
	permissionsManager.EXPECT().
		ValidateUserPermissions(gomock.Any(), accountID, userID, modules.Settings, operations.Update).
		Return(true, context.Background(), nil)
	pusher := &relayConfigPusherMock{count: 2}
	h := &Handler{
		permissions: permissionsManager,
		accountManager: &mock_server.MockAccountManager{
			GetAccountByIDFunc: func(context.Context, string, string) (*types.Account, error) {
				return &types.Account{Id: accountID}, nil
			},
			GetPeersFunc: func(context.Context, string, string, string, string) ([]*nbpeer.Peer, error) {
				return []*nbpeer.Peer{{ID: "peer-a"}, {ID: "embedded", ProxyMeta: nbpeer.ProxyMeta{Embedded: true}}, {ID: "peer-b"}}, nil
			},
		},
		configPusher: pusher,
	}
	req := withRelayUser(httptest.NewRequest(http.MethodPost, "/api/relays/apply", nil), accountID, userID)
	recorder := httptest.NewRecorder()

	h.applyRelayConfig(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, accountID, pusher.accountID)
	require.Equal(t, []string{"peer-a", "peer-b"}, pusher.peerIDs)
}

func TestRelayPermissionsFailClosed(t *testing.T) {
	h := &Handler{}
	req := withRelayUser(httptest.NewRequest(http.MethodGet, "/api/relays", nil), "account-id", "user-id")
	recorder := httptest.NewRecorder()

	h.getAllRelays(recorder, req)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestRegisterRelayKeepsStoredPriorityAndScopesRegistry(t *testing.T) {
	const accountID, secret = "account-id", "relay-secret"
	activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)}
	t.Cleanup(func() { activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)} })

	ctrl := gomock.NewController(t)
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"relay-old": {ID: "relay-old", Address: "rels://relay.example.com:443", Priority: 80, LastSeen: time.Now()},
	}}}
	storeMock := store.NewMockStore(ctrl)
	gomock.InOrder(
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthNone, accountID).Return(settings, nil),
		storeMock.EXPECT().ExecuteInTransaction(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, f func(store.Store) error) error { return f(storeMock) }),
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthUpdate, accountID).Return(settings, nil),
		storeMock.EXPECT().SaveAccountSettings(gomock.Any(), accountID, gomock.Any()).Return(nil),
	)
	h := &Handler{
		config:         &nbconfig.Relay{Secret: secret},
		accountManager: &mock_server.MockAccountManager{GetStoreFunc: func() store.Store { return storeMock }},
	}
	setupKey, err := signRelaySetupToken(secret, relaySetupTokenNeverExpires, accountID)
	require.NoError(t, err)
	body, err := json.Marshal(registerRelayRequest{SetupKey: setupKey, ID: "relay-new", Address: "rels://relay.example.com:443", Priority: 30})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()

	h.registerRelay(recorder, httptest.NewRequest(http.MethodPost, "/api/relays/register", bytes.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	priority, ok := activeRelayRegistry.priorityFor(accountID, "relay-new", "")
	require.True(t, ok)
	require.Equal(t, 80, priority)
	_, ok = activeRelayRegistry.priorityFor("other-account", "relay-new", "")
	require.False(t, ok)
}

func TestRegisterRelayAcceptsExpiredTokenForStoredIdentity(t *testing.T) {
	const accountID, secret = "account-id", "relay-secret"
	activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)}
	t.Cleanup(func() { activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)} })

	ctrl := gomock.NewController(t)
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"relay-1": {ID: "relay-1", Address: "rels://relay.example.com:443", Priority: 80, LastSeen: time.Now().Add(-time.Minute)},
	}}}
	storeMock := store.NewMockStore(ctrl)
	gomock.InOrder(
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthNone, accountID).Return(settings, nil),
		storeMock.EXPECT().ExecuteInTransaction(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, f func(store.Store) error) error { return f(storeMock) }),
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthUpdate, accountID).Return(settings, nil),
		storeMock.EXPECT().SaveAccountSettings(gomock.Any(), accountID, gomock.Any()).Return(nil),
	)
	h := &Handler{
		config:         &nbconfig.Relay{Secret: secret},
		accountManager: &mock_server.MockAccountManager{GetStoreFunc: func() store.Store { return storeMock }},
	}
	setupKey, err := signRelaySetupToken(secret, time.Now().Add(-time.Minute).Unix(), accountID)
	require.NoError(t, err)
	body, err := json.Marshal(registerRelayRequest{SetupKey: setupKey, ID: "relay-1", Address: "rels://relay.example.com:443", Priority: 30})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()

	h.registerRelay(recorder, httptest.NewRequest(http.MethodPost, "/api/relays/register", bytes.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	priority, ok := activeRelayRegistry.priorityFor(accountID, "relay-1", "")
	require.True(t, ok)
	require.Equal(t, 80, priority)
}

func TestRegisterRelayRejectsExpiredTokenForNewIdentity(t *testing.T) {
	const accountID, secret = "account-id", "relay-secret"
	ctrl := gomock.NewController(t)
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{}}}
	storeMock := store.NewMockStore(ctrl)
	storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthNone, accountID).Return(settings, nil)
	h := &Handler{
		config:         &nbconfig.Relay{Secret: secret},
		accountManager: &mock_server.MockAccountManager{GetStoreFunc: func() store.Store { return storeMock }},
	}
	setupKey, err := signRelaySetupToken(secret, time.Now().Add(-time.Minute).Unix(), accountID)
	require.NoError(t, err)
	body, err := json.Marshal(registerRelayRequest{SetupKey: setupKey, ID: "relay-new", Address: "rels://relay.example.com:443"})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()

	h.registerRelay(recorder, httptest.NewRequest(http.MethodPost, "/api/relays/register", bytes.NewReader(body)))

	require.Equal(t, http.StatusUnauthorized, recorder.Code, recorder.Body.String())
}

func TestRelayRegistryIsAccountScoped(t *testing.T) {
	registry := &relayRegistry{relays: make(map[string]registeredRelay)}
	registry.upsert("account-a", registeredRelay{ID: "same-id", Address: "rels://a", Priority: 10})
	registry.upsert("account-b", registeredRelay{ID: "same-id", Address: "rels://b", Priority: 20})

	require.True(t, registry.updatePriority("account-a", "same-id", 50))
	priority, ok := registry.priorityFor("account-a", "same-id", "")
	require.True(t, ok)
	require.Equal(t, 50, priority)
	priority, ok = registry.priorityFor("account-b", "same-id", "")
	require.True(t, ok)
	require.Equal(t, 20, priority)
	require.True(t, registry.delete("account-a", "same-id"))
	require.Len(t, registry.list("account-a"), 0)
	require.Len(t, registry.list("account-b"), 1)
}

func TestRelayServersForAccountExpiresDynamicRelays(t *testing.T) {
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"online":  {ID: "online", Address: "rels://online.example.com:443", LastSeen: time.Now().Add(-relayRegistrationTTL + time.Second)},
		"expired": {ID: "expired", Address: "rels://expired.example.com:443", LastSeen: time.Now().Add(-relayRegistrationTTL - time.Second)},
	}}}

	servers := RelayServersForAccount(nil, settings)

	require.Equal(t, []RelayServerDescriptor{{ID: "online", Address: "rels://online.example.com:443", Priority: defaultRelayPriority}}, servers)
}

func TestRelayServersForAccountSortsAndDeduplicates(t *testing.T) {
	const address = "rels://relay.example.com:443"
	config := &nbconfig.Relay{Servers: []*nbconfig.RelayServer{
		{ID: "relay-a", Address: "rels://a.example.com:443", Priority: 40},
		{Address: address, Priority: 30},
	}}
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"relay-dynamic": {ID: "relay-dynamic", Address: address, Priority: 60, LastSeen: time.Now()},
	}}}

	servers := RelayServersForAccount(config, settings)

	require.Len(t, servers, 2)
	require.Equal(t, "relay-dynamic", servers[0].ID)
	require.Equal(t, 60, servers[0].Priority)
	require.Equal(t, "relay-a", servers[1].ID)
}

func TestDeleteRelayPushesUpdatedAccountPeers(t *testing.T) {
	const accountID, userID = "account-id", "user-id"
	activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)}
	activeRelayRegistry.upsert(accountID, registeredRelay{ID: "relay-1", Address: "rels://relay.example.com:443"})
	t.Cleanup(func() { activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)} })

	ctrl := gomock.NewController(t)
	permissionsManager := permissions.NewMockManager(ctrl)
	permissionsManager.EXPECT().
		ValidateUserPermissions(gomock.Any(), accountID, userID, modules.Settings, operations.Update).
		Return(true, context.Background(), nil)
	pusher := &relayConfigPusherMock{count: 1}
	h := &Handler{
		permissions:  permissionsManager,
		configPusher: pusher,
		accountManager: &mock_server.MockAccountManager{
			GetStoreFunc: func() store.Store { return nil },
			GetPeersFunc: func(context.Context, string, string, string, string) ([]*nbpeer.Peer, error) {
				return []*nbpeer.Peer{{ID: "peer-1"}}, nil
			},
		},
	}

	req := withRelayUser(httptest.NewRequest(http.MethodDelete, "/api/relays/relay-1", nil), accountID, userID)
	req = mux.SetURLVars(req, map[string]string{"id": "relay-1"})
	recorder := httptest.NewRecorder()

	h.deleteRelay(recorder, req)

	require.Equal(t, http.StatusNoContent, recorder.Code)
	require.Empty(t, activeRelayRegistry.list(accountID))
	require.Equal(t, accountID, pusher.accountID)
	require.Equal(t, []string{"peer-1"}, pusher.peerIDs)
}

func TestRelaySetupTokenRequiresBoundAccountAndValidSignature(t *testing.T) {
	const secret, accountID = "relay-secret", "account-id"
	token, err := signRelaySetupToken(secret, relaySetupTokenNeverExpires, accountID)
	require.NoError(t, err)
	actualAccountID, err := verifyRelaySetupToken(token, secret)
	require.NoError(t, err)
	require.Equal(t, accountID, actualAccountID)

	_, err = verifyRelaySetupToken(token+"x", secret)
	require.Error(t, err)
	expiredToken, err := signRelaySetupToken(secret, time.Now().Add(-time.Second).Unix(), accountID)
	require.NoError(t, err)
	_, err = verifyRelaySetupToken(expiredToken, secret)
	require.EqualError(t, err, "relay setup token has expired")

	legacyToken, err := signRelaySetupToken(secret, relaySetupTokenNeverExpires, "")
	require.NoError(t, err)
	actualAccountID, err = verifyRelaySetupToken(legacyToken, secret)
	require.NoError(t, err)
	require.Empty(t, actualAccountID)
}

func withRelayUser(req *http.Request, accountID, userID string) *http.Request {
	return nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: accountID, UserId: userID})
}
