package relays

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

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

func TestRelaySetupTokenRequiresBoundAccountAndValidSignature(t *testing.T) {
	const secret, accountID = "relay-secret", "account-id"
	token, err := signRelaySetupToken(secret, relaySetupTokenNeverExpires, accountID)
	require.NoError(t, err)
	actualAccountID, err := verifyRelaySetupToken(token, secret)
	require.NoError(t, err)
	require.Equal(t, accountID, actualAccountID)

	_, err = verifyRelaySetupToken(token+"x", secret)
	require.Error(t, err)
	legacyToken, err := signRelaySetupToken(secret, relaySetupTokenNeverExpires, "")
	require.NoError(t, err)
	actualAccountID, err = verifyRelaySetupToken(legacyToken, secret)
	require.NoError(t, err)
	require.Empty(t, actualAccountID)
}

func withRelayUser(req *http.Request, accountID, userID string) *http.Request {
	return nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: accountID, UserId: userID})
}
