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

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/server/mock_server"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

func TestNormalizeRelayGroups(t *testing.T) {
	groups, err := normalizeRelayGroups([]string{" eng ", "eng", "", "ops"})
	require.NoError(t, err)
	require.Equal(t, []string{"eng", "ops"}, groups)

	_, err = normalizeRelayGroups(make([]string, maxRelayGroups+1))
	require.ErrorContains(t, err, "must not exceed")

	_, err = normalizeRelayGroups([]string{strings.Repeat("a", maxRelayGroupNameLength+1)})
	require.ErrorContains(t, err, "exceeds")

	_, err = normalizeRelayGroups([]string{"eng\nops"})
	require.ErrorContains(t, err, "invalid characters")

	groups, err = normalizeRelayGroups(nil)
	require.NoError(t, err)
	require.Empty(t, groups)
}

func TestFilterRelayDescriptorsForGroups(t *testing.T) {
	// Both relay Groups and the peer's groups are group IDs; names are
	// resolved to IDs at the API write path.
	relays := []RelayServerDescriptor{
		{ID: "global", Address: "rels://global.example:443"},
		{ID: "eng-only", Address: "rels://eng.example:443", Groups: []string{"group-eng-id"}},
		{ID: "multi", Address: "rels://multi.example:443", Groups: []string{"group-eng-id", "group-ops-id"}},
	}

	filtered := FilterRelayDescriptorsForGroups(relays, []string{"group-ops-id"})
	require.Len(t, filtered, 2)
	require.Equal(t, "global", filtered[0].ID)
	require.Equal(t, "multi", filtered[1].ID)

	filtered = FilterRelayDescriptorsForGroups(relays, nil)
	require.Len(t, filtered, 1)
	require.Equal(t, "global", filtered[0].ID)

	filtered = FilterRelayDescriptorsForGroups(relays, []string{"group-eng-id", "group-ops-id"})
	require.Len(t, filtered, 3)

	filtered = FilterRelayDescriptorsForGroups(relays, []string{"group-unknown-id"})
	require.Len(t, filtered, 1)
	require.Equal(t, "global", filtered[0].ID)
}

func TestResolveRelayGroupIDs(t *testing.T) {
	const accountID = "account-id"
	h := &Handler{
		accountManager: &mock_server.MockAccountManager{
			GetAccountFunc: func(_ context.Context, _ string) (*types.Account, error) {
				return &types.Account{
					Id: accountID,
					Groups: map[string]*types.Group{
						"group-eng-id": {ID: "group-eng-id", Name: "eng"},
						"group-ops-id": {ID: "group-ops-id", Name: "ops"},
					},
				}, nil
			},
		},
	}

	ids, err := h.resolveRelayGroupIDs(context.Background(), accountID, []string{"eng", "ops"})
	require.NoError(t, err)
	require.Equal(t, []string{"group-eng-id", "group-ops-id"}, ids)

	ids, err = h.resolveRelayGroupIDs(context.Background(), accountID, []string{"eng", "eng"})
	require.NoError(t, err)
	require.Equal(t, []string{"group-eng-id"}, ids, "duplicate names must not produce duplicate IDs")

	ids, err = h.resolveRelayGroupIDs(context.Background(), accountID, nil)
	require.NoError(t, err)
	require.Empty(t, ids)

	_, err = h.resolveRelayGroupIDs(context.Background(), accountID, []string{"eng", "nope"})
	require.ErrorContains(t, err, "unknown groups: nope")
}

func TestGroupNamesForIDs(t *testing.T) {
	const accountID = "account-id"
	h := &Handler{
		accountManager: &mock_server.MockAccountManager{
			GetAccountFunc: func(_ context.Context, _ string) (*types.Account, error) {
				return &types.Account{
					Id: accountID,
					Groups: map[string]*types.Group{
						"group-eng-id": {ID: "group-eng-id", Name: "eng"},
					},
				}, nil
			},
		},
	}

	require.Equal(t,
		[]string{"eng", "group-gone-id"},
		h.groupNamesForIDs(context.Background(), accountID, []string{"group-eng-id", "group-gone-id"}),
		"unresolvable IDs stay visible instead of vanishing")
	require.Empty(t, h.groupNamesForIDs(context.Background(), accountID, nil))
}

func TestRelayRegistryUpdateGroups(t *testing.T) {
	registry := &relayRegistry{relays: make(map[string]registeredRelay)}
	registry.upsert("account-a", registeredRelay{ID: "relay-1", Address: "rels://relay.example:443", Groups: []string{"eng"}})

	require.True(t, registry.updateGroups("account-a", "relay-1", []string{"ops"}))
	groups, ok := registry.groupsFor("account-a", "relay-1", "")
	require.True(t, ok)
	require.Equal(t, []string{"ops"}, groups)

	require.True(t, registry.updateGroups("account-a", "relay-1", nil))
	groups, ok = registry.groupsFor("account-a", "relay-1", "")
	require.True(t, ok)
	require.Empty(t, groups)

	require.False(t, registry.updateGroups("account-a", "missing", []string{"ops"}))
}

func TestRelayServersForAccountPropagatesGroupsAndLoad(t *testing.T) {
	clients := 85
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"scoped": {ID: "scoped", Address: "rels://scoped.example.com:443", Priority: 60, ConnectedClients: &clients, Groups: []string{"eng"}, LastSeen: time.Now()},
	}}}

	servers := RelayServersForAccount(nil, settings)

	require.Len(t, servers, 1)
	require.Equal(t, "scoped", servers[0].ID)
	require.Equal(t, 85, servers[0].ConnectedPeers)
	require.Equal(t, []string{"eng"}, servers[0].Groups)
}

func TestRegisterRelayPreservesStoredGroups(t *testing.T) {
	const accountID, secret = "account-id", "relay-secret"
	activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)}
	t.Cleanup(func() { activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)} })

	ctrl := gomock.NewController(t)
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"relay-1": {ID: "relay-1", Address: "rels://relay.example.com:443", Priority: 80, Groups: []string{"eng"}, LastSeen: time.Now().Add(-time.Minute)},
	}}}
	storeMock := store.NewMockStore(ctrl)
	var saved *types.Settings
	gomock.InOrder(
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthNone, accountID).Return(settings, nil),
		storeMock.EXPECT().ExecuteInTransaction(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, f func(store.Store) error) error { return f(storeMock) }),
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthUpdate, accountID).Return(settings, nil),
		storeMock.EXPECT().SaveAccountSettings(gomock.Any(), accountID, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, s *types.Settings) error {
			saved = s
			return nil
		}),
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
	require.NotNil(t, saved, "registration must persist the relay")
	relay, ok := saved.Extra.RegisteredRelays["relay-1"]
	require.True(t, ok)
	require.Equal(t, []string{"eng"}, relay.Groups, "re-registration without groups must keep the stored groups")
	groups, ok := activeRelayRegistry.groupsFor(accountID, "relay-1", "")
	require.True(t, ok)
	require.Equal(t, []string{"eng"}, groups)
}

func TestRemoveGroupFromRelays(t *testing.T) {
	const accountID = "account-id"
	activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)}
	t.Cleanup(func() { activeRelayRegistry = &relayRegistry{relays: make(map[string]registeredRelay)} })

	// One relay scoped to the deleted group plus another group, one scoped
	// only to the deleted group, one global, and one from another account.
	activeRelayRegistry.upsert(accountID, registeredRelay{ID: "multi", Address: "rels://multi.example:443", Groups: []string{"group-gone-id", "group-ops-id"}})
	activeRelayRegistry.upsert(accountID, registeredRelay{ID: "solo", Address: "rels://solo.example:443", Groups: []string{"group-gone-id"}})
	activeRelayRegistry.upsert(accountID, registeredRelay{ID: "global", Address: "rels://global.example:443"})
	activeRelayRegistry.upsert("other-account", registeredRelay{ID: "other", Address: "rels://other.example:443", Groups: []string{"group-gone-id"}})

	ctrl := gomock.NewController(t)
	settings := &types.Settings{Extra: &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"multi": {ID: "multi", Address: "rels://multi.example:443", Groups: []string{"group-gone-id", "group-ops-id"}, LastSeen: time.Now()},
		"solo":  {ID: "solo", Address: "rels://solo.example:443", Groups: []string{"group-gone-id"}, LastSeen: time.Now()},
	}}}
	storeMock := store.NewMockStore(ctrl)
	var saved *types.Settings
	runTx := func(ctx context.Context, f func(store.Store) error) error { return f(storeMock) }
	gomock.InOrder(
		storeMock.EXPECT().ExecuteInTransaction(gomock.Any(), gomock.Any()).DoAndReturn(runTx),
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthUpdate, accountID).Return(settings, nil),
		storeMock.EXPECT().SaveAccountSettings(gomock.Any(), accountID, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, s *types.Settings) error {
			saved = s
			return nil
		}),
		// Second call finds nothing left to remove and must skip the write.
		storeMock.EXPECT().ExecuteInTransaction(gomock.Any(), gomock.Any()).DoAndReturn(runTx),
		storeMock.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthUpdate, accountID).DoAndReturn(func(_ context.Context, _ store.LockingStrength, _ string) (*types.Settings, error) {
			return saved, nil
		}),
	)

	require.True(t, RemoveGroupFromRelays(context.Background(), storeMock, accountID, "group-gone-id"))

	groups, ok := activeRelayRegistry.groupsFor(accountID, "multi", "")
	require.True(t, ok)
	require.Equal(t, []string{"group-ops-id"}, groups, "surviving groups must be kept")

	groups, ok = activeRelayRegistry.groupsFor(accountID, "solo", "")
	require.True(t, ok)
	require.Empty(t, groups, "a relay with no groups left reverts to global distribution")

	groups, ok = activeRelayRegistry.groupsFor(accountID, "global", "")
	require.True(t, ok)
	require.Empty(t, groups)

	groups, ok = activeRelayRegistry.groupsFor("other-account", "other", "")
	require.True(t, ok)
	require.Equal(t, []string{"group-gone-id"}, groups, "other accounts are untouched")

	require.NotNil(t, saved, "stored registrations must be updated")
	require.Equal(t, []string{"group-ops-id"}, saved.Extra.RegisteredRelays["multi"].Groups)
	require.Empty(t, saved.Extra.RegisteredRelays["solo"].Groups)

	require.False(t, RemoveGroupFromRelays(context.Background(), storeMock, accountID, "group-gone-id"),
		"a repeat call must report nothing removed and skip the settings write")
}
