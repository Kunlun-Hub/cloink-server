package networktraffic

import (
	"testing"

	"github.com/stretchr/testify/require"

	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/proto"
)

func TestConfigManagerTokenBinding(t *testing.T) {
	t.Setenv(envFlowTokenSecret, "test-secret")
	manager, err := NewConfigManager(&nbconfig.Config{})
	require.NoError(t, err)

	payload, signature := manager.Sign("account-1", "peer-1")
	claims, err := manager.Validate(payload, signature)
	require.NoError(t, err)
	require.Equal(t, TokenClaims{Version: flowTokenVersion, AccountID: "account-1", PeerID: "peer-1"}, claims)

	_, err = manager.Validate(payload+"x", signature)
	require.Error(t, err)
	_, err = manager.Validate(payload, signature+"x")
	require.Error(t, err)
}

func TestConfigManagerApplyFiltersGroups(t *testing.T) {
	t.Setenv(envFlowTokenSecret, "test-secret")
	manager, err := NewConfigManager(&nbconfig.Config{})
	require.NoError(t, err)
	settings := &types.ExtraSettings{FlowEnabled: true, FlowGroups: []string{"group-1"}}

	response := &proto.SyncResponse{}
	manager.Apply(response, "account-1", "peer-1", []string{"group-2"}, settings)
	require.False(t, response.GetNetbirdConfig().GetFlow().GetEnabled())

	manager.Apply(response, "account-1", "peer-1", []string{"group-1"}, settings)
	flow := response.GetNetbirdConfig().GetFlow()
	require.True(t, flow.GetEnabled())
	require.NotEmpty(t, flow.GetTokenPayload())
	require.NotEmpty(t, flow.GetTokenSignature())
}
