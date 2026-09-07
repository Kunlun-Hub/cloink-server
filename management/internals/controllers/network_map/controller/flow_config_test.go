package controller

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map/update_channel"
	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/server/settings"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/telemetry"
	"github.com/netbirdio/netbird/management/server/types"
	sharedgrpc "github.com/netbirdio/netbird/shared/management/grpc"
	"github.com/netbirdio/netbird/shared/management/networkmap"
	"github.com/netbirdio/netbird/shared/management/networkmap/nmdata"
)

func TestSendUpdatesFromDataPreservesSelfHostedFlow(t *testing.T) {
	for _, syncVersion := range []sharedgrpc.SyncMessageVersion{sharedgrpc.Base, sharedgrpc.ComponentNetworkMap} {
		for _, scenario := range []struct {
			name    string
			groups  []string
			enabled bool
		}{
			{name: "all groups", enabled: true},
			{name: "selected group", groups: []string{"group-1"}, enabled: true},
			{name: "excluded group", groups: []string{"other-group"}},
		} {
			t.Run(scenario.name+"/"+strconv.Itoa(int(syncVersion)), func(t *testing.T) {
				ctx := context.Background()
				t.Setenv("NB_FLOW_TOKEN_SECRET", "upgrade-regression-test")
				t.Setenv("NB_FLOW_REPORT_INTERVAL", "37s")
				flowManager, err := networktraffic.NewConfigManager(&nbconfig.Config{})
				require.NoError(t, err)
				dbStore, cleanup, err := store.NewTestStoreFromSQL(ctx, "", t.TempDir())
				require.NoError(t, err)
				t.Cleanup(cleanup)
				require.NoError(t, dbStore.SaveAccount(ctx, &types.Account{
					Id: "account-1", Network: &types.Network{},
					Settings: &types.Settings{Extra: &types.ExtraSettings{
						FlowEnabled: true, FlowGroups: scenario.groups, FlowPacketCounterEnabled: true,
					}},
				}))
				appMetrics, err := telemetry.NewAppMetricsWithMeter(ctx, noop.NewMeterProvider().Meter("upgrade-test"))
				require.NoError(t, err)
				updateManager := update_channel.NewPeersUpdateManager(appMetrics)
				updates := updateManager.CreateChannel(ctx, "peer-1")
				t.Cleanup(func() { updateManager.CloseChannel(ctx, "peer-1") })
				controller := NewController(ctx, dbStore, appMetrics, updateManager, nil, nil,
					settings.NewManager(dbStore, nil, nil, nil, settings.IdpConfig{}), "cloink.test", nil, nil, &nbconfig.Config{}, nil)
				controller.serverSupportedSyncMessageVersion = syncVersion
				controller.SetFlowConfigManager(flowManager)
				peer := &nmdata.Peer{
					ID: "peer-1", IP: netip.MustParseAddr("100.64.0.1"), DNSLabel: "peer-1",
					Meta: nmdata.PeerSystemMeta{WtVersion: "0.78.1", SyncMessageVersion: int(syncVersion)},
				}
				data := &networkmap.NetworkMapData{
					Network:         &nmdata.Network{Net: net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(16, 32)}},
					AccountSettings: &nmdata.AccountSettingsInfo{}, DNSSettings: &nmdata.DNSSettings{},
					Peers:          map[string]*nmdata.Peer{peer.ID: peer},
					Groups:         map[string]*nmdata.Group{"group-1": {ID: "group-1", Name: "All", Peers: []string{peer.ID}}},
					ValidatedPeers: map[string]struct{}{peer.ID: {}},
				}
				require.NoError(t, controller.sendUpdatesFromData(ctx, "account-1", data, []*nmdata.Peer{peer}, nil))
				select {
				case message := <-updates:
					flow := message.Update.GetNetbirdConfig().GetFlow()
					require.NotNil(t, flow, "SQLite-backed updates must retain self-hosted flow configuration")
					require.Equal(t, scenario.enabled, flow.Enabled, "flow collection must respect account groups")
					require.True(t, flow.Counters, "flow counters must survive the network map conversion")
					require.Equal(t, 37*time.Second, flow.Interval.AsDuration(), "reporting interval must remain configurable")
					require.Nil(t, message.Update.GetNetbirdConfig().GetRelay(), "flow-only updates must not disable configured relays")
					if scenario.enabled {
						claims, err := flowManager.Validate(flow.TokenPayload, flow.TokenSignature)
						require.NoError(t, err)
						require.Equal(t, "account-1", claims.AccountID, "flow token must bind the account")
						require.Equal(t, peer.ID, claims.PeerID, "flow token must bind the peer")
					} else {
						require.Empty(t, flow.TokenPayload, "excluded peers must not receive a flow token")
					}
				case <-time.After(time.Second):
					t.Fatal("network map update was not delivered")
				}
			})
		}
	}
}
