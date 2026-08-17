package grpc

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	flowproto "github.com/netbirdio/netbird/flow/proto"
	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/server/account"
	"github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

func TestFlowServerEventsPersistsAndAcknowledges(t *testing.T) {
	t.Setenv("NB_FLOW_TOKEN_SECRET", "flow-test-secret")
	manager, err := networktraffic.NewConfigManager(&config.Config{})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	accountManager := account.NewMockManager(ctrl)
	dbStore := store.NewMockStore(ctrl)
	accountManager.EXPECT().GetStore().Return(dbStore).AnyTimes()

	privateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	publicKey := privateKey.PublicKey()
	reporter := &peer.Peer{
		ID:        "peer-1",
		AccountID: "account-1",
		Key:       publicKey.String(),
		IP:        netipMustParse("100.64.0.1"),
		Name:      "laptop",
	}
	settings := &types.Settings{Extra: &types.ExtraSettings{FlowEnabled: true}}
	dbStore.EXPECT().GetPeerByPeerPubKey(gomock.Any(), store.LockingStrengthNone, publicKey.String()).Return(reporter, nil)
	dbStore.EXPECT().GetAccountSettings(gomock.Any(), store.LockingStrengthNone, "account-1").Return(settings, nil)
	dbStore.EXPECT().GetPeerGroupIDs(gomock.Any(), store.LockingStrengthNone, "account-1", "peer-1").Return(nil, nil)
	dbStore.EXPECT().GetPeerByIP(gomock.Any(), store.LockingStrengthNone, "account-1", gomock.Any()).DoAndReturn(func(_ context.Context, _ store.LockingStrength, _ string, ip net.IP) (*peer.Peer, error) {
		if ip.String() == reporter.IP.String() {
			return reporter, nil
		}
		return nil, status.Errorf(status.NotFound, "peer not found")
	}).AnyTimes()
	eventSaved := make(chan *networktraffic.Event, 1)
	dbStore.EXPECT().CreateNetworkTrafficEvent(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, event *networktraffic.Event) error {
		eventSaved <- event
		return nil
	})

	flowServer := NewFlowServer(accountManager)
	flowServer.SetConfigManager(manager)
	conn, cleanup := startFlowTestServer(t, flowServer)
	defer cleanup()

	payload, signature := manager.Sign("account-1", "peer-1")
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+signature+"."+payload)
	stream, err := flowproto.NewFlowServiceClient(conn).Events(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&flowproto.FlowEvent{IsInitiator: true}))
	initiatorAck, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, initiatorAck.IsInitiator)

	eventID := uuid.New()
	flowID := uuid.New()
	now := time.Now().UTC()
	event := &flowproto.FlowEvent{
		EventId:     eventID[:],
		Timestamp:   timestamppb.New(now),
		PublicKey:   publicKey[:],
		WindowStart: timestamppb.New(now.Add(-time.Second)),
		WindowEnd:   timestamppb.New(now),
		FlowFields: &flowproto.FlowFields{
			FlowId:    flowID[:],
			Type:      flowproto.Type_TYPE_UNKNOWN,
			Direction: flowproto.Direction_EGRESS,
			Protocol:  6,
			SourceIp:  net.ParseIP("100.64.0.1").To4(),
			DestIp:    net.ParseIP("100.64.0.2").To4(),
			ConnectionInfo: &flowproto.FlowFields_PortInfo{PortInfo: &flowproto.PortInfo{
				SourcePort: 50000,
				DestPort:   443,
			}},
			TxBytes: 128,
		},
	}
	require.NoError(t, stream.Send(event))
	eventAck, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, eventID[:], eventAck.EventId)
	saved := <-eventSaved
	require.Equal(t, eventID.String(), saved.ID)
	require.Equal(t, flowID.String(), saved.FlowID)
	require.Equal(t, "account-1", saved.AccountID)
	require.Equal(t, networktraffic.EndpointTypePeer, saved.SourceType)
	require.Equal(t, networktraffic.EndpointTypeUnknown, saved.DestinationType)
	require.NoError(t, stream.CloseSend())
}

func TestFlowServerEventsRejectsMissingOrTamperedToken(t *testing.T) {
	t.Setenv("NB_FLOW_TOKEN_SECRET", "flow-test-secret")
	manager, err := networktraffic.NewConfigManager(&config.Config{})
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	accountManager := account.NewMockManager(ctrl)
	flowServer := NewFlowServer(accountManager)
	flowServer.SetConfigManager(manager)
	conn, cleanup := startFlowTestServer(t, flowServer)
	defer cleanup()

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "tampered", ctx: metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer bad.payload")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(tt.ctx, time.Second)
			defer cancel()
			stream, err := flowproto.NewFlowServiceClient(conn).Events(ctx)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Error(t, err)
			require.Contains(t, err.Error(), "Unauthenticated")
		})
	}
}

func TestValidateFlowEventRejectsInvalidWindowAndConnection(t *testing.T) {
	eventID := uuid.New()
	flowID := uuid.New()
	now := time.Now().UTC()
	base := &flowproto.FlowEvent{
		EventId:     eventID[:],
		Timestamp:   timestamppb.New(now),
		PublicKey:   make([]byte, wgtypes.KeyLen),
		WindowStart: timestamppb.New(now),
		WindowEnd:   timestamppb.New(now),
		FlowFields: &flowproto.FlowFields{
			FlowId:    flowID[:],
			Type:      flowproto.Type_TYPE_UNKNOWN,
			Direction: flowproto.Direction_EGRESS,
			Protocol:  6,
			SourceIp:  net.ParseIP("100.64.0.1").To4(),
			DestIp:    net.ParseIP("100.64.0.2").To4(),
		},
	}
	require.Error(t, validateFlowEvent(base))

	base.FlowFields.ConnectionInfo = &flowproto.FlowFields_PortInfo{PortInfo: &flowproto.PortInfo{DestPort: 443}}
	base.WindowStart = timestamppb.New(now.Add(time.Second))
	require.Error(t, validateFlowEvent(base))
}

func startFlowTestServer(t *testing.T, flowServer *FlowServer) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	flowproto.RegisterFlowServiceServer(grpcServer, flowServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn, func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

func netipMustParse(value string) (addr netip.Addr) {
	addr, err := netip.ParseAddr(value)
	if err != nil {
		panic(err)
	}
	return addr
}
