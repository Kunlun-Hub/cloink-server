package healthcheck

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/relay/protocol"
	"github.com/netbirdio/netbird/relay/server/listener/ws"
)

type statusServiceChecker struct {
	instanceURL    url.URL
	instanceID     string
	connectedPeers int
}

func (s statusServiceChecker) ListenerProtocols() []protocol.Protocol {
	return []protocol.Protocol{ws.Proto}
}

func (s statusServiceChecker) InstanceURL() url.URL {
	return s.instanceURL
}

func (s statusServiceChecker) InstanceID() string {
	return s.instanceID
}

func (s statusServiceChecker) ConnectedPeerCount() int {
	return s.connectedPeers
}

func TestHealthStatusIncludesRelayIdentityAndConnections(t *testing.T) {
	checker := statusServiceChecker{
		instanceURL:    url.URL{Scheme: "rel", Host: "127.0.0.1:1"},
		instanceID:     "relay-hk-1",
		connectedPeers: 7,
	}
	server, err := NewServer(Config{ListenAddress: ":0", ServiceChecker: checker})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, _ := server.getHealthStatus(ctx)

	require.Equal(t, "relay-hk-1", status.RelayID)
	require.Equal(t, 7, status.ConnectedPeers)
}

func TestHealthStatusWithDisabledRelay(t *testing.T) {
	// Regression test for netbirdio/netbird#7476: a disabled relay must not
	// fail the healthcheck server at startup, and /health must report healthy
	// with the relay marked as disabled (no listeners, no relay id).
	server, err := NewServer(Config{ListenAddress: ":0", ServiceChecker: DisabledServiceChecker()})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, ok := server.getHealthStatus(ctx)

	require.True(t, ok)
	require.Equal(t, "healthy", status.Status)
	require.Empty(t, status.Listeners)
	require.Empty(t, status.RelayID)
}

func TestNewServerRejectsNilServiceChecker(t *testing.T) {
	_, err := NewServer(Config{ListenAddress: ":0", ServiceChecker: nil})
	require.Error(t, err)
}
