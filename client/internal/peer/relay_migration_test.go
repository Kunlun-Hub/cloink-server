package peer

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/internal/peer/conntype"
)

type migrationProxy struct {
	closed     atomic.Int32
	paused     atomic.Int32
	worked     atomic.Int32
	redirected atomic.Int32
	listener   func()
}

func (p *migrationProxy) AddRelayedConn(context.Context, *net.UDPAddr, net.Conn) error { return nil }
func (p *migrationProxy) EndpointAddr() *net.UDPAddr                                   { return &net.UDPAddr{} }
func (p *migrationProxy) Work()                                                        { p.worked.Add(1) }
func (p *migrationProxy) Pause()                                                       { p.paused.Add(1) }
func (p *migrationProxy) RedirectAs(*net.UDPAddr)                                      { p.redirected.Add(1) }
func (p *migrationProxy) CloseConn() error {
	p.closed.Add(1)
	return nil
}
func (p *migrationProxy) SetDisconnectListener(listener func()) { p.listener = listener }
func (p *migrationProxy) InjectPacket([]byte) error             { return nil }

func TestRelayProxyMigrationKeepsOldPathUntilHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	oldProxy := &migrationProxy{listener: func() {}}
	newProxy := &migrationProxy{listener: func() {}}
	conn := &Conn{ctx: ctx, wgProxyRelay: oldProxy}

	conn.setRelayedProxy(newProxy, true)

	require.Same(t, newProxy, conn.wgProxyRelay)
	require.Len(t, conn.retiredRelayProxies, 1)
	require.Zero(t, oldProxy.closed.Load(), "old Relay path must remain alive before the replacement handshakes")
	require.Nil(t, oldProxy.listener, "retired path must not report a disconnect for the replacement path")

	conn.onWGCheckSuccess()
	require.EqualValues(t, 1, oldProxy.closed.Load())
	require.Empty(t, conn.retiredRelayProxies)
	require.Zero(t, newProxy.closed.Load())
}

func TestRelayProxyMigrationExpiresOldPath(t *testing.T) {
	originalGrace := relayPeerMigrationGrace
	relayPeerMigrationGrace = 20 * time.Millisecond
	t.Cleanup(func() { relayPeerMigrationGrace = originalGrace })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	oldProxy := &migrationProxy{listener: func() {}}
	newProxy := &migrationProxy{listener: func() {}}
	conn := &Conn{ctx: ctx, wgProxyRelay: oldProxy}

	conn.setRelayedProxy(newProxy, true)

	require.Eventually(t, func() bool { return oldProxy.closed.Load() == 1 }, time.Second, 5*time.Millisecond)
	require.Empty(t, conn.retiredRelayProxies)
	require.Zero(t, newProxy.closed.Load())
}

func TestActiveRelayMigrationRedirectsOldPathToNewEndpoint(t *testing.T) {
	oldProxy := &migrationProxy{listener: func() {}}
	newProxy := &migrationProxy{listener: func() {}}
	conn := &Conn{wgProxyRelay: oldProxy, currentConnPriority: conntype.Relay}

	old, migrating := conn.beginRelayProxyMigrationLocked()
	require.True(t, migrating)
	require.Same(t, oldProxy, old)
	require.EqualValues(t, 1, oldProxy.paused.Load())

	conn.commitRelayProxyMigrationLocked(old, newProxy, migrating)
	require.EqualValues(t, 1, oldProxy.redirected.Load())
	require.Zero(t, oldProxy.closed.Load())
}

func TestActiveRelayMigrationFailureResumesOldPath(t *testing.T) {
	oldProxy := &migrationProxy{listener: func() {}}
	conn := &Conn{wgProxyRelay: oldProxy, currentConnPriority: conntype.Relay}

	old, migrating := conn.beginRelayProxyMigrationLocked()
	conn.rollbackRelayProxyMigrationLocked(old, migrating)

	require.EqualValues(t, 1, oldProxy.paused.Load())
	require.EqualValues(t, 1, oldProxy.worked.Load())
	require.Zero(t, oldProxy.closed.Load())
}
