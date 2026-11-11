package client

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/relay/server"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
	"github.com/netbirdio/netbird/shared/relay/healthcheck"
)

// startFailoverRelayServer starts an in-process relay server on a free port
// and returns the server handle with its client-facing URL. The server is shut
// down on test cleanup, so the test can also kill it explicitly mid-test to
// simulate a relay failure (Server.Shutdown is safe to call twice).
func startFailoverRelayServer(t *testing.T, name string) (*server.Server, string) {
	t.Helper()
	address, _ := freeAddr(t)
	srv, err := server.NewServer(server.Config{
		Meter:          otel.Meter("relay-failover-" + name),
		ExposedAddress: address,
		TLSSupport:     false,
		AuthValidator:  &allow.Auth{},
	})
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Listen(server.ListenerConfig{Address: address}) }()
	require.NoError(t, waitForServerToStart(errCh))
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	return srv, "rel://" + address
}

// TestManagerFailoverToBackupRelay kills the primary relay server while two
// peers communicate through it. Both managers must observe the disconnect,
// immediately promote their standby relay to primary, and resume traffic there.
func TestManagerFailoverToBackupRelay(t *testing.T) {
	primarySrv, primaryURL := startFailoverRelayServer(t, "primary")
	_, backupURL := startFailoverRelayServer(t, "backup")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	newPeerManager := func(peerID string) *Manager {
		t.Helper()
		// Start with primaryURL first so both peers deterministically connect
		// to primaryURL, then add backupURL so it is probed and selected as
		// the standby relay.
		mgr := NewManager(ctx, []string{primaryURL}, peerID, iface.DefaultMTU,
			WithMaxBackoffInterval(5*time.Second))
		require.NoError(t, mgr.Serve())
		require.NoError(t, waitForReady(ctx, mgr, 15*time.Second))
		require.Equal(t, primaryURL, mgr.currentRelayURL(),
			"peer %s should start on the primary relay", peerID)

		mgr.UpdateServerURLs([]string{primaryURL, backupURL})
		require.Eventually(t, func() bool {
			return mgr.StandbyURL() == backupURL
		}, 5*time.Second, 20*time.Millisecond, "peer %s should select backupURL as standby", peerID)
		return mgr
	}

	alice := newPeerManager("alice")
	bob := newPeerManager("bob")

	// Sanity check: traffic flows through the primary relay before the kill.
	requireEcho(t, ctx, alice, bob, "pre-failover")

	// Kill the primary relay server. The graceful shutdown closes every peer
	// session, which drives both managers through disconnect and the
	// reconnect guard must immediately promote the standby server to primary.
	require.NoError(t, primarySrv.Shutdown(context.Background()))

	peers := []struct {
		id  string
		mgr *Manager
	}{{"alice", alice}, {"bob", bob}}

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(id string, mgr *Manager) {
			defer wg.Done()
			require.Eventually(t, func() bool {
				return mgr.Ready() && mgr.currentRelayURL() == backupURL
			}, 15*time.Second, 20*time.Millisecond,
				"peer %s did not fail over its home relay to the standby server", id)
		}(peer.id, peer.mgr)
	}
	wg.Wait()

	// Traffic must recover through the backup relay with fresh connections.
	requireEcho(t, ctx, alice, bob, "post-failover")
}

// requireEcho asserts a payload round-trips between the two managers in both
// directions. It retries until the deadline because a peer that just migrated
// may not be registered on the new relay server yet.
func requireEcho(t *testing.T, ctx context.Context, alice, bob *Manager, payload string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if tryEcho(ctx, alice, bob, payload) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer echo did not succeed within 15s (payload %q)", payload)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tryEcho(ctx context.Context, alice, bob *Manager, payload string) bool {
	aliceAddr, _, err := alice.RelayInstanceAddress()
	if err != nil {
		return false
	}
	bobAddr, _, err := bob.RelayInstanceAddress()
	if err != nil {
		return false
	}
	a2b, err := alice.OpenConn(ctx, bobAddr, "bob", netip.Addr{})
	if err != nil {
		return false
	}
	defer func() { _ = a2b.Close() }()
	b2a, err := bob.OpenConn(ctx, aliceAddr, "alice", netip.Addr{})
	if err != nil {
		return false
	}
	defer func() { _ = b2a.Close() }()

	buf := make([]byte, 65535)
	return writeAndRead(a2b, b2a, buf, "ping:"+payload) &&
		writeAndRead(b2a, a2b, buf, "pong:"+payload)
}

func writeAndRead(w, r net.Conn, buf []byte, payload string) bool {
	if _, err := w.Write([]byte(payload)); err != nil {
		return false
	}
	return readWithTimeout(r, buf, payload, 5*time.Second)
}

// readWithTimeout reads one message without relying on SetReadDeadline: the
// relay Conn does not implement deadlines, so the read runs in a goroutine and
// the caller gives up after the timeout. The caller must close the connection,
// which unblocks the stray read.
func readWithTimeout(r net.Conn, buf []byte, payload string, timeout time.Duration) bool {
	type readResult struct {
		n   int
		err error
	}
	done := make(chan readResult, 1)
	go func() {
		n, err := r.Read(buf)
		done <- readResult{n, err}
	}()
	select {
	case res := <-done:
		return res.err == nil && string(buf[:res.n]) == payload
	case <-time.After(timeout):
		return false
	}
}

type fakeSilentQUICConn struct {
	net.Conn
	recvPackets atomic.Uint64
	blackholed  atomic.Bool
	closed      atomic.Bool
}

func (f *fakeSilentQUICConn) Write(b []byte) (int, error) {
	if f.closed.Load() {
		return 0, net.ErrClosed
	}
	if !f.blackholed.Load() {
		f.recvPackets.Add(1)
	}
	return len(b), nil
}

func (f *fakeSilentQUICConn) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *fakeSilentQUICConn) PacketsReceived() uint64 {
	if !f.blackholed.Load() {
		return f.recvPackets.Add(1)
	}
	return f.recvPackets.Load()
}

type fakeSilentWSConn struct {
	net.Conn
	blackholed atomic.Bool
	closed     atomic.Bool
}

func (f *fakeSilentWSConn) Write(b []byte) (int, error) {
	if f.closed.Load() {
		return 0, net.ErrClosed
	}
	return len(b), nil
}

func (f *fakeSilentWSConn) Ping(ctx context.Context) error {
	if f.closed.Load() {
		return net.ErrClosed
	}
	if f.blackholed.Load() {
		<-ctx.Done()
		return errors.New("pong timeout")
	}
	return nil
}

func (f *fakeSilentWSConn) Close() error {
	f.closed.Store(true)
	return nil
}

func TestClientProbeConnectionLivenessClosesSilentQUICAndWebSocket(t *testing.T) {
	prevInterval := relayLivenessProbeInterval
	prevTimeout := relayLivenessProbeTimeout
	prevThreshold := relayLivenessFailureThreshold
	relayLivenessProbeInterval = 30 * time.Millisecond
	relayLivenessProbeTimeout = 30 * time.Millisecond
	relayLivenessFailureThreshold = 2
	t.Cleanup(func() {
		relayLivenessProbeInterval = prevInterval
		relayLivenessProbeTimeout = prevTimeout
		relayLivenessFailureThreshold = prevThreshold
	})

	t.Run("silent QUIC blackhole closes connection after threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := NewClient("rels://quic-relay:443", hmacTokenStore, "alice", iface.DefaultMTU)
		stopFlag := newInternalStopFlag()
		qConn := &fakeSilentQUICConn{}

		go client.probeConnectionLiveness(ctx, qConn, nil, qConn, stopFlag)

		// Healthy while ACKs arrive.
		time.Sleep(80 * time.Millisecond)
		require.False(t, qConn.closed.Load(), "healthy QUIC connection must stay open")

		// Simulate container force-kill / silent UDP blackhole.
		qConn.blackholed.Store(true)
		require.Eventually(t, func() bool {
			return qConn.closed.Load() && stopFlag.isSet()
		}, 500*time.Millisecond, 10*time.Millisecond, "silent QUIC blackhole must close connection promptly")
	})

	t.Run("silent WebSocket blackhole closes connection after threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := NewClient("rels://ws-relay:443", hmacTokenStore, "bob", iface.DefaultMTU)
		stopFlag := newInternalStopFlag()
		wsConn := &fakeSilentWSConn{}

		go client.probeConnectionLiveness(ctx, wsConn, wsConn, nil, stopFlag)

		time.Sleep(80 * time.Millisecond)
		require.False(t, wsConn.closed.Load(), "healthy WebSocket connection must stay open")

		wsConn.blackholed.Store(true)
		require.Eventually(t, func() bool {
			return wsConn.closed.Load() && stopFlag.isSet()
		}, 500*time.Millisecond, 10*time.Millisecond, "silent WebSocket blackhole must close connection promptly")
	})

	t.Run("transport liveness does not mask application health", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := NewClient("rels://quic-relay:443", hmacTokenStore, "carol", iface.DefaultMTU)
		hc := healthcheck.NewReceiverWithOpts(client.log, healthcheck.ReceiverOptions{
			HeartbeatTimeout: 100 * time.Millisecond,
		})
		defer hc.Stop()
		stopFlag := newInternalStopFlag()
		qConn := &fakeSilentQUICConn{}

		go client.probeConnectionLiveness(ctx, qConn, nil, qConn, stopFlag)

		select {
		case <-hc.OnTimeout:
		case <-time.After(time.Second):
			t.Fatal("transport liveness probe incorrectly refreshed application health")
		}
	})
}
