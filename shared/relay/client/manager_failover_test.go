package client

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/relay/server"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
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
// migrate their home relay to the backup server, and resume traffic there.
func TestManagerFailoverToBackupRelay(t *testing.T) {
	primarySrv, primaryURL := startFailoverRelayServer(t, "primary")
	_, backupURL := startFailoverRelayServer(t, "backup")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	weights := map[string]int{primaryURL: 80, backupURL: 20}
	newPeerManager := func(peerID string) *Manager {
		t.Helper()
		// The higher weight pins the initial home relay to the primary
		// server deterministically: the picker dials the top-weight group
		// first and takes the first success.
		mgr := NewManager(ctx, []string{primaryURL, backupURL}, peerID, iface.DefaultMTU,
			WithMaxBackoffInterval(5*time.Second))
		mgr.UpdateServerURLsWithWeights([]string{primaryURL, backupURL}, weights)
		require.NoError(t, mgr.Serve())
		require.NoError(t, waitForReady(ctx, mgr, 15*time.Second))
		require.Equal(t, primaryURL, mgr.currentRelayURL(),
			"peer %s should start on the primary relay", peerID)
		return mgr
	}

	alice := newPeerManager("alice")
	bob := newPeerManager("bob")

	// Sanity check: traffic flows through the primary relay before the kill.
	requireEcho(t, ctx, alice, bob, "pre-failover")

	// Kill the primary relay server. The graceful shutdown closes every peer
	// session, which drives both managers through disconnect and the
	// reconnect guard must pick the backup server.
	require.NoError(t, primarySrv.Shutdown(context.Background()))

	peers := []struct {
		id  string
		mgr *Manager
	}{{"alice", alice}, {"bob", bob}}

	// Phase 1: both managers must observe the disconnect. The watches run
	// concurrently because the reconnect guard migrates concurrently too:
	// a sequential check could miss the disconnect window of a peer that
	// already migrated while another peer was being awaited.
	observed := make([]bool, len(peers))
	var wg sync.WaitGroup
	for i := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if !peers[i].mgr.Ready() {
					observed[i] = true
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	for i, peer := range peers {
		require.True(t, observed[i], "peer %s did not observe the primary relay failure", peer.id)
	}

	// Phase 2: both managers must migrate their home relay to the backup
	// server. Migration takes at least the guard's quick-reconnect budget,
	// so phase 1 always completes first.
	for _, peer := range peers {
		require.Eventually(t, func() bool {
			return peer.mgr.Ready() && peer.mgr.currentRelayURL() == backupURL
		}, 30*time.Second, 50*time.Millisecond,
			"peer %s did not migrate its home relay to the backup server", peer.id)
	}

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
