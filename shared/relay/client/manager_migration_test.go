package client

import (
	"context"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/relay/server"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
)

func TestManagerPriorityMigrationConnectsBeforeRetiringOldRelay(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "low")
	highURL := startMigrationRelayServer(t, "high")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "peer-local", iface.DefaultMTU,
		WithRelayMigrationGrace(300*time.Millisecond))
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.NotNil(t, oldClient)
	require.True(t, oldClient.Ready())

	manager.UpdateServerURLsWithWeights([]string{highURL, lowURL}, map[string]int{highURL: 80, lowURL: 20})

	require.Eventually(t, func() bool {
		manager.relayClientMu.RLock()
		defer manager.relayClientMu.RUnlock()
		return manager.relayClient != nil && manager.relayClient != oldClient && manager.relayClient.connectionURL == highURL && manager.relayClient.Ready()
	}, 3*time.Second, 10*time.Millisecond)
	require.True(t, oldClient.Ready(), "old Relay must remain alive after the atomic home-client swap")
	require.Eventually(t, func() bool { return !oldClient.Ready() }, 2*time.Second, 10*time.Millisecond,
		"old Relay should retire after peer migration grace")
}

func TestManagerPriorityMigrationKeepsOldRelayWhenCandidateFails(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "low")
	const unavailableHighURL = "rel://127.0.0.1:1"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "peer-local", iface.DefaultMTU)
	manager.serverPicker.ConnectionTimeout = 300 * time.Millisecond
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()

	manager.UpdateServerURLsWithWeights([]string{unavailableHighURL, lowURL}, map[string]int{unavailableHighURL: 80, lowURL: 20})
	time.Sleep(700 * time.Millisecond)

	manager.relayClientMu.RLock()
	defer manager.relayClientMu.RUnlock()
	require.Same(t, oldClient, manager.relayClient)
	require.True(t, oldClient.Ready(), "failed candidate must not interrupt the working Relay")
}

func TestManagerIdenticalRefreshDoesNotReplaceHomeRelay(t *testing.T) {
	relayURL := startMigrationRelayServer(t, "same-config")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{relayURL}, "peer-local", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()

	manager.UpdateServerURLsWithWeights([]string{relayURL}, map[string]int{relayURL: defaultRelayWeight})
	time.Sleep(100 * time.Millisecond)

	manager.relayClientMu.RLock()
	defer manager.relayClientMu.RUnlock()
	require.Same(t, oldClient, manager.relayClient)
}

func TestManagerPriorityMigrationKeepsOldRelayUntilPeerConnLeaves(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "low-with-peer")
	highURL := startMigrationRelayServer(t, "high-with-peer")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "peer-local", iface.DefaultMTU,
		WithRelayMigrationGrace(2*time.Second))
	require.NoError(t, manager.Serve())
	remote := NewClient(lowURL, manager.tokenStore, "peer-remote", iface.DefaultMTU)
	require.NoError(t, remote.Connect(ctx))
	t.Cleanup(func() { _ = remote.Close() })
	peerConn, err := manager.OpenConn(ctx, lowURL, "peer-remote", netip.Addr{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peerConn.Close() })

	manager.relayClientMu.RLock()
	oldClient := manager.relayClient
	manager.relayClientMu.RUnlock()
	require.True(t, oldClient.HasConns())
	manager.UpdateServerURLsWithWeights([]string{highURL, lowURL}, map[string]int{highURL: 80, lowURL: 20})
	require.Eventually(t, func() bool {
		manager.relayClientMu.RLock()
		defer manager.relayClientMu.RUnlock()
		return manager.relayClient != oldClient && manager.relayClient.connectionURL == highURL
	}, 3*time.Second, 10*time.Millisecond)
	manager.relayClientsMutex.RLock()
	retiredTrack := manager.relayClients[lowURL]
	manager.relayClientsMutex.RUnlock()
	require.NotNil(t, retiredTrack, "retired home Relay must remain reusable during peer migration")
	retiredTrack.RLock()
	require.Same(t, oldClient, retiredTrack.relayClient)
	retiredTrack.RUnlock()

	time.Sleep(2200 * time.Millisecond)
	require.True(t, oldClient.Ready(), "old Relay must remain alive past the home migration grace while peer paths still use it")
	require.NoError(t, peerConn.Close())
	require.Eventually(t, func() bool { return !oldClient.Ready() }, time.Second, 10*time.Millisecond)
}

func TestManagerRejectsCandidateFromStaleConfigGeneration(t *testing.T) {
	manager := NewManager(context.Background(), []string{"rel://old"}, "peer-local", iface.DefaultMTU)
	oldClient := NewClient("rel://old", manager.tokenStore, "peer-local", iface.DefaultMTU)
	candidate := NewClient("rel://candidate", manager.tokenStore, "peer-local", iface.DefaultMTU)
	candidate.mu.Lock()
	candidate.serviceIsRunning = true
	candidate.mu.Unlock()
	manager.running.Store(true)
	manager.relayClient = oldClient
	staleGeneration := manager.relayConfigGeneration.Load()
	manager.relayConfigGeneration.Add(1)

	require.False(t, manager.swapHomeRelay(oldClient, candidate, staleGeneration))
	require.Same(t, oldClient, manager.relayClient)
}

func TestManagerMigrationKeepsExistingPeerStreamAlive(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "stream-low")
	highURL := startMigrationRelayServer(t, "stream-high")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "alice", iface.DefaultMTU,
		WithRelayMigrationGrace(2*time.Second))
	remote := NewClient(lowURL, manager.tokenStore, "bob", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	require.NoError(t, remote.Connect(ctx))
	t.Cleanup(func() { _ = remote.Close() })

	aliceConn, err := manager.OpenConn(ctx, lowURL, "bob", netip.Addr{})
	require.NoError(t, err)
	bobConn, err := remote.OpenConn(ctx, "alice")
	require.NoError(t, err)
	t.Cleanup(func() { _ = aliceConn.Close() })
	t.Cleanup(func() { _ = bobConn.Close() })

	manager.UpdateServerURLsWithWeights([]string{highURL, lowURL}, map[string]int{highURL: 80, lowURL: 20})
	require.Eventually(t, func() bool {
		manager.relayClientMu.RLock()
		defer manager.relayClientMu.RUnlock()
		return manager.relayClient != nil && manager.relayClient.connectionURL == highURL
	}, 3*time.Second, 10*time.Millisecond)

	payload := []byte("old peer stream remains usable during home Relay migration")
	_, err = aliceConn.Write(payload)
	require.NoError(t, err)
	received := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.ReadFull(bobConn, received)
		readDone <- readErr
	}()
	select {
	case err = <-readDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out reading from the existing peer stream during Relay migration")
	}
	require.Equal(t, payload, received)
}

func TestManagerPriorityMigrationPromotesExistingForeignRelay(t *testing.T) {
	lowURL := startMigrationRelayServer(t, "promote-low")
	highURL := startMigrationRelayServer(t, "promote-high")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	manager := NewManager(ctx, []string{lowURL}, "alice", iface.DefaultMTU,
		WithRelayMigrationGrace(2*time.Second))
	remote := NewClient(highURL, manager.tokenStore, "bob", iface.DefaultMTU)
	require.NoError(t, manager.Serve())
	require.NoError(t, remote.Connect(ctx))
	t.Cleanup(func() { _ = remote.Close() })

	aliceConn, err := manager.OpenConn(ctx, highURL, "bob", netip.Addr{})
	require.NoError(t, err)
	bobConn, err := remote.OpenConn(ctx, "alice")
	require.NoError(t, err)
	t.Cleanup(func() { _ = aliceConn.Close() })
	t.Cleanup(func() { _ = bobConn.Close() })

	manager.relayClientsMutex.RLock()
	track := manager.relayClients[highURL]
	manager.relayClientsMutex.RUnlock()
	require.NotNil(t, track)
	track.RLock()
	foreignClient := track.relayClient
	track.RUnlock()
	require.NotNil(t, foreignClient)

	manager.UpdateServerURLsWithWeights([]string{highURL, lowURL}, map[string]int{highURL: 80, lowURL: 20})
	require.Eventually(t, func() bool {
		manager.relayClientMu.RLock()
		defer manager.relayClientMu.RUnlock()
		return manager.relayClient == foreignClient && manager.relayClient.Ready()
	}, 3*time.Second, 10*time.Millisecond)
	manager.relayClientsMutex.RLock()
	_, stillForeign := manager.relayClients[highURL]
	manager.relayClientsMutex.RUnlock()
	require.False(t, stillForeign, "promoted Relay must no longer be cleaned up as a foreign connection")

	payload := []byte("existing foreign stream survives promotion to home Relay")
	_, err = aliceConn.Write(payload)
	require.NoError(t, err)
	received := make([]byte, len(payload))
	_, err = io.ReadFull(bobConn, received)
	require.NoError(t, err)
	require.Equal(t, payload, received)
}

func TestManagerRelayProbeDoesNotDisconnectHomeRelay(t *testing.T) {
	relayURL := startMigrationRelayServer(t, "probe")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	manager := NewManager(ctx, []string{relayURL}, "peer-local", iface.DefaultMTU)
	require.NoError(t, manager.Serve())

	disconnected := make(chan struct{}, 1)
	require.NoError(t, manager.AddCloseListener(relayURL, func() {
		select {
		case disconnected <- struct{}{}:
		default:
		}
	}))
	relays := manager.ProbeRelayServers(ctx)
	require.Len(t, relays, 1)
	require.True(t, relays[0].Available)

	select {
	case <-disconnected:
		t.Fatal("Relay availability probe disconnected the live home Relay")
	case <-time.After(200 * time.Millisecond):
	}
	require.True(t, manager.Ready())
}

func startMigrationRelayServer(t *testing.T, name string) string {
	t.Helper()
	address, _ := freeAddr(t)
	srv, err := server.NewServer(server.Config{
		Meter:          otel.Meter("relay-migration-" + name),
		ExposedAddress: address,
		TLSSupport:     false,
		AuthValidator:  &allow.Auth{},
	})
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Listen(server.ListenerConfig{Address: address}) }()
	require.NoError(t, waitForServerToStart(errCh))
	t.Cleanup(func() { require.NoError(t, srv.Shutdown(context.Background())) })
	return "rel://" + address
}
