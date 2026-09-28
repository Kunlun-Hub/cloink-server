package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/shared/relay/messages"
)

type MocPeer struct {
	id messages.PeerID
}

func (m *MocPeer) Close() {

}

func (m *MocPeer) ID() messages.PeerID {
	return m.id
}

func TestStore_DeletePeer(t *testing.T) {
	s := NewStore()

	pID := messages.HashID("peer_one")
	p := &MocPeer{id: pID}
	s.AddPeer(p)
	s.DeletePeer(p)
	if _, ok := s.Peer(pID); ok {
		t.Errorf("peer was not deleted")
	}
}

func TestStore_DeleteDeprecatedPeer(t *testing.T) {
	s := NewStore()

	pID1 := messages.HashID("peer_one")
	pID2 := messages.HashID("peer_one")

	p1 := &MocPeer{id: pID1}
	p2 := &MocPeer{id: pID2}

	s.AddPeer(p1)
	s.AddPeer(p2)
	s.DeletePeer(p1)

	if _, ok := s.Peer(pID2); !ok {
		t.Errorf("second peer was deleted")
	}
}

func TestStore_ShardedAddAndGet(t *testing.T) {
	s := NewStore()

	const n = 1000
	for i := 0; i < n; i++ {
		id := messages.HashID(fmt.Sprintf("shard-test-peer-%d", i))
		if replaced := s.AddPeer(&MocPeer{id: id}); replaced {
			t.Fatalf("peer %d reported as replaced on first add", i)
		}
	}

	peers := s.Peers()
	assert.Len(t, peers, n)

	seen := make(map[messages.PeerID]struct{}, n)
	for _, p := range peers {
		seen[p.ID()] = struct{}{}
	}
	for i := 0; i < n; i++ {
		id := messages.HashID(fmt.Sprintf("shard-test-peer-%d", i))
		got, ok := s.Peer(id)
		require.True(t, ok, "peer %d not found", i)
		assert.Equal(t, id, got.ID())
		assert.Contains(t, seen, id)
	}
}

func TestStore_ShardsDistributePeers(t *testing.T) {
	s := NewStore()

	const n = 1000
	for i := 0; i < n; i++ {
		s.AddPeer(&MocPeer{id: messages.HashID(fmt.Sprintf("dist-%d", i))})
	}

	nonEmpty := 0
	for i := range s.shards {
		s.shards[i].mu.RLock()
		if len(s.shards[i].peers) > 0 {
			nonEmpty++
		}
		s.shards[i].mu.RUnlock()
	}
	assert.Greater(t, nonEmpty, 1, "peers should spread across shards")
}

func TestStore_ShardedConcurrentAccess(t *testing.T) {
	s := NewStore()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := messages.HashID(fmt.Sprintf("worker-%d-peer-%d", w, i))
				p := &MocPeer{id: id}
				s.AddPeer(p)
				if _, ok := s.Peer(id); !ok {
					t.Errorf("peer %s not found right after add", id)
					return
				}
				s.Peers()
				s.DeletePeer(p)
			}
		}(w)
	}
	wg.Wait()
	assert.Empty(t, s.Peers())
}

func TestStore_GetOnlinePeersAndRegisterInterest(t *testing.T) {
	s := NewStore()
	notifier := NewPeerNotifier()
	listener := notifier.NewListener(func([]messages.PeerID) {}, func([]messages.PeerID) {})
	defer notifier.RemoveListener(listener)

	onlineID := messages.HashID("online-peer")
	offlineID := messages.HashID("offline-peer")
	s.AddPeer(&MocPeer{id: onlineID})

	online := s.GetOnlinePeersAndRegisterInterest([]messages.PeerID{onlineID, offlineID}, listener)
	require.Len(t, online, 1)
	assert.Equal(t, onlineID, online[0])
}
