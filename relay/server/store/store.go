package store

import (
	"hash/fnv"
	"sync"

	"github.com/netbirdio/netbird/shared/relay/messages"
)

// shardCount is the number of peer map shards. Sharding spreads the per-packet
// store lookups in the data path across independent locks.
const shardCount = 32

type IPeer interface {
	Close()
	ID() messages.PeerID
}

// shard holds a slice of the peer map guarded by its own lock.
type shard struct {
	mu    sync.RWMutex
	peers map[messages.PeerID]IPeer
}

// Store is a thread-safe store of peers
// It is used to store the peers that are connected to the relay server.
// The peer map is sharded by FNV-1a hash of the peer ID to reduce lock
// contention on the per-packet lookup path.
type Store struct {
	shards [shardCount]shard
}

// NewStore creates a new Store instance
func NewStore() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i].peers = make(map[messages.PeerID]IPeer)
	}
	return s
}

// shardFor returns the shard owning the given peer ID.
func (s *Store) shardFor(id messages.PeerID) *shard {
	h := fnv.New32a()
	_, _ = h.Write(id[:])
	return &s.shards[h.Sum32()%shardCount]
}

// AddPeer adds a peer to the store
// If the peer already exists, it will be replaced and the old peer will be closed
// Returns true if the peer was replaced, false if it was added for the first time.
func (s *Store) AddPeer(peer IPeer) bool {
	sh := s.shardFor(peer.ID())
	sh.mu.Lock()
	defer sh.mu.Unlock()
	odlPeer, ok := sh.peers[peer.ID()]
	if ok {
		odlPeer.Close()
	}

	sh.peers[peer.ID()] = peer
	return ok
}

// DeletePeer deletes a peer from the store
func (s *Store) DeletePeer(peer IPeer) bool {
	sh := s.shardFor(peer.ID())
	sh.mu.Lock()
	defer sh.mu.Unlock()

	dp, ok := sh.peers[peer.ID()]
	if !ok {
		return false
	}
	if dp != peer {
		return false
	}

	delete(sh.peers, peer.ID())
	return true
}

// Peer returns a peer by its ID
func (s *Store) Peer(id messages.PeerID) (IPeer, bool) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	p, ok := sh.peers[id]
	return p, ok
}

// Peers returns all the peers in the store
func (s *Store) Peers() []IPeer {
	var peers []IPeer
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, p := range sh.peers {
			peers = append(peers, p)
		}
		sh.mu.RUnlock()
	}
	return peers
}

func (s *Store) GetOnlinePeersAndRegisterInterest(peerIDs []messages.PeerID, listener *Listener) []messages.PeerID {
	// Hold the read locks of all shards, in index order, across the check and
	// the interest registration. A peer coming online concurrently is then
	// either observed here or notified through the listener afterwards, never
	// missed. No other path nests shard locks, so this cannot deadlock.
	for i := range s.shards {
		s.shards[i].mu.RLock()
	}
	defer func() {
		for i := range s.shards {
			s.shards[i].mu.RUnlock()
		}
	}()

	listener.AddInterestedPeers(peerIDs)

	// Check for currently online peers
	onlinePeers := make([]messages.PeerID, 0, len(peerIDs))
	for _, id := range peerIDs {
		if _, ok := s.shardFor(id).peers[id]; ok {
			onlinePeers = append(onlinePeers, id)
		}
	}

	return onlinePeers
}
