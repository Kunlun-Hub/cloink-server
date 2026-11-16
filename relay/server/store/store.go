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

// shard holds the published and handshake-staged peer maps guarded by its own lock.
type shard struct {
	mu     sync.RWMutex
	peers  map[messages.PeerID]IPeer
	staged map[messages.PeerID]IPeer
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
		s.shards[i].staged = make(map[messages.PeerID]IPeer)
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

	if stagedPeer, ok := sh.staged[peer.ID()]; ok && stagedPeer != peer {
		stagedPeer.Close()
		delete(sh.staged, peer.ID())
	}

	odlPeer, ok := sh.peers[peer.ID()]
	if ok {
		odlPeer.Close()
	}

	sh.peers[peer.ID()] = peer
	return ok
}

// StagePeer records an authenticated peer without publishing it as online. Staged
// peers can receive forwarded transport, but they are hidden from Peer, Peers,
// and online-peer notifications until PublishPeer is called after the auth
// response has been written.
func (s *Store) StagePeer(peer IPeer) {
	sh := s.shardFor(peer.ID())
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if old, ok := sh.staged[peer.ID()]; ok && old != peer {
		old.Close()
	}
	sh.staged[peer.ID()] = peer
}

// PublishPeer moves a staged peer into the published set after authentication.
// The second return value is false when another handshake staged the same peer ID
// after this peer, in which case the caller must close this connection instead of
// starting it. The first return value reports whether an online peer was replaced.
func (s *Store) PublishPeer(peer IPeer) (replaced bool, published bool) {
	sh := s.shardFor(peer.ID())
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if stagedPeer, ok := sh.staged[peer.ID()]; ok {
		if stagedPeer != peer {
			return false, false
		}
		delete(sh.staged, peer.ID())
	}

	oldPeer, ok := sh.peers[peer.ID()]
	if ok && oldPeer != peer {
		oldPeer.Close()
	}
	sh.peers[peer.ID()] = peer
	return ok, true
}

// DeletePeer deletes a peer from the store
func (s *Store) DeletePeer(peer IPeer) bool {
	sh := s.shardFor(peer.ID())
	sh.mu.Lock()
	defer sh.mu.Unlock()

	deleted := false
	if dp, ok := sh.peers[peer.ID()]; ok && dp == peer {
		delete(sh.peers, peer.ID())
		deleted = true
	}
	if sp, ok := sh.staged[peer.ID()]; ok && sp == peer {
		delete(sh.staged, peer.ID())
		deleted = true
	}
	return deleted
}

// Peer returns a peer by its ID
func (s *Store) Peer(id messages.PeerID) (IPeer, bool) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	p, ok := sh.peers[id]
	return p, ok
}

// PeerForRouting returns a published peer or a staged peer whose auth response
// is still being written. Only the transport forwarding path should use it.
func (s *Store) PeerForRouting(id messages.PeerID) (IPeer, bool) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	if p, ok := sh.peers[id]; ok {
		return p, true
	}
	p, ok := sh.staged[id]
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
