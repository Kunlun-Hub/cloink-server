package client

import (
	"sync"
	"time"
)

const (
	dataPlaneFailureWindow       = 2 * time.Minute
	dataPlaneRecoveryCooldown    = 2 * time.Minute
	dataPlaneDistinctPeerTrigger = 2
)

type relayPeerFailure struct {
	last time.Time
}

type relayFailureState struct {
	peers        map[string]relayPeerFailure
	lastRecovery time.Time
}

// relayDataPlaneFailures detects a Relay transport that remains connected while
// WireGuard handshakes through it repeatedly time out.
type relayDataPlaneFailures struct {
	mu     sync.Mutex
	states map[string]*relayFailureState
	now    func() time.Time
}

func newRelayDataPlaneFailures() *relayDataPlaneFailures {
	return &relayDataPlaneFailures{
		states: make(map[string]*relayFailureState),
		now:    time.Now,
	}
}

func (f *relayDataPlaneFailures) reportFailure(relayAddress, peerKey string) bool {
	if relayAddress == "" || peerKey == "" {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.now()
	state := f.states[relayAddress]
	if state == nil {
		state = &relayFailureState{peers: make(map[string]relayPeerFailure)}
		f.states[relayAddress] = state
	}

	for key, failure := range state.peers {
		if now.Sub(failure.last) > dataPlaneFailureWindow {
			delete(state.peers, key)
		}
	}

	state.peers[peerKey] = relayPeerFailure{last: now}

	if !state.lastRecovery.IsZero() && now.Sub(state.lastRecovery) < dataPlaneRecoveryCooldown {
		return false
	}
	if len(state.peers) < dataPlaneDistinctPeerTrigger {
		return false
	}

	state.lastRecovery = now
	clear(state.peers)
	return true
}

func (f *relayDataPlaneFailures) reportSuccess(relayAddress, peerKey string) {
	if relayAddress == "" || peerKey == "" {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if state := f.states[relayAddress]; state != nil {
		delete(state.peers, peerKey)
	}
}
