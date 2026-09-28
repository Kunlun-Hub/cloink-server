package client

import (
	"sync"
	"time"
)

const (
	dataPlaneFailureWindow       = 2 * time.Minute
	dataPlaneRecoveryCooldown    = 2 * time.Minute
	dataPlaneDistinctPeerTrigger = 2
	// dataPlaneSinglePeerFailureTrigger is the number of consecutive WireGuard
	// handshake timeouts from one peer that alone triggers a Relay rebuild.
	// The distinct-peer trigger never fires for a client talking to a single
	// peer, so a blackholed Relay would go undetected there; consecutive
	// timeouts with no handshake success in between are the same signal for
	// that case. Three matches the rosenpass escalation threshold
	// (wgTimeoutEscalationThreshold) for consecutive handshake timeouts,
	// keeping both subsystems equally sensitive: one or two timeouts may be
	// transient, while three with zero successes means the data plane is not
	// recovering on its own. Recovery stays safe because any handshake
	// success clears the count, the rebuild itself is a soft reconnect, and
	// the recovery cooldown caps how often it can fire.
	dataPlaneSinglePeerFailureTrigger = 3
)

type relayPeerFailure struct {
	last        time.Time
	consecutive int
}

type relayFailureState struct {
	peers        map[string]relayPeerFailure
	lastRecovery time.Time
}

// relayDataPlaneFailures detects a Relay transport that remains connected while
// WireGuard handshakes through it repeatedly time out. Recovery triggers when
// either two distinct peers fail within the window, or one peer fails
// consecutively without any handshake success in between.
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

	failure := state.peers[peerKey]
	failure.last = now
	failure.consecutive++
	state.peers[peerKey] = failure

	if !state.lastRecovery.IsZero() && now.Sub(state.lastRecovery) < dataPlaneRecoveryCooldown {
		return false
	}
	// Distinct peers failing is the stronger signal and triggers first; a
	// lone peer needs enough consecutive timeouts to rule out transients.
	if len(state.peers) < dataPlaneDistinctPeerTrigger &&
		failure.consecutive < dataPlaneSinglePeerFailureTrigger {
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
