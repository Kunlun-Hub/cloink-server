package client

import (
	"context"
	"slices"
	"time"

	log "github.com/sirupsen/logrus"
)

const relayHotStandbyWait = 2 * time.Second

// recordHomeSwitch updates the diagnostics exposed by `cloink relay` and
// starts the minimum-residence window for subsequent proactive migrations.
func (m *Manager) recordHomeSwitch(reason string) {
	m.relayConfigMu.Lock()
	m.lastHomeSwitchAt = time.Now()
	m.lastHomeSwitchReason = reason
	m.homeSwitchCount++
	m.relayConfigMu.Unlock()
}

func (m *Manager) proactiveSwitchRemaining() time.Duration {
	if relayProactiveSwitchMinResidence <= 0 {
		return 0
	}
	m.relayConfigMu.RLock()
	last := m.lastHomeSwitchAt
	m.relayConfigMu.RUnlock()
	if last.IsZero() {
		return 0
	}
	remaining := relayProactiveSwitchMinResidence - time.Since(last)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (m *Manager) scheduleStableReset(client *Client) {
	if client == nil || relayStableResetInterval <= 0 {
		return
	}
	m.stableResetMu.Lock()
	if m.stableResetTimer != nil {
		m.stableResetTimer.Stop()
	}
	m.stableResetTimer = time.AfterFunc(relayStableResetInterval, func() {
		m.onStableHomeRelay(client)
	})
	m.stableResetMu.Unlock()
}

func (m *Manager) stopStableReset() {
	m.stableResetMu.Lock()
	if m.stableResetTimer != nil {
		m.stableResetTimer.Stop()
		m.stableResetTimer = nil
	}
	m.stableResetMu.Unlock()
}

func (m *Manager) onStableHomeRelay(client *Client) {
	m.relayClientMu.RLock()
	current := m.relayClient == client
	m.relayClientMu.RUnlock()
	if !current || client == nil || !client.Ready() {
		return
	}

	m.serverPicker.clearServerFailure(client.connectionURL)
	if m.dataPlaneFailures != nil {
		m.dataPlaneFailures.resetRelay(client.connectionURL)
	}
	m.reconnectGuard.ResetBackoff()
	log.Infof("relay %s has been stable for %s; reconnect backoff and transient failures reset", client.connectionURL, relayStableResetInterval)
}

// ensureHotStandby keeps one ready standby Relay session around after a
// materially better candidate is detected. The session is not used for peer
// traffic until promotion, but it removes the connection dial from the
// failover critical path.
func (m *Manager) ensureHotStandby(targetURL string) {
	if targetURL == "" {
		// A probe round without a superior candidate is not evidence that an
		// already connected standby has become invalid. Keep it armed for a
		// future failover; explicit lifecycle events close it.
		return
	}

	m.relayConfigMu.RLock()
	forced := m.forcedRelayURL
	configured := slices.Contains(m.configuredRelayURLs, targetURL)
	m.relayConfigMu.RUnlock()
	if forced != "" || !configured || targetURL == m.currentRelayURL() {
		return
	}

	m.hotStandbyMu.Lock()
	if m.hotStandbyClient != nil && m.hotStandbyURL == targetURL && m.hotStandbyClient.Ready() {
		m.hotStandbyMu.Unlock()
		return
	}
	if m.hotStandbyDialing && m.hotStandbyURL == targetURL {
		m.hotStandbyMu.Unlock()
		return
	}
	m.hotStandbyGeneration++
	generation := m.hotStandbyGeneration
	oldClient := m.hotStandbyClient
	oldCancel := m.hotStandbyCancel
	hotStandbyCtx, cancel := context.WithCancel(m.ctx)
	m.hotStandbyClient = nil
	m.hotStandbyURL = targetURL
	m.hotStandbyDialing = true
	m.hotStandbyCancel = cancel
	m.hotStandbyMu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	if oldClient != nil {
		_ = oldClient.Close()
	}

	go func() {
		candidate, err := m.serverPicker.PickServerFromWithCooldownBypass(hotStandbyCtx, []string{targetURL})
		if err != nil {
			log.Debugf("hot standby preconnect to %s failed: %v", targetURL, err)
			m.hotStandbyMu.Lock()
			if generation == m.hotStandbyGeneration {
				m.hotStandbyDialing = false
				m.hotStandbyURL = ""
				m.hotStandbyCancel = nil
			}
			m.hotStandbyMu.Unlock()
			return
		}

		m.relayConfigMu.RLock()
		valid := m.forcedRelayURL == "" && slices.Contains(m.configuredRelayURLs, targetURL)
		m.relayConfigMu.RUnlock()
		currentHome := m.currentRelayURL()
		m.hotStandbyMu.Lock()
		if generation != m.hotStandbyGeneration {
			m.hotStandbyMu.Unlock()
			_ = candidate.Close()
			return
		}
		if !valid || !m.running.Load() || m.ctx.Err() != nil || currentHome == targetURL {
			if m.hotStandbyDialing && m.hotStandbyURL == targetURL {
				m.hotStandbyDialing = false
				m.hotStandbyURL = ""
				m.hotStandbyCancel = nil
			}
			m.hotStandbyMu.Unlock()
			_ = candidate.Close()
			return
		}
		previous := m.hotStandbyClient
		m.hotStandbyClient = candidate
		m.hotStandbyURL = targetURL
		m.hotStandbyDialing = false
		m.hotStandbyCancel = nil
		m.hotStandbyMu.Unlock()

		if previous != nil {
			_ = previous.Close()
		}
		log.Infof("hot standby Relay %s is connected and ready", targetURL)
	}()
}

func (m *Manager) takeHotStandby() *Client {
	if candidate := m.takeHotStandbyFor(""); candidate != nil {
		return candidate
	}

	// A failover can arrive while the preconnect is still completing. Wait for
	// that in-flight dial instead of opening a second session to the same relay;
	// duplicate sessions for one peer ID can replace each other on the server.
	m.hotStandbyMu.Lock()
	targetURL := m.hotStandbyURL
	dialing := m.hotStandbyDialing
	m.hotStandbyMu.Unlock()
	if !dialing || targetURL == "" {
		return nil
	}
	if candidate := m.waitForHotStandby(targetURL, relayHotStandbyWait); candidate != nil {
		return candidate
	}
	if candidate := m.takeHotStandbyFor(""); candidate != nil {
		return candidate
	}

	// The failover path cannot wait for the full connection timeout. Cancel the
	// in-flight preconnect before falling back to a cold failover, otherwise two
	// sessions for the same peer ID could race on the relay server.
	m.hotStandbyMu.Lock()
	cancel := m.hotStandbyCancel
	if m.hotStandbyDialing && m.hotStandbyURL == targetURL {
		m.hotStandbyGeneration++
		m.hotStandbyURL = ""
		m.hotStandbyDialing = false
		m.hotStandbyCancel = nil
	}
	m.hotStandbyMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (m *Manager) takeHotStandbyFor(targetURL string) *Client {
	m.hotStandbyMu.Lock()
	candidate := m.hotStandbyClient
	if candidate == nil || (targetURL != "" && m.hotStandbyURL != targetURL) {
		m.hotStandbyMu.Unlock()
		return nil
	}
	m.hotStandbyClient = nil
	m.hotStandbyURL = ""
	m.hotStandbyDialing = false
	cancel := m.hotStandbyCancel
	m.hotStandbyCancel = nil
	m.hotStandbyMu.Unlock()
	if cancel != nil {
		cancel()
	}

	m.relayConfigMu.RLock()
	forcedURL := m.forcedRelayURL
	configured := slices.Contains(m.configuredRelayURLs, candidate.connectionURL)
	m.relayConfigMu.RUnlock()
	valid := configured && (forcedURL == "" || forcedURL == candidate.connectionURL)
	if !valid || !candidate.Ready() {
		_ = candidate.Close()
		return nil
	}
	return candidate
}

func (m *Manager) waitForHotStandby(targetURL string, timeout time.Duration) *Client {
	if targetURL == "" || timeout <= 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if candidate := m.takeHotStandbyFor(targetURL); candidate != nil {
			return candidate
		}
		m.hotStandbyMu.Lock()
		dialing := m.hotStandbyDialing && m.hotStandbyURL == targetURL
		m.hotStandbyMu.Unlock()
		if !dialing || time.Now().After(deadline) {
			return nil
		}
		<-ticker.C
	}
}

func (m *Manager) closeHotStandby(reason string) {
	m.hotStandbyMu.Lock()
	m.hotStandbyGeneration++
	client := m.hotStandbyClient
	url := m.hotStandbyURL
	cancel := m.hotStandbyCancel
	m.hotStandbyClient = nil
	m.hotStandbyURL = ""
	m.hotStandbyDialing = false
	m.hotStandbyCancel = nil
	m.hotStandbyMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if client != nil {
		log.Debugf("closing hot standby Relay %s: %s", url, reason)
		_ = client.Close()
	}
}

// HandleNetworkChange invalidates measurements from the old path and closes
// Relay sockets immediately. The reconnect guard then redials the best relay on
// the new network instead of waiting for a liveness timeout.
func (m *Manager) HandleNetworkChange(reason string) {
	if !m.running.Load() || m.ctx.Err() != nil {
		return
	}
	now := time.Now()
	m.networkChangeMu.Lock()
	if !m.lastNetworkChange.IsZero() && now.Sub(m.lastNetworkChange) < time.Second {
		m.networkChangeMu.Unlock()
		return
	}
	m.lastNetworkChange = now
	m.networkChangeMu.Unlock()

	log.Infof("Relay network change detected (%s); rebuilding relay sessions immediately", reason)
	m.rttCache.clear()
	m.resetRTTMigrationStreak()
	m.closeHotStandby("network changed")
	m.stopStableReset()
	m.serverPicker.clearTransientFailures()
	if m.dataPlaneFailures != nil {
		m.dataPlaneFailures.resetAll()
	}
	m.reconnectGuard.ResetBackoff()
	m.setStandbyURL("")

	m.relayClientMu.RLock()
	home := m.relayClient
	m.relayClientMu.RUnlock()
	if home != nil {
		go func(client *Client) {
			_ = client.Close()
		}(home)
	}

	m.relayClientsMutex.RLock()
	foreign := make([]*Client, 0, len(m.relayClients))
	for _, track := range m.relayClients {
		track.RLock()
		if track.relayClient != nil {
			foreign = append(foreign, track.relayClient)
		}
		track.RUnlock()
	}
	m.relayClientsMutex.RUnlock()
	for _, client := range foreign {
		go func(client *Client) {
			_ = client.Close()
		}(client)
	}
}

type networkChangeSource interface {
	NetworkChanged() <-chan struct{}
}

func (m *Manager) listenNetworkChanges(ctx context.Context) {
	source, ok := m.netEvents.(networkChangeSource)
	if !ok {
		return
	}
	changes := source.NetworkChanged()
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			m.HandleNetworkChange("os-network-change")
		}
	}
}
