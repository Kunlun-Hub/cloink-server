package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/rand/v2"
	"strings"
	"time"
)

type relayFailureKind uint8

const (
	relayFailureTransient relayFailureKind = iota
	relayFailureDeterministic
)

const relayDeterministicCooldown = 5 * time.Minute

// classifyRelayFailure separates a network timeout, which a later probe can
// immediately disprove, from certificate or authentication failures where a
// stale cooldown must not be cleared just because the TCP/TLS endpoint answers.
func classifyRelayFailure(err error) relayFailureKind {
	if err == nil {
		return relayFailureTransient
	}

	var certVerifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	if errors.As(err, &certVerifyErr) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameErr) || errors.As(err, &invalidCert) {
		return relayFailureDeterministic
	}

	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"x509:", "certificate", "certificate has expired",
		"invalid auth", "authentication failed", "permission denied", "unauthorized",
		"invalid token", "token expired", "validate version", "version mismatch",
		"unsupported protocol version", "unexpected message type",
	} {
		if strings.Contains(message, marker) {
			return relayFailureDeterministic
		}
	}
	return relayFailureTransient
}

// cooldownDuration applies exponential backoff to transient failures. A
// deterministic configuration or identity failure gets the long ceiling
// immediately, so repeated reconnect rounds do not hammer it every few seconds.
// Caller holds cooldownMu.
func (sp *ServerPicker) cooldownDuration(relayURL string, kind relayFailureKind) time.Duration {
	duration := sp.CooldownDuration
	if duration <= 0 {
		duration = relayServerCooldown
	}
	if kind == relayFailureDeterministic {
		duration = max(duration, relayDeterministicCooldown)
	} else {
		for range min(max(0, sp.failures[relayURL]-1), 6) {
			duration = min(duration*2, 5*time.Minute)
		}
	}
	return duration + time.Duration(rand.Int64N(max(1, int64(duration/5))))
}

// clearTransientFailure removes a short-lived cooldown after a successful
// probe. Deterministic errors retain their cooldown until expiry or an explicit
// forced-relay selection.
func (sp *ServerPicker) clearTransientFailure(relayURL string) {
	if sp == nil || relayURL == "" {
		return
	}
	sp.cooldownMu.Lock()
	defer sp.cooldownMu.Unlock()
	if sp.failureKinds[relayURL] == relayFailureDeterministic {
		return
	}
	delete(sp.cooldowns, relayURL)
	delete(sp.failures, relayURL)
	delete(sp.failureKinds, relayURL)
}

func (sp *ServerPicker) cooldownRemaining(relayURL string) time.Duration {
	if sp == nil || relayURL == "" || sp.CooldownDuration <= 0 {
		return 0
	}
	sp.cooldownMu.Lock()
	defer sp.cooldownMu.Unlock()
	until, ok := sp.cooldowns[relayURL]
	if !ok {
		return 0
	}
	remaining := time.Until(until)
	if remaining <= 0 {
		delete(sp.cooldowns, relayURL)
		delete(sp.failures, relayURL)
		delete(sp.failureKinds, relayURL)
		return 0
	}
	return remaining
}

func (sp *ServerPicker) clearTransientFailures() {
	if sp == nil {
		return
	}
	sp.cooldownMu.Lock()
	defer sp.cooldownMu.Unlock()
	for relayURL, kind := range sp.failureKinds {
		if kind == relayFailureDeterministic {
			continue
		}
		delete(sp.cooldowns, relayURL)
		delete(sp.failures, relayURL)
		delete(sp.failureKinds, relayURL)
	}
}
