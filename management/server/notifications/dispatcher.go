package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	webhookRequestTimeout    = 10 * time.Second
	webhookMaxRetries        = 2
	webhookRetryBackoff      = 500 * time.Millisecond
	maxWebhookWorkers        = 16
	webhookTaskQueueCapacity = 1024

	// EnvAllowPrivateWebhookTargets allows webhook deliveries to loopback or
	// RFC1918 private addresses when explicitly set to "true" in self-hosted
	// environments.
	EnvAllowPrivateWebhookTargets = "NB_ALLOW_PRIVATE_WEBHOOK_TARGETS"
)

var (
	cgnatPrefix     = netip.MustParsePrefix("100.64.0.0/10")
	benchmarkPrefix = netip.MustParsePrefix("198.18.0.0/15")

	allowPrivateTargetsForTest atomic.Bool

	webhookPoolOnce    sync.Once
	webhookTaskQueue   chan func()
	webhookTasks       sync.WaitGroup
	webhookDisabled    atomic.Bool
	webhookDeliverySem = make(chan struct{}, maxWebhookWorkers)
)

// ChannelLister is the minimal store interface required to list notification channels for an account.
type ChannelLister interface {
	ListNotificationChannels(ctx context.Context, lockStrength store.LockingStrength, accountID string) ([]*types.NotificationChannel, error)
}

// PeerGroupSummary represents a group entry in the peer webhook payload.
type PeerGroupSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// PeerWebhookInfo holds the device details sent in a peer webhook event.
type PeerWebhookInfo struct {
	ID             string             `json:"id"`
	Name           string             `json:"name"`
	Hostname       string             `json:"hostname"`
	IP             string             `json:"ip"`
	IPv6           string             `json:"ipv6,omitempty"`
	ConnectionIP   string             `json:"connection_ip"`
	DNSLabel       string             `json:"dns_label"`
	ExtraDNSLabels []string           `json:"extra_dns_labels"`
	CountryCode    string             `json:"country_code"`
	CityName       string             `json:"city_name"`
	GeonameID      uint               `json:"geoname_id"`
	OS             string             `json:"os"`
	Version        string             `json:"version"`
	Connected      bool               `json:"connected"`
	LastSeen       time.Time          `json:"last_seen"`
	CreatedAt      time.Time          `json:"created_at"`
	UserID         string             `json:"user_id,omitempty"`
	Groups         []PeerGroupSummary `json:"groups"`
}

// PeerWebhookEvent is the top-level JSON payload POSTed to webhook notification channels.
type PeerWebhookEvent struct {
	ID        string          `json:"id"`
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	AccountID string          `json:"account_id"`
	Peer      PeerWebhookInfo `json:"peer"`
}

type webhookTargetConfig struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

var defaultWebhookHTTPClient = newSafeWebhookHTTPClient()

func newSafeWebhookHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 nil,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if isPrivateWebhookTargetAllowed() {
				return dialer.DialContext(ctx, network, addr)
			}
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid webhook dial address %q: %w", addr, err)
			}
			if isDisallowedWebhookHostname(host) {
				return nil, fmt.Errorf("webhook target hostname %q is not allowed", host)
			}
			if ip, err := netip.ParseAddr(host); err == nil {
				unmapped := ip.Unmap()
				if isBlockedWebhookIP(unmapped) {
					return nil, fmt.Errorf("webhook target IP %s is not allowed", unmapped)
				}
				return dialer.DialContext(ctx, network, net.JoinHostPort(unmapped.String(), port))
			}

			ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("resolve webhook target %q: %w", host, err)
			}
			if len(ips) == 0 {
				return nil, fmt.Errorf("webhook target %q did not resolve to any IP address", host)
			}
			for _, ip := range ips {
				unmapped := ip.Unmap()
				if isBlockedWebhookIP(unmapped) {
					return nil, fmt.Errorf("webhook target %q resolved to disallowed IP %s", host, unmapped)
				}
			}

			var lastErr error
			for _, ip := range ips {
				unmapped := ip.Unmap()
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(unmapped.String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
	}

	return &http.Client{
		Timeout:   webhookRequestTimeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func isPrivateWebhookTargetAllowed() bool {
	if allowPrivateTargetsForTest.Load() {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv(EnvAllowPrivateWebhookTargets)), "true")
}

func isDisallowedWebhookHostname(host string) bool {
	lower := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if lower == "" {
		return true
	}
	return lower == "localhost" ||
		strings.HasSuffix(lower, ".localhost") ||
		strings.HasSuffix(lower, ".local") ||
		strings.HasSuffix(lower, ".internal")
}

func isBlockedWebhookIP(ip netip.Addr) bool {
	if !ip.IsValid() {
		return true
	}
	ip = ip.Unmap()
	if ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified() {
		return true
	}
	if ip.Is4() && (cgnatPrefix.Contains(ip) || benchmarkPrefix.Contains(ip)) {
		return true
	}
	return false
}

// ValidateWebhookURL checks that rawURL is a valid http/https URL and does not
// target localhost or private/internal IP literals.
func ValidateWebhookURL(rawURL string) error {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return errors.New("webhook URL is required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("unsupported webhook URL scheme %q: only http and https are allowed", parsed.Scheme)
	}
	host := parsed.Hostname()
	if host == "" {
		return errors.New("webhook URL must include a valid host")
	}
	if isPrivateWebhookTargetAllowed() {
		return nil
	}
	if isDisallowedWebhookHostname(host) {
		return fmt.Errorf("webhook URL host %q is not allowed", host)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if isBlockedWebhookIP(ip.Unmap()) {
			return fmt.Errorf("webhook URL IP %s is not allowed", ip.Unmap())
		}
	}
	return nil
}

func initWebhookWorkerPool() {
	webhookPoolOnce.Do(func() {
		webhookTaskQueue = make(chan func(), webhookTaskQueueCapacity)
		for i := 0; i < maxWebhookWorkers; i++ {
			go func() {
				for task := range webhookTaskQueue {
					if task != nil {
						task()
					}
				}
			}()
		}
	})
}

// EnqueuePeerWebhook submits an asynchronous webhook task to the bounded worker
// pool without blocking the caller. It returns false if the queue is full.
func EnqueuePeerWebhook(task func()) bool {
	if task == nil || webhookDisabled.Load() {
		return false
	}
	initWebhookWorkerPool()
	webhookTasks.Add(1)
	select {
	case webhookTaskQueue <- func() {
		defer webhookTasks.Done()
		task()
	}:
		return true
	default:
		webhookTasks.Done()
		log.Warn("peer webhook worker queue is full; dropping webhook notification task")
		return false
	}
}

// WaitForPendingWebhooks blocks until every enqueued webhook task has
// completed. It is intended for tests and graceful shutdown coordination.
func WaitForPendingWebhooks() {
	webhookTasks.Wait()
}

// SetWebhookDispatchDisabledForTest disables asynchronous webhook enqueueing.
// Tests using a synthetic clock must disable dispatch because worker goroutines
// run outside the synthetic-clock bubble.
func SetWebhookDispatchDisabledForTest(disabled bool) {
	webhookDisabled.Store(disabled)
}

// MatchingWebhookChannels returns enabled webhook channels for accountID that
// subscribe to eventType.
func MatchingWebhookChannels(ctx context.Context, lister ChannelLister, accountID, eventType string) ([]*types.NotificationChannel, error) {
	if lister == nil || accountID == "" || eventType == "" {
		return nil, nil
	}
	channels, err := lister.ListNotificationChannels(ctx, store.LockingStrengthNone, accountID)
	if err != nil {
		return nil, err
	}
	matching := make([]*types.NotificationChannel, 0, len(channels))
	for _, ch := range channels {
		if ch == nil || !ch.Enabled || ch.Type != types.NotificationChannelTypeWebhook {
			continue
		}
		if !slices.Contains(ch.EventTypes, eventType) {
			continue
		}
		matching = append(matching, ch)
	}
	return matching, nil
}

// DispatchPeerWebhook queries enabled webhook notification channels for accountID subscribed to
// eventType and delivers the PeerWebhookEvent via HTTP POST with retry.
func DispatchPeerWebhook(ctx context.Context, lister ChannelLister, accountID, eventType string, peer PeerWebhookInfo) {
	matching, err := MatchingWebhookChannels(ctx, lister, accountID, eventType)
	if err != nil {
		log.WithContext(ctx).Warnf("failed to list notification channels for account %s: %v", accountID, err)
		return
	}
	DispatchPeerWebhookToChannels(ctx, matching, accountID, eventType, peer)
}

// DispatchPeerWebhookToChannels delivers a PeerWebhookEvent to the pre-filtered
// matching notification channels with bounded concurrency.
func DispatchPeerWebhookToChannels(ctx context.Context, matching []*types.NotificationChannel, accountID, eventType string, peer PeerWebhookInfo) {
	if len(matching) == 0 || accountID == "" || eventType == "" {
		return
	}

	if peer.ExtraDNSLabels == nil {
		peer.ExtraDNSLabels = []string{}
	}
	if peer.Groups == nil {
		peer.Groups = []PeerGroupSummary{}
	}

	event := PeerWebhookEvent{
		ID:        "evt_" + xid.New().String(),
		EventType: eventType,
		Timestamp: time.Now().UTC(),
		AccountID: accountID,
		Peer:      peer,
	}

	bodyBytes, err := json.Marshal(event)
	if err != nil {
		log.WithContext(ctx).Errorf("failed to marshal peer webhook event %s: %v", eventType, err)
		return
	}

	var wg sync.WaitGroup
	for _, ch := range matching {
		var target webhookTargetConfig
		if err := json.Unmarshal([]byte(ch.Target), &target); err != nil {
			log.WithContext(ctx).Warnf("invalid webhook target JSON on channel %s: %v", ch.ID, err)
			continue
		}
		target.URL = strings.TrimSpace(target.URL)
		if target.URL == "" {
			continue
		}
		if err := ValidateWebhookURL(target.URL); err != nil {
			log.WithContext(ctx).Warnf("disallowed webhook target URL on channel %s (%s): %v", ch.ID, target.URL, err)
			continue
		}

		channelID := ch.ID
		channelTarget := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case webhookDeliverySem <- struct{}{}:
				defer func() { <-webhookDeliverySem }()
			case <-ctx.Done():
				return
			}
			if err := postWebhookWithRetry(ctx, defaultWebhookHTTPClient, channelTarget, eventType, bodyBytes); err != nil {
				log.WithContext(ctx).Warnf("webhook delivery failed for channel %s (%s): %v", channelID, channelTarget.URL, err)
			}
		}()
	}
	wg.Wait()
}

func postWebhookWithRetry(ctx context.Context, client *http.Client, target webhookTargetConfig, eventType string, bodyBytes []byte) error {
	var lastErr error
	for attempt := 0; attempt <= webhookMaxRetries; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(webhookRetryBackoff * time.Duration(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		reqCtx, cancel := context.WithTimeout(ctx, webhookRequestTimeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, target.URL, bytes.NewReader(bodyBytes))
		if err != nil {
			cancel()
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Cloink-Webhook/1.0")
		req.Header.Set("X-Cloink-Event", eventType)
		for k, v := range target.Headers {
			if strings.TrimSpace(k) != "" {
				req.Header.Set(k, v)
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		cancel()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			// Do not retry non-429 4xx or 3xx responses.
			return lastErr
		}
	}
	return lastErr
}
