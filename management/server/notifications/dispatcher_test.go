package notifications

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

type fakeChannelLister struct {
	channels []*types.NotificationChannel
}

func (f *fakeChannelLister) ListNotificationChannels(_ context.Context, _ store.LockingStrength, _ string) ([]*types.NotificationChannel, error) {
	return f.channels, nil
}

func enableLoopbackWebhooksForTest(t *testing.T) {
	t.Helper()
	allowPrivateTargetsForTest.Store(true)
	t.Cleanup(func() { allowPrivateTargetsForTest.Store(false) })
}

func TestDispatchPeerWebhook_DeliversToMatchingEnabledWebhookChannel(t *testing.T) {
	enableLoopbackWebhooksForTest(t)

	var received PeerWebhookEvent
	var rawMap map[string]any
	var customSecret string
	var eventHeader string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customSecret = r.Header.Get("X-Webhook-Secret")
		eventHeader = r.Header.Get("X-Cloink-Event")
		var body []byte
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		body = buf[:n]
		assert.NoError(t, json.Unmarshal(body, &received))
		assert.NoError(t, json.Unmarshal(body, &rawMap))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	targetJSON, err := json.Marshal(map[string]any{
		"url": srv.URL,
		"headers": map[string]string{
			"X-Webhook-Secret": "top-secret-token",
		},
	})
	require.NoError(t, err)

	lister := &fakeChannelLister{
		channels: []*types.NotificationChannel{
			{
				ID:         "ch-disabled",
				AccountID:  "acc-1",
				Type:       types.NotificationChannelTypeWebhook,
				Enabled:    false,
				EventTypes: []string{types.NotificationEventPeerAdd},
				Target:     string(targetJSON),
			},
			{
				ID:         "ch-active",
				AccountID:  "acc-1",
				Type:       types.NotificationChannelTypeWebhook,
				Enabled:    true,
				EventTypes: []string{types.NotificationEventPeerAdd, types.NotificationEventPeerOnline},
				Target:     string(targetJSON),
			},
		},
	}

	now := time.Now().UTC().Truncate(time.Second)
	peerInfo := PeerWebhookInfo{
		ID:             "peer-1",
		Name:           "office-gw-01",
		Hostname:       "office-gw-01",
		IP:             "100.64.0.15",
		IPv6:           "fd00::15",
		ConnectionIP:   "203.0.113.42",
		DNSLabel:       "office-gw-01.cloink.local",
		ExtraDNSLabels: []string{"gw-shanghai.cloink.local"},
		CountryCode:    "CN",
		CityName:       "Shanghai",
		GeonameID:      1796236,
		OS:             "Linux Ubuntu 24.04",
		Version:        "0.65.0",
		Connected:      true,
		LastSeen:       now,
		CreatedAt:      now,
		UserID:         "usr-admin",
		Groups: []PeerGroupSummary{
			{ID: "grp-all", Name: "All"},
		},
	}

	DispatchPeerWebhook(context.Background(), lister, "acc-1", types.NotificationEventPeerOnline, peerInfo)

	assert.Equal(t, "top-secret-token", customSecret)
	assert.Equal(t, types.NotificationEventPeerOnline, eventHeader)
	assert.Equal(t, types.NotificationEventPeerOnline, received.EventType)
	assert.Equal(t, "acc-1", received.AccountID)
	assert.NotEmpty(t, received.ID)
	assert.Contains(t, rawMap, "id", "top-level JSON payload must use 'id' per specification")
	assert.NotContains(t, rawMap, "event_id")
	assert.Equal(t, "peer-1", received.Peer.ID)
	assert.Equal(t, "100.64.0.15", received.Peer.IP)
	assert.Equal(t, "203.0.113.42", received.Peer.ConnectionIP)
	assert.Equal(t, "office-gw-01.cloink.local", received.Peer.DNSLabel)
	assert.Equal(t, "CN", received.Peer.CountryCode)
	assert.Equal(t, "Shanghai", received.Peer.CityName)
	assert.True(t, received.Peer.Connected)
}

func TestDispatchPeerWebhook_RetriesOn5xx(t *testing.T) {
	enableLoopbackWebhooksForTest(t)

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	targetJSON, err := json.Marshal(map[string]any{
		"url": srv.URL,
	})
	require.NoError(t, err)

	lister := &fakeChannelLister{
		channels: []*types.NotificationChannel{
			{
				ID:         "ch-retry",
				AccountID:  "acc-1",
				Type:       types.NotificationChannelTypeWebhook,
				Enabled:    true,
				EventTypes: []string{types.NotificationEventPeerAdd},
				Target:     string(targetJSON),
			},
		},
	}

	DispatchPeerWebhook(context.Background(), lister, "acc-1", types.NotificationEventPeerAdd, PeerWebhookInfo{
		ID:   "peer-retry",
		Name: "peer-retry",
		IP:   "100.64.0.99",
	})

	assert.Equal(t, int32(2), attempts.Load())
}

func TestValidateWebhookURL_BlocksSSRFTargetsByDefault(t *testing.T) {
	allowPrivateTargetsForTest.Store(false)

	blockedURLs := []string{
		"",
		"file:///etc/passwd",
		"gopher://127.0.0.1:6379/_INFO",
		"http://localhost:8080/hook",
		"https://sub.localhost/hook",
		"http://internal.local/hook",
		"http://127.0.0.1:8080/hook",
		"http://[::1]:8080/hook",
		"http://10.0.0.1/hook",
		"http://172.16.5.4/hook",
		"http://192.168.1.10/hook",
		"http://169.254.169.254/latest/meta-data/",
		"http://100.64.0.1/hook",
		"http://0.0.0.0/hook",
	}
	for _, u := range blockedURLs {
		require.Errorf(t, ValidateWebhookURL(u), "expected webhook URL %q to be rejected", u)
	}

	require.NoError(t, ValidateWebhookURL("https://hooks.example.com/cloink/events"))
	require.NoError(t, ValidateWebhookURL("http://8.8.8.8:8080/webhook"))

	// Loopback server must also be blocked at dispatch/dial time when private targets are not allowed.
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	targetJSON, err := json.Marshal(map[string]any{"url": srv.URL})
	require.NoError(t, err)
	lister := &fakeChannelLister{
		channels: []*types.NotificationChannel{
			{
				ID:         "ch-ssrf",
				AccountID:  "acc-1",
				Type:       types.NotificationChannelTypeWebhook,
				Enabled:    true,
				EventTypes: []string{types.NotificationEventPeerAdd},
				Target:     string(targetJSON),
			},
		},
	}
	DispatchPeerWebhook(context.Background(), lister, "acc-1", types.NotificationEventPeerAdd, PeerWebhookInfo{ID: "p1"})
	require.False(t, hit.Load(), "loopback webhook target must not be called when private targets are disallowed")
}

func TestDispatchPeerWebhook_DoesNotFollowRedirects(t *testing.T) {
	enableLoopbackWebhooksForTest(t)

	var redirectedHit atomic.Bool
	internalSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer internalSrv.Close()

	redirectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, internalSrv.URL, http.StatusFound)
	}))
	defer redirectSrv.Close()

	targetJSON, err := json.Marshal(map[string]any{"url": redirectSrv.URL})
	require.NoError(t, err)
	lister := &fakeChannelLister{
		channels: []*types.NotificationChannel{
			{
				ID:         "ch-redirect",
				AccountID:  "acc-1",
				Type:       types.NotificationChannelTypeWebhook,
				Enabled:    true,
				EventTypes: []string{types.NotificationEventPeerAdd},
				Target:     string(targetJSON),
			},
		},
	}

	DispatchPeerWebhook(context.Background(), lister, "acc-1", types.NotificationEventPeerAdd, PeerWebhookInfo{ID: "p1"})
	require.False(t, redirectedHit.Load(), "webhook HTTP client must not follow 3xx redirects")
}

func TestDispatchPeerWebhook_BoundsConcurrency(t *testing.T) {
	enableLoopbackWebhooksForTest(t)

	var inFlight atomic.Int32
	var maxObserved atomic.Int32
	var totalDelivered atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			prev := maxObserved.Load()
			if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
		inFlight.Add(-1)
		totalDelivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	targetJSON, err := json.Marshal(map[string]any{"url": srv.URL})
	require.NoError(t, err)

	const totalChannels = 32
	channels := make([]*types.NotificationChannel, 0, totalChannels)
	for i := 0; i < totalChannels; i++ {
		channels = append(channels, &types.NotificationChannel{
			ID:         "ch-conc",
			AccountID:  "acc-1",
			Type:       types.NotificationChannelTypeWebhook,
			Enabled:    true,
			EventTypes: []string{types.NotificationEventPeerOnline},
			Target:     string(targetJSON),
		})
	}

	var done sync.WaitGroup
	done.Add(1)
	ok := EnqueuePeerWebhook(func() {
		defer done.Done()
		DispatchPeerWebhookToChannels(context.Background(), channels, "acc-1", types.NotificationEventPeerOnline, PeerWebhookInfo{ID: "peer-conc"})
	})
	require.True(t, ok)
	done.Wait()

	require.Equal(t, int32(totalChannels), totalDelivered.Load())
	require.LessOrEqual(t, maxObserved.Load(), int32(maxWebhookWorkers), "concurrent webhook HTTP requests must not exceed maxWebhookWorkers")
}
