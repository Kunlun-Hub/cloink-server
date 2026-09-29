package eventstreaming

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/activity"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

type fakeForwarderStore struct {
	mu           sync.Mutex
	accounts     []*types.Account
	integrations map[string][]*types.EventStreamingIntegration
	cursors      map[uint64]*types.EventStreamingCursor
	flowEvents   map[string][]*networktraffic.Event
}

func (f *fakeForwarderStore) GetAllAccounts(_ context.Context) []*types.Account {
	return f.accounts
}

func (f *fakeForwarderStore) ListEventStreamingIntegrations(_ context.Context, _ store.LockingStrength, accountID string) ([]*types.EventStreamingIntegration, error) {
	return f.integrations[accountID], nil
}

func (f *fakeForwarderStore) GetEventStreamingCursor(_ context.Context, accountID string, integrationID uint64) (*types.EventStreamingCursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.cursors[integrationID]; ok {
		cp := *c
		return &cp, nil
	}
	return &types.EventStreamingCursor{AccountID: accountID, IntegrationID: integrationID}, nil
}

func (f *fakeForwarderStore) SaveEventStreamingCursor(_ context.Context, cursor *types.EventStreamingCursor) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *cursor
	f.cursors[cursor.IntegrationID] = &cp
	return nil
}

func (f *fakeForwarderStore) GetAccountNetworkTrafficEvents(_ context.Context, _ store.LockingStrength, accountID string, filter networktraffic.Filter) ([]*networktraffic.Event, int64, error) {
	events := f.flowEvents[accountID]
	// apply StartDate filter like the real store
	var out []*networktraffic.Event
	for _, e := range events {
		if filter.StartDate != nil && e.Timestamp.Before(*filter.StartDate) {
			continue
		}
		out = append(out, e)
	}
	if len(out) > filter.PageSize && filter.PageSize > 0 {
		out = out[:filter.PageSize]
	}
	return out, int64(len(out)), nil
}

type fakeActivityStore struct {
	events []*activity.Event
}

func (f *fakeActivityStore) Save(_ context.Context, event *activity.Event) (*activity.Event, error) {
	f.events = append(f.events, event)
	return event, nil
}

func (f *fakeActivityStore) GetAfterID(_ context.Context, accountID string, afterID uint64, limit int) ([]*activity.Event, error) {
	var out []*activity.Event
	for _, e := range f.events {
		if e.AccountID == accountID && e.ID > afterID {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeActivityStore) Get(_ context.Context, _ string, _, _ int, _ bool) ([]*activity.Event, error) {
	return nil, nil
}

func (f *fakeActivityStore) Close(_ context.Context) error { return nil }

type recordingSender struct {
	platform string
	mu       sync.Mutex
	batches  [][]StreamEvent
	failNext int
}

func (s *recordingSender) Platform() string { return s.platform }

func (s *recordingSender) Push(_ context.Context, _ map[string]string, events []StreamEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return errors.New("boom")
	}
	s.batches = append(s.batches, events)
	return nil
}

func (s *recordingSender) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func (s *recordingSender) eventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

func TestForwarderAdvancesActivityCursor(t *testing.T) {
	ctx := context.Background()
	accountID := "fw-account"

	activityStore := &fakeActivityStore{}
	for i := uint64(1); i <= 3; i++ {
		activityStore.events = append(activityStore.events, &activity.Event{
			ID: i, AccountID: accountID, Timestamp: time.Now(), Activity: activity.PeerAddedByUser,
		})
	}

	sender := &recordingSender{platform: types.EventStreamingPlatformGenericHTTP}
	fs := &fakeForwarderStore{
		accounts: []*types.Account{{Id: accountID}},
		integrations: map[string][]*types.EventStreamingIntegration{
			accountID: {{
				ID: 1, AccountID: accountID, Platform: types.EventStreamingPlatformGenericHTTP,
				Enabled: true, Config: map[string]string{"url": "http://localhost:1"},
			}},
		},
		cursors:    map[uint64]*types.EventStreamingCursor{},
		flowEvents: map[string][]*networktraffic.Event{},
	}

	fwd := NewForwarder(fs, activityStore, []Sender{sender})
	fwd.interval = time.Millisecond // not used by forwardOnce

	fwd.forwardOnce(ctx)
	assert.Equal(t, 3, sender.eventCount())

	// second cycle: no new events, nothing pushed
	fwd.forwardOnce(ctx)
	assert.Equal(t, 3, sender.eventCount())

	// cursor persisted
	cursor, err := fs.GetEventStreamingCursor(ctx, accountID, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), cursor.ActivityID)

	// new event arrives -> only it is pushed
	activityStore.events = append(activityStore.events, &activity.Event{
		ID: 4, AccountID: accountID, Timestamp: time.Now(), Activity: activity.PeerAddedByUser,
	})
	fwd.forwardOnce(ctx)
	assert.Equal(t, 4, sender.eventCount())
}

func TestForwarderSkipsDisabledAndUnknownPlatform(t *testing.T) {
	ctx := context.Background()
	accountID := "fw-account-2"

	activityStore := &fakeActivityStore{
		events: []*activity.Event{{ID: 1, AccountID: accountID, Timestamp: time.Now()}},
	}
	sender := &recordingSender{platform: types.EventStreamingPlatformGenericHTTP}
	fs := &fakeForwarderStore{
		accounts: []*types.Account{{Id: accountID}},
		integrations: map[string][]*types.EventStreamingIntegration{
			accountID: {
				{ID: 1, AccountID: accountID, Platform: types.EventStreamingPlatformGenericHTTP, Enabled: false, Config: map[string]string{}},
				{ID: 2, AccountID: accountID, Platform: "splunk", Enabled: true, Config: map[string]string{}},
			},
		},
		cursors:    map[uint64]*types.EventStreamingCursor{},
		flowEvents: map[string][]*networktraffic.Event{},
	}

	fwd := NewForwarder(fs, activityStore, []Sender{sender})
	fwd.forwardOnce(ctx)
	assert.Equal(t, 0, sender.eventCount())
}

func TestForwarderFlowCursorTuple(t *testing.T) {
	ctx := context.Background()
	accountID := "fw-account-3"
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

	flowEvents := []*networktraffic.Event{
		{ID: "b", AccountID: accountID, Timestamp: base, EventType: "start"},
		{ID: "a", AccountID: accountID, Timestamp: base, EventType: "start"},
		{ID: "c", AccountID: accountID, Timestamp: base.Add(time.Minute), EventType: "end"},
	}
	sender := &recordingSender{platform: types.EventStreamingPlatformGenericHTTP}
	fs := &fakeForwarderStore{
		accounts: []*types.Account{{Id: accountID}},
		integrations: map[string][]*types.EventStreamingIntegration{
			accountID: {{
				ID: 1, AccountID: accountID, Platform: types.EventStreamingPlatformGenericHTTP,
				Enabled: true, Config: map[string]string{},
			}},
		},
		cursors:    map[uint64]*types.EventStreamingCursor{},
		flowEvents: map[string][]*networktraffic.Event{accountID: flowEvents},
	}

	fwd := NewForwarder(fs, &fakeActivityStore{}, []Sender{sender})
	fwd.forwardOnce(ctx)
	// all three pushed exactly once despite identical timestamps
	assert.Equal(t, 3, sender.eventCount())

	cursor, err := fs.GetEventStreamingCursor(ctx, accountID, 1)
	require.NoError(t, err)
	assert.Equal(t, base.Add(time.Minute), cursor.FlowTimestamp)
	assert.Equal(t, "c", cursor.FlowID)

	// second cycle pushes nothing (no duplicates)
	fwd.forwardOnce(ctx)
	assert.Equal(t, 3, sender.eventCount())
}

func TestForwarderRetryThenCursorAdvance(t *testing.T) {
	ctx := context.Background()
	accountID := "fw-account-4"

	activityStore := &fakeActivityStore{
		events: []*activity.Event{{ID: 1, AccountID: accountID, Timestamp: time.Now()}},
	}
	sender := &recordingSender{platform: types.EventStreamingPlatformGenericHTTP, failNext: 100}
	fs := &fakeForwarderStore{
		accounts: []*types.Account{{Id: accountID}},
		integrations: map[string][]*types.EventStreamingIntegration{
			accountID: {{
				ID: 1, AccountID: accountID, Platform: types.EventStreamingPlatformGenericHTTP,
				Enabled: true, Config: map[string]string{},
			}},
		},
		cursors:    map[uint64]*types.EventStreamingCursor{},
		flowEvents: map[string][]*networktraffic.Event{},
	}

	fwd := NewForwarder(fs, activityStore, []Sender{sender})
	fwd.retryBackoff = time.Millisecond
	fwd.forwardOnce(ctx)
	// push failed after retries: nothing delivered, cursor NOT advanced
	assert.Equal(t, 0, sender.eventCount())
	cursor, err := fs.GetEventStreamingCursor(ctx, accountID, 1)
	require.NoError(t, err)
	assert.Zero(t, cursor.ActivityID)
}
