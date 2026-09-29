package eventstreaming

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/activity"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	defaultForwardInterval = 30 * time.Second
	defaultForwardBatch    = 500
	maxForwardAttempts     = 5
)

// ForwarderStore is the persistence surface the forwarder needs.
type ForwarderStore interface {
	GetAllAccounts(ctx context.Context) []*types.Account
	ListEventStreamingIntegrations(ctx context.Context, lockStrength store.LockingStrength, accountID string) ([]*types.EventStreamingIntegration, error)
	GetEventStreamingCursor(ctx context.Context, accountID string, integrationID uint64) (*types.EventStreamingCursor, error)
	SaveEventStreamingCursor(ctx context.Context, cursor *types.EventStreamingCursor) error
	GetAccountNetworkTrafficEvents(ctx context.Context, lockStrength store.LockingStrength, accountID string, filter networktraffic.Filter) ([]*networktraffic.Event, int64, error)
}

// Forwarder polls activity and flow events per account and pushes new events
// to each enabled streaming integration. Delivery is at-least-once: the
// watermark advances only after a batch is accepted by the destination.
type Forwarder struct {
	store         ForwarderStore
	activityStore activity.Store
	senders       map[string]Sender
	interval      time.Duration
	batchSize     int
	retryBackoff  time.Duration
}

// NewForwarder creates a forwarder. senders maps platform -> Sender.
func NewForwarder(store ForwarderStore, activityStore activity.Store, senders []Sender) *Forwarder {
	m := make(map[string]Sender, len(senders))
	for _, s := range senders {
		m[s.Platform()] = s
	}
	return &Forwarder{
		store:         store,
		activityStore: activityStore,
		senders:       m,
		interval:      defaultForwardInterval,
		batchSize:     defaultForwardBatch,
		retryBackoff:  time.Second,
	}
}

// Run starts the polling loop; it returns when ctx is cancelled.
func (f *Forwarder) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.forwardOnce(ctx)
		}
	}
}

func (f *Forwarder) forwardOnce(ctx context.Context) {
	for _, account := range f.store.GetAllAccounts(ctx) {
		integrations, err := f.store.ListEventStreamingIntegrations(ctx, store.LockingStrengthNone, account.Id)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("event streaming: failed to list integrations for account %s", account.Id)
			continue
		}
		for _, integration := range integrations {
			if !integration.Enabled {
				continue
			}
			sender, ok := f.senders[integration.Platform]
			if !ok {
				log.WithContext(ctx).Warnf("event streaming: no sender for platform %s", integration.Platform)
				continue
			}
			f.forwardIntegration(ctx, account.Id, integration, sender)
		}
	}
}

func (f *Forwarder) forwardIntegration(ctx context.Context, accountID string, integration *types.EventStreamingIntegration, sender Sender) {
	cursor, err := f.store.GetEventStreamingCursor(ctx, accountID, integration.ID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("event streaming: failed to get cursor for integration %d", integration.ID)
		return
	}

	// One poll cycle drains both sources; each source advances its own cursor
	// only after its batch is accepted.
	if err := f.forwardActivity(ctx, accountID, integration, sender, cursor); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("event streaming: activity forwarding failed for integration %d", integration.ID)
		return
	}
	if err := f.forwardFlow(ctx, accountID, integration, sender, cursor); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("event streaming: flow forwarding failed for integration %d", integration.ID)
		return
	}
	if err := f.store.SaveEventStreamingCursor(ctx, cursor); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("event streaming: failed to save cursor for integration %d", integration.ID)
	}
}

func (f *Forwarder) forwardActivity(ctx context.Context, accountID string, integration *types.EventStreamingIntegration, sender Sender, cursor *types.EventStreamingCursor) error {
	for {
		events, err := f.activityStore.GetAfterID(ctx, accountID, cursor.ActivityID, f.batchSize)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		batch := make([]StreamEvent, 0, len(events))
		maxID := cursor.ActivityID
		for _, e := range events {
			batch = append(batch, streamEventFromActivity(e))
			if e.ID > maxID {
				maxID = e.ID
			}
		}
		if err := f.pushWithRetry(ctx, sender, integration.Config, batch); err != nil {
			return err
		}
		cursor.ActivityID = maxID
		if len(events) < f.batchSize {
			return nil
		}
	}
}

func (f *Forwarder) forwardFlow(ctx context.Context, accountID string, integration *types.EventStreamingIntegration, sender Sender, cursor *types.EventStreamingCursor) error {
	for {
		filter := networktraffic.Filter{
			Page:     1,
			PageSize: f.batchSize,
			SortBy:   networktraffic.DefaultSortBy,
			SortOrd:  "asc",
		}
		if !cursor.FlowTimestamp.IsZero() {
			// StartDate is inclusive; the (timestamp, id) tuple comparison below
			// skips the row the cursor already points at.
			ts := cursor.FlowTimestamp
			filter.StartDate = &ts
		}
		events, _, err := f.store.GetAccountNetworkTrafficEvents(ctx, store.LockingStrengthNone, accountID, filter)
		if err != nil {
			return err
		}
		batch := make([]StreamEvent, 0, len(events))
		maxTS := cursor.FlowTimestamp
		maxID := cursor.FlowID
		advanced := false
		for _, e := range events {
			if !e.Timestamp.After(cursor.FlowTimestamp) &&
				!(e.Timestamp.Equal(cursor.FlowTimestamp) && e.ID > cursor.FlowID) &&
				!cursor.FlowTimestamp.IsZero() {
				continue
			}
			batch = append(batch, streamEventFromFlow(e))
			if e.Timestamp.After(maxTS) || (e.Timestamp.Equal(maxTS) && e.ID > maxID) {
				maxTS = e.Timestamp
				maxID = e.ID
				advanced = true
			}
		}
		if len(batch) > 0 {
			if err := f.pushWithRetry(ctx, sender, integration.Config, batch); err != nil {
				return err
			}
		}
		if advanced {
			cursor.FlowTimestamp = maxTS
			cursor.FlowID = maxID
		}
		if len(events) < f.batchSize {
			return nil
		}
	}
}

// pushWithRetry delivers a batch with exponential backoff. The cursor is only
// advanced by the caller after this returns nil.
func (f *Forwarder) pushWithRetry(ctx context.Context, sender Sender, config map[string]string, batch []StreamEvent) error {
	backoff := f.retryBackoff
	var err error
	for attempt := 0; attempt < maxForwardAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		if err = sender.Push(ctx, config, batch); err == nil {
			return nil
		}
		log.WithContext(ctx).WithError(err).Warnf("event streaming: push attempt %d/%d to %s failed", attempt+1, maxForwardAttempts, sender.Platform())
	}
	return err
}

// drainAll is a test helper that runs a single forward cycle synchronously.
func (f *Forwarder) drainAll(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	f.forwardOnce(ctx)
}
