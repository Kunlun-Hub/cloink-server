package activity

import (
	"context"
	"sync"
)

// Store provides an interface to store or stream events.
type Store interface {
	// Save an event in the store
	Save(ctx context.Context, event *Event) (*Event, error)
	// Get returns "limit" number of events from the "offset" index ordered descending or ascending by a timestamp
	Get(ctx context.Context, accountID string, offset, limit int, descending bool) ([]*Event, error)
	// GetAfterID returns up to "limit" events with ID greater than afterID, ordered by ID ascending.
	// Used by the event-streaming forwarder for cursor-based pagination.
	GetAfterID(ctx context.Context, accountID string, afterID uint64, limit int) ([]*Event, error)
	// Close the sink flushing events if necessary
	Close(ctx context.Context) error
}

// InMemoryEventStore implements the Store interface storing data in-memory
type InMemoryEventStore struct {
	mu     sync.Mutex
	nextID uint64
	events []*Event
}

// Save sets the Event.ID to 1
func (store *InMemoryEventStore) Save(_ context.Context, event *Event) (*Event, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.events == nil {
		store.events = make([]*Event, 0)
	}
	event.ID = store.nextID
	store.nextID++
	store.events = append(store.events, event)
	return event, nil
}

// Get returns a list of ALL events that belong to the given accountID without taking offset, limit and order into consideration
func (store *InMemoryEventStore) Get(_ context.Context, accountID string, offset, limit int, descending bool) ([]*Event, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	events := make([]*Event, 0)
	for _, event := range store.events {
		if event.AccountID == accountID {
			events = append(events, event)
		}
	}
	return events, nil
}

// GetAfterID returns up to limit events with ID greater than afterID, ordered by ID ascending.
func (store *InMemoryEventStore) GetAfterID(_ context.Context, accountID string, afterID uint64, limit int) ([]*Event, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	events := make([]*Event, 0, limit)
	for _, event := range store.events {
		if event.AccountID == accountID && event.ID > afterID {
			events = append(events, event)
			if len(events) >= limit {
				break
			}
		}
	}
	return events, nil
}

// Close cleans up the event list
func (store *InMemoryEventStore) Close(_ context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.events = make([]*Event, 0)
	return nil
}
