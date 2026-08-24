package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/internal/netflow/types"
)

func TestBoundedMemoryRejectsNewAndReleasesCapacity(t *testing.T) {
	first := &types.Event{ID: uuid.New()}
	second := &types.Event{ID: uuid.New()}
	store := NewBoundedMemoryStore(1, eventSize(first))

	require.True(t, store.TryStoreEvent(first))
	assert.True(t, store.TryStoreEvent(first), "retrying the same event is idempotent")
	assert.False(t, store.TryStoreEvent(second))
	assert.Equal(t, uint64(1), store.IncompleteCount(IncompleteRetryQueueFull))
	assert.Equal(t, first.ID, store.GetEvents()[0].ID)

	store.DeleteEvents([]uuid.UUID{first.ID})
	require.True(t, store.TryStoreEvent(second))
	assert.Equal(t, second.ID, store.GetEvents()[0].ID)
}

func TestBoundedMemoryEnforcesByteLimit(t *testing.T) {
	base := &types.Event{ID: uuid.New()}
	large := &types.Event{ID: uuid.New(), EventFields: types.EventFields{RuleID: []byte("x")}}
	store := NewBoundedMemoryStore(2, eventSize(base))

	require.True(t, store.TryStoreEvent(base))
	assert.False(t, store.TryStoreEvent(large))
}

func TestBoundedMemoryReturnsOldestFirst(t *testing.T) {
	now := time.Now()
	newer := &types.Event{ID: uuid.New(), Timestamp: now.Add(time.Second)}
	older := &types.Event{ID: uuid.New(), Timestamp: now}
	store := NewBoundedMemoryStore(2, 2*eventSize(older))

	require.True(t, store.TryStoreEvent(newer))
	require.True(t, store.TryStoreEvent(older))
	events := store.GetEvents()
	assert.Equal(t, []uuid.UUID{older.ID, newer.ID}, []uuid.UUID{
		events[0].ID,
		events[1].ID,
	})
}
