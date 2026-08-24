package store

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/internal/netflow/types"
)

var testOutboxKey = []byte("test-peer-public-key")

func testOutboxDir(stateDir string) string {
	return filepath.Join(stateDir, outboxDirName, fmt.Sprintf("%x", testOutboxKey))
}

func TestOutboxRecoversAndDeletesAcknowledgedEvent(t *testing.T) {
	stateDir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	event := &types.Event{
		ID: uuid.New(), Timestamp: now, WindowStart: now.Add(-30 * time.Second), WindowEnd: now,
		EventFields: types.EventFields{
			FlowID: uuid.New(), Protocol: types.TCP, SourceIP: netip.MustParseAddr("100.64.0.1"), DestIP: netip.MustParseAddr("100.64.0.2"),
			SourcePort: 1234, DestPort: 443, RuleID: []byte("rule"), RxBytes: 10,
		},
	}

	outbox, err := NewOutboxStore(stateDir, testOutboxKey, 2, 4096)
	require.NoError(t, err)
	require.True(t, outbox.TryStoreEvent(event))
	info, err := os.Stat(filepath.Join(testOutboxDir(stateDir), event.ID.String()+outboxFileExt))
	require.NoError(t, err)
	assert.Zero(t, info.Mode().Perm()&0o077)

	otherPeer, err := NewOutboxStore(stateDir, []byte("other-peer-public-key"), 2, 4096)
	require.NoError(t, err)
	assert.Empty(t, otherPeer.GetEvents())

	recovered, err := NewOutboxStore(stateDir, testOutboxKey, 2, 4096)
	require.NoError(t, err)
	require.Len(t, recovered.GetEvents(), 1)
	assert.Equal(t, event, recovered.GetEvents()[0])

	recovered.DeleteEvents([]uuid.UUID{event.ID})
	empty, err := NewOutboxStore(stateDir, testOutboxKey, 2, 4096)
	require.NoError(t, err)
	assert.Empty(t, empty.GetEvents())
}

func TestOutboxQuarantinesCorruptEvent(t *testing.T) {
	stateDir := t.TempDir()
	dir := testOutboxDir(stateDir)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, uuid.New().String()+outboxFileExt)
	require.NoError(t, os.WriteFile(path, []byte("truncated"), 0o600))

	outbox, err := NewOutboxStore(stateDir, testOutboxKey, 2, 4096)
	require.NoError(t, err)
	assert.Empty(t, outbox.GetEvents())
	assert.Equal(t, uint64(1), outbox.IncompleteCount(IncompleteOutboxCorrupt))
	_, err = os.Stat(path + ".corrupt")
	assert.NoError(t, err)
}

func TestOutboxRejectsOverCapacityWithoutPublishing(t *testing.T) {
	stateDir := t.TempDir()
	outbox, err := NewOutboxStore(stateDir, testOutboxKey, 1, 4096)
	require.NoError(t, err)
	now := time.Now().UTC()
	newEvent := func() *types.Event {
		return &types.Event{ID: uuid.New(), Timestamp: now, WindowStart: now, WindowEnd: now,
			EventFields: types.EventFields{FlowID: uuid.New(), Protocol: types.TCP,
				SourceIP: netip.MustParseAddr("100.64.0.1"), DestIP: netip.MustParseAddr("100.64.0.2")}}
	}
	first, second := newEvent(), newEvent()
	require.True(t, outbox.TryStoreEvent(first))
	assert.False(t, outbox.TryStoreEvent(second))
	_, err = os.Stat(filepath.Join(testOutboxDir(stateDir), second.ID.String()+outboxFileExt))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestOutboxQuarantineDoesNotConsumeLiveCapacity(t *testing.T) {
	stateDir := t.TempDir()
	dir := testOutboxDir(stateDir)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	for i := 0; i < maxOutboxQuarantine+1; i++ {
		name := fmt.Sprintf("%02d%s.corrupt", i, outboxFileExt)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("evidence"), 0o600))
	}

	outbox, err := NewOutboxStore(stateDir, testOutboxKey, 1, 4096)
	require.NoError(t, err)
	now := time.Now().UTC()
	event := &types.Event{
		ID: uuid.New(), Timestamp: now, WindowStart: now, WindowEnd: now,
		EventFields: types.EventFields{FlowID: uuid.New(), Protocol: types.TCP,
			SourceIP: netip.MustParseAddr("100.64.0.1"), DestIP: netip.MustParseAddr("100.64.0.2")},
	}
	assert.True(t, outbox.TryStoreEvent(event))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, maxOutboxQuarantine+1)
}
