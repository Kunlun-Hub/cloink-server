package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/types"
)

func TestSqlStoreNetworkTrafficEventsIdempotenceFiltersAndCleanup(t *testing.T) {
	ctx := context.Background()
	dbStore, cleanup, err := NewTestStoreFromSQL(ctx, "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, dbStore.CreatePolicy(ctx, &types.Policy{
		ID: "policy-1", AccountID: "account-1", Name: "Allow Web",
		Rules: []*types.PolicyRule{{ID: "rule-1", PolicyID: "policy-1", Name: "HTTPS"}},
	}))
	policy, err := dbStore.GetNetworkTrafficPolicy(ctx, LockingStrengthNone, "account-1", "rule-1")
	require.NoError(t, err)
	require.Equal(t, "policy-1", policy.ID)
	require.Equal(t, "Allow Web", policy.Name)

	now := time.Now().UTC().Truncate(time.Microsecond)
	base := func(id string, timestamp time.Time) *networktraffic.Event {
		return &networktraffic.Event{
			ID: "event-" + id, AccountID: "account-1", FlowID: "flow-" + id,
			Timestamp: timestamp, WindowStart: timestamp, WindowEnd: timestamp,
			EventType: networktraffic.EndpointTypeUnknown, Direction: "EGRESS", Protocol: 6,
			UserEmail: "Alice@Example.com", SourceName: "Laptop", DestinationName: "Server",
		}
	}

	require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, base("old", now.Add(-2*time.Hour))))
	require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, base("new-1", now.Add(-10*time.Minute))))
	require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, base("new-2", now.Add(-5*time.Minute))))
	require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, base("new-3", now)))
	// Retries of the same client event must not create another row.
	require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, base("new-3", now)))

	start := now.Add(-30 * time.Minute)
	end := now.Add(time.Minute)
	filter := networktraffic.Filter{
		Page: 1, PageSize: 10, SortBy: "timestamp", SortOrd: "desc",
		Search:    func() *string { value := "alice@example"; return &value }(),
		StartDate: &start, EndDate: &end,
	}
	events, total, err := dbStore.GetAccountNetworkTrafficEvents(ctx, LockingStrengthNone, "account-1", filter)
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Len(t, events, 3)

	deleted, err := dbStore.CleanupNetworkTrafficEvents(ctx, now.Add(-time.Hour), 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)

	filter.Search = nil
	filter.StartDate = nil
	filter.EndDate = nil
	events, total, err = dbStore.GetAccountNetworkTrafficEvents(ctx, LockingStrengthNone, "account-1", filter)
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	require.Len(t, events, 2)
	require.Equal(t, "event-new-3", events[0].ID)
}
