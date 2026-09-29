package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
)

// The grouped query folds rows by these columns (see the Group(...) clause of
// GetAccountNetworkTrafficGroups). Every one of them has to take part in the
// ORDER BY: the GROUP BY combination is unique, so covering all of its columns
// makes the ordering total. Without them, groups sharing a latest_timestamp
// (e.g. connections observed in the same minute) can come back in an arbitrary
// order and consecutive pages may overlap.
func TestNetworkTrafficGroupOrderCoversGroupBy(t *testing.T) {
	groupByColumns := []string{
		"user_id", "reporter_id", "source_key", "destination_id",
		"destination_address", "protocol", "direction", "connection_type",
	}
	require.True(t, strings.HasPrefix(networkTrafficGroupOrderBy, "latest_timestamp DESC"),
		"ORDER BY must start with latest_timestamp")
	for _, column := range groupByColumns {
		require.Containsf(t, networkTrafficGroupOrderBy, column+" DESC",
			"ORDER BY must cover GROUP BY column %q", column)
	}
}

// Paginating groups that share a single latest_timestamp must return every
// group exactly once, independent of the storage engine's tie-breaking.
func TestSqlStoreNetworkTrafficGroupsPaginationIsStable(t *testing.T) {
	ctx := context.Background()
	dbStore, cleanup, err := NewTestStoreFromSQL(ctx, "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	window := time.Now().UTC().Truncate(time.Microsecond)
	shared := window.Add(2 * time.Second)
	const groupCount = 6
	for i := 0; i < groupCount; i++ {
		require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, &networktraffic.Event{
			ID: fmt.Sprintf("tie-%d", i), AccountID: "account-a", FlowID: fmt.Sprintf("flow-tie-%d", i),
			Timestamp: shared, WindowStart: window, WindowEnd: window.Add(30 * time.Second),
			UserID: "user-a", UserName: "user", UserEmail: "user@example.com", ReporterID: "peer-a",
			Protocol: 6, Direction: "EGRESS",
			DestinationAddress: fmt.Sprintf("10.0.0.%d", i),
			RxBytes:            1, RxPackets: 1, NumOfStarts: 1,
		}))
	}

	const pageSize = 2
	seen := map[string]bool{}
	for page := 1; page <= groupCount/pageSize; page++ {
		filter := networktraffic.Filter{Page: page, PageSize: pageSize}
		result, total, err := dbStore.GetAccountNetworkTrafficGroups(ctx, LockingStrengthNone, "account-a", filter)
		require.NoError(t, err)
		require.Equal(t, int64(groupCount), total)
		require.Len(t, result, pageSize)
		for _, group := range result {
			key := group.DestinationAddress
			require.Falsef(t, seen[key], "group %q returned on more than one page", key)
			seen[key] = true
		}
	}
	require.Len(t, seen, groupCount)
}
