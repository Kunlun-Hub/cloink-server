package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
)

func TestSqlStoreNetworkTrafficGroupsRejectInvalidPagination(t *testing.T) {
	ctx := context.Background()
	dbStore, cleanup, err := NewTestStoreFromSQL(ctx, "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	_, _, err = dbStore.GetAccountNetworkTrafficGroups(ctx, LockingStrengthNone, "account", networktraffic.Filter{})
	require.Error(t, err)
	_, _, err = dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account", networktraffic.Filter{}, time.Now(), "", "")
	require.Error(t, err)
}

func TestSqlStoreNetworkTrafficGroupsAreScopedAndDeterministic(t *testing.T) {
	ctx := context.Background()
	dbStore, cleanup, err := NewTestStoreFromSQL(ctx, "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	window := time.Now().UTC().Truncate(time.Microsecond)
	add := func(id, accountID, userID, reporterID string, windowStart, timestamp time.Time, rxBytes int64) {
		require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, &networktraffic.Event{
			ID: id, AccountID: accountID, FlowID: "flow-" + id,
			Timestamp: timestamp, WindowStart: windowStart, WindowEnd: windowStart.Add(30 * time.Second),
			UserID: userID, UserName: "user", UserEmail: "user@example.com", ReporterID: reporterID,
			SourceName: "source", DestinationName: "destination", Protocol: 6, Direction: "EGRESS",
			RxBytes: rxBytes, RxPackets: 1, NumOfStarts: 1,
		}))
	}
	add("a-2", "account-a", "user-a", "peer-a", window, window.Add(2*time.Second), 20)
	add("a-3", "account-a", "user-a", "peer-a", window, window.Add(2*time.Second), 1)
	add("a-1", "account-a", "user-a", "peer-a", window, window.Add(time.Second), 10)
	add("a-unknown", "account-a", "", "peer-a", window, window.Add(time.Second), 5)
	add("a-next", "account-a", "user-b", "peer-b", window.Add(time.Minute), window.Add(time.Minute), 7)
	add("a-other-reporter", "account-a", "user-a", "peer-z", window, window.Add(4*time.Second), 8)
	add("b-1", "account-b", "user-a", "peer-a", window, window.Add(3*time.Second), 999)

	filter := networktraffic.Filter{Page: 1, PageSize: 2}
	groups, total, err := dbStore.GetAccountNetworkTrafficGroups(ctx, LockingStrengthNone, "account-a", filter)
	require.NoError(t, err)
	require.Equal(t, int64(4), total)
	require.Len(t, groups, 2)
	require.Equal(t, "user-b", groups[0].UserID)
	require.Equal(t, int64(7), groups[0].RxBytes)
	require.Equal(t, "peer-z", groups[1].ReporterID)
	require.Equal(t, int64(8), groups[1].RxBytes)

	filter.Page, filter.PageSize = 2, 2
	groups, total, err = dbStore.GetAccountNetworkTrafficGroups(ctx, LockingStrengthNone, "account-a", filter)
	require.NoError(t, err)
	require.Equal(t, int64(4), total)
	require.Len(t, groups, 2)
	require.Equal(t, "user-a", groups[0].UserID)
	require.Equal(t, "peer-a", groups[0].ReporterID)
	require.Equal(t, int64(3), groups[0].DetailCount)
	require.Equal(t, int64(31), groups[0].RxBytes)
	require.Equal(t, "", groups[1].UserID)
	require.Equal(t, int64(5), groups[1].RxBytes)

	filter.Page, filter.PageSize = 1, 2
	details, detailTotal, err := dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account-a", filter, window, "user-a", "peer-a")
	require.NoError(t, err)
	require.Equal(t, int64(3), detailTotal)
	require.Len(t, details, 2)
	require.Equal(t, "a-3", details[0].ID)
	require.Equal(t, "a-2", details[1].ID)
	filter.Page = 2
	details, detailTotal, err = dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account-a", filter, window, "user-a", "peer-a")
	require.NoError(t, err)
	require.Equal(t, int64(3), detailTotal)
	require.Len(t, details, 1)
	require.Equal(t, "a-1", details[0].ID)

	filter.Page, filter.PageSize = 1, 10
	unknown, unknownTotal, err := dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account-a", filter, window, "", "peer-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), unknownTotal)
	require.Len(t, unknown, 1)
	require.Equal(t, "a-unknown", unknown[0].ID)

	search := "destination"
	filter.Search = &search
	filteredDetails, _, err := dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account-a", filter, window, "user-a", "peer-a")
	require.NoError(t, err)
	require.Len(t, filteredDetails, 3)
	search = "does-not-match"
	filteredDetails, _, err = dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account-a", filter, window, "user-a", "peer-a")
	require.NoError(t, err)
	require.Empty(t, filteredDetails)
	filter.Search = nil

	otherAccount, otherTotal, err := dbStore.GetAccountNetworkTrafficGroupEvents(ctx, LockingStrengthNone, "account-b", filter, window, "user-a", "peer-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), otherTotal)
	require.Len(t, otherAccount, 1)
	require.Equal(t, "b-1", otherAccount[0].ID)

	protocol := 17
	filter.Protocol = &protocol
	filteredGroups, filteredTotal, err := dbStore.GetAccountNetworkTrafficGroups(ctx, LockingStrengthNone, "account-a", filter)
	require.NoError(t, err)
	require.Zero(t, filteredTotal)
	require.Empty(t, filteredGroups)
}

func TestSqlStoreNetworkTrafficGroupsFoldByFlowSignature(t *testing.T) {
	ctx := context.Background()
	dbStore, cleanup, err := NewTestStoreFromSQL(ctx, "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	window := time.Now().UTC().Truncate(time.Microsecond)
	add := func(id, sourceID, sourceAddress, destinationAddress, direction string, protocol int) {
		require.NoError(t, dbStore.CreateNetworkTrafficEvent(ctx, &networktraffic.Event{
			ID: id, AccountID: "account", FlowID: "flow-" + id,
			Timestamp: window.Add(time.Duration(len(id)) * time.Millisecond), WindowStart: window, WindowEnd: window.Add(time.Minute),
			UserID: "user", ReporterID: "reporter", SourceID: sourceID, SourceName: "client", SourceAddress: sourceAddress,
			DestinationID: "resource", DestinationName: "target", DestinationAddress: destinationAddress,
			Protocol: protocol, Direction: direction, ConnectionType: networktraffic.ConnectionTypeRouted,
			RxBytes: 10, TxBytes: 20, NumOfStarts: 1,
		}))
	}

	add("same-a", "client-a", "100.64.0.1:51001", "10.0.0.5:443", "EGRESS", 6)
	add("same-b", "client-a", "100.64.0.1:51002", "10.0.0.5:443", "EGRESS", 6)
	add("other-port", "client-a", "100.64.0.1:51003", "10.0.0.5:8443", "EGRESS", 6)
	add("other-direction", "client-a", "100.64.0.1:51004", "10.0.0.5:443", "INGRESS", 6)
	add("other-target", "client-a", "100.64.0.1:51005", "10.0.0.6:443", "EGRESS", 6)
	add("other-protocol", "client-a", "100.64.0.1:51006", "10.0.0.5:443", "EGRESS", 17)
	add("other-client", "client-b", "100.64.0.2:51001", "10.0.0.5:443", "EGRESS", 6)

	start, end := window.Add(-time.Minute), window.Add(time.Minute)
	filter := networktraffic.Filter{Page: 1, PageSize: 20, StartDate: &start, EndDate: &end}
	groups, total, err := dbStore.GetAccountNetworkTrafficGroups(ctx, LockingStrengthNone, "account", filter)
	require.NoError(t, err)
	require.Equal(t, int64(6), total)
	require.Len(t, groups, 6)

	var folded *networktraffic.Group
	for _, group := range groups {
		if group.SourceKey == "client-a" && group.DestinationAddress == "10.0.0.5:443" && group.Direction == "EGRESS" && group.Protocol == 6 {
			folded = group
			break
		}
	}
	require.NotNil(t, folded)
	require.Equal(t, int64(2), folded.DetailCount)
	require.Equal(t, int64(2), folded.FlowCount)
	require.Equal(t, int64(20), folded.RxBytes)
	require.Equal(t, int64(40), folded.TxBytes)

	sourceKey := folded.SourceKey
	destinationAddress := folded.DestinationAddress
	protocol := folded.Protocol
	direction := folded.Direction
	connectionType := folded.ConnectionType
	filter.SourceKey = &sourceKey
	filter.DestinationAddress = &destinationAddress
	filter.Protocol = &protocol
	filter.Direction = &direction
	filter.ConnectionType = &connectionType
	details, detailTotal, err := dbStore.GetAccountNetworkTrafficGroupEvents(
		ctx, LockingStrengthNone, "account", filter, window, "user", "reporter",
	)
	require.NoError(t, err)
	require.Equal(t, int64(2), detailTotal)
	require.Len(t, details, 2)
}
