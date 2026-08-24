package events

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/mock_server"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/shared/auth"
	"github.com/netbirdio/netbird/shared/management/http/api"
)

func TestNetworkTrafficHandlerModes(t *testing.T) {
	const accountID, userID = "account-a", "user-a"
	window := time.Date(2026, 8, 22, 6, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		query string
		set   func(*store.MockStore)
		check func(*testing.T, *httptest.ResponseRecorder)
	}{
		{
			name: "raw remains default",
			set: func(db *store.MockStore) {
				db.EXPECT().GetAccountNetworkTrafficEvents(gomock.Any(), store.LockingStrengthNone, accountID, gomock.Any()).
					Return([]*networktraffic.Event{{ID: "event-a", FlowID: "flow-a", Timestamp: window, WindowStart: window, WindowEnd: window}}, int64(1), nil)
			},
			check: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				var response api.NetworkTrafficEventsResponse
				require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
				require.Len(t, response.Data, 1)
				require.Equal(t, "event-a", response.Data[0].Id)
			},
		},
		{
			name:  "grouped parent",
			query: "grouped=true&start_date=2026-08-22T05%3A55%3A00Z&end_date=2026-08-22T06%3A00%3A00Z",
			set: func(db *store.MockStore) {
				db.EXPECT().GetAccountNetworkTrafficGroups(gomock.Any(), store.LockingStrengthNone, accountID, gomock.Any()).
					Return([]*networktraffic.Group{{WindowStart: window, UserID: "owner", ReporterID: "peer-a", DetailCount: 2}}, int64(1), nil)
			},
			check: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				var response api.NetworkTrafficGroupsResponse
				require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
				require.Len(t, response.Data, 1)
				require.Equal(t, "owner", response.Data[0].User.Id)
				require.Equal(t, int64(2), response.Data[0].DetailCount)
				require.NotEmpty(t, response.Data[0].Key)
			},
		},
		{
			name:  "unknown user detail",
			query: "window_start=2026-08-22T14%3A00%3A00%2B08%3A00&group_user_id=&reporter_id=peer-a&start_date=2026-08-22T05%3A55%3A00Z&end_date=2026-08-22T06%3A00%3A00Z",
			set: func(db *store.MockStore) {
				db.EXPECT().GetAccountNetworkTrafficGroupEvents(gomock.Any(), store.LockingStrengthNone, accountID, gomock.Any(), window, "", "peer-a").
					Return([]*networktraffic.Event{{ID: "detail-a", FlowID: "flow-a", Timestamp: window, WindowStart: window, WindowEnd: window}}, int64(1), nil)
			},
			check: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				var response api.NetworkTrafficEventsResponse
				require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
				require.Equal(t, "detail-a", response.Data[0].Id)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			db := store.NewMockStore(ctrl)
			permissionManager := permissions.NewMockManager(ctrl)
			permissionManager.EXPECT().ValidateUserPermissions(gomock.Any(), accountID, userID, modules.NetworkTraffic, operations.Read).
				Return(true, context.Background(), nil)
			test.set(db)
			h := &handler{
				accountManager:     &mock_server.MockAccountManager{GetStoreFunc: func() store.Store { return db }},
				permissionsManager: permissionManager,
			}
			recorder := httptest.NewRecorder()
			h.getAllNetworkTrafficEvents(recorder, networkTrafficRequest(test.query, accountID, userID))
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			test.check(t, recorder)
		})
	}
}

func TestNetworkTrafficHandlerRejectsInvalidQueries(t *testing.T) {
	tests := []string{
		"grouped=maybe",
		"grouped=true&grouped=false",
		"grouped=true&window_start=2026-08-22T06%3A00%3A00Z&group_user_id=user",
		"window_start=invalid&group_user_id=user&reporter_id=peer",
		"window_start=2026-08-22T06%3A00%3A00Z&reporter_id=peer",
		"window_start=2026-08-22T06%3A00%3A00Z&group_user_id=user",
		"window_start=2026-08-22T06%3A00%3A00Z&group_user_id=user&reporter_id=",
		"window_start=2026-08-22T06%3A00%3A00Z&group_user_id=user&reporter_id=" + strings.Repeat("a", maxNetworkTrafficDetailIDLength+1),
		"page=invalid",
		"page_size=50001",
		"start_date=invalid",
	}
	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			permissionManager := permissions.NewMockManager(ctrl)
			permissionManager.EXPECT().ValidateUserPermissions(gomock.Any(), "account-a", "user-a", modules.NetworkTraffic, operations.Read).
				Return(true, context.Background(), nil)
			h := &handler{permissionsManager: permissionManager}
			recorder := httptest.NewRecorder()
			h.getAllNetworkTrafficEvents(recorder, networkTrafficRequest(query, "account-a", "user-a"))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestNetworkTrafficHandlerPermissionFailsClosed(t *testing.T) {
	t.Run("missing manager", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		(&handler{}).getAllNetworkTrafficEvents(recorder, networkTrafficRequest("", "account-a", "user-a"))
		require.Equal(t, http.StatusForbidden, recorder.Code)
	})
	t.Run("denied", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		permissionManager := permissions.NewMockManager(ctrl)
		permissionManager.EXPECT().ValidateUserPermissions(gomock.Any(), "account-a", "user-a", modules.NetworkTraffic, operations.Read).
			Return(false, context.Background(), nil)
		recorder := httptest.NewRecorder()
		(&handler{permissionsManager: permissionManager}).getAllNetworkTrafficEvents(recorder, networkTrafficRequest("", "account-a", "user-a"))
		require.Equal(t, http.StatusForbidden, recorder.Code)
	})
}

func networkTrafficRequest(query, accountID, userID string) *http.Request {
	path := "/api/events/network-traffic"
	if query != "" {
		path += "?" + query
	}
	return nbcontext.SetUserAuthInRequest(httptest.NewRequest(http.MethodGet, path, nil), auth.UserAuth{AccountId: accountID, UserId: userID})
}
