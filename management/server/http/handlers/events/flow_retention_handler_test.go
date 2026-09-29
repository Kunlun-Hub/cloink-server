package events

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/mock_server"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/shared/auth"
)

// The flow retention setting is a settings surface, so it must be gated on the
// Settings module. The NetworkTraffic module is read-only for every built-in
// role, which would make the setting impossible to change.
func TestFlowRetentionHandlerUsesSettingsPermission(t *testing.T) {
	const accountID, userID = "account-a", "user-a"

	t.Run("GET requires settings read", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		db := store.NewMockStore(ctrl)
		permissionManager := permissions.NewMockManager(ctrl)
		permissionManager.EXPECT().ValidateUserPermissions(gomock.Any(), accountID, userID, modules.Settings, operations.Read).
			Return(true, context.Background(), nil)
		db.EXPECT().GetFlowRetention(gomock.Any()).Return(7*24*time.Hour, nil)
		h := &handler{
			accountManager:     &mock_server.MockAccountManager{GetStoreFunc: func() store.Store { return db }},
			permissionsManager: permissionManager,
		}
		recorder := httptest.NewRecorder()
		h.getFlowRetention(recorder, newFlowRetentionRequest(http.MethodGet, "", accountID, userID))
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response flowRetentionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		require.Equal(t, float64(7), response.RetentionDays)
	})

	t.Run("PUT requires settings update", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		db := store.NewMockStore(ctrl)
		permissionManager := permissions.NewMockManager(ctrl)
		permissionManager.EXPECT().ValidateUserPermissions(gomock.Any(), accountID, userID, modules.Settings, operations.Update).
			Return(true, context.Background(), nil)
		db.EXPECT().SetFlowRetention(gomock.Any(), 30*24*time.Hour).Return(nil)
		h := &handler{
			accountManager:     &mock_server.MockAccountManager{GetStoreFunc: func() store.Store { return db }},
			permissionsManager: permissionManager,
		}
		recorder := httptest.NewRecorder()
		h.updateFlowRetention(recorder, newFlowRetentionRequest(http.MethodPut, `{"retention_days":30}`, accountID, userID))
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	})

	t.Run("denied update fails closed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		permissionManager := permissions.NewMockManager(ctrl)
		permissionManager.EXPECT().ValidateUserPermissions(gomock.Any(), accountID, userID, modules.Settings, operations.Update).
			Return(false, context.Background(), nil)
		recorder := httptest.NewRecorder()
		(&handler{permissionsManager: permissionManager}).
			updateFlowRetention(recorder, newFlowRetentionRequest(http.MethodPut, `{"retention_days":30}`, accountID, userID))
		require.Equal(t, http.StatusForbidden, recorder.Code)
	})

	t.Run("missing manager fails closed", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		(&handler{}).getFlowRetention(recorder, newFlowRetentionRequest(http.MethodGet, "", accountID, userID))
		require.Equal(t, http.StatusForbidden, recorder.Code)
	})
}

func newFlowRetentionRequest(method, body, accountID, userID string) *http.Request {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, "/api/events/network-traffic/retention", reader)
	return nbcontext.SetUserAuthInRequest(request, auth.UserAuth{AccountId: accountID, UserId: userID})
}
