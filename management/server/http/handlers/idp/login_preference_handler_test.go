package idp

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/netbirdio/netbird/management/server/mock_server"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

func TestLoginPreferenceHandler_RedirectsToLocalConnector(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	storeMock.EXPECT().GetAllAccounts(gomock.Any()).Return([]*types.Account{
		{
			Id:                     "account-1",
			IsDomainPrimaryAccount: true,
			CreatedAt:              time.Unix(1, 0),
			Settings: &types.Settings{
				LoginMethod: types.LoginMethodEmail,
			},
		},
	}).AnyTimes()

	handler := &loginPreferenceHandler{
		accountManager: &mock_server.MockAccountManager{
			GetStoreFunc: func() store.Store {
				return storeMock
			},
		},
		dexHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "/oauth2/auth?client_id=test&state=abc", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusFound, recorder.Code)
	assert.Equal(t, "/oauth2/auth/local?client_id=test&state=abc", recorder.Header().Get("Location"))
}

func TestLoginPreferenceHandler_AllowsDexLoginPageWhenModeIsAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	storeMock.EXPECT().GetAllAccounts(gomock.Any()).Return([]*types.Account{
		{
			Id:                     "account-1",
			IsDomainPrimaryAccount: true,
			CreatedAt:              time.Unix(1, 0),
			Settings: &types.Settings{
				LoginMethod: types.LoginMethodAll,
			},
		},
	}).AnyTimes()

	handler := &loginPreferenceHandler{
		accountManager: &mock_server.MockAccountManager{
			GetStoreFunc: func() store.Store {
				return storeMock
			},
		},
		dexHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "/oauth2/auth?client_id=test", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusNoContent, recorder.Code)
}

func TestLoginPreferenceHandler_DoesNotOpenLoginWhenProviderIsMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	storeMock.EXPECT().GetAllAccounts(gomock.Any()).Return([]*types.Account{
		{
			Id:                     "account-1",
			IsDomainPrimaryAccount: true,
			CreatedAt:              time.Unix(1, 0),
			Settings: &types.Settings{
				LoginMethod: types.LoginMethodWeChatWork,
			},
		},
	}).AnyTimes()

	handler := &loginPreferenceHandler{
		accountManager: &mock_server.MockAccountManager{
			GetStoreFunc: func() store.Store {
				return storeMock
			},
		},
		dexHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "/oauth2/auth?client_id=test", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.NotEqual(t, http.StatusNoContent, recorder.Code)
}
