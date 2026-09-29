package eventstreaming

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	testAccountID = "es-test-account"
	testUserID    = "es-test-user"
)

func allowAllPermissions(permissionsMock *permissions.MockManager) {
	permissionsMock.EXPECT().
		ValidateUserPermissions(gomock.Any(), testAccountID, testUserID, modules.EventStreaming, gomock.Any()).
		Return(true, context.Background(), nil).AnyTimes()
}

func datadogConfig() map[string]string {
	return map[string]string{
		"api_key": "dd-secret",
		"api_url": "https://http-intake.logs.datadoghq.eu/api/v2/logs",
	}
}

func TestCreateIntegrationValidatesPlatformAndConfig(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	permissionsMock := permissions.NewMockManager(ctrl)
	allowAllPermissions(permissionsMock)

	manager := NewManager(storeMock, permissionsMock)

	// unsupported platform
	_, err := manager.CreateIntegration(ctx, testAccountID, testUserID, &types.EventStreamingIntegration{
		Platform: "splunk",
		Config:   map[string]string{},
	})
	require.Error(t, err)

	// missing required keys
	_, err = manager.CreateIntegration(ctx, testAccountID, testUserID, &types.EventStreamingIntegration{
		Platform: types.EventStreamingPlatformDatadog,
		Config:   map[string]string{"api_key": "x"},
	})
	require.Error(t, err)

	// invalid url
	_, err = manager.CreateIntegration(ctx, testAccountID, testUserID, &types.EventStreamingIntegration{
		Platform: types.EventStreamingPlatformGenericHTTP,
		Config:   map[string]string{"url": "not-a-url"},
	})
	require.Error(t, err)

	// valid: store is called
	storeMock.EXPECT().
		CreateEventStreamingIntegration(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error) {
			assert.Equal(t, testAccountID, in.AccountID)
			in.ID = 7
			return in, nil
		})
	created, err := manager.CreateIntegration(ctx, testAccountID, testUserID, &types.EventStreamingIntegration{
		Platform: types.EventStreamingPlatformDatadog,
		Enabled:  true,
		Config:   datadogConfig(),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), created.ID)
}

func TestGetIntegrationMasksSecrets(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	permissionsMock := permissions.NewMockManager(ctrl)
	allowAllPermissions(permissionsMock)

	storeMock.EXPECT().
		GetEventStreamingIntegration(gomock.Any(), store.LockingStrengthNone, testAccountID, uint64(1)).
		Return(&types.EventStreamingIntegration{
			ID:        1,
			AccountID: testAccountID,
			Platform:  types.EventStreamingPlatformDatadog,
			Enabled:   true,
			Config:    datadogConfig(),
		}, nil)

	manager := NewManager(storeMock, permissionsMock)
	got, err := manager.GetIntegration(ctx, testAccountID, testUserID, 1)
	require.NoError(t, err)
	assert.Equal(t, types.MaskedSecretValue, got.Config["api_key"])
	assert.Equal(t, "https://http-intake.logs.datadoghq.eu/api/v2/logs", got.Config["api_url"])
}

func TestUpdateIntegrationKeepsMaskedSecrets(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	permissionsMock := permissions.NewMockManager(ctrl)
	allowAllPermissions(permissionsMock)

	stored := &types.EventStreamingIntegration{
		ID:        1,
		AccountID: testAccountID,
		Platform:  types.EventStreamingPlatformDatadog,
		Enabled:   true,
		Config:    datadogConfig(),
	}
	storeMock.EXPECT().
		GetEventStreamingIntegration(gomock.Any(), store.LockingStrengthNone, testAccountID, uint64(1)).
		Return(stored, nil)
	var saved *types.EventStreamingIntegration
	storeMock.EXPECT().
		UpdateEventStreamingIntegration(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *types.EventStreamingIntegration) error {
			saved = in
			return nil
		})

	manager := NewManager(storeMock, permissionsMock)
	updated, err := manager.UpdateIntegration(ctx, testAccountID, testUserID, 1, &types.EventStreamingIntegration{
		Enabled: false,
		Config: map[string]string{
			"api_key": types.MaskedSecretValue, // user didn't change the key
			"api_url": "https://http-intake.logs.datadoghq.com/api/v2/logs",
		},
	})
	require.NoError(t, err)
	require.False(t, updated.Enabled)
	// stored secret preserved, not overwritten by the mask
	require.Equal(t, "dd-secret", saved.Config["api_key"])
	require.Equal(t, "https://http-intake.logs.datadoghq.com/api/v2/logs", saved.Config["api_url"])
	// response is masked
	assert.Equal(t, types.MaskedSecretValue, updated.Config["api_key"])
}

func TestDeleteIntegration(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	permissionsMock := permissions.NewMockManager(ctrl)
	allowAllPermissions(permissionsMock)

	storeMock.EXPECT().
		DeleteEventStreamingIntegration(gomock.Any(), testAccountID, uint64(3)).
		Return(nil)

	manager := NewManager(storeMock, permissionsMock)
	require.NoError(t, manager.DeleteIntegration(ctx, testAccountID, testUserID, 3))
}

func TestPermissionDenied(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeMock := store.NewMockStore(ctrl)
	permissionsMock := permissions.NewMockManager(ctrl)
	permissionsMock.EXPECT().
		ValidateUserPermissions(gomock.Any(), testAccountID, testUserID, modules.EventStreaming, operations.Read).
		Return(false, ctx, nil)

	manager := NewManager(storeMock, permissionsMock)
	_, err := manager.ListIntegrations(ctx, testAccountID, testUserID)
	require.Error(t, err)
}
