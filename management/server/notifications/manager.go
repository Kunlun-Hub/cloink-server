package notifications

import (
	"context"

	"github.com/google/uuid"

	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

// Store is the narrow persistence surface the manager needs.
type Store interface {
	CreateNotificationChannel(ctx context.Context, channel *types.NotificationChannel) (*types.NotificationChannel, error)
	GetNotificationChannel(ctx context.Context, lockStrength store.LockingStrength, accountID string, id string) (*types.NotificationChannel, error)
	ListNotificationChannels(ctx context.Context, lockStrength store.LockingStrength, accountID string) ([]*types.NotificationChannel, error)
	UpdateNotificationChannel(ctx context.Context, channel *types.NotificationChannel) error
	DeleteNotificationChannel(ctx context.Context, accountID string, id string) error
}

// Manager implements notification channel CRUD.
type Manager struct {
	store              Store
	permissionsManager permissions.Manager
}

// NewManager creates a notifications manager.
func NewManager(store Store, permissionsManager permissions.Manager) *Manager {
	return &Manager{store: store, permissionsManager: permissionsManager}
}

func (m *Manager) checkPermission(ctx context.Context, accountID, userID string, operation operations.Operation) error {
	ok, _, err := m.permissionsManager.ValidateUserPermissions(ctx, accountID, userID, modules.Notifications, operation)
	if err != nil {
		return err
	}
	if !ok {
		return status.Errorf(status.PermissionDenied, "permission denied")
	}
	return nil
}

// validateChannel checks the channel type and event types.
func validateChannel(channel *types.NotificationChannel) error {
	if _, ok := types.NotificationChannelTypes[channel.Type]; !ok {
		return status.Errorf(status.InvalidArgument, "invalid channel type: %s", channel.Type)
	}
	for _, et := range channel.EventTypes {
		if _, ok := types.NotificationEventTypes[et]; !ok {
			return status.Errorf(status.InvalidArgument, "invalid event type: %s", et)
		}
	}
	return nil
}

// CreateChannel creates a new notification channel.
func (m *Manager) CreateChannel(ctx context.Context, accountID, userID string, channel *types.NotificationChannel) (*types.NotificationChannel, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Create); err != nil {
		return nil, err
	}
	if err := validateChannel(channel); err != nil {
		return nil, err
	}
	channel.ID = uuid.New().String()
	channel.AccountID = accountID
	return m.store.CreateNotificationChannel(ctx, channel)
}

// GetChannel returns a single notification channel.
func (m *Manager) GetChannel(ctx context.Context, accountID, userID string, id string) (*types.NotificationChannel, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Read); err != nil {
		return nil, err
	}
	return m.store.GetNotificationChannel(ctx, store.LockingStrengthNone, accountID, id)
}

// ListChannels returns all notification channels of an account.
func (m *Manager) ListChannels(ctx context.Context, accountID, userID string) ([]*types.NotificationChannel, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Read); err != nil {
		return nil, err
	}
	return m.store.ListNotificationChannels(ctx, store.LockingStrengthNone, accountID)
}

// UpdateChannel updates an existing notification channel.
func (m *Manager) UpdateChannel(ctx context.Context, accountID, userID string, id string, update *types.NotificationChannel) (*types.NotificationChannel, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Update); err != nil {
		return nil, err
	}
	if err := validateChannel(update); err != nil {
		return nil, err
	}
	channel, err := m.store.GetNotificationChannel(ctx, store.LockingStrengthNone, accountID, id)
	if err != nil {
		return nil, err
	}
	channel.Type = update.Type
	channel.Enabled = update.Enabled
	channel.EventTypes = update.EventTypes
	channel.Target = update.Target
	if err := m.store.UpdateNotificationChannel(ctx, channel); err != nil {
		return nil, err
	}
	return channel, nil
}

// DeleteChannel removes a notification channel.
func (m *Manager) DeleteChannel(ctx context.Context, accountID, userID string, id string) error {
	if err := m.checkPermission(ctx, accountID, userID, operations.Delete); err != nil {
		return err
	}
	return m.store.DeleteNotificationChannel(ctx, accountID, id)
}
