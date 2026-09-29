package eventstreaming

import (
	"context"
	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

// requiredConfigKeys lists the config keys that must be present (non-empty)
// per platform.
var requiredConfigKeys = map[string][]string{
	types.EventStreamingPlatformDatadog:     {"api_key", "api_url"},
	types.EventStreamingPlatformS3:          {"access_key", "secret_key", "bucket_name", "region"},
	types.EventStreamingPlatformFirehose:    {"access_key", "secret_key", "stream_name", "region"},
	types.EventStreamingPlatformGenericHTTP: {"url"},
}

// Store is the narrow persistence surface the manager needs.
type Store interface {
	CreateEventStreamingIntegration(ctx context.Context, integration *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error)
	GetEventStreamingIntegration(ctx context.Context, lockStrength store.LockingStrength, accountID string, id uint64) (*types.EventStreamingIntegration, error)
	ListEventStreamingIntegrations(ctx context.Context, lockStrength store.LockingStrength, accountID string) ([]*types.EventStreamingIntegration, error)
	UpdateEventStreamingIntegration(ctx context.Context, integration *types.EventStreamingIntegration) error
	DeleteEventStreamingIntegration(ctx context.Context, accountID string, id uint64) error
}

// Manager implements event streaming integration CRUD.
type Manager struct {
	store              Store
	permissionsManager permissions.Manager
}

// NewManager creates an event streaming manager.
func NewManager(store Store, permissionsManager permissions.Manager) *Manager {
	return &Manager{store: store, permissionsManager: permissionsManager}
}

func (m *Manager) checkPermission(ctx context.Context, accountID, userID string, operation operations.Operation) error {
	ok, _, err := m.permissionsManager.ValidateUserPermissions(ctx, accountID, userID, modules.EventStreaming, operation)
	if err != nil {
		return err
	}
	if !ok {
		return status.Errorf(status.PermissionDenied, "user does not have permission to manage event streaming integrations")
	}
	return nil
}

// CreateIntegration validates and persists a new integration.
func (m *Manager) CreateIntegration(ctx context.Context, accountID, userID string, integration *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Create); err != nil {
		return nil, err
	}
	if integration == nil {
		return nil, status.Errorf(status.InvalidArgument, "integration is required")
	}
	if _, ok := types.EventStreamingPlatforms[integration.Platform]; !ok {
		return nil, status.Errorf(status.InvalidArgument, "unsupported event streaming platform: %s", integration.Platform)
	}
	if err := validateConfig(integration.Platform, integration.Config); err != nil {
		return nil, err
	}
	integration.AccountID = accountID
	created, err := m.store.CreateEventStreamingIntegration(ctx, integration)
	if err != nil {
		return nil, err
	}
	log.WithContext(ctx).Infof("created event streaming integration %d (%s) for account %s", created.ID, created.Platform, accountID)
	return created, nil
}

// GetIntegration returns a single integration with secrets masked.
func (m *Manager) GetIntegration(ctx context.Context, accountID, userID string, id uint64) (*types.EventStreamingIntegration, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Read); err != nil {
		return nil, err
	}
	integration, err := m.store.GetEventStreamingIntegration(ctx, store.LockingStrengthNone, accountID, id)
	if err != nil {
		return nil, err
	}
	return maskIntegration(integration), nil
}

// ListIntegrations returns all integrations of an account with secrets masked.
func (m *Manager) ListIntegrations(ctx context.Context, accountID, userID string) ([]*types.EventStreamingIntegration, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Read); err != nil {
		return nil, err
	}
	integrations, err := m.store.ListEventStreamingIntegrations(ctx, store.LockingStrengthNone, accountID)
	if err != nil {
		return nil, err
	}
	for i, integration := range integrations {
		integrations[i] = maskIntegration(integration)
	}
	return integrations, nil
}

// UpdateIntegration updates enabled flag and config. Masked secret values
// (****) in the payload keep the stored values. Platform cannot be changed.
func (m *Manager) UpdateIntegration(ctx context.Context, accountID, userID string, id uint64, update *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Update); err != nil {
		return nil, err
	}
	if update == nil {
		return nil, status.Errorf(status.InvalidArgument, "integration update is required")
	}
	stored, err := m.store.GetEventStreamingIntegration(ctx, store.LockingStrengthNone, accountID, id)
	if err != nil {
		return nil, err
	}
	stored.Enabled = update.Enabled
	if update.Config != nil {
		stored.MergeMaskedConfig(update.Config)
	}
	if err := validateConfig(stored.Platform, stored.Config); err != nil {
		return nil, err
	}
	if err := m.store.UpdateEventStreamingIntegration(ctx, stored); err != nil {
		return nil, err
	}
	log.WithContext(ctx).Infof("updated event streaming integration %d (%s) for account %s", id, stored.Platform, accountID)
	return maskIntegration(stored), nil
}

// DeleteIntegration removes an integration and its forwarding cursor.
func (m *Manager) DeleteIntegration(ctx context.Context, accountID, userID string, id uint64) error {
	if err := m.checkPermission(ctx, accountID, userID, operations.Delete); err != nil {
		return err
	}
	if err := m.store.DeleteEventStreamingIntegration(ctx, accountID, id); err != nil {
		return err
	}
	log.WithContext(ctx).Infof("deleted event streaming integration %d for account %s", id, accountID)
	return nil
}

// maskIntegration returns a copy with secret config values masked.
func maskIntegration(integration *types.EventStreamingIntegration) *types.EventStreamingIntegration {
	c := integration.Copy()
	c.Config = integration.MaskedConfig()
	return c
}

func validateConfig(platform string, config map[string]string) error {
	required, ok := requiredConfigKeys[platform]
	if !ok {
		return status.Errorf(status.InvalidArgument, "unsupported event streaming platform: %s", platform)
	}
	var missing []string
	for _, key := range required {
		if strings.TrimSpace(config[key]) == "" || config[key] == types.MaskedSecretValue {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return status.Errorf(status.InvalidArgument, "missing required config keys for platform %s: %s", platform, strings.Join(missing, ", "))
	}
	if platform == types.EventStreamingPlatformGenericHTTP {
		u, err := url.Parse(config["url"])
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return status.Errorf(status.InvalidArgument, "invalid url for generic_http integration: %s", config["url"])
		}
	}
	if platform == types.EventStreamingPlatformDatadog {
		u, err := url.Parse(config["api_url"])
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return status.Errorf(status.InvalidArgument, "invalid api_url for datadog integration: %s", config["api_url"])
		}
	}
	return nil
}
