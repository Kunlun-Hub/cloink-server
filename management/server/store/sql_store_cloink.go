package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/netbirdio/netbird/management/internals/modules/reverseproxy/accesslogs"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

// UpdateUserInviteTokenIfMatches updates token metadata only when the
// persisted invite still contains expectedHashedToken. The compare-and-swap
// is used both to claim a resend and to restore a token after a failed send.
func (s *SqlStore) UpdateUserInviteTokenIfMatches(ctx context.Context, invite *types.UserInviteRecord, expectedHashedToken string) (bool, error) {
	if invite == nil || invite.ID == "" || invite.AccountID == "" || expectedHashedToken == "" {
		return false, status.Errorf(status.InvalidArgument, "invite and expected token are required")
	}

	result := s.db.WithContext(ctx).
		Model(&types.UserInviteRecord{}).
		Where("id = ? AND account_id = ? AND hashed_token = ?", invite.ID, invite.AccountID, expectedHashedToken).
		Updates(map[string]any{
			"hashed_token": invite.HashedToken,
			"expires_at":   invite.ExpiresAt,
			"created_by":   invite.CreatedBy,
		})
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to conditionally update user invite token")
		return false, status.Errorf(status.Internal, "failed to update user invite token")
	}
	return result.RowsAffected == 1, nil
}

// GetEmailSettings loads account-scoped SMTP settings. Missing settings are
// represented by normalized defaults so callers can render a first-run form.
func (s *SqlStore) GetEmailSettings(ctx context.Context, lockStrength LockingStrength, accountID string) (*types.EmailSettings, error) {
	tx := s.db
	if lockStrength != LockingStrengthNone {
		tx = tx.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var settings types.EmailSettings
	result := tx.Take(&settings, "account_id = ?", accountID)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			settings = types.EmailSettings{AccountID: accountID}
			settings.Normalize()
			return &settings, nil
		}
		log.WithContext(ctx).WithError(result.Error).Error("failed to get email settings from store")
		return nil, status.Errorf(status.Internal, "failed to get email settings from store")
	}

	if err := settings.DecryptSensitiveData(s.fieldEncrypt); err != nil {
		return nil, fmt.Errorf("decrypt email settings: %w", err)
	}
	settings.Normalize()
	settings.PasswordConfigured = settings.Password != "" || settings.PasswordEncrypted != ""
	return &settings, nil
}

// SaveEmailSettings persists SMTP settings while encrypting the password at
// rest. The caller may pass a copy containing the clear password for updates.
func (s *SqlStore) SaveEmailSettings(ctx context.Context, settings *types.EmailSettings) error {
	if settings == nil {
		return status.Errorf(status.InvalidArgument, "email settings are required")
	}
	settingsCopy := settings.Copy()
	settingsCopy.Normalize()
	settingsCopy.PasswordConfigured = false
	if settingsCopy.Password != "" {
		if err := settingsCopy.EncryptSensitiveData(s.fieldEncrypt); err != nil {
			return fmt.Errorf("encrypt email settings: %w", err)
		}
	}

	result := s.db.Save(settingsCopy)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to save email settings to store")
		return status.Errorf(status.Internal, "failed to save email settings to store")
	}
	return nil
}

// CreateEventStreamingIntegration persists a new event streaming integration.
func (s *SqlStore) CreateEventStreamingIntegration(ctx context.Context, integration *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error) {
	if integration == nil {
		return nil, status.Errorf(status.InvalidArgument, "event streaming integration is required")
	}
	integrationCopy := integration.Copy()
	if err := integrationCopy.EncryptSensitiveData(s.fieldEncrypt); err != nil {
		return nil, err
	}
	result := s.db.Create(integrationCopy)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to create event streaming integration in store")
		return nil, status.Errorf(status.Internal, "failed to create event streaming integration in store")
	}
	integrationCopy.Config = integration.Config
	return integrationCopy, nil
}

// GetEventStreamingIntegration returns a single integration with its config decrypted.
func (s *SqlStore) GetEventStreamingIntegration(ctx context.Context, lockStrength LockingStrength, accountID string, id uint64) (*types.EventStreamingIntegration, error) {
	tx := s.db
	if lockStrength != LockingStrengthNone {
		tx = tx.Clauses(clause.Locking{Strength: string(lockStrength)})
	}
	var integration types.EventStreamingIntegration
	result := tx.Take(&integration, "account_id = ? AND id = ?", accountID, id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, status.Errorf(status.NotFound, "event streaming integration not found")
		}
		log.WithContext(ctx).WithError(result.Error).Error("failed to get event streaming integration from store")
		return nil, status.Errorf(status.Internal, "failed to get event streaming integration from store")
	}
	if err := integration.DecryptSensitiveData(s.fieldEncrypt); err != nil {
		return nil, err
	}
	return &integration, nil
}

// ListEventStreamingIntegrations returns all integrations of an account.
func (s *SqlStore) ListEventStreamingIntegrations(ctx context.Context, lockStrength LockingStrength, accountID string) ([]*types.EventStreamingIntegration, error) {
	tx := s.db
	if lockStrength != LockingStrengthNone {
		tx = tx.Clauses(clause.Locking{Strength: string(lockStrength)})
	}
	var integrations []*types.EventStreamingIntegration
	result := tx.Where("account_id = ?", accountID).Order("id ASC").Find(&integrations)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to list event streaming integrations from store")
		return nil, status.Errorf(status.Internal, "failed to list event streaming integrations from store")
	}
	for _, integration := range integrations {
		if err := integration.DecryptSensitiveData(s.fieldEncrypt); err != nil {
			return nil, err
		}
	}
	return integrations, nil
}

// UpdateEventStreamingIntegration persists changes to an integration.
func (s *SqlStore) UpdateEventStreamingIntegration(ctx context.Context, integration *types.EventStreamingIntegration) error {
	if integration == nil {
		return status.Errorf(status.InvalidArgument, "event streaming integration is required")
	}
	integrationCopy := integration.Copy()
	if err := integrationCopy.EncryptSensitiveData(s.fieldEncrypt); err != nil {
		return err
	}
	result := s.db.Where("account_id = ? AND id = ?", integrationCopy.AccountID, integrationCopy.ID).Save(integrationCopy)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to update event streaming integration in store")
		return status.Errorf(status.Internal, "failed to update event streaming integration in store")
	}
	if result.RowsAffected == 0 {
		return status.Errorf(status.NotFound, "event streaming integration not found")
	}
	return nil
}

// DeleteEventStreamingIntegration removes an integration and its cursor.
func (s *SqlStore) DeleteEventStreamingIntegration(ctx context.Context, accountID string, id uint64) error {
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("account_id = ? AND integration_id = ?", accountID, id).Delete(&types.EventStreamingCursor{}).Error; err != nil {
			return err
		}
		result := tx.Where("account_id = ? AND id = ?", accountID, id).Delete(&types.EventStreamingIntegration{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return status.Errorf(status.NotFound, "event streaming integration not found")
		}
		return nil
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			return st
		}
		log.WithContext(ctx).WithError(err).Error("failed to delete event streaming integration from store")
		return status.Errorf(status.Internal, "failed to delete event streaming integration from store")
	}
	return nil
}

// GetEventStreamingCursor returns the forwarding watermark for an integration.
func (s *SqlStore) GetEventStreamingCursor(ctx context.Context, accountID string, integrationID uint64) (*types.EventStreamingCursor, error) {
	var cursor types.EventStreamingCursor
	result := s.db.Take(&cursor, "account_id = ? AND integration_id = ?", accountID, integrationID)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return &types.EventStreamingCursor{AccountID: accountID, IntegrationID: integrationID}, nil
		}
		log.WithContext(ctx).WithError(result.Error).Error("failed to get event streaming cursor from store")
		return nil, status.Errorf(status.Internal, "failed to get event streaming cursor from store")
	}
	return &cursor, nil
}

// SaveEventStreamingCursor persists the forwarding watermark.
func (s *SqlStore) SaveEventStreamingCursor(ctx context.Context, cursor *types.EventStreamingCursor) error {
	if cursor == nil {
		return status.Errorf(status.InvalidArgument, "event streaming cursor is required")
	}
	result := s.db.Save(cursor)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to save event streaming cursor in store")
		return status.Errorf(status.Internal, "failed to save event streaming cursor in store")
	}
	return nil
}

// SaveVersionRelease creates or updates a release, keeping one latest build
// per account, platform, architecture, and channel.
func (s *SqlStore) SaveVersionRelease(ctx context.Context, release *types.VersionRelease) error {
	if release == nil || release.ID == "" || release.AccountID == "" {
		return fmt.Errorf("version release requires release and account IDs")
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if release.IsLatest {
			result := tx.Model(&types.VersionRelease{}).
				Where("account_id = ? AND platform = ? AND architecture = ? AND channel = ? AND id <> ?",
					release.AccountID, release.Platform, release.Architecture, release.Channel, release.ID).
				Update("is_latest", false)
			if result.Error != nil {
				return result.Error
			}
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(release).Error
	})
}

// GetVersionRelease returns one release owned by the account.
func (s *SqlStore) GetVersionRelease(ctx context.Context, accountID, releaseID string) (*types.VersionRelease, error) {
	var release types.VersionRelease
	result := s.db.WithContext(ctx).Where("account_id = ? AND id = ?", accountID, releaseID).First(&release)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, status.Errorf(status.NotFound, "version release not found")
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &release, nil
}

// ListVersionReleases returns an account's releases in newest-first order.
func (s *SqlStore) ListVersionReleases(ctx context.Context, accountID string) ([]*types.VersionRelease, error) {
	var releases []*types.VersionRelease
	result := s.db.WithContext(ctx).Where("account_id = ?", accountID).
		Order("created_at DESC").Find(&releases)
	return releases, result.Error
}

// DeleteVersionRelease removes an account-owned release.
func (s *SqlStore) DeleteVersionRelease(ctx context.Context, accountID, releaseID string) error {
	result := s.db.WithContext(ctx).Where("account_id = ? AND id = ?", accountID, releaseID).
		Delete(&types.VersionRelease{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return status.Errorf(status.NotFound, "version release not found")
	}
	return nil
}

// SaveVersionReleaseArtifact creates or updates release artifact metadata.
func (s *SqlStore) SaveVersionReleaseArtifact(ctx context.Context, artifact *types.VersionReleaseArtifact) error {
	if artifact == nil || artifact.ID == "" || artifact.AccountID == "" {
		return fmt.Errorf("version release artifact requires artifact and account IDs")
	}
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(artifact).Error
}

// GetVersionReleaseArtifact returns one artifact owned by the account.
func (s *SqlStore) GetVersionReleaseArtifact(ctx context.Context, accountID, artifactID string) (*types.VersionReleaseArtifact, error) {
	var artifact types.VersionReleaseArtifact
	result := s.db.WithContext(ctx).Where("account_id = ? AND id = ?", accountID, artifactID).First(&artifact)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, status.Errorf(status.NotFound, "version release artifact not found")
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &artifact, nil
}

// ListOrphanedVersionReleaseArtifacts returns unreferenced artifacts older
// than the supplied cutoff for garbage collection.
func (s *SqlStore) ListOrphanedVersionReleaseArtifacts(ctx context.Context, olderThan time.Time) ([]*types.VersionReleaseArtifact, error) {
	var artifacts []*types.VersionReleaseArtifact
	result := s.db.WithContext(ctx).
		Where("created_at < ?", olderThan).
		Where("NOT EXISTS (?)", s.db.Model(&types.VersionRelease{}).
			Select("1").
			Where("version_releases.artifact_id = version_release_artifacts.id")).
		Find(&artifacts)
	return artifacts, result.Error
}

// DeleteVersionReleaseArtifact removes an account-owned release artifact.
func (s *SqlStore) DeleteVersionReleaseArtifact(ctx context.Context, accountID, artifactID string) error {
	result := s.db.WithContext(ctx).Where("account_id = ? AND id = ?", accountID, artifactID).
		Delete(&types.VersionReleaseArtifact{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return status.Errorf(status.NotFound, "version release artifact not found")
	}
	return nil
}

// DeleteVersionReleaseArtifactIfUnreferenced removes an artifact only when no
// release still references it. It reports whether a row was deleted.
func (s *SqlStore) DeleteVersionReleaseArtifactIfUnreferenced(ctx context.Context, accountID, artifactID string) (bool, error) {
	result := s.db.WithContext(ctx).
		Where("account_id = ? AND id = ?", accountID, artifactID).
		Where("NOT EXISTS (?)", s.db.Model(&types.VersionRelease{}).
			Select("1").
			Where("version_releases.artifact_id = ?", artifactID)).
		Delete(&types.VersionReleaseArtifact{})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// CreateAccessLog creates a new access log entry in the database
func (s *SqlStore) CreateAccessLog(ctx context.Context, logEntry *accesslogs.AccessLogEntry) error {
	result := s.db.Create(logEntry)
	if result.Error != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"service_id": logEntry.ServiceID,
			"method":     logEntry.Method,
			"host":       logEntry.Host,
			"path":       logEntry.Path,
		}).Errorf("failed to create access log entry in store: %v", result.Error)
		return status.Errorf(status.Internal, "failed to create access log entry in store")
	}
	return nil
}

// GetAccountAccessLogs retrieves access logs for a given account with pagination and filtering
func (s *SqlStore) GetAccountAccessLogs(ctx context.Context, lockStrength LockingStrength, accountID string, filter accesslogs.AccessLogFilter) ([]*accesslogs.AccessLogEntry, int64, error) {
	var logs []*accesslogs.AccessLogEntry
	var totalCount int64

	baseQuery := s.db.
		Model(&accesslogs.AccessLogEntry{}).
		Where(accountIDCondition, accountID)

	baseQuery = s.applyAccessLogFilters(baseQuery, filter)

	if err := baseQuery.Count(&totalCount).Error; err != nil {
		log.WithContext(ctx).Errorf("failed to count access logs: %v", err)
		return nil, 0, status.Errorf(status.Internal, "failed to count access logs")
	}

	query := s.db.
		Where(accountIDCondition, accountID)

	query = s.applyAccessLogFilters(query, filter)

	sortColumns := filter.GetSortColumn()
	sortOrder := strings.ToUpper(filter.GetSortOrder())

	var orderClauses []string
	for _, col := range strings.Split(sortColumns, ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			orderClauses = append(orderClauses, col+" "+sortOrder)
		}
	}
	orderClause := strings.Join(orderClauses, ", ")

	query = query.
		Order(orderClause).
		Limit(filter.GetLimit()).
		Offset(filter.GetOffset())

	if lockStrength != LockingStrengthNone {
		query = query.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	result := query.Find(&logs)
	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to get access logs from store: %v", result.Error)
		return nil, 0, status.Errorf(status.Internal, "failed to get access logs from store")
	}

	return logs, totalCount, nil
}

// DeleteOldAccessLogs deletes all access logs older than the specified time
func (s *SqlStore) DeleteOldAccessLogs(ctx context.Context, olderThan time.Time) (int64, error) {
	result := s.db.
		Where("timestamp < ?", olderThan).
		Delete(&accesslogs.AccessLogEntry{})

	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to delete old access logs: %v", result.Error)
		return 0, status.Errorf(status.Internal, "failed to delete old access logs")
	}

	return result.RowsAffected, nil
}

// applyAccessLogFilters applies filter conditions to the query
func (s *SqlStore) applyAccessLogFilters(query *gorm.DB, filter accesslogs.AccessLogFilter) *gorm.DB {
	if filter.Search != nil {
		searchPattern := "%" + *filter.Search + "%"
		query = query.Where(
			"id LIKE ? OR location_connection_ip LIKE ? OR host LIKE ? OR path LIKE ? OR CONCAT(host, path) LIKE ? OR user_id IN (SELECT id FROM users WHERE email LIKE ? OR name LIKE ?)",
			searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern, searchPattern,
		)
	}

	if filter.SourceIP != nil {
		query = query.Where("location_connection_ip = ?", *filter.SourceIP)
	}

	if filter.Host != nil {
		query = query.Where("host = ?", *filter.Host)
	}

	if filter.Path != nil {
		// Support LIKE pattern for path filtering
		query = query.Where("path LIKE ?", "%"+*filter.Path+"%")
	}

	if filter.UserID != nil {
		query = query.Where("user_id = ?", *filter.UserID)
	}

	if filter.Method != nil {
		query = query.Where("method = ?", *filter.Method)
	}

	if filter.Status != nil {
		switch *filter.Status {
		case "success":
			query = query.Where("status_code >= ? AND status_code < ?", 200, 400)
		case "failed":
			query = query.Where("status_code < ? OR status_code >= ?", 200, 400)
		}
	}

	if filter.StatusCode != nil {
		query = query.Where("status_code = ?", *filter.StatusCode)
	}

	if filter.StartDate != nil {
		query = query.Where("timestamp >= ?", *filter.StartDate)
	}

	if filter.EndDate != nil {
		query = query.Where("timestamp <= ?", *filter.EndDate)
	}

	return query
}

// CreateNotificationChannel persists a new notification channel for an account.
func (s *SqlStore) CreateNotificationChannel(ctx context.Context, channel *types.NotificationChannel) (*types.NotificationChannel, error) {
	if channel == nil {
		return nil, status.Errorf(status.InvalidArgument, "notification channel is required")
	}
	channelCopy := *channel
	result := s.db.Create(&channelCopy)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to create notification channel in store")
		return nil, status.Errorf(status.Internal, "failed to create notification channel in store")
	}
	return &channelCopy, nil
}

// GetNotificationChannel returns a single notification channel by ID.
func (s *SqlStore) GetNotificationChannel(ctx context.Context, lockStrength LockingStrength, accountID string, id string) (*types.NotificationChannel, error) {
	tx := s.db
	if lockStrength != LockingStrengthNone {
		tx = tx.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var channel types.NotificationChannel
	result := tx.Take(&channel, "account_id = ? AND id = ?", accountID, id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, status.Errorf(status.NotFound, "notification channel not found")
		}
		log.WithContext(ctx).WithError(result.Error).Error("failed to get notification channel from store")
		return nil, status.Errorf(status.Internal, "failed to get notification channel from store")
	}
	return &channel, nil
}

// ListNotificationChannels returns all notification channels of an account.
func (s *SqlStore) ListNotificationChannels(ctx context.Context, lockStrength LockingStrength, accountID string) ([]*types.NotificationChannel, error) {
	tx := s.db
	if lockStrength != LockingStrengthNone {
		tx = tx.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var channels []*types.NotificationChannel
	result := tx.Where("account_id = ?", accountID).Order("id ASC").Find(&channels)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to list notification channels from store")
		return nil, status.Errorf(status.Internal, "failed to list notification channels from store")
	}
	return channels, nil
}

// UpdateNotificationChannel persists changes to an existing notification channel.
func (s *SqlStore) UpdateNotificationChannel(ctx context.Context, channel *types.NotificationChannel) error {
	if channel == nil {
		return status.Errorf(status.InvalidArgument, "notification channel is required")
	}
	result := s.db.Where("account_id = ? AND id = ?", channel.AccountID, channel.ID).Save(channel)
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to update notification channel in store")
		return status.Errorf(status.Internal, "failed to update notification channel in store")
	}
	if result.RowsAffected == 0 {
		return status.Errorf(status.NotFound, "notification channel not found")
	}
	return nil
}

// DeleteNotificationChannel removes a notification channel by ID.
func (s *SqlStore) DeleteNotificationChannel(ctx context.Context, accountID string, id string) error {
	result := s.db.Where("account_id = ? AND id = ?", accountID, id).Delete(&types.NotificationChannel{})
	if result.Error != nil {
		log.WithContext(ctx).WithError(result.Error).Error("failed to delete notification channel from store")
		return status.Errorf(status.Internal, "failed to delete notification channel from store")
	}
	if result.RowsAffected == 0 {
		return status.Errorf(status.NotFound, "notification channel not found")
	}
	return nil
}
