package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

// SavePasswordResetToken persists a password reset token with its email
// encrypted at rest.
func (s *SqlStore) SavePasswordResetToken(ctx context.Context, record *types.PasswordResetRecord) error {
	if record == nil {
		return status.Errorf(status.InvalidArgument, "password reset record is required")
	}

	recordCopy := record.Copy()
	if err := recordCopy.EncryptSensitiveData(s.fieldEncrypt); err != nil {
		return fmt.Errorf("encrypt password reset record: %w", err)
	}

	if err := s.db.WithContext(ctx).Save(recordCopy).Error; err != nil {
		log.WithContext(ctx).Errorf("failed to save password reset token: %s", err)
		return status.Errorf(status.Internal, "failed to save password reset token")
	}
	return nil
}

// GetPasswordResetByHashedToken looks up a reset record by the hash of its token.
func (s *SqlStore) GetPasswordResetByHashedToken(ctx context.Context, lockStrength LockingStrength, hashedToken string) (*types.PasswordResetRecord, error) {
	if hashedToken == "" {
		return nil, status.Errorf(status.InvalidArgument, "hashed token is required")
	}

	tx := s.db.WithContext(ctx)
	if lockStrength != LockingStrengthNone {
		tx = tx.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var record types.PasswordResetRecord
	if err := tx.Take(&record, "hashed_token = ?", hashedToken).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Errorf(status.NotFound, "password reset token not found")
		}
		log.WithContext(ctx).Errorf("failed to get password reset token: %s", err)
		return nil, status.Errorf(status.Internal, "failed to get password reset token")
	}

	if err := record.DecryptSensitiveData(s.fieldEncrypt); err != nil {
		return nil, fmt.Errorf("decrypt password reset record: %w", err)
	}
	return &record, nil
}

// ConsumePasswordResetToken deletes a reset record by ID. It returns false when
// the record was already consumed, which makes the token strictly single-use
// even under concurrent submissions.
func (s *SqlStore) ConsumePasswordResetToken(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, status.Errorf(status.InvalidArgument, "password reset id is required")
	}

	result := s.db.WithContext(ctx).Where(idQueryCondition, id).Delete(&types.PasswordResetRecord{})
	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to consume password reset token: %s", result.Error)
		return false, status.Errorf(status.Internal, "failed to consume password reset token")
	}
	return result.RowsAffected == 1, nil
}

// DeletePasswordResetTokensByEmailHash invalidates every outstanding reset
// record for an email, so only the newest link can be used.
func (s *SqlStore) DeletePasswordResetTokensByEmailHash(ctx context.Context, emailHash string) error {
	if emailHash == "" {
		return status.Errorf(status.InvalidArgument, "email hash is required")
	}

	result := s.db.WithContext(ctx).Where("email_hash = ?", emailHash).Delete(&types.PasswordResetRecord{})
	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to delete password reset tokens: %s", result.Error)
		return status.Errorf(status.Internal, "failed to delete password reset tokens")
	}
	return nil
}

// CountPasswordResetTokensSince counts reset records issued for an email since a
// point in time. It backs the per-email rate limit.
func (s *SqlStore) CountPasswordResetTokensSince(ctx context.Context, emailHash string, since time.Time) (int64, error) {
	if emailHash == "" {
		return 0, status.Errorf(status.InvalidArgument, "email hash is required")
	}

	var count int64
	result := s.db.WithContext(ctx).
		Model(&types.PasswordResetRecord{}).
		Where("email_hash = ? AND created_at >= ?", emailHash, since.UTC()).
		Count(&count)
	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to count password reset tokens: %s", result.Error)
		return 0, status.Errorf(status.Internal, "failed to count password reset tokens")
	}
	return count, nil
}

// DeleteExpiredPasswordResetTokens removes records whose expiry has passed.
func (s *SqlStore) DeleteExpiredPasswordResetTokens(ctx context.Context, now time.Time) error {
	result := s.db.WithContext(ctx).
		Where("expires_at < ?", now.UTC()).
		Delete(&types.PasswordResetRecord{})
	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to delete expired password reset tokens: %s", result.Error)
		return status.Errorf(status.Internal, "failed to delete expired password reset tokens")
	}
	return nil
}
