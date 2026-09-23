package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/util/crypt"
)

func newPasswordResetTestStore(t *testing.T) Store {
	t.Helper()

	t.Setenv("NETBIRD_STORE_ENGINE", string(types.SqliteStoreEngine))
	s, cleanup, err := NewTestStoreFromSQL(context.Background(), "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	key, err := crypt.GenerateKey()
	require.NoError(t, err)
	fieldEncrypt, err := crypt.NewFieldEncrypt(key)
	require.NoError(t, err)
	s.SetFieldEncrypt(fieldEncrypt)

	return s
}

func newPasswordResetRecord(t *testing.T, email string, expiresAt time.Time) *types.PasswordResetRecord {
	t.Helper()

	hashed, plain, err := types.GeneratePasswordResetToken()
	require.NoError(t, err)
	require.NotEmpty(t, plain)

	return &types.PasswordResetRecord{
		ID:          types.NewPasswordResetID(),
		AccountID:   "account-1",
		UserID:      "user-1",
		Email:       email,
		EmailHash:   types.HashPasswordResetEmail(email),
		HashedToken: hashed,
		ExpiresAt:   expiresAt,
		CreatedAt:   time.Now().UTC(),
	}
}

func TestSqlStorePasswordResetTokenRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newPasswordResetTestStore(t)

	record := newPasswordResetRecord(t, "user@example.com", time.Now().Add(30*time.Minute))
	require.NoError(t, s.SavePasswordResetToken(ctx, record))

	sqlStore, ok := s.(*SqlStore)
	require.True(t, ok)
	require.True(t, sqlStore.db.Migrator().HasTable(&types.PasswordResetRecord{}), "password_reset_tokens table must be created by AutoMigrate")

	var raw struct {
		Email string `gorm:"column:email"`
	}
	require.NoError(t, sqlStore.db.Table("password_reset_tokens").Select("email").Where("id = ?", record.ID).Take(&raw).Error)
	require.NotEqual(t, "user@example.com", raw.Email, "email must not be persisted in plaintext")

	loaded, err := s.GetPasswordResetByHashedToken(ctx, LockingStrengthNone, record.HashedToken)
	require.NoError(t, err)
	require.Equal(t, "user@example.com", loaded.Email, "email must be decrypted on read")
	require.Equal(t, record.ID, loaded.ID)
	require.False(t, loaded.IsExpired())
}

func TestSqlStorePasswordResetTokenIsSingleUse(t *testing.T) {
	ctx := context.Background()
	s := newPasswordResetTestStore(t)

	record := newPasswordResetRecord(t, "user@example.com", time.Now().Add(30*time.Minute))
	require.NoError(t, s.SavePasswordResetToken(ctx, record))

	consumed, err := s.ConsumePasswordResetToken(ctx, record.ID)
	require.NoError(t, err)
	require.True(t, consumed, "first use must consume the token")

	consumed, err = s.ConsumePasswordResetToken(ctx, record.ID)
	require.NoError(t, err)
	require.False(t, consumed, "a token must not be usable twice")

	_, err = s.GetPasswordResetByHashedToken(ctx, LockingStrengthNone, record.HashedToken)
	require.Error(t, err, "a consumed token must no longer resolve")
}

func TestSqlStorePasswordResetTokensByEmailHash(t *testing.T) {
	ctx := context.Background()
	s := newPasswordResetTestStore(t)

	first := newPasswordResetRecord(t, "user@example.com", time.Now().Add(30*time.Minute))
	second := newPasswordResetRecord(t, "user@example.com", time.Now().Add(30*time.Minute))
	other := newPasswordResetRecord(t, "other@example.com", time.Now().Add(30*time.Minute))
	require.NoError(t, s.SavePasswordResetToken(ctx, first))
	require.NoError(t, s.SavePasswordResetToken(ctx, second))
	require.NoError(t, s.SavePasswordResetToken(ctx, other))

	count, err := s.CountPasswordResetTokensSince(ctx, first.EmailHash, time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	require.NoError(t, s.DeletePasswordResetTokensByEmailHash(ctx, first.EmailHash))

	count, err = s.CountPasswordResetTokensSince(ctx, first.EmailHash, time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	_, err = s.GetPasswordResetByHashedToken(ctx, LockingStrengthNone, other.HashedToken)
	require.NoError(t, err, "tokens for other addresses must survive")
}

func TestSqlStoreCountPasswordResetTokensSinceHonoursWindow(t *testing.T) {
	ctx := context.Background()
	s := newPasswordResetTestStore(t)

	stale := newPasswordResetRecord(t, "user@example.com", time.Now().Add(30*time.Minute))
	stale.CreatedAt = time.Now().UTC().Add(-2 * time.Hour)
	require.NoError(t, s.SavePasswordResetToken(ctx, stale))

	fresh := newPasswordResetRecord(t, "user@example.com", time.Now().Add(30*time.Minute))
	require.NoError(t, s.SavePasswordResetToken(ctx, fresh))

	count, err := s.CountPasswordResetTokensSince(ctx, fresh.EmailHash, time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 1, count, "records older than the window must not count towards the rate limit")
}

func TestSqlStoreDeleteExpiredPasswordResetTokens(t *testing.T) {
	ctx := context.Background()
	s := newPasswordResetTestStore(t)

	expired := newPasswordResetRecord(t, "expired@example.com", time.Now().Add(-time.Minute))
	valid := newPasswordResetRecord(t, "valid@example.com", time.Now().Add(30*time.Minute))
	require.NoError(t, s.SavePasswordResetToken(ctx, expired))
	require.NoError(t, s.SavePasswordResetToken(ctx, valid))

	require.NoError(t, s.DeleteExpiredPasswordResetTokens(ctx, time.Now().UTC()))

	_, err := s.GetPasswordResetByHashedToken(ctx, LockingStrengthNone, expired.HashedToken)
	require.Error(t, err)

	_, err = s.GetPasswordResetByHashedToken(ctx, LockingStrengthNone, valid.HashedToken)
	require.NoError(t, err)
}
