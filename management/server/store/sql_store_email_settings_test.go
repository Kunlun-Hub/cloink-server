package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/util/crypt"
)

func TestSqlStoreEmailSettingsMigrationAndEncryption(t *testing.T) {
	t.Setenv("NETBIRD_STORE_ENGINE", string(types.SqliteStoreEngine))

	s, cleanup, err := NewTestStoreFromSQL(context.Background(), "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	sqlStore, ok := s.(*SqlStore)
	require.True(t, ok, "SQLite test store should use SqlStore")
	require.True(t, sqlStore.db.Migrator().HasTable(&types.EmailSettings{}), "email_settings table must be created by AutoMigrate")

	key, err := crypt.GenerateKey()
	require.NoError(t, err)
	fieldEncrypt, err := crypt.NewFieldEncrypt(key)
	require.NoError(t, err)
	s.SetFieldEncrypt(fieldEncrypt)

	accountID := "email-settings-account"
	settings := &types.EmailSettings{
		AccountID:  accountID,
		Enabled:    true,
		Host:       "smtp.example.com",
		Port:       587,
		FromEmail:  "no-reply@example.com",
		Encryption: types.EmailEncryptionStartTLS,
		Password:   "smtp-secret",
	}
	require.NoError(t, s.SaveEmailSettings(context.Background(), settings))

	var raw struct {
		PasswordEncrypted string `gorm:"column:password_encrypted"`
	}
	require.NoError(t, sqlStore.db.Table("email_settings").Select("password_encrypted").Where("account_id = ?", accountID).Take(&raw).Error)
	require.NotEmpty(t, raw.PasswordEncrypted)
	require.NotEqual(t, "smtp-secret", raw.PasswordEncrypted, "SMTP password must not be persisted in plaintext")

	loaded, err := s.GetEmailSettings(context.Background(), LockingStrengthNone, accountID)
	require.NoError(t, err)
	require.Equal(t, "smtp-secret", loaded.Password)
	require.True(t, loaded.PasswordConfigured)

	missing, err := s.GetEmailSettings(context.Background(), LockingStrengthNone, "missing-account")
	require.NoError(t, err)
	require.Equal(t, 587, missing.Port)
	require.Equal(t, types.EmailEncryptionStartTLS, missing.Encryption)
}
