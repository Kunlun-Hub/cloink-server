package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/util/crypt"
)

func newEventStreamingTestStore(t *testing.T) *SqlStore {
	t.Helper()
	t.Setenv("NETBIRD_STORE_ENGINE", string(types.SqliteStoreEngine))

	s, cleanup, err := NewTestStoreFromSQL(context.Background(), "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)

	sqlStore, ok := s.(*SqlStore)
	require.True(t, ok)
	require.True(t, sqlStore.db.Migrator().HasTable(&types.EventStreamingIntegration{}))
	require.True(t, sqlStore.db.Migrator().HasTable(&types.EventStreamingCursor{}))

	key, err := crypt.GenerateKey()
	require.NoError(t, err)
	fieldEncrypt, err := crypt.NewFieldEncrypt(key)
	require.NoError(t, err)
	s.SetFieldEncrypt(fieldEncrypt)

	return sqlStore
}

func TestSqlStoreEventStreamingIntegrationCRUD(t *testing.T) {
	s := newEventStreamingTestStore(t)
	ctx := context.Background()
	accountID := "es-account-1"

	created, err := s.CreateEventStreamingIntegration(ctx, &types.EventStreamingIntegration{
		AccountID: accountID,
		Platform:  types.EventStreamingPlatformDatadog,
		Enabled:   true,
		Config: map[string]string{
			"api_key": "dd-secret-key",
			"api_url": "https://http-intake.logs.datadoghq.eu/api/v2/logs",
		},
	})
	require.NoError(t, err)
	require.NotZero(t, created.ID)

	// config must not be persisted in plaintext
	var raw struct {
		ConfigEncrypted string `gorm:"column:config_encrypted"`
	}
	require.NoError(t, s.db.Table("event_streaming_integrations").Select("config_encrypted").Where("id = ?", created.ID).Take(&raw).Error)
	require.NotEmpty(t, raw.ConfigEncrypted)
	require.NotContains(t, raw.ConfigEncrypted, "dd-secret-key")

	// get decrypts
	got, err := s.GetEventStreamingIntegration(ctx, LockingStrengthNone, accountID, created.ID)
	require.NoError(t, err)
	require.Equal(t, "dd-secret-key", got.Config["api_key"])
	require.Equal(t, "https://http-intake.logs.datadoghq.eu/api/v2/logs", got.Config["api_url"])

	// list
	list, err := s.ListEventStreamingIntegrations(ctx, LockingStrengthNone, accountID)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "dd-secret-key", list[0].Config["api_key"])

	// other account sees nothing
	other, err := s.ListEventStreamingIntegrations(ctx, LockingStrengthNone, "other-account")
	require.NoError(t, err)
	require.Empty(t, other)

	// update
	got.Enabled = false
	got.Config["api_url"] = "https://http-intake.logs.datadoghq.com/api/v2/logs"
	require.NoError(t, s.UpdateEventStreamingIntegration(ctx, got))
	updated, err := s.GetEventStreamingIntegration(ctx, LockingStrengthNone, accountID, created.ID)
	require.NoError(t, err)
	require.False(t, updated.Enabled)
	require.Equal(t, "https://http-intake.logs.datadoghq.com/api/v2/logs", updated.Config["api_url"])
	require.Equal(t, "dd-secret-key", updated.Config["api_key"])

	// cursor roundtrip
	cursor, err := s.GetEventStreamingCursor(ctx, accountID, created.ID)
	require.NoError(t, err)
	require.Zero(t, cursor.ActivityID)
	cursor.ActivityID = 42
	require.NoError(t, s.SaveEventStreamingCursor(ctx, cursor))
	cursor2, err := s.GetEventStreamingCursor(ctx, accountID, created.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(42), cursor2.ActivityID)

	// delete removes integration and cursor
	require.NoError(t, s.DeleteEventStreamingIntegration(ctx, accountID, created.ID))
	_, err = s.GetEventStreamingIntegration(ctx, LockingStrengthNone, accountID, created.ID)
	require.Error(t, err)
	cursor3, err := s.GetEventStreamingCursor(ctx, accountID, created.ID)
	require.NoError(t, err)
	require.Zero(t, cursor3.ActivityID)

	// delete missing -> NotFound
	require.Error(t, s.DeleteEventStreamingIntegration(ctx, accountID, 99999))
}

func TestSqlStoreEventStreamingIntegrationRequiresEncryption(t *testing.T) {
	t.Setenv("NETBIRD_STORE_ENGINE", string(types.SqliteStoreEngine))
	s, cleanup, err := NewTestStoreFromSQL(context.Background(), "", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(cleanup)
	// no SetFieldEncrypt: saving credentials must fail loudly, not silently plaintext
	_, err = s.CreateEventStreamingIntegration(context.Background(), &types.EventStreamingIntegration{
		AccountID: "es-account-2",
		Platform:  types.EventStreamingPlatformS3,
		Config:    map[string]string{"secret_key": "shhh"},
	})
	require.Error(t, err)
}
