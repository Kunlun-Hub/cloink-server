package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
)

func TestSqlStore_UpdateUserInviteTokenIfMatches(t *testing.T) {
	runTestForAllEngines(t, "", func(t *testing.T, testStore Store) {
		if testStore == nil {
			t.Skip("store is nil")
		}

		ctx := context.Background()
		originalExpiry := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
		original := &types.UserInviteRecord{
			ID:          "invite-conditional-restore",
			AccountID:   "account-conditional-restore",
			Email:       "restore@example.com",
			Name:        "Restore User",
			Role:        string(types.UserRoleUser),
			AutoGroups:  []string{},
			HashedToken: "original-token",
			ExpiresAt:   originalExpiry,
			CreatedAt:   time.Now().UTC(),
			CreatedBy:   "original-admin",
		}
		require.NoError(t, testStore.SaveUserInvite(ctx, original))

		current := original.Copy()
		current.HashedToken = "current-token"
		current.ExpiresAt = originalExpiry.Add(24 * time.Hour)
		current.CreatedBy = "resend-admin"
		require.NoError(t, testStore.SaveUserInvite(ctx, current))

		restored, err := testStore.UpdateUserInviteTokenIfMatches(ctx, original, "different-token")
		require.NoError(t, err)
		require.False(t, restored)

		persisted, err := testStore.GetUserInviteByID(ctx, LockingStrengthNone, original.AccountID, original.ID)
		require.NoError(t, err)
		require.Equal(t, current.HashedToken, persisted.HashedToken)

		restored, err = testStore.UpdateUserInviteTokenIfMatches(ctx, original, current.HashedToken)
		require.NoError(t, err)
		require.True(t, restored)

		persisted, err = testStore.GetUserInviteByID(ctx, LockingStrengthNone, original.AccountID, original.ID)
		require.NoError(t, err)
		require.Equal(t, original.HashedToken, persisted.HashedToken)
		require.WithinDuration(t, original.ExpiresAt, persisted.ExpiresAt, time.Second)
		require.Equal(t, original.CreatedBy, persisted.CreatedBy)
	})
}
