package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
)

func TestVersionReleaseStoreSwitchesLatestPerTarget(t *testing.T) {
	ctx := context.Background()
	t.Setenv("NB_STORE_ENGINE_SQLITE_FILE", "version-releases.db")
	store, err := NewSqliteStore(ctx, t.TempDir(), nil, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	first := &types.VersionRelease{
		ID: "first", AccountID: "account-a", Version: "0.76.0",
		Platform: types.VersionReleasePlatformLinux, Architecture: types.VersionReleaseArchitectureAMD64,
		Channel: "stable", DownloadURL: "https://example.com/first", IsLatest: true,
	}
	second := &types.VersionRelease{
		ID: "second", AccountID: "account-a", Version: "0.77.0",
		Platform: types.VersionReleasePlatformLinux, Architecture: types.VersionReleaseArchitectureAMD64,
		Channel: "stable", DownloadURL: "https://example.com/second", IsLatest: true,
	}
	require.NoError(t, store.SaveVersionRelease(ctx, first))
	require.NoError(t, store.SaveVersionRelease(ctx, second))

	releases, err := store.ListVersionReleases(ctx, "account-a")
	require.NoError(t, err)
	require.Len(t, releases, 2)
	latest := make([]string, 0, 1)
	for _, release := range releases {
		if release.IsLatest {
			latest = append(latest, release.ID)
		}
	}
	require.Equal(t, []string{"second"}, latest)
}
