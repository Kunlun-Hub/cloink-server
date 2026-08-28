package store

import (
	"context"
	"testing"
	"time"

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

func TestVersionReleaseArtifactOrphanCleanup(t *testing.T) {
	ctx := context.Background()
	t.Setenv("NB_STORE_ENGINE_SQLITE_FILE", "version-release-artifacts.db")
	store, err := NewSqliteStore(ctx, t.TempDir(), nil, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	old := time.Now().UTC().Add(-48 * time.Hour)
	linked := &types.VersionReleaseArtifact{ID: "linked", AccountID: "account-a", FileName: "linked.bin", Size: 1, SHA256: "a", CreatedAt: old}
	orphan := &types.VersionReleaseArtifact{ID: "orphan", AccountID: "account-a", FileName: "orphan.bin", Size: 1, SHA256: "b", CreatedAt: old}
	recent := &types.VersionReleaseArtifact{ID: "recent", AccountID: "account-a", FileName: "recent.bin", Size: 1, SHA256: "c", CreatedAt: time.Now().UTC()}
	for _, artifact := range []*types.VersionReleaseArtifact{linked, orphan, recent} {
		require.NoError(t, store.SaveVersionReleaseArtifact(ctx, artifact))
	}
	require.NoError(t, store.SaveVersionRelease(ctx, &types.VersionRelease{
		ID: "release", AccountID: "account-a", Version: "0.77.1", Platform: types.VersionReleasePlatformLinux,
		Architecture: types.VersionReleaseArchitectureAMD64, Channel: "stable", DownloadURL: artifactURLPrefixForStoreTest + linked.ID,
		ArtifactID: linked.ID,
	}))

	orphans, err := store.ListOrphanedVersionReleaseArtifacts(ctx, time.Now().UTC().Add(-24*time.Hour))
	require.NoError(t, err)
	require.Len(t, orphans, 1)
	require.Equal(t, orphan.ID, orphans[0].ID)

	deleted, err := store.DeleteVersionReleaseArtifactIfUnreferenced(ctx, "account-a", linked.ID)
	require.NoError(t, err)
	require.False(t, deleted)
	deleted, err = store.DeleteVersionReleaseArtifactIfUnreferenced(ctx, "account-a", orphan.ID)
	require.NoError(t, err)
	require.True(t, deleted)
}

const artifactURLPrefixForStoreTest = "/api/version-releases/files/"
