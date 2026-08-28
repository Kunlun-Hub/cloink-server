package version_releases

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/auth"
	clientversion "github.com/netbirdio/netbird/version"
)

const validTestSignature = "ygHmBMLUYsS7Dy4bQf9gmz0sighEg4z5ZzOjomJSFq2ufKMvWXpIijffyDohtHnWhjKTW/UyluShW92rgQgHCQ=="

func TestUploadStreamsArtifactAndReturnsChecksum(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	mockPermissions := permissions.NewMockManager(ctrl)
	ctx := context.Background()
	mockPermissions.EXPECT().
		ValidateUserPermissions(gomock.Any(), "account-a", "admin-a", modules.VersionReleases, operations.Create).
		Return(true, ctx, nil)
	mockStore.EXPECT().ListOrphanedVersionReleaseArtifacts(ctx, gomock.Any()).Return(nil, nil)
	mockStore.EXPECT().SaveVersionReleaseArtifact(ctx, gomock.Any()).DoAndReturn(
		func(_ context.Context, artifact *types.VersionReleaseArtifact) error {
			require.Equal(t, "cloink-linux-amd64.deb", artifact.FileName)
			require.Equal(t, int64(11), artifact.Size)
			require.Equal(t, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", artifact.SHA256)
			return nil
		},
	)

	h := &handler{
		store:              mockStore,
		permissionsManager: mockPermissions,
		storage:            newArtifactStorage(t.TempDir()),
	}
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "cloink-linux-amd64.deb")
	require.NoError(t, err)
	_, err = io.WriteString(part, "hello world")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/version-releases/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = withUserAuth(req)
	recorder := httptest.NewRecorder()
	h.upload(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response uploadResponse
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	require.NotEmpty(t, response.ID)
	require.Equal(t, artifactURLPrefix+response.ID, response.DownloadURL)
}

func TestDeleteReleaseRemovesUnreferencedArtifactFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	mockPermissions := permissions.NewMockManager(ctrl)
	ctx := context.Background()
	artifactID := uuid.NewString()
	releaseID := uuid.NewString()
	artifact := &types.VersionReleaseArtifact{ID: artifactID, AccountID: "account-a", FileName: "installer.bin"}
	mockPermissions.EXPECT().
		ValidateUserPermissions(gomock.Any(), "account-a", "admin-a", modules.VersionReleases, operations.Delete).
		Return(true, ctx, nil)
	mockStore.EXPECT().GetVersionRelease(ctx, "account-a", releaseID).Return(&types.VersionRelease{
		ID: releaseID, AccountID: "account-a", ArtifactID: artifactID,
	}, nil)
	mockStore.EXPECT().DeleteVersionRelease(ctx, "account-a", releaseID).Return(nil)
	mockStore.EXPECT().GetVersionReleaseArtifact(ctx, "account-a", artifactID).Return(artifact, nil)
	mockStore.EXPECT().DeleteVersionReleaseArtifactIfUnreferenced(ctx, "account-a", artifactID).Return(true, nil)

	storage := newArtifactStorage(t.TempDir())
	_, _, err := storage.save(artifactID, bytes.NewBufferString("installer"))
	require.NoError(t, err)
	h := &handler{store: mockStore, permissionsManager: mockPermissions, storage: storage}
	req := mux.SetURLVars(withUserAuth(httptest.NewRequest(http.MethodDelete, "/version-releases/"+releaseID, nil)), map[string]string{"id": releaseID})
	recorder := httptest.NewRecorder()
	h.delete(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	_, err = storage.open(artifactID)
	require.Error(t, err)
}

func TestRejectedReleaseRemovesUploadedArtifact(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	mockPermissions := permissions.NewMockManager(ctrl)
	ctx := context.Background()
	artifactID := uuid.NewString()
	checksum := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	artifact := &types.VersionReleaseArtifact{ID: artifactID, AccountID: "account-a", FileName: "installer.bin", SHA256: checksum}
	mockPermissions.EXPECT().
		ValidateUserPermissions(gomock.Any(), "account-a", "admin-a", modules.VersionReleases, operations.Create).
		Return(true, ctx, nil)
	mockStore.EXPECT().GetVersionReleaseArtifact(ctx, "account-a", artifactID).Return(artifact, nil).Times(2)
	mockStore.EXPECT().DeleteVersionReleaseArtifactIfUnreferenced(ctx, "account-a", artifactID).Return(true, nil)

	storage := newArtifactStorage(t.TempDir())
	_, _, err := storage.save(artifactID, bytes.NewBufferString("installer"))
	require.NoError(t, err)
	h := &handler{store: mockStore, permissionsManager: mockPermissions, storage: storage}
	body := bytes.NewBufferString(`{"version":"0.77.1","platform":"linux","architecture":"amd64","downloadUrl":"` + artifactURLPrefix + artifactID + `","isLatest":true}`)
	req := withUserAuth(httptest.NewRequest(http.MethodPost, "/version-releases", body))
	recorder := httptest.NewRecorder()
	h.create(recorder, req)

	require.Equal(t, http.StatusPreconditionFailed, recorder.Code, recorder.Body.String())
	_, err = storage.open(artifactID)
	require.Error(t, err)
}

func TestCreateSignedLatestRelease(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	mockPermissions := permissions.NewMockManager(ctrl)
	ctx := context.Background()
	artifactID := uuid.NewString()
	checksum := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	mockPermissions.EXPECT().
		ValidateUserPermissions(gomock.Any(), "account-a", "admin-a", modules.VersionReleases, operations.Create).
		Return(true, ctx, nil)
	mockStore.EXPECT().GetVersionReleaseArtifact(ctx, "account-a", artifactID).Return(&types.VersionReleaseArtifact{
		ID: artifactID, AccountID: "account-a", SHA256: checksum,
	}, nil)
	mockStore.EXPECT().SaveVersionRelease(ctx, gomock.Any()).DoAndReturn(
		func(_ context.Context, release *types.VersionRelease) error {
			require.True(t, release.IsLatest)
			require.Equal(t, defaultChannel, release.Channel)
			require.Equal(t, checksum, release.SHA256)
			require.Equal(t, artifactID, release.ArtifactID)
			return nil
		},
	)

	h := &handler{store: mockStore, permissionsManager: mockPermissions, storage: newArtifactStorage(t.TempDir())}
	body := bytes.NewBufferString(`{"version":"0.77.0","platform":"linux","architecture":"amd64","downloadUrl":"` + artifactURLPrefix + artifactID + `","signature":` + quoteJSON(validTestSignature) + `,"isLatest":true}`)
	req := withUserAuth(httptest.NewRequest(http.MethodPost, "/version-releases", body))
	recorder := httptest.NewRecorder()
	h.create(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPublicListExcludesReleasesWithoutChecksum(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	mockStore.EXPECT().GetAllAccounts(gomock.Any()).Return([]*types.Account{{
		Id: "account-a", IsDomainPrimaryAccount: true,
	}})
	mockStore.EXPECT().ListVersionReleases(gomock.Any(), "account-a").Return([]*types.VersionRelease{
		{ID: "unsigned", Platform: types.VersionReleasePlatformLinux, Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel, SHA256: "abc", IsLatest: true},
		{ID: "draft", Platform: types.VersionReleasePlatformLinux, Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel, IsLatest: false},
	}, nil)

	h := &handler{store: mockStore}
	req := httptest.NewRequest(http.MethodGet, "/version-releases/public?platform=linux&architecture=amd64&channel=stable&latest=true", nil)
	recorder := httptest.NewRecorder()
	h.listPublic(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response []releaseResponse
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	require.Len(t, response, 1)
	require.Equal(t, "unsigned", response[0].ID)
}

func TestDownloadUsesPrimaryAccountArtifact(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	artifactID := uuid.NewString()
	createdAt := time.Date(2026, time.August, 14, 6, 0, 0, 0, time.UTC)
	mockStore.EXPECT().GetAllAccounts(gomock.Any()).Return([]*types.Account{{Id: "account-a", IsDomainPrimaryAccount: true}})
	mockStore.EXPECT().GetVersionReleaseArtifact(gomock.Any(), "account-a", artifactID).Return(&types.VersionReleaseArtifact{
		ID: artifactID, AccountID: "account-a", FileName: "cloink.exe", SHA256: "checksum", CreatedAt: createdAt,
	}, nil)

	storage := newArtifactStorage(t.TempDir())
	_, _, err := storage.save(artifactID, bytes.NewBufferString("installer"))
	require.NoError(t, err)
	h := &handler{store: mockStore, storage: storage}
	req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, artifactURLPrefix+artifactID, nil), map[string]string{"id": artifactID})
	recorder := httptest.NewRecorder()
	h.download(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "installer", recorder.Body.String())
	require.Equal(t, "checksum", recorder.Header().Get("X-Checksum-Sha256"))
	mediaType, params, err := mime.ParseMediaType(recorder.Header().Get("Content-Disposition"))
	require.NoError(t, err)
	require.Equal(t, "attachment", mediaType)
	require.Equal(t, "cloink.exe", params["filename"])
}

func TestDownloadSanitizesArtifactFilename(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	artifactID := uuid.NewString()
	mockStore.EXPECT().GetAllAccounts(gomock.Any()).Return([]*types.Account{{Id: "account-a", IsDomainPrimaryAccount: true}})
	mockStore.EXPECT().GetVersionReleaseArtifact(gomock.Any(), "account-a", artifactID).Return(&types.VersionReleaseArtifact{
		ID: artifactID, AccountID: "account-a", FileName: "installer\r\nX-Injected: yes.exe", SHA256: "checksum",
	}, nil)

	storage := newArtifactStorage(t.TempDir())
	_, _, err := storage.save(artifactID, bytes.NewBufferString("installer"))
	require.NoError(t, err)
	h := &handler{store: mockStore, storage: storage}
	req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, artifactURLPrefix+artifactID, nil), map[string]string{"id": artifactID})
	recorder := httptest.NewRecorder()
	h.download(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	disposition := recorder.Header().Get("Content-Disposition")
	require.NotContains(t, disposition, "\r")
	require.NotContains(t, disposition, "\n")
	mediaType, params, err := mime.ParseMediaType(disposition)
	require.NoError(t, err)
	require.Equal(t, "attachment", mediaType)
	require.Equal(t, "installerX-Injected: yes.exe", params["filename"])
}

func TestLatestReleaseRequiresChecksum(t *testing.T) {
	withChecksum := &types.VersionRelease{
		Version: "0.77.0", Platform: types.VersionReleasePlatformLinux,
		Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel,
		DownloadURL: "https://download.example.com/cloink.deb", SHA256: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		Signature: validTestSignature, IsLatest: true,
	}
	h := &handler{}
	require.NoError(t, h.prepareRelease(context.Background(), withChecksum))

	withoutChecksum := &types.VersionRelease{
		Version: "0.77.0", Platform: types.VersionReleasePlatformLinux,
		Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel,
		DownloadURL: "https://download.example.com/cloink.deb",
		IsLatest:    true,
	}
	err := h.prepareRelease(context.Background(), withoutChecksum)
	require.ErrorContains(t, err, "require sha256")
}

func TestPrepareReleaseAutomaticallySignsMetadata(t *testing.T) {
	release := &types.VersionRelease{
		Version: "0.77.0", Platform: types.VersionReleasePlatformLinux,
		Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel,
		DownloadURL: "https://download.example.com/cloink.deb",
		SHA256:      "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		IsLatest:    true,
	}
	h := &handler{signer: func(metadata clientversion.PublicRelease) (string, error) {
		require.Equal(t, release.Version, metadata.Version)
		require.Equal(t, string(release.Platform), metadata.Platform)
		require.Equal(t, string(release.Architecture), metadata.Architecture)
		require.Equal(t, release.Channel, metadata.Channel)
		require.Equal(t, release.SHA256, metadata.SHA256)
		return validTestSignature, nil
	}}

	require.NoError(t, h.prepareRelease(context.Background(), release))
	require.Equal(t, validTestSignature, release.Signature)
}

func TestLatestReleaseRequiresAutomaticSignerOrManualSignature(t *testing.T) {
	release := &types.VersionRelease{
		Version: "0.77.0", Platform: types.VersionReleasePlatformLinux,
		Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel,
		DownloadURL: "https://download.example.com/cloink.deb",
		SHA256:      "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		IsLatest:    true,
	}

	err := (&handler{}).prepareRelease(context.Background(), release)
	require.ErrorContains(t, err, "automatic release signing is not configured")
}

func withUserAuth(req *http.Request) *http.Request {
	return nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: "account-a", UserId: "admin-a"})
}

func quoteJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func TestArtifactStorageRejectsInvalidID(t *testing.T) {
	storage := newArtifactStorage(filepath.Join(t.TempDir(), "artifacts"))
	_, _, err := storage.save("../escape", bytes.NewBufferString("data"))
	require.Error(t, err)
}
