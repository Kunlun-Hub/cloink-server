package version_releases

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
)

const validTestSignature = `{"signature":"AQID","key_id":"test-key","timestamp":"2026-08-14T06:00:00Z","algorithm":"ed25519","hash_algo":"sha512"}`

func TestUploadStreamsArtifactAndReturnsChecksum(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := store.NewMockStore(ctrl)
	mockPermissions := permissions.NewMockManager(ctrl)
	ctx := context.Background()
	mockPermissions.EXPECT().
		ValidateUserPermissions(gomock.Any(), "account-a", "admin-a", modules.VersionReleases, operations.Create).
		Return(true, ctx, nil)
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
}

func TestLatestReleaseRequiresChecksum(t *testing.T) {
	withChecksum := &types.VersionRelease{
		Version: "0.77.0", Platform: types.VersionReleasePlatformLinux,
		Architecture: types.VersionReleaseArchitectureAMD64, Channel: defaultChannel,
		DownloadURL: "https://download.example.com/cloink.deb", SHA256: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		IsLatest: true,
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
