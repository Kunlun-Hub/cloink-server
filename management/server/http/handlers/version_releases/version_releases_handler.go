package version_releases

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/netbirdio/netbird/management/server/account"
	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/auth"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
	clientversion "github.com/netbirdio/netbird/version"
)

const (
	defaultChannel       = "stable"
	artifactURLPrefix    = "/api/version-releases/files/"
	maxMetadataBodyBytes = 1024 * 1024
	maxSignatureBytes    = 16 * 1024
)

var (
	channelPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,31}$`)
	checksumPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

type handler struct {
	store              store.Store
	permissionsManager permissions.Manager
	storage            *artifactStorage
}

type releaseRequest struct {
	Version      string                           `json:"version"`
	Platform     types.VersionReleasePlatform     `json:"platform"`
	Architecture types.VersionReleaseArchitecture `json:"architecture"`
	Channel      string                           `json:"channel,omitempty"`
	DownloadURL  string                           `json:"downloadUrl"`
	Description  string                           `json:"description,omitempty"`
	SHA256       string                           `json:"sha256,omitempty"`
	Signature    string                           `json:"signature,omitempty"`
	IsLatest     bool                             `json:"isLatest,omitempty"`
}

type releaseResponse struct {
	ID           string                           `json:"id"`
	Version      string                           `json:"version"`
	Platform     types.VersionReleasePlatform     `json:"platform"`
	Architecture types.VersionReleaseArchitecture `json:"architecture"`
	Channel      string                           `json:"channel"`
	DownloadURL  string                           `json:"downloadUrl"`
	Description  string                           `json:"description,omitempty"`
	SHA256       string                           `json:"sha256,omitempty"`
	Signature    string                           `json:"signature,omitempty"`
	IsLatest     bool                             `json:"isLatest,omitempty"`
	CreatedAt    time.Time                        `json:"createdAt"`
	UpdatedAt    time.Time                        `json:"updatedAt"`
}

type uploadResponse struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	DownloadURL string `json:"downloadUrl"`
}

// AddEndpoints registers version release administration and download routes.
func AddEndpoints(accountManager account.Manager, permissionsManager permissions.Manager, router *mux.Router) {
	rootDir := os.Getenv("NB_VERSION_RELEASES_DIR")
	if rootDir == "" {
		// Keep uploaded installers inside the persisted data directory so they
		// survive container rebuilds; /var/lib/netbird is the mounted volume.
		rootDir = "/var/lib/netbird/version-releases"
	}
	h := &handler{
		store:              accountManager.GetStore(),
		permissionsManager: permissionsManager,
		storage:            newArtifactStorage(rootDir),
	}
	router.HandleFunc("/version-releases", h.list).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/version-releases", h.create).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/version-releases/upload", h.upload).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/version-releases/public", h.listPublic).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/version-releases/files/{id}", h.download).Methods(http.MethodGet, http.MethodHead, http.MethodOptions)
	router.HandleFunc("/version-releases/{id}", h.get).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/version-releases/{id}", h.update).Methods(http.MethodPut, http.MethodOptions)
	router.HandleFunc("/version-releases/{id}", h.delete).Methods(http.MethodDelete, http.MethodOptions)
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Read)
	if !ok {
		return
	}
	releases, err := h.store.ListVersionReleases(ctx, userAuth.AccountId)
	if err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	util.WriteJSONObject(ctx, w, toReleaseResponses(releases))
}

func (h *handler) listPublic(w http.ResponseWriter, r *http.Request) {
	accountID := h.publicAccountID(r)
	if accountID == "" {
		util.WriteJSONObject(r.Context(), w, []releaseResponse{})
		return
	}
	releases, err := h.store.ListVersionReleases(r.Context(), accountID)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}

	platform := types.VersionReleasePlatform(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("platform"))))
	architecture := types.VersionReleaseArchitecture(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("architecture"))))
	channel := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("channel")))
	latestOnly := r.URL.Query().Get("latest") == "true"
	filtered := make([]*types.VersionRelease, 0, len(releases))
	for _, release := range releases {
		if release == nil || release.SHA256 == "" {
			continue
		}
		if platform != "" && release.Platform != platform ||
			architecture != "" && release.Architecture != architecture ||
			channel != "" && release.Channel != channel ||
			latestOnly && !release.IsLatest {
			continue
		}
		filtered = append(filtered, release)
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	util.WriteJSONObject(r.Context(), w, toReleaseResponses(filtered))
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Read)
	if !ok {
		return
	}
	release, err := h.store.GetVersionRelease(ctx, userAuth.AccountId, mux.Vars(r)["id"])
	if err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	util.WriteJSONObject(ctx, w, toReleaseResponse(release))
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Create)
	if !ok {
		return
	}
	request, ok := decodeReleaseRequest(w, r)
	if !ok {
		return
	}
	release := &types.VersionRelease{
		ID:           uuid.NewString(),
		AccountID:    userAuth.AccountId,
		Version:      request.Version,
		Platform:     request.Platform,
		Architecture: request.Architecture,
		Channel:      request.Channel,
		DownloadURL:  request.DownloadURL,
		Description:  request.Description,
		SHA256:       strings.ToLower(request.SHA256),
		Signature:    request.Signature,
		IsLatest:     request.IsLatest,
		CreatedAt:    time.Now().UTC(),
	}
	if err := h.prepareRelease(ctx, release); err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	if err := h.store.SaveVersionRelease(ctx, release); err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	util.WriteJSONObject(ctx, w, toReleaseResponse(release))
}

func (h *handler) update(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Update)
	if !ok {
		return
	}
	existing, err := h.store.GetVersionRelease(ctx, userAuth.AccountId, mux.Vars(r)["id"])
	if err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	request, ok := decodeReleaseRequest(w, r)
	if !ok {
		return
	}
	existing.Version = request.Version
	existing.Platform = request.Platform
	existing.Architecture = request.Architecture
	existing.Channel = request.Channel
	existing.DownloadURL = request.DownloadURL
	existing.Description = request.Description
	existing.SHA256 = strings.ToLower(request.SHA256)
	existing.Signature = request.Signature
	existing.IsLatest = request.IsLatest
	if err := h.prepareRelease(ctx, existing); err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	if err := h.store.SaveVersionRelease(ctx, existing); err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	util.WriteJSONObject(ctx, w, toReleaseResponse(existing))
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Delete)
	if !ok {
		return
	}
	release, err := h.store.GetVersionRelease(ctx, userAuth.AccountId, mux.Vars(r)["id"])
	if err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	if err := h.store.DeleteVersionRelease(ctx, userAuth.AccountId, release.ID); err != nil {
		util.WriteError(ctx, err, w)
		return
	}
	if release.ArtifactID != "" {
		if err := h.store.DeleteVersionReleaseArtifact(ctx, userAuth.AccountId, release.ArtifactID); err != nil {
			util.WriteError(ctx, err, w)
			return
		}
		if err := h.storage.delete(release.ArtifactID); err != nil {
			util.WriteError(ctx, status.Errorf(status.Internal, "%v", err), w)
			return
		}
	}
	util.WriteJSONObject(ctx, w, util.EmptyObject{})
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request) {
	userAuth, ctx, ok := h.authorize(w, r, operations.Create)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.storage.maxBytes+1024*1024)
	if err := r.ParseMultipartForm(32 * 1024 * 1024); err != nil {
		util.WriteError(ctx, status.Errorf(status.InvalidArgument, "invalid upload: %v", err), w)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		util.WriteError(ctx, status.Errorf(status.InvalidArgument, "installer file is required"), w)
		return
	}
	defer file.Close()

	filename := filepath.Base(strings.TrimSpace(header.Filename))
	if filename == "" || filename == "." {
		util.WriteError(ctx, status.Errorf(status.InvalidArgument, "installer filename is required"), w)
		return
	}
	artifactID := uuid.NewString()
	size, checksum, err := h.storage.save(artifactID, file)
	if err != nil {
		util.WriteError(ctx, status.Errorf(status.InvalidArgument, "%v", err), w)
		return
	}
	artifact := &types.VersionReleaseArtifact{
		ID:        artifactID,
		AccountID: userAuth.AccountId,
		FileName:  filename,
		Size:      size,
		SHA256:    checksum,
		CreatedAt: time.Now().UTC(),
	}
	if err := h.store.SaveVersionReleaseArtifact(ctx, artifact); err != nil {
		_ = h.storage.delete(artifactID)
		util.WriteError(ctx, err, w)
		return
	}
	util.WriteJSONObject(ctx, w, uploadResponse{
		ID:          artifactID,
		Filename:    filename,
		Size:        size,
		SHA256:      checksum,
		DownloadURL: artifactURLPrefix + artifactID,
	})
}

func (h *handler) download(w http.ResponseWriter, r *http.Request) {
	accountID := h.publicAccountID(r)
	if accountID == "" {
		http.NotFound(w, r)
		return
	}
	artifact, err := h.store.GetVersionReleaseArtifact(r.Context(), accountID, mux.Vars(r)["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	file, err := h.storage.open(artifact.ID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer file.Close()
	w.Header().Set("X-Checksum-Sha256", artifact.SHA256)
	w.Header().Set("Cache-Control", "public, max-age=3600, immutable")
	w.Header().Set("Content-Disposition", attachmentDisposition(artifact.FileName))
	http.ServeContent(w, r, artifact.FileName, artifact.CreatedAt, file)
}

func attachmentDisposition(filename string) string {
	filename = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, filepath.Base(filename))
	if filename == "" || filename == "." {
		filename = "download"
	}
	if disposition := mime.FormatMediaType("attachment", map[string]string{
		"filename": filename,
	}); disposition != "" {
		return disposition
	}
	return `attachment; filename="download"`
}

func (h *handler) authorize(w http.ResponseWriter, r *http.Request, operation operations.Operation) (auth.UserAuth, context.Context, bool) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return auth.UserAuth{}, r.Context(), false
	}
	if h.permissionsManager == nil {
		util.WriteError(r.Context(), status.Errorf(status.Internal, "version release permissions are not available"), w)
		return auth.UserAuth{}, r.Context(), false
	}
	allowed, ctx, err := h.permissionsManager.ValidateUserPermissions(
		r.Context(), userAuth.AccountId, userAuth.UserId, modules.VersionReleases, operation,
	)
	if err != nil {
		util.WriteError(ctx, status.NewPermissionValidationError(err), w)
		return auth.UserAuth{}, ctx, false
	}
	if !allowed {
		util.WriteError(ctx, status.NewPermissionDeniedError(), w)
		return auth.UserAuth{}, ctx, false
	}
	return userAuth, ctx, true
}

func (h *handler) prepareRelease(ctx context.Context, release *types.VersionRelease) error {
	if err := validateRelease(release); err != nil {
		return err
	}
	release.ArtifactID = ""
	if strings.HasPrefix(release.DownloadURL, artifactURLPrefix) {
		artifactID := strings.TrimPrefix(release.DownloadURL, artifactURLPrefix)
		artifact, err := h.store.GetVersionReleaseArtifact(ctx, release.AccountID, artifactID)
		if err != nil {
			return err
		}
		if release.SHA256 != "" && release.SHA256 != artifact.SHA256 {
			return status.Errorf(status.InvalidArgument, "sha256 does not match uploaded artifact")
		}
		release.ArtifactID = artifact.ID
		release.SHA256 = artifact.SHA256
	}
	if release.IsLatest && release.SHA256 == "" {
		return status.Errorf(status.InvalidArgument, "latest releases require sha256")
	}
	if release.IsLatest && release.Signature == "" {
		return status.Errorf(status.InvalidArgument, "latest releases require an Ed25519 signature")
	}
	if release.Signature != "" {
		if err := clientversion.VerifyReleaseSignature(clientversion.PublicRelease{
			Version:      release.Version,
			Platform:     string(release.Platform),
			Architecture: string(release.Architecture),
			Channel:      release.Channel,
			SHA256:       release.SHA256,
			Signature:    release.Signature,
		}); err != nil {
			return status.Errorf(status.InvalidArgument, "invalid release signature: %v", err)
		}
	}
	return nil
}

func validateRelease(release *types.VersionRelease) error {
	release.Version = strings.TrimSpace(release.Version)
	release.Channel = strings.ToLower(strings.TrimSpace(release.Channel))
	if release.Channel == "" {
		release.Channel = defaultChannel
	}
	if release.Version == "" {
		return status.Errorf(status.InvalidArgument, "version is required")
	}
	if !validPlatform(release.Platform) {
		return status.Errorf(status.InvalidArgument, "invalid platform")
	}
	if release.Architecture == "" {
		release.Architecture = types.VersionReleaseArchitectureAMD64
	}
	if !validArchitecture(release.Architecture) {
		return status.Errorf(status.InvalidArgument, "invalid architecture")
	}
	if !channelPattern.MatchString(release.Channel) {
		return status.Errorf(status.InvalidArgument, "invalid release channel")
	}
	if err := validateDownloadURL(release.DownloadURL); err != nil {
		return err
	}
	if release.SHA256 != "" && !checksumPattern.MatchString(strings.ToLower(release.SHA256)) {
		return status.Errorf(status.InvalidArgument, "sha256 must contain 64 hexadecimal characters")
	}
	if release.Signature != "" {
		if err := validateSignature(release.Signature); err != nil {
			return err
		}
	}
	return nil
}

func validateDownloadURL(downloadURL string) error {
	if strings.HasPrefix(downloadURL, artifactURLPrefix) {
		if _, err := uuid.Parse(strings.TrimPrefix(downloadURL, artifactURLPrefix)); err != nil {
			return status.Errorf(status.InvalidArgument, "invalid artifact download URL")
		}
		return nil
	}
	parsed, err := url.ParseRequestURI(downloadURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return status.Errorf(status.InvalidArgument, "downloadUrl must be an uploaded artifact or HTTPS URL")
	}
	return nil
}

func validateSignature(raw string) error {
	if len(raw) > maxSignatureBytes {
		return status.Errorf(status.InvalidArgument, "signature is too large")
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return status.Errorf(status.InvalidArgument, "signature is not valid base64")
	}
	if len(signature) != ed25519.SignatureSize {
		return status.Errorf(status.InvalidArgument, "signature must be a 64-byte Ed25519 signature")
	}
	return nil
}

func decodeReleaseRequest(w http.ResponseWriter, r *http.Request) (releaseRequest, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxMetadataBodyBytes)
	var request releaseRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		util.WriteError(r.Context(), status.Errorf(status.InvalidArgument, "invalid request body: %v", err), w)
		return releaseRequest{}, false
	}
	if request.Channel == "" {
		request.Channel = defaultChannel
	}
	return request, true
}

func (h *handler) publicAccountID(r *http.Request) string {
	accounts := h.store.GetAllAccounts(r.Context())
	validAccounts := accounts[:0]
	for _, account := range accounts {
		if account != nil {
			validAccounts = append(validAccounts, account)
		}
	}
	accounts = validAccounts
	if len(accounts) == 0 {
		return ""
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].IsDomainPrimaryAccount != accounts[j].IsDomainPrimaryAccount {
			return accounts[i].IsDomainPrimaryAccount
		}
		if !accounts[i].CreatedAt.Equal(accounts[j].CreatedAt) {
			return accounts[i].CreatedAt.Before(accounts[j].CreatedAt)
		}
		return accounts[i].Id < accounts[j].Id
	})
	return accounts[0].Id
}

func validPlatform(platform types.VersionReleasePlatform) bool {
	switch platform {
	case types.VersionReleasePlatformMacOS, types.VersionReleasePlatformWindows,
		types.VersionReleasePlatformLinux, types.VersionReleasePlatformAndroid:
		return true
	default:
		return false
	}
}

func validArchitecture(architecture types.VersionReleaseArchitecture) bool {
	switch architecture {
	case types.VersionReleaseArchitectureAMD64, types.VersionReleaseArchitectureARM64,
		types.VersionReleaseArchitectureARMv7, types.VersionReleaseArchitectureUniversal:
		return true
	default:
		return false
	}
}

func toReleaseResponses(releases []*types.VersionRelease) []releaseResponse {
	responses := make([]releaseResponse, 0, len(releases))
	for _, release := range releases {
		if release != nil {
			responses = append(responses, toReleaseResponse(release))
		}
	}
	return responses
}

func toReleaseResponse(release *types.VersionRelease) releaseResponse {
	return releaseResponse{
		ID:           release.ID,
		Version:      release.Version,
		Platform:     release.Platform,
		Architecture: release.Architecture,
		Channel:      release.Channel,
		DownloadURL:  release.DownloadURL,
		Description:  release.Description,
		SHA256:       release.SHA256,
		Signature:    release.Signature,
		IsLatest:     release.IsLatest,
		CreatedAt:    release.CreatedAt,
		UpdatedAt:    release.UpdatedAt,
	}
}
