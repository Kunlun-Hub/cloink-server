package debug_bundles

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
)

const (
	defaultRetention = 7 * 24 * time.Hour
	clientHeader     = "x-nb-client"
	clientValue      = "netbird"
)

type handler struct {
	storage            *storage
	permissionsManager permissions.Manager
}

type uploadURLResponse struct {
	URL string `json:"URL"`
	Key string `json:"Key"`
}

func AddEndpoints(permissionsManager permissions.Manager, router *mux.Router) {
	root := strings.TrimSpace(os.Getenv("NB_DEBUG_BUNDLES_DIR"))
	if root == "" {
		root = "/var/lib/netbird/debug-bundles"
	}
	h := &handler{storage: newStorage(root), permissionsManager: permissionsManager}
	if err := h.storage.cleanup(defaultRetention); err != nil {
		log.Warnf("failed to clean expired debug bundles: %v", err)
	}
	router.HandleFunc("/debug-bundles/upload-url", h.uploadURL).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/debug-bundles/upload/{namespace}/{id}", h.upload).Methods(http.MethodPut, http.MethodOptions)
	router.HandleFunc("/debug-bundles/download", h.download).Methods(http.MethodGet, http.MethodHead, http.MethodOptions)
	router.HandleFunc("/debug-bundles", h.delete).Methods(http.MethodDelete, http.MethodOptions)
}

func (h *handler) uploadURL(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(clientHeader) != clientValue {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	namespace := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("id")))
	if !validNamespaceForRequest(namespace, r) {
		http.Error(w, "invalid upload namespace", http.StatusBadRequest)
		return
	}
	if err := h.storage.cleanup(defaultRetention); err != nil {
		log.Warnf("failed to clean expired debug bundles: %v", err)
	}
	key, token, err := h.storage.reserve(namespace)
	if err != nil {
		http.Error(w, "invalid upload request", http.StatusBadRequest)
		return
	}
	scheme := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0])
	if scheme != "http" && scheme != "https" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	url := fmt.Sprintf("%s://%s/api/debug-bundles/upload/%s?token=%s", scheme, r.Host, key, token)
	util.WriteJSONObject(r.Context(), w, uploadURLResponse{URL: url, Key: key})
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["namespace"] + "/" + mux.Vars(r)["id"]
	meta, err := h.storage.put(key, r.URL.Query().Get("token"), http.MaxBytesReader(w, r.Body, maxBundleSize+1))
	if err != nil {
		http.Error(w, "debug bundle upload rejected", http.StatusBadRequest)
		return
	}
	util.WriteJSONObject(r.Context(), w, map[string]any{"key": meta.Key, "size": meta.Size, "sha256": meta.SHA256})
}

func (h *handler) download(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, operations.Read) {
		return
	}
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	file, meta, err := h.storage.open(key)
	if err != nil {
		http.Error(w, "debug bundle not found", http.StatusNotFound)
		return
	}
	defer file.Close()
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": "cloink-debug-bundle.zip"}))
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.Copy(w, file)
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, operations.Delete) {
		return
	}
	var request struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || h.storage.delete(strings.TrimSpace(request.Key)) != nil {
		http.Error(w, "debug bundle not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) authorize(w http.ResponseWriter, r *http.Request, operation operations.Operation) bool {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return false
	}
	if h.permissionsManager == nil {
		util.WriteError(r.Context(), status.Errorf(status.Internal, "debug bundle permissions are unavailable"), w)
		return false
	}
	allowed, ctx, err := h.permissionsManager.ValidateUserPermissions(r.Context(), userAuth.AccountId, userAuth.UserId, modules.Peers, operation)
	if err != nil {
		util.WriteError(ctx, status.NewPermissionValidationError(err), w)
		return false
	}
	if !allowed {
		util.WriteError(ctx, status.NewPermissionDeniedError(), w)
		return false
	}
	return true
}

func validNamespaceForRequest(namespace string, r *http.Request) bool {
	host := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0])
	if host == "" {
		host = r.Host
	}
	host = strings.TrimSuffix(host, ":443")
	if host == "" {
		return false
	}
	for _, origin := range []string{"https://" + host, "https://" + host + ":443"} {
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(origin)))
		if namespace == digest {
			return true
		}
	}
	return false
}
