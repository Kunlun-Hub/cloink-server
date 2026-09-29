package eventstreaming

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/http/util"
)

// Manager is the interface the handler needs from the eventstreaming manager.
type Manager interface {
	CreateIntegration(ctx context.Context, accountID, userID string, integration *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error)
	GetIntegration(ctx context.Context, accountID, userID string, id uint64) (*types.EventStreamingIntegration, error)
	ListIntegrations(ctx context.Context, accountID, userID string) ([]*types.EventStreamingIntegration, error)
	UpdateIntegration(ctx context.Context, accountID, userID string, id uint64, update *types.EventStreamingIntegration) (*types.EventStreamingIntegration, error)
	DeleteIntegration(ctx context.Context, accountID, userID string, id uint64) error
}

type handler struct {
	manager Manager
}

type createIntegrationRequest struct {
	Platform string            `json:"platform"`
	Enabled  bool              `json:"enabled"`
	Config   map[string]string `json:"config"`
}

type updateIntegrationRequest struct {
	Enabled bool              `json:"enabled"`
	Config  map[string]string `json:"config"`
}

// AddEndpoints registers the event streaming CRUD endpoints.
// The path prefix matches what the dashboard calls: /api/integrations/event-streaming.
func AddEndpoints(manager Manager, router *mux.Router) {
	h := &handler{manager: manager}
	router.HandleFunc("/integrations/event-streaming", h.listIntegrations).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/integrations/event-streaming", h.createIntegration).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/integrations/event-streaming/{id}", h.getIntegration).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/integrations/event-streaming/{id}", h.updateIntegration).Methods(http.MethodPut, http.MethodOptions)
	router.HandleFunc("/integrations/event-streaming/{id}", h.deleteIntegration).Methods(http.MethodDelete, http.MethodOptions)
}

func (h *handler) createIntegration(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	var req createIntegrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	integration := &types.EventStreamingIntegration{
		Platform: req.Platform,
		Enabled:  req.Enabled,
		Config:   req.Config,
	}
	created, err := h.manager.CreateIntegration(r.Context(), userAuth.AccountId, userAuth.UserId, integration)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, toResponse(created))
}

func (h *handler) listIntegrations(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	integrations, err := h.manager.ListIntegrations(r.Context(), userAuth.AccountId, userAuth.UserId)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	resp := make([]integrationResponse, 0, len(integrations))
	for _, integration := range integrations {
		resp = append(resp, toResponse(integration))
	}
	util.WriteJSONObject(r.Context(), w, resp)
}

func (h *handler) getIntegration(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	integration, err := h.manager.GetIntegration(r.Context(), userAuth.AccountId, userAuth.UserId, id)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, toResponse(integration))
}

func (h *handler) updateIntegration(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req updateIntegrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	update := &types.EventStreamingIntegration{
		Enabled: req.Enabled,
		Config:  req.Config,
	}
	updated, err := h.manager.UpdateIntegration(r.Context(), userAuth.AccountId, userAuth.UserId, id, update)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, toResponse(updated))
}

func (h *handler) deleteIntegration(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := h.manager.DeleteIntegration(r.Context(), userAuth.AccountId, userAuth.UserId, id); err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, struct{}{})
}

func parseID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	id, err := strconv.ParseUint(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		util.WriteErrorResponse("invalid integration id", http.StatusBadRequest, w)
		return 0, false
	}
	return id, true
}

type integrationResponse struct {
	ID        uint64            `json:"id"`
	AccountID string            `json:"account_id"`
	Platform  string            `json:"platform"`
	Enabled   bool              `json:"enabled"`
	Config    map[string]string `json:"config,omitempty"`
	CreatedAt string            `json:"created_at"`
	UpdatedAt string            `json:"updated_at"`
}

func toResponse(integration *types.EventStreamingIntegration) integrationResponse {
	return integrationResponse{
		ID:        integration.ID,
		AccountID: integration.AccountID,
		Platform:  integration.Platform,
		Enabled:   integration.Enabled,
		Config:    integration.Config,
		CreatedAt: integration.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UpdatedAt: integration.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}
