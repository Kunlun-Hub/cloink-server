package notifications

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/http/util"
)

// Manager is the interface the handler needs from the notifications manager.
type Manager interface {
	CreateChannel(ctx context.Context, accountID, userID string, channel *types.NotificationChannel) (*types.NotificationChannel, error)
	GetChannel(ctx context.Context, accountID, userID string, id string) (*types.NotificationChannel, error)
	ListChannels(ctx context.Context, accountID, userID string) ([]*types.NotificationChannel, error)
	UpdateChannel(ctx context.Context, accountID, userID string, id string, update *types.NotificationChannel) (*types.NotificationChannel, error)
	DeleteChannel(ctx context.Context, accountID, userID string, id string) error
}

type handler struct {
	manager Manager
}

type channelRequest struct {
	Type       string                 `json:"type"`
	Enabled    bool                   `json:"enabled"`
	EventTypes []string               `json:"event_types"`
	Target     map[string]interface{} `json:"target"`
}

// AddEndpoints registers the notification channel endpoints.
// The path prefix matches what the dashboard calls: /api/integrations/notifications.
func AddEndpoints(manager Manager, router *mux.Router) {
	h := &handler{manager: manager}
	router.HandleFunc("/integrations/notifications/types", h.getTypes).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/integrations/notifications/channels", h.listChannels).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/integrations/notifications/channels", h.createChannel).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/integrations/notifications/channels/{id}", h.updateChannel).Methods(http.MethodPut, http.MethodOptions)
	router.HandleFunc("/integrations/notifications/channels/{id}", h.deleteChannel).Methods(http.MethodDelete, http.MethodOptions)
}

func (h *handler) getTypes(w http.ResponseWriter, r *http.Request) {
	// Types are static; no auth needed beyond the existing middleware.
	util.WriteJSONObject(r.Context(), w, types.NotificationEventTypes)
}

func (h *handler) listChannels(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	channels, err := h.manager.ListChannels(r.Context(), userAuth.AccountId, userAuth.UserId)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	resp := make([]map[string]interface{}, 0, len(channels))
	for _, ch := range channels {
		resp = append(resp, ch.ToAPI())
	}
	util.WriteJSONObject(r.Context(), w, resp)
}

func (h *handler) createChannel(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	var req channelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	targetJSON, err := json.Marshal(req.Target)
	if err != nil {
		util.WriteErrorResponse("couldn't parse target", http.StatusBadRequest, w)
		return
	}
	channel := &types.NotificationChannel{
		Type:       req.Type,
		Enabled:    req.Enabled,
		EventTypes: req.EventTypes,
		Target:     string(targetJSON),
	}
	created, err := h.manager.CreateChannel(r.Context(), userAuth.AccountId, userAuth.UserId, channel)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, created.ToAPI())
}

func (h *handler) updateChannel(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	id := mux.Vars(r)["id"]
	var req channelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	targetJSON, err := json.Marshal(req.Target)
	if err != nil {
		util.WriteErrorResponse("couldn't parse target", http.StatusBadRequest, w)
		return
	}
	update := &types.NotificationChannel{
		Type:       req.Type,
		Enabled:    req.Enabled,
		EventTypes: req.EventTypes,
		Target:     string(targetJSON),
	}
	updated, err := h.manager.UpdateChannel(r.Context(), userAuth.AccountId, userAuth.UserId, id, update)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, updated.ToAPI())
}

func (h *handler) deleteChannel(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	id := mux.Vars(r)["id"]
	if err := h.manager.DeleteChannel(r.Context(), userAuth.AccountId, userAuth.UserId, id); err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, struct{}{})
}
