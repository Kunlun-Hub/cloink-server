package email

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	emailmanager "github.com/netbirdio/netbird/management/server/email"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
)

type Manager interface {
	GetSettings(ctx context.Context, accountID, userID string) (*types.EmailSettings, error)
	UpdateSettings(ctx context.Context, accountID, userID string, update *types.EmailSettings) (*types.EmailSettings, error)
	SendTestEmail(ctx context.Context, accountID, userID, recipient string) error
	PreviewTemplate(ctx context.Context, accountID, userID string, kind types.EmailTemplateKind, data emailmanager.TemplateData) (*emailmanager.RenderedTemplate, error)
}

type handler struct {
	manager Manager
}

type updateSettingsRequest struct {
	Enabled            bool                           `json:"enabled"`
	Host               string                         `json:"host"`
	Port               int                            `json:"port"`
	Username           string                         `json:"username"`
	Password           *string                        `json:"password"`
	ClearPassword      bool                           `json:"clear_password"`
	FromName           string                         `json:"from_name"`
	FromEmail          string                         `json:"from_email"`
	ReplyTo            string                         `json:"reply_to"`
	Encryption         string                         `json:"encryption"`
	InsecureSkipVerify bool                           `json:"insecure_skip_verify"`
	AdminRecipients    []string                       `json:"admin_recipients"`
	Templates          map[string]types.EmailTemplate `json:"templates"`
}

type testEmailRequest struct {
	Recipient string `json:"recipient"`
}

type previewTemplateRequest struct {
	Data emailmanager.TemplateData `json:"data"`
}

func AddEndpoints(manager Manager, router *mux.Router) {
	h := &handler{manager: manager}
	router.HandleFunc("/settings/email", h.getSettings).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/settings/email", h.updateSettings).Methods(http.MethodPut, http.MethodOptions)
	router.HandleFunc("/settings/email/test", h.sendTestEmail).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/settings/email/templates/{kind}/preview", h.previewTemplate).Methods(http.MethodPost, http.MethodOptions)
}

func (h *handler) getSettings(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	settings, err := h.manager.GetSettings(r.Context(), userAuth.AccountId, userAuth.UserId)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, settings)
}

func (h *handler) updateSettings(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	var req updateSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	update := &types.EmailSettings{
		Enabled:            req.Enabled,
		Host:               req.Host,
		Port:               req.Port,
		Username:           req.Username,
		ClearPassword:      req.ClearPassword,
		FromName:           req.FromName,
		FromEmail:          req.FromEmail,
		ReplyTo:            req.ReplyTo,
		Encryption:         req.Encryption,
		InsecureSkipVerify: req.InsecureSkipVerify,
		AdminRecipients:    req.AdminRecipients,
		Templates:          req.Templates,
	}
	if req.Password != nil {
		update.Password = *req.Password
		update.PasswordUpdatePresent = true
	}

	settings, err := h.manager.UpdateSettings(r.Context(), userAuth.AccountId, userAuth.UserId, update)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, settings)
}

func (h *handler) sendTestEmail(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	var req testEmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	if err := h.manager.SendTestEmail(r.Context(), userAuth.AccountId, userAuth.UserId, req.Recipient); err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, util.EmptyObject{})
}

func (h *handler) previewTemplate(w http.ResponseWriter, r *http.Request) {
	userAuth, err := nbcontext.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	kind := types.EmailTemplateKind(mux.Vars(r)["kind"])
	if kind == "" {
		util.WriteError(r.Context(), status.Errorf(status.InvalidArgument, "template kind is required"), w)
		return
	}
	var req previewTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteErrorResponse("couldn't parse JSON request", http.StatusBadRequest, w)
		return
	}
	rendered, err := h.manager.PreviewTemplate(r.Context(), userAuth.AccountId, userAuth.UserId, kind, req.Data)
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	util.WriteJSONObject(r.Context(), w, rendered)
}
