package events

import (
	stdcontext "context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/shared/auth"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
)

// flowRetentionResponse is the API representation of the global flow retention.
type flowRetentionResponse struct {
	// RetentionDays is how long flow events are kept before cleanup.
	RetentionDays float64 `json:"retention_days"`
}

// flowRetentionRequest is the API payload for updating the global flow retention.
type flowRetentionRequest struct {
	RetentionDays float64 `json:"retention_days"`
}

// getFlowRetention returns the global flow event retention setting.
func (h *handler) getFlowRetention(w http.ResponseWriter, r *http.Request) {
	permissionCtx, userAuth, ok := h.checkNetworkTrafficPermission(w, r, operations.Read)
	if !ok {
		return
	}
	_ = userAuth

	retention, err := h.accountManager.GetStore().GetFlowRetention(permissionCtx)
	if err != nil {
		// FileStore and other non-SQL stores do not persist retention;
		// fall back to the environment default instead of failing.
		retention = networktraffic.FlowRetention()
	}
	if retention <= 0 {
		retention = networktraffic.FlowRetention()
	}
	util.WriteJSONObject(permissionCtx, w, flowRetentionResponse{
		RetentionDays: retention.Hours() / 24,
	})
}

// updateFlowRetention updates the global flow event retention setting.
// The flow cleanup worker picks up the new value on its next cycle.
func (h *handler) updateFlowRetention(w http.ResponseWriter, r *http.Request) {
	permissionCtx, userAuth, ok := h.checkNetworkTrafficPermission(w, r, operations.Update)
	if !ok {
		return
	}
	_ = userAuth

	var req flowRetentionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(permissionCtx, status.Errorf(status.BadRequest, "invalid request body: %v", err), w)
		return
	}
	if req.RetentionDays <= 0 || req.RetentionDays > 3650 {
		util.WriteError(permissionCtx, status.Errorf(status.BadRequest, "retention_days must be between 0 and 3650"), w)
		return
	}
	retention := time.Duration(req.RetentionDays * 24 * float64(time.Hour))
	if err := h.accountManager.GetStore().SetFlowRetention(permissionCtx, retention); err != nil {
		util.WriteError(permissionCtx, err, w)
		return
	}
	util.WriteJSONObject(permissionCtx, w, flowRetentionResponse{
		RetentionDays: req.RetentionDays,
	})
}

// checkNetworkTrafficPermission validates the user and their permission for
// the network traffic module, returning the permission context on success.
func (h *handler) checkNetworkTrafficPermission(w http.ResponseWriter, r *http.Request, op operations.Operation) (stdcontext.Context, auth.UserAuth, bool) {
	userAuth, err := context.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return nil, auth.UserAuth{}, false
	}
	if h.permissionsManager == nil {
		util.WriteError(r.Context(), status.NewPermissionDeniedError(), w)
		return nil, auth.UserAuth{}, false
	}
	allowed, permissionCtx, err := h.permissionsManager.ValidateUserPermissions(
		r.Context(), userAuth.AccountId, userAuth.UserId, modules.NetworkTraffic, op,
	)
	if err != nil {
		util.WriteError(permissionCtx, status.NewPermissionValidationError(err), w)
		return nil, auth.UserAuth{}, false
	}
	if !allowed {
		util.WriteError(permissionCtx, status.NewPermissionDeniedError(), w)
		return nil, auth.UserAuth{}, false
	}
	return permissionCtx, userAuth, true
}
