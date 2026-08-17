package events

import (
	"net/http"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/shared/management/http/api"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
)

func (h *handler) getAllNetworkTrafficEvents(w http.ResponseWriter, r *http.Request) {
	userAuth, err := context.GetUserAuthFromContext(r.Context())
	if err != nil {
		util.WriteError(r.Context(), err, w)
		return
	}
	if h.permissionsManager == nil {
		util.WriteError(r.Context(), status.NewPermissionDeniedError(), w)
		return
	}
	allowed, permissionCtx, err := h.permissionsManager.ValidateUserPermissions(
		r.Context(), userAuth.AccountId, userAuth.UserId, modules.Events, operations.Read,
	)
	if err != nil {
		util.WriteError(permissionCtx, status.NewPermissionValidationError(err), w)
		return
	}
	if !allowed {
		util.WriteError(permissionCtx, status.NewPermissionDeniedError(), w)
		return
	}

	var filter networktraffic.Filter
	filter.ParseFromRequest(r)
	events, total, err := h.accountManager.GetStore().GetAccountNetworkTrafficEvents(
		permissionCtx, store.LockingStrengthNone, userAuth.AccountId, filter,
	)
	if err != nil {
		util.WriteError(permissionCtx, err, w)
		return
	}

	data := make([]api.NetworkTrafficEvent, 0, len(events))
	for _, event := range events {
		if event != nil {
			data = append(data, *event.ToAPIResponse())
		}
	}
	util.WriteJSONObject(permissionCtx, w, &api.NetworkTrafficEventsResponse{
		Data:         data,
		Page:         filter.Page,
		PageSize:     filter.PageSize,
		TotalRecords: int(total),
		TotalPages:   totalPages(total, filter.PageSize),
	})
}

func totalPages(total int64, pageSize int) int {
	if total <= 0 || pageSize <= 0 {
		return 0
	}
	return int((total + int64(pageSize) - 1) / int64(pageSize))
}
