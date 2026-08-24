package events

import (
	stdcontext "context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/context"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/shared/management/http/api"
	"github.com/netbirdio/netbird/shared/management/http/util"
	"github.com/netbirdio/netbird/shared/management/status"
)

const maxNetworkTrafficDetailIDLength = 1024

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
		r.Context(), userAuth.AccountId, userAuth.UserId, modules.NetworkTraffic, operations.Read,
	)
	if err != nil {
		util.WriteError(permissionCtx, status.NewPermissionValidationError(err), w)
		return
	}
	if !allowed {
		util.WriteError(permissionCtx, status.NewPermissionDeniedError(), w)
		return
	}

	grouped, windowStart, groupUserID, err := parseNetworkTrafficMode(r)
	if err != nil {
		util.WriteError(permissionCtx, err, w)
		return
	}

	var filter networktraffic.Filter
	if err := filter.ParseFromRequest(r); err != nil {
		util.WriteError(permissionCtx, status.Errorf(status.BadRequest, "%v", err), w)
		return
	}
	switch {
	case grouped:
		h.writeNetworkTrafficGroups(permissionCtx, w, userAuth.AccountId, filter)
	case windowStart != nil:
		h.writeNetworkTrafficGroupEvents(permissionCtx, w, userAuth.AccountId, filter, *windowStart, groupUserID)
	default:
		h.writeNetworkTrafficEvents(permissionCtx, w, userAuth.AccountId, filter)
	}
}

func parseNetworkTrafficMode(r *http.Request) (bool, *time.Time, string, error) {
	query := r.URL.Query()
	grouped := false
	if values, ok := query["grouped"]; ok {
		if len(values) != 1 {
			return false, nil, "", status.Errorf(status.BadRequest, "grouped must be provided once")
		}
		value, err := strconv.ParseBool(values[0])
		if err != nil {
			return false, nil, "", status.Errorf(status.BadRequest, "grouped must be a boolean")
		}
		grouped = value
	}

	windowValues, hasWindow := query["window_start"]
	userValues, hasUser := query["group_user_id"]
	detail := hasWindow || hasUser
	if grouped && detail {
		return false, nil, "", status.Errorf(status.BadRequest, "grouped cannot be combined with detail parameters")
	}
	if !detail {
		return grouped, nil, "", nil
	}
	if !hasWindow || !hasUser || len(windowValues) != 1 || len(userValues) != 1 {
		return false, nil, "", status.Errorf(status.BadRequest, "detail requests require one window_start and group_user_id")
	}

	reporterValues, hasReporter := query["reporter_id"]
	if !hasReporter || len(reporterValues) != 1 || !validNetworkTrafficDetailID(reporterValues[0], false) {
		return false, nil, "", status.Errorf(status.BadRequest, "detail requests require one valid reporter_id")
	}
	if !validNetworkTrafficDetailID(userValues[0], true) {
		return false, nil, "", status.Errorf(status.BadRequest, "group_user_id is invalid")
	}
	windowStart, err := time.Parse(time.RFC3339, windowValues[0])
	if err != nil {
		return false, nil, "", status.Errorf(status.BadRequest, "window_start must be RFC3339")
	}
	windowStart = windowStart.UTC()
	return false, &windowStart, userValues[0], nil
}

func validNetworkTrafficDetailID(value string, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	return len(value) <= maxNetworkTrafficDetailIDLength &&
		value == strings.TrimSpace(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func (h *handler) writeNetworkTrafficEvents(ctx stdcontext.Context, w http.ResponseWriter, accountID string, filter networktraffic.Filter) {
	started := time.Now()
	events, total, err := h.accountManager.GetStore().GetAccountNetworkTrafficEvents(
		ctx, store.LockingStrengthNone, accountID, filter,
	)
	if err != nil {
		recordNetworkTrafficQuery(h.metrics, ctx, "raw", started, 0, err)
		util.WriteError(ctx, err, w)
		return
	}
	recordNetworkTrafficQuery(h.metrics, ctx, "raw", started, len(events), nil)
	util.WriteJSONObject(ctx, w, networkTrafficEventsResponse(events, total, filter))
}

func (h *handler) writeNetworkTrafficGroupEvents(ctx stdcontext.Context, w http.ResponseWriter, accountID string, filter networktraffic.Filter, windowStart time.Time, groupUserID string) {
	started := time.Now()
	events, total, err := h.accountManager.GetStore().GetAccountNetworkTrafficGroupEvents(
		ctx, store.LockingStrengthNone, accountID, filter, windowStart, groupUserID, *filter.ReporterID,
	)
	if err != nil {
		recordNetworkTrafficQuery(h.metrics, ctx, "details", started, 0, err)
		util.WriteError(ctx, err, w)
		return
	}
	recordNetworkTrafficQuery(h.metrics, ctx, "details", started, len(events), nil)
	util.WriteJSONObject(ctx, w, networkTrafficEventsResponse(events, total, filter))
}

func networkTrafficEventsResponse(events []*networktraffic.Event, total int64, filter networktraffic.Filter) *api.NetworkTrafficEventsResponse {
	data := make([]api.NetworkTrafficEvent, 0, len(events))
	for _, event := range events {
		if event != nil {
			data = append(data, *event.ToAPIResponse())
		}
	}
	return &api.NetworkTrafficEventsResponse{
		Data:         data,
		Page:         filter.Page,
		PageSize:     filter.PageSize,
		TotalRecords: total,
		TotalPages:   totalPages(total, filter.PageSize),
	}
}

func (h *handler) writeNetworkTrafficGroups(ctx stdcontext.Context, w http.ResponseWriter, accountID string, filter networktraffic.Filter) {
	started := time.Now()
	groups, total, err := h.accountManager.GetStore().GetAccountNetworkTrafficGroups(
		ctx, store.LockingStrengthNone, accountID, filter,
	)
	if err != nil {
		recordNetworkTrafficQuery(h.metrics, ctx, "grouped", started, 0, err)
		util.WriteError(ctx, err, w)
		return
	}
	recordNetworkTrafficQuery(h.metrics, ctx, "grouped", started, len(groups), nil)

	data := make([]api.NetworkTrafficGroup, 0, len(groups))
	for _, group := range groups {
		if group == nil {
			continue
		}
		data = append(data, api.NetworkTrafficGroup{
			Key:         networkTrafficGroupKey(group),
			Scope:       api.NetworkTrafficGroupScopeOVERLAYDATAPLANE,
			WindowStart: group.WindowStart,
			User:        api.NetworkTrafficUser{Id: group.UserID, Name: group.UserName, Email: group.UserEmail},
			ReporterId:  group.ReporterID,
			DetailCount: group.DetailCount,
			RxBytes:     group.RxBytes,
			RxPackets:   group.RxPackets,
			TxBytes:     group.TxBytes,
			TxPackets:   group.TxPackets,
			NumOfStarts: group.NumOfStarts,
			NumOfEnds:   group.NumOfEnds,
			NumOfDrops:  group.NumOfDrops,
		})
	}
	util.WriteJSONObject(ctx, w, &api.NetworkTrafficGroupsResponse{
		Data:         data,
		Page:         filter.Page,
		PageSize:     filter.PageSize,
		TotalRecords: total,
		TotalPages:   totalPages(total, filter.PageSize),
	})
}

func networkTrafficGroupKey(group *networktraffic.Group) string {
	value := fmt.Sprintf("%q:%q:%q", group.WindowStart.UTC().Format(time.RFC3339Nano), group.UserID, group.ReporterID)
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func totalPages(total int64, pageSize int) int {
	if total <= 0 || pageSize <= 0 {
		return 0
	}
	return int(1 + (total-1)/int64(pageSize))
}
