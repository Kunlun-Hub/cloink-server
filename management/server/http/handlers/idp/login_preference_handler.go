package idp

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/mux"

	"github.com/netbirdio/netbird/management/server/account"
	idpmanager "github.com/netbirdio/netbird/management/server/idp"
	"github.com/netbirdio/netbird/management/server/types"
)

type loginPreferenceHandler struct {
	accountManager account.Manager
	embeddedIDP    *idpmanager.EmbeddedIdPManager
	dexHandler     http.Handler
}

func NewLoginPreferenceHandler(accountManager account.Manager, embeddedIDP *idpmanager.EmbeddedIdPManager) http.Handler {
	return &loginPreferenceHandler{
		accountManager: accountManager,
		embeddedIDP:    embeddedIDP,
		dexHandler:     embeddedIDP.Handler(),
	}
}

func (h *loginPreferenceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	loginMethod, preferredConnectorID, err := h.resolveLoginPreference(r.Context())
	if err != nil {
		http.Error(w, "configured login method is temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if loginMethod == types.LoginMethodAll {
		h.dexHandler.ServeHTTP(w, r)
		return
	}

	switch {
	case r.URL.Path == "/oauth2/auth":
		h.redirectToPreferredConnector(w, r, preferredConnectorID)
		return
	case strings.HasPrefix(r.URL.Path, "/oauth2/auth/"):
		requestedConnectorID := mux.Vars(r)["connector"]
		if requestedConnectorID == "" {
			requestedConnectorID = strings.TrimPrefix(r.URL.Path, "/oauth2/auth/")
		}
		if requestedConnectorID != preferredConnectorID {
			h.redirectToPreferredConnector(w, r, preferredConnectorID)
			return
		}
	}

	h.dexHandler.ServeHTTP(w, r)
}

func (h *loginPreferenceHandler) redirectToPreferredConnector(w http.ResponseWriter, r *http.Request, connectorID string) {
	if connectorID == "" {
		h.dexHandler.ServeHTTP(w, r)
		return
	}

	redirectURL := *r.URL
	redirectURL.Path = "/oauth2/auth/" + connectorID
	http.Redirect(w, r, redirectURL.String(), http.StatusFound)
}

func (h *loginPreferenceHandler) resolveLoginPreference(ctx context.Context) (types.LoginMethod, string, error) {
	settings := h.getPrimaryAccountSettings(ctx)
	if settings == nil {
		return types.LoginMethodAll, "", nil
	}

	loginMethod := settings.LoginMethod
	if loginMethod == "" {
		loginMethod = types.LoginMethodAll
	}

	switch loginMethod {
	case types.LoginMethodEmail:
		return loginMethod, "local", nil
	case types.LoginMethodWeChatWork:
		connectorID, err := h.getWeChatWorkConnectorID(ctx)
		if err != nil {
			return types.LoginMethodAll, "", err
		}
		return loginMethod, connectorID, nil
	default:
		return types.LoginMethodAll, "", nil
	}
}

func (h *loginPreferenceHandler) getPrimaryAccountSettings(ctx context.Context) *types.Settings {
	accounts := h.accountManager.GetStore().GetAllAccounts(ctx)
	if len(accounts) == 0 {
		return nil
	}

	sort.SliceStable(accounts, func(i, j int) bool {
		if accounts[i].IsDomainPrimaryAccount != accounts[j].IsDomainPrimaryAccount {
			return accounts[i].IsDomainPrimaryAccount
		}
		if !accounts[i].CreatedAt.Equal(accounts[j].CreatedAt) {
			return accounts[i].CreatedAt.Before(accounts[j].CreatedAt)
		}
		return accounts[i].Id < accounts[j].Id
	})

	return accounts[0].Settings
}

func (h *loginPreferenceHandler) getWeChatWorkConnectorID(ctx context.Context) (string, error) {
	if h.embeddedIDP == nil {
		return "", errors.New("embedded identity provider is unavailable")
	}

	connectors, err := h.embeddedIDP.ListConnectors(ctx)
	if err != nil {
		return "", err
	}

	for _, connector := range connectors {
		if connector != nil && connector.Type == "wechatwork" {
			return connector.ID, nil
		}
	}

	return "", errors.New("wechat work identity provider is not configured")
}
