package email

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nbcontext "github.com/netbirdio/netbird/management/server/context"
	emailmanager "github.com/netbirdio/netbird/management/server/email"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/auth"
)

func TestGetSettingsEndpointUsesAuthContextAndDoesNotSerializePassword(t *testing.T) {
	manager := &fakeEmailManager{
		settings: &types.EmailSettings{
			AccountID:             "account-1",
			Enabled:               true,
			Host:                  "smtp.example.com",
			Password:              "secret",
			PasswordEncrypted:     "encrypted-secret",
			PasswordConfigured:    true,
			ClearPassword:         true,
			PasswordUpdatePresent: true,
		},
	}
	router := mux.NewRouter()
	AddEndpoints(manager, router)

	req := httptest.NewRequest(http.MethodGet, "/settings/email", nil)
	req = nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: "account-1", UserId: "user-1"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	assert.Equal(t, "account-1", manager.lastAccountID)
	assert.Equal(t, "user-1", manager.lastUserID)
	assert.NotContains(t, recorder.Body.String(), "secret")
	assert.NotContains(t, recorder.Body.String(), "encrypted-secret")

	var response map[string]any
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	assert.Equal(t, true, response["password_configured"])
	assert.NotContains(t, response, "password")
	assert.NotContains(t, response, "password_encrypted")
}

func TestUpdateSettingsEndpointMapsPasswordFields(t *testing.T) {
	manager := &fakeEmailManager{
		settings: &types.EmailSettings{
			AccountID:          "account-1",
			Enabled:            true,
			Host:               "smtp.example.com",
			Port:               465,
			FromEmail:          "notice@example.com",
			Encryption:         types.EmailEncryptionTLS,
			PasswordConfigured: true,
		},
	}
	router := mux.NewRouter()
	AddEndpoints(manager, router)

	body := strings.NewReader(`{
		"enabled": true,
		"host": "smtp.example.com",
		"port": 465,
		"username": "mailer",
		"password": "new-secret",
		"clear_password": true,
		"from_name": "Cloink",
		"from_email": "notice@example.com",
		"reply_to": "reply@example.com",
		"encryption": "tls",
		"insecure_skip_verify": true,
		"admin_recipients": ["admin@example.com"],
		"templates": {
			"invite_user": {
				"enabled": true,
				"subject": "Invite",
				"body_text": "Hello"
			}
		}
	}`)
	req := httptest.NewRequest(http.MethodPut, "/settings/email", body)
	req = nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: "account-1", UserId: "user-1"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.NotNil(t, manager.lastUpdate)
	assert.Equal(t, "account-1", manager.lastAccountID)
	assert.Equal(t, "user-1", manager.lastUserID)
	assert.True(t, manager.lastUpdate.Enabled)
	assert.Equal(t, "smtp.example.com", manager.lastUpdate.Host)
	assert.Equal(t, 465, manager.lastUpdate.Port)
	assert.Equal(t, "mailer", manager.lastUpdate.Username)
	assert.Equal(t, "new-secret", manager.lastUpdate.Password)
	assert.True(t, manager.lastUpdate.PasswordUpdatePresent)
	assert.True(t, manager.lastUpdate.ClearPassword)
	assert.Equal(t, "Cloink", manager.lastUpdate.FromName)
	assert.Equal(t, "notice@example.com", manager.lastUpdate.FromEmail)
	assert.Equal(t, "reply@example.com", manager.lastUpdate.ReplyTo)
	assert.Equal(t, types.EmailEncryptionTLS, manager.lastUpdate.Encryption)
	assert.True(t, manager.lastUpdate.InsecureSkipVerify)
	assert.Equal(t, []string{"admin@example.com"}, manager.lastUpdate.AdminRecipients)
	assert.Equal(t, "Invite", manager.lastUpdate.Templates[string(types.EmailTemplateInviteUser)].Subject)
}

func TestUpdateSettingsEndpointLeavesPasswordUpdateUnsetWhenOmitted(t *testing.T) {
	manager := &fakeEmailManager{settings: &types.EmailSettings{AccountID: "account-1"}}
	router := mux.NewRouter()
	AddEndpoints(manager, router)

	req := httptest.NewRequest(http.MethodPut, "/settings/email", strings.NewReader(`{"enabled": false}`))
	req = nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: "account-1", UserId: "user-1"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.NotNil(t, manager.lastUpdate)
	assert.False(t, manager.lastUpdate.PasswordUpdatePresent)
	assert.Empty(t, manager.lastUpdate.Password)
}

func TestPreviewTemplateEndpointUsesPathKindAndData(t *testing.T) {
	manager := &fakeEmailManager{
		rendered: &emailmanager.RenderedTemplate{
			Subject:  "Invite user@example.com",
			BodyHTML: "<p>Hello</p>",
			BodyText: "Hello",
		},
	}
	router := mux.NewRouter()
	AddEndpoints(manager, router)

	req := httptest.NewRequest(
		http.MethodPost,
		"/settings/email/templates/invite_user/preview",
		strings.NewReader(`{"data":{"user":{"email":"user@example.com"}}}`),
	)
	req = nbcontext.SetUserAuthInRequest(req, auth.UserAuth{AccountId: "account-1", UserId: "user-1"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	assert.Equal(t, types.EmailTemplateInviteUser, manager.lastKind)
	require.NotNil(t, manager.lastData)
	user, ok := manager.lastData["user"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "user@example.com", user["email"])

	var response emailmanager.RenderedTemplate
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	assert.Equal(t, "Invite user@example.com", response.Subject)
}

func TestSettingsEndpointRequiresAuthContext(t *testing.T) {
	manager := &fakeEmailManager{settings: &types.EmailSettings{AccountID: "account-1"}}
	router := mux.NewRouter()
	AddEndpoints(manager, router)

	req := httptest.NewRequest(http.MethodGet, "/settings/email", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	assert.NotEqual(t, http.StatusOK, recorder.Code)
	assert.Empty(t, manager.lastAccountID)
	assert.Empty(t, manager.lastUserID)
}

type fakeEmailManager struct {
	settings      *types.EmailSettings
	rendered      *emailmanager.RenderedTemplate
	lastAccountID string
	lastUserID    string
	lastUpdate    *types.EmailSettings
	lastRecipient string
	lastKind      types.EmailTemplateKind
	lastData      emailmanager.TemplateData
}

func (m *fakeEmailManager) GetSettings(_ context.Context, accountID, userID string) (*types.EmailSettings, error) {
	m.lastAccountID = accountID
	m.lastUserID = userID
	return m.settings, nil
}

func (m *fakeEmailManager) UpdateSettings(_ context.Context, accountID, userID string, update *types.EmailSettings) (*types.EmailSettings, error) {
	m.lastAccountID = accountID
	m.lastUserID = userID
	m.lastUpdate = update
	return m.settings, nil
}

func (m *fakeEmailManager) SendTestEmail(_ context.Context, accountID, userID, recipient string) error {
	m.lastAccountID = accountID
	m.lastUserID = userID
	m.lastRecipient = recipient
	return nil
}

func (m *fakeEmailManager) PreviewTemplate(_ context.Context, accountID, userID string, kind types.EmailTemplateKind, data emailmanager.TemplateData) (*emailmanager.RenderedTemplate, error) {
	m.lastAccountID = accountID
	m.lastUserID = userID
	m.lastKind = kind
	m.lastData = data
	return m.rendered, nil
}
