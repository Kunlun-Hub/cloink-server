package idp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/mock_server"
	"github.com/netbirdio/netbird/shared/management/status"
)

func newPasswordResetHandler(t *testing.T, manager *mock_server.MockAccountManager) *PasswordResetHandler {
	t.Helper()

	handler, err := NewPasswordResetHandler(manager, nil)
	require.NoError(t, err, "the reset pages must always parse from the embedded assets")
	return handler
}

func TestNewPasswordResetHandlerParsesTemplates(t *testing.T) {
	handler := newPasswordResetHandler(t, &mock_server.MockAccountManager{})
	require.NotNil(t, handler.forgotTmpl)
	require.NotNil(t, handler.resetTmpl)
}

func TestForgotPasswordRendersRequestForm(t *testing.T) {
	handler := newPasswordResetHandler(t, &mock_server.MockAccountManager{})

	recorder := httptest.NewRecorder()
	handler.ForgotPassword(recorder, httptest.NewRequest(http.MethodGet, ForgotPasswordPath, nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	body := recorder.Body.String()
	assert.Contains(t, body, `name="email"`)
	assert.Contains(t, body, "Send reset link")
	assert.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
}

func TestForgotPasswordSubmitsEmailAndHidesExistence(t *testing.T) {
	var received string
	manager := &mock_server.MockAccountManager{
		RequestPasswordResetFunc: func(_ context.Context, email string) error {
			received = email
			return nil
		},
	}
	handler := newPasswordResetHandler(t, manager)

	form := url.Values{"email": {"user@example.com"}}
	req := httptest.NewRequest(http.MethodPost, ForgotPasswordPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	recorder := httptest.NewRecorder()
	handler.ForgotPassword(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "user@example.com", received)
	body := recorder.Body.String()
	assert.Contains(t, body, "If that email address is registered, a password reset link has been sent.")
	assert.NotContains(t, body, `name="email"`, "the confirmation must replace the form")
}

func TestForgotPasswordShowsManagerErrors(t *testing.T) {
	tests := map[string]struct {
		err     error
		wantKey string
	}{
		"rate limited": {
			err:     status.Errorf(status.TooManyRequests, "slow down"),
			wantKey: errKeyRateLimited,
		},
		"email not configured": {
			err:     status.Errorf(status.PreconditionFailed, "no smtp"),
			wantKey: errKeyNoEmail,
		},
		"unexpected failure": {
			err:     status.Errorf(status.Internal, "boom"),
			wantKey: errKeySendFailed,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			manager := &mock_server.MockAccountManager{
				RequestPasswordResetFunc: func(_ context.Context, _ string) error {
					return test.err
				},
			}
			handler := newPasswordResetHandler(t, manager)

			form := url.Values{"email": {"user@example.com"}}
			req := httptest.NewRequest(http.MethodPost, ForgotPasswordPath, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			recorder := httptest.NewRecorder()
			handler.ForgotPassword(recorder, req)

			body := recorder.Body.String()
			assert.Contains(t, body, `data-i18n="`+test.wantKey+`"`)
			assert.Contains(t, body, `name="email"`, "the form must stay so the user can retry")
		})
	}
}

func TestResetPasswordRejectsUnknownToken(t *testing.T) {
	manager := &mock_server.MockAccountManager{
		ValidatePasswordResetTokenFunc: func(_ context.Context, _ string) error {
			return status.Errorf(status.InvalidArgument, "nope")
		},
		ResetPasswordWithTokenFunc: func(_ context.Context, _, _ string) error {
			t.Fatal("the password must not be changed for an invalid link")
			return nil
		},
	}
	handler := newPasswordResetHandler(t, manager)

	recorder := httptest.NewRecorder()
	handler.ResetPassword(recorder, httptest.NewRequest(http.MethodGet, ResetPasswordPath+"?token=bad", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	body := recorder.Body.String()
	assert.Contains(t, body, errKeyInvalidLink)
	assert.NotContains(t, body, `name="password"`)
}

func TestResetPasswordRejectsMismatchedConfirmation(t *testing.T) {
	manager := &mock_server.MockAccountManager{
		ValidatePasswordResetTokenFunc: func(_ context.Context, _ string) error { return nil },
		ResetPasswordWithTokenFunc: func(_ context.Context, _, _ string) error {
			t.Fatal("the password must not be changed when the confirmation differs")
			return nil
		},
	}
	handler := newPasswordResetHandler(t, manager)

	form := url.Values{
		"token":    {"nbr_valid"},
		"password": {"Str0ng!Pass"},
		"confirm":  {"Str0ng!Pas5"},
	}
	req := httptest.NewRequest(http.MethodPost, ResetPasswordPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	recorder := httptest.NewRecorder()
	handler.ResetPassword(recorder, req)

	assert.Contains(t, recorder.Body.String(), errKeyMismatch)
}

func TestResetPasswordRejectsWeakPassword(t *testing.T) {
	manager := &mock_server.MockAccountManager{
		ValidatePasswordResetTokenFunc: func(_ context.Context, _ string) error { return nil },
		ResetPasswordWithTokenFunc: func(_ context.Context, _, _ string) error {
			return status.Errorf(status.InvalidArgument, "password must be at least 8 characters long")
		},
	}
	handler := newPasswordResetHandler(t, manager)

	form := url.Values{
		"token":    {"nbr_valid"},
		"password": {"weak"},
		"confirm":  {"weak"},
	}
	req := httptest.NewRequest(http.MethodPost, ResetPasswordPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	recorder := httptest.NewRecorder()
	handler.ResetPassword(recorder, req)

	assert.Contains(t, recorder.Body.String(), errKeyWeakPassword)
}

func TestResetPasswordAppliesNewPassword(t *testing.T) {
	var gotToken, gotPassword string
	manager := &mock_server.MockAccountManager{
		ValidatePasswordResetTokenFunc: func(_ context.Context, _ string) error { return nil },
		ResetPasswordWithTokenFunc: func(_ context.Context, token, password string) error {
			gotToken, gotPassword = token, password
			return nil
		},
	}
	handler := newPasswordResetHandler(t, manager)

	const token = "nbr_abcdefghijklmnopqrstuvwxyz0123456789AB"
	form := url.Values{
		"token":    {token},
		"password": {"Str0ng!Pass"},
		"confirm":  {"Str0ng!Pass"},
	}
	req := httptest.NewRequest(http.MethodPost, ResetPasswordPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	recorder := httptest.NewRecorder()
	handler.ResetPassword(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, token, gotToken)
	assert.Equal(t, "Str0ng!Pass", gotPassword)

	body := recorder.Body.String()
	assert.Contains(t, body, "Your password has been changed. You can sign in with your new password now.")
	assert.NotContains(t, body, `name="password"`)
}

func TestPasswordResetRejectsUnsupportedMethods(t *testing.T) {
	handler := newPasswordResetHandler(t, &mock_server.MockAccountManager{})

	for _, target := range []string{ForgotPasswordPath, ResetPasswordPath} {
		req := httptest.NewRequest(http.MethodDelete, target, nil)
		recorder := httptest.NewRecorder()

		if target == ForgotPasswordPath {
			handler.ForgotPassword(recorder, req)
		} else {
			handler.ResetPassword(recorder, req)
		}

		assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestRelativeURLMatchesDexAssetResolution(t *testing.T) {
	tests := map[string]struct {
		serverPath string
		reqPath    string
		assetPath  string
		want       string
	}{
		"connector sign-in page": {
			serverPath: "/oauth2",
			reqPath:    "/oauth2/auth/local",
			assetPath:  "forgot-password",
			want:       "../forgot-password",
		},
		"connector chooser": {
			serverPath: "/oauth2",
			reqPath:    "/oauth2/auth",
			assetPath:  "forgot-password",
			want:       "forgot-password",
		},
		"theme asset from a nested page": {
			serverPath: "/oauth2",
			reqPath:    "/oauth2/auth/local",
			assetPath:  "theme/favicon.ico",
			want:       "../theme/favicon.ico",
		},
		"issuer mounted on the root": {
			serverPath: "",
			reqPath:    "/oauth2/forgot-password",
			assetPath:  "theme/favicon.ico",
			want:       "../theme/favicon.ico",
		},
		"absolute asset urls are untouched": {
			serverPath: "/oauth2",
			reqPath:    "/oauth2/auth",
			assetPath:  "https://cdn.example.com/logo.png",
			want:       "https://cdn.example.com/logo.png",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, relativeURL(test.serverPath, test.reqPath, test.assetPath))
		})
	}
}
