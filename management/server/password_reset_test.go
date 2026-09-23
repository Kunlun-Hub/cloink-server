package server

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	idpmanager "github.com/netbirdio/netbird/management/server/idp"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

func TestRequestPasswordResetIgnoresEmptyEmail(t *testing.T) {
	am := &DefaultAccountManager{}

	// An empty address is not a user enumeration signal, so it is a no-op
	// rather than an error.
	require.NoError(t, am.RequestPasswordReset(context.Background(), "   "))
}

func TestRequestPasswordResetRequiresEmbeddedIdP(t *testing.T) {
	am := &DefaultAccountManager{}

	err := am.RequestPasswordReset(context.Background(), "user@example.com")
	require.Error(t, err)

	statusErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, status.PreconditionFailed, statusErr.Type())
}

func TestCreatePasswordResetLinkRequiresEmbeddedIdP(t *testing.T) {
	am := &DefaultAccountManager{}

	link, err := am.CreatePasswordResetLink(context.Background(), "account-1", "admin-1", "user-1")
	require.Error(t, err)
	assert.Nil(t, link)

	statusErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, status.PreconditionFailed, statusErr.Type())
}

func TestResetPasswordWithTokenRejectsMalformedToken(t *testing.T) {
	am := &DefaultAccountManager{idpManager: &idpmanager.EmbeddedIdPManager{}}

	err := am.ResetPasswordWithToken(context.Background(), "nbr_not-a-real-token", "Str0ng!Pass")
	require.Error(t, err)

	statusErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, status.InvalidArgument, statusErr.Type())
	assert.Contains(t, statusErr.Message, "invalid or has expired")
}

func TestResetPasswordWithTokenRejectsEmptyToken(t *testing.T) {
	am := &DefaultAccountManager{idpManager: &idpmanager.EmbeddedIdPManager{}}

	err := am.ResetPasswordWithToken(context.Background(), "", "Str0ng!Pass")
	require.Error(t, err)

	err = am.ValidatePasswordResetToken(context.Background(), "")
	require.Error(t, err)
}

func TestPasswordResetURLUsesTheEmbeddedIssuerOrigin(t *testing.T) {
	am := &DefaultAccountManager{}

	link := am.passwordResetURL("nbr_token")
	assert.True(t, strings.HasPrefix(link, passwordResetURLPath+"?token="), "fallback must target the reset path, got %q", link)
	assert.Contains(t, link, "nbr_token")
}

func TestPasswordResetEmailSettingsRequiresEnabledSMTP(t *testing.T) {
	tests := map[string]struct {
		settings *types.EmailSettings
		wantNil  bool
	}{
		"enabled": {
			settings: &types.EmailSettings{AccountID: "account-1", Enabled: true},
		},
		"enabled without a password (auth-less relay)": {
			settings: &types.EmailSettings{AccountID: "account-1", Enabled: true, PasswordConfigured: false},
		},
		"disabled": {
			settings: &types.EmailSettings{AccountID: "account-1", Enabled: false},
			wantNil:  true,
		},
		"template disabled": {
			settings: &types.EmailSettings{
				AccountID: "account-1",
				Enabled:   true,
				Templates: map[string]types.EmailTemplate{
					string(types.EmailTemplatePasswordReset): {Enabled: false},
				},
			},
			wantNil: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			storeMock := store.NewMockStore(ctrl)
			storeMock.EXPECT().
				GetEmailSettings(gomock.Any(), gomock.Any(), "account-1").
				Return(test.settings, nil)

			am := &DefaultAccountManager{Store: storeMock}

			settings, err := am.passwordResetEmailSettings(context.Background(), "account-1")
			require.NoError(t, err)
			if test.wantNil {
				assert.Nil(t, settings)
				return
			}
			assert.NotNil(t, settings)
		})
	}
}

func TestPasswordResetEmailSettingsWithoutAccount(t *testing.T) {
	am := &DefaultAccountManager{}

	settings, err := am.passwordResetEmailSettings(context.Background(), "")
	require.NoError(t, err)
	assert.Nil(t, settings)
}
