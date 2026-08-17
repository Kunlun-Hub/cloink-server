package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	emailmanager "github.com/netbirdio/netbird/management/server/email"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

// resendEmailService gives the account manager a deterministic strict sender
// without opening a network connection in invite tests.
type resendEmailService struct {
	strictErr    error
	beforeStrict func()
}

func (s *resendEmailService) GetSettings(context.Context, string, string) (*types.EmailSettings, error) {
	return nil, nil
}

func (s *resendEmailService) UpdateSettings(context.Context, string, string, *types.EmailSettings) (*types.EmailSettings, error) {
	return nil, nil
}

func (s *resendEmailService) SendTestEmail(context.Context, string, string, string) error {
	return nil
}

func (s *resendEmailService) PreviewTemplate(context.Context, string, string, types.EmailTemplateKind, emailmanager.TemplateData) (*emailmanager.RenderedTemplate, error) {
	return nil, nil
}

func (s *resendEmailService) Notify(context.Context, string, types.EmailTemplateKind, emailmanager.TemplateData) error {
	return nil
}

func (s *resendEmailService) NotifyStrict(context.Context, string, types.EmailTemplateKind, emailmanager.TemplateData) error {
	if s.beforeStrict != nil {
		s.beforeStrict()
	}
	return s.strictErr
}

func TestResendUserInviteSMTPFailureRestoresPreviousToken(t *testing.T) {
	am, cleanup := setupInviteTestManagerWithEmbeddedIdP(t)
	defer cleanup()

	created, err := am.CreateUserInvite(context.Background(), testAccountID, testAdminUserID, &types.UserInfo{
		Email:      "resend@example.com",
		Name:       "Resend User",
		Role:       string(types.UserRoleUser),
		AutoGroups: []string{},
	}, 0)
	require.NoError(t, err)

	am.SetEmailService(&resendEmailService{strictErr: errors.New("SMTP unavailable")})
	_, err = am.ResendUserInvite(context.Background(), testAccountID, testAdminUserID, created.UserInfo.ID, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "SMTP unavailable")

	// The failed send must not invalidate the only usable invitation link.
	invite, err := am.Store.GetUserInviteByID(context.Background(), store.LockingStrengthNone, testAccountID, created.UserInfo.ID)
	require.NoError(t, err)
	require.Equal(t, types.HashInviteToken(created.InviteToken), invite.HashedToken)
	_, err = am.GetUserInviteInfo(context.Background(), created.InviteToken)
	require.NoError(t, err)
}

func TestResendUserInviteSMTPFailureDoesNotOverwriteConcurrentToken(t *testing.T) {
	am, cleanup := setupInviteTestManagerWithEmbeddedIdP(t)
	defer cleanup()

	created, err := am.CreateUserInvite(context.Background(), testAccountID, testAdminUserID, &types.UserInfo{
		Email:      "concurrent-resend@example.com",
		Name:       "Concurrent Resend User",
		Role:       string(types.UserRoleUser),
		AutoGroups: []string{},
	}, 0)
	require.NoError(t, err)

	const concurrentHash = "concurrent-success-token"
	am.SetEmailService(&resendEmailService{
		strictErr: errors.New("SMTP unavailable"),
		beforeStrict: func() {
			invite, getErr := am.Store.GetUserInviteByID(context.Background(), store.LockingStrengthNone, testAccountID, created.UserInfo.ID)
			require.NoError(t, getErr)
			invite.HashedToken = concurrentHash
		require.NoError(t, am.Store.SaveUserInvite(context.Background(), invite))
		},
	})

	_, err = am.ResendUserInvite(context.Background(), testAccountID, testAdminUserID, created.UserInfo.ID, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "invite changed concurrently")

	invite, err := am.Store.GetUserInviteByID(context.Background(), store.LockingStrengthNone, testAccountID, created.UserInfo.ID)
	require.NoError(t, err)
	require.Equal(t, concurrentHash, invite.HashedToken)
}
