package server

import (
	"context"
	"errors"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"

	dexstorage "github.com/dexidp/dex/storage"

	dex "github.com/netbirdio/netbird/idp/dex"
	"github.com/netbirdio/netbird/management/server/activity"
	idpmanager "github.com/netbirdio/netbird/management/server/idp"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

// passwordResetURLPath is the embedded IdP route that renders the "set a new
// password" form. It is served by the same Dex instance as the sign-in page.
const passwordResetURLPath = "/oauth2/reset-password"

// RequestPasswordReset issues a self-service reset link for the given email
// address. The caller is unauthenticated, so the method must not reveal whether
// the address belongs to an account: unknown addresses get the same treatment
// as known ones (a record is stored, no email is sent) so behaviour and
// rate limiting stay uniform.
func (am *DefaultAccountManager) RequestPasswordReset(ctx context.Context, email string) error {
	email = types.NormalizePasswordResetEmail(email)
	if email == "" {
		return nil
	}

	embeddedIdp, err := am.embeddedIdPManager()
	if err != nil {
		return err
	}

	emailHash := types.HashPasswordResetEmail(email)
	allowed, err := am.passwordResetWithinRateLimit(ctx, emailHash)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Errorf(status.TooManyRequests, "too many password reset requests, please try again later")
	}

	// Always invalidate previous links and store a fresh record first, so the
	// work done for unknown addresses matches the work done for known ones.
	user, lookupErr := am.lookupLocalUser(ctx, embeddedIdp, email)
	if lookupErr != nil {
		log.WithContext(ctx).WithError(lookupErr).Errorf("failed to look up local user for password reset")
		return status.Errorf(status.Internal, "failed to process password reset request")
	}

	accountID, userID := "", ""
	if user != nil {
		accountID, userID = user.AccountID, user.Id
	}

	link, plainToken, err := am.issuePasswordResetLink(ctx, accountID, userID, email, "")
	if err != nil {
		return err
	}

	if user == nil {
		log.WithContext(ctx).Debugf("password reset requested for unknown email %s", email)
		return nil
	}

	settings, err := am.passwordResetEmailSettings(ctx, accountID)
	if err != nil {
		return err
	}
	if settings == nil {
		return status.Errorf(status.PreconditionFailed, "email notifications are not configured, please contact your administrator")
	}

	if err := am.sendPasswordResetEmail(ctx, accountID, user, link, plainToken); err != nil {
		return status.Errorf(status.Internal, "failed to send password reset email")
	}
	return nil
}

// CreatePasswordResetLink issues a reset link on behalf of an administrator and
// returns it so it can be handed to the user when they cannot receive mail.
func (am *DefaultAccountManager) CreatePasswordResetLink(ctx context.Context, accountID, initiatorUserID, targetUserID string) (*types.PasswordResetLink, error) {
	embeddedIdp, err := am.embeddedIdPManager()
	if err != nil {
		return nil, err
	}

	allowed, _, err := am.permissionsManager.ValidateUserPermissions(ctx, accountID, initiatorUserID, modules.Users, operations.Update)
	if err != nil {
		return nil, status.NewPermissionValidationError(err)
	}
	if !allowed {
		return nil, status.NewPermissionDeniedError()
	}

	target, err := am.Store.GetUserByUserID(ctx, store.LockingStrengthNone, targetUserID)
	if err != nil || target == nil {
		return nil, status.Errorf(status.NotFound, "user not found")
	}
	if target.IsServiceUser {
		return nil, status.Errorf(status.InvalidArgument, "password reset is not available for service users")
	}
	if !dex.IsLocalUserID(target.Id) {
		return nil, status.Errorf(status.PreconditionFailed, "password reset is only available for accounts with an email and password")
	}
	email := types.NormalizePasswordResetEmail(target.Email)
	if email == "" {
		return nil, status.Errorf(status.InvalidArgument, "user has no email address")
	}

	if _, err := embeddedIdp.GetLocalUser(ctx, email); err != nil {
		if errors.Is(err, dexstorage.ErrNotFound) {
			return nil, status.Errorf(status.NotFound, "user is not registered in the embedded identity provider")
		}
		return nil, status.Errorf(status.Internal, "failed to load user from the embedded identity provider")
	}

	link, plainToken, err := am.issuePasswordResetLink(ctx, accountID, targetUserID, email, initiatorUserID)
	if err != nil {
		return nil, err
	}

	settings, settingsErr := am.passwordResetEmailSettings(ctx, accountID)
	switch {
	case settingsErr != nil:
		link.EmailErr = settingsErr.Error()
	case settings == nil:
		link.EmailErr = "email notifications are not configured"
	default:
		if err := am.sendPasswordResetEmail(ctx, accountID, target, link, plainToken); err != nil {
			link.EmailErr = err.Error()
		} else {
			link.EmailSent = true
		}
	}

	return link, nil
}

// ValidatePasswordResetToken reports whether a reset link can still be used.
func (am *DefaultAccountManager) ValidatePasswordResetToken(ctx context.Context, token string) error {
	_, err := am.lookupPasswordResetRecord(ctx, token, store.LockingStrengthNone)
	return err
}

// lookupPasswordResetRecord resolves and validates a reset token.
func (am *DefaultAccountManager) lookupPasswordResetRecord(ctx context.Context, token string, lockStrength store.LockingStrength) (*types.PasswordResetRecord, error) {
	if err := types.ValidatePasswordResetToken(token); err != nil {
		return nil, errInvalidPasswordResetLink
	}

	record, err := am.Store.GetPasswordResetByHashedToken(ctx, lockStrength, types.HashPasswordResetToken(token))
	if err != nil {
		return nil, errInvalidPasswordResetLink
	}
	if record.IsExpired() {
		_, _ = am.Store.ConsumePasswordResetToken(ctx, record.ID)
		return nil, errInvalidPasswordResetLink
	}
	return record, nil
}

// errInvalidPasswordResetLink is deliberately identical for malformed, unknown
// and expired links so the page cannot be used to probe tokens.
var errInvalidPasswordResetLink = status.Errorf(status.InvalidArgument, "this password reset link is invalid or has expired")

// ResetPasswordWithToken consumes a single-use reset token and sets the new
// password for the token's owner.
func (am *DefaultAccountManager) ResetPasswordWithToken(ctx context.Context, token, newPassword string) error {
	embeddedIdp, err := am.embeddedIdPManager()
	if err != nil {
		return err
	}

	record, err := am.lookupPasswordResetRecord(ctx, token, store.LockingStrengthUpdate)
	if err != nil {
		return err
	}

	if err := ValidatePassword(newPassword); err != nil {
		return status.Errorf(status.InvalidArgument, "%s", err.Error())
	}

	if err := embeddedIdp.SetUserPassword(ctx, record.UserID, newPassword); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to set password for user %s", record.UserID)
		return status.Errorf(status.Internal, "failed to update password")
	}

	// Single use: consume after the password is stored. A concurrent second
	// submission loses the race and is rejected.
	consumed, err := am.Store.ConsumePasswordResetToken(ctx, record.ID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warnf("failed to consume password reset token %s", record.ID)
	}
	_ = consumed

	if err := embeddedIdp.DeleteLocalAuthSessions(ctx, record.UserID); err != nil {
		log.WithContext(ctx).WithError(err).Warnf("failed to invalidate sessions for user %s", record.UserID)
	}

	am.StoreEvent(ctx, record.UserID, record.UserID, record.AccountID, activity.UserPasswordReset, map[string]any{
		"email": record.Email,
	})
	return nil
}

// embeddedIdPManager returns the embedded identity provider, or an error when
// the deployment does not run one.
func (am *DefaultAccountManager) embeddedIdPManager() (*idpmanager.EmbeddedIdPManager, error) {
	if !IsEmbeddedIdp(am.idpManager) {
		return nil, status.Errorf(status.PreconditionFailed, "password reset is only available with the embedded identity provider")
	}
	embeddedIdp, ok := am.idpManager.(*idpmanager.EmbeddedIdPManager)
	if !ok || embeddedIdp == nil {
		return nil, status.Errorf(status.Internal, "failed to load the embedded identity provider")
	}
	return embeddedIdp, nil
}

// lookupLocalUser resolves an email address to the management user behind it.
// It returns (nil, nil) when the address does not belong to a local user.
func (am *DefaultAccountManager) lookupLocalUser(ctx context.Context, embeddedIdp *idpmanager.EmbeddedIdPManager, email string) (*types.User, error) {
	password, err := embeddedIdp.GetLocalUser(ctx, email)
	if err != nil {
		if errors.Is(err, dexstorage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// The management user ID is the Dex-encoded form of the same subject.
	encodedUserID := dex.EncodeDexUserID(password.UserID, dex.LocalConnectorID)
	user, err := am.Store.GetUserByUserID(ctx, store.LockingStrengthNone, encodedUserID)
	if err != nil {
		if statusErr, ok := status.FromError(err); ok && statusErr.Type() == status.NotFound {
			return nil, nil
		}
		return nil, err
	}
	return user, nil
}

// issuePasswordResetLink stores a fresh single-use token, invalidating any
// outstanding links for the same address.
func (am *DefaultAccountManager) issuePasswordResetLink(ctx context.Context, accountID, userID, email, createdBy string) (*types.PasswordResetLink, string, error) {
	hashedToken, plainToken, err := types.GeneratePasswordResetToken()
	if err != nil {
		return nil, "", status.Errorf(status.Internal, "failed to generate password reset token")
	}

	emailHash := types.HashPasswordResetEmail(email)
	if err := am.Store.DeletePasswordResetTokensByEmailHash(ctx, emailHash); err != nil {
		log.WithContext(ctx).WithError(err).Warnf("failed to invalidate previous password reset links for %s", email)
	}

	now := time.Now().UTC()
	expiresAt := now.Add(types.DefaultPasswordResetExpirationSeconds * time.Second)

	record := &types.PasswordResetRecord{
		ID:          types.NewPasswordResetID(),
		AccountID:   accountID,
		UserID:      userID,
		Email:       email,
		EmailHash:   emailHash,
		HashedToken: hashedToken,
		ExpiresAt:   expiresAt,
		CreatedAt:   now,
		CreatedBy:   createdBy,
	}
	if err := am.Store.SavePasswordResetToken(ctx, record); err != nil {
		return nil, "", status.Errorf(status.Internal, "failed to store password reset token")
	}

	return &types.PasswordResetLink{
		URL:       am.passwordResetURL(plainToken),
		Email:     email,
		ExpiresAt: expiresAt,
	}, plainToken, nil
}

func (am *DefaultAccountManager) passwordResetWithinRateLimit(ctx context.Context, emailHash string) (bool, error) {
	since := time.Now().UTC().Add(-types.PasswordResetRateLimitWindow)
	count, err := am.Store.CountPasswordResetTokensSince(ctx, emailHash, since)
	if err != nil {
		return false, status.Errorf(status.Internal, "failed to check password reset rate limit")
	}
	return count < types.PasswordResetRateLimitMax, nil
}

// passwordResetEmailSettings returns the account's SMTP settings when email
// delivery is possible, and nil otherwise.
func (am *DefaultAccountManager) passwordResetEmailSettings(ctx context.Context, accountID string) (*types.EmailSettings, error) {
	if accountID == "" {
		return nil, nil
	}

	settings, err := am.Store.GetEmailSettings(ctx, store.LockingStrengthNone, accountID)
	if err != nil {
		return nil, err
	}
	// Mirror the email manager's own contract: only "enabled" is required.
	// SMTP servers that need credentials are validated when the mail is sent,
	// and relays without authentication are common on internal networks.
	if settings == nil || !settings.Enabled {
		return nil, nil
	}
	if template, ok := settings.Templates[string(types.EmailTemplatePasswordReset)]; ok && !template.Enabled {
		return nil, nil
	}
	return settings, nil
}

// passwordResetURL builds the absolute link that lands on the embedded IdP
// reset page. The IdP issuer is preferred because the page is served there.
func (am *DefaultAccountManager) passwordResetURL(token string) string {
	query := url.Values{"token": []string{token}}.Encode()
	if embeddedIdp, ok := am.idpManager.(*idpmanager.EmbeddedIdPManager); ok && embeddedIdp != nil {
		if origin := publicOrigin(embeddedIdp.GetIssuer()); origin != "" {
			return origin + passwordResetURLPath + "?" + query
		}
	}
	return am.dashboardURL(passwordResetURLPath + "?" + query)
}
