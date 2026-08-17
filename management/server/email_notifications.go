package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	emailmanager "github.com/netbirdio/netbird/management/server/email"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
)

func (am *DefaultAccountManager) notifyInviteCreated(ctx context.Context, accountID string, invite *types.UserInviteRecord, plainToken string) {
	_ = am.sendInviteCreated(ctx, accountID, invite, plainToken, false)
}

// notifyInviteCreatedStrict is used by an explicit resend action. Unlike
// lifecycle notifications, the caller must know when SMTP delivery failed.
func (am *DefaultAccountManager) notifyInviteCreatedStrict(ctx context.Context, accountID string, invite *types.UserInviteRecord, plainToken string) error {
	return am.sendInviteCreated(ctx, accountID, invite, plainToken, true)
}

func (am *DefaultAccountManager) sendInviteCreated(ctx context.Context, accountID string, invite *types.UserInviteRecord, plainToken string, strict bool) error {
	if am.emailService == nil || invite == nil {
		return nil
	}
	inviter := am.lookupNotificationUser(ctx, invite.CreatedBy)
	data := emailmanager.TemplateData{
		"recipients": []string{invite.Email},
		"account":    am.emailAccountData(ctx, accountID),
		"dashboard":  am.emailDashboardData(),
		"user":       emailUserData(invite.Name, invite.Email, invite.Role),
		"invite": map[string]any{
			"url":              am.inviteURL(plainToken),
			"expires_at":       formatEmailDisplayTime(invite.ExpiresAt),
			"expires_at_utc":   invite.ExpiresAt.UTC().Format(timeLayoutUTC),
			"created_by_name":  inviter["name"],
			"created_by_email": inviter["email"],
		},
	}
	if strict {
		if notifier, ok := am.emailService.(emailmanager.StrictNotifier); ok {
			return notifier.NotifyStrict(ctx, accountID, types.EmailTemplateInviteUser, data)
		}
	}
	return am.emailService.Notify(ctx, accountID, types.EmailTemplateInviteUser, data)
}

func (am *DefaultAccountManager) notifyUserCreated(ctx context.Context, accountID string, user *types.User) {
	if am.emailService == nil || user == nil || strings.TrimSpace(user.Email) == "" {
		return
	}
	_ = am.emailService.Notify(ctx, accountID, types.EmailTemplateCreateUser, emailmanager.TemplateData{
		"recipients": []string{user.Email},
		"account":    am.emailAccountData(ctx, accountID),
		"dashboard":  am.emailDashboardData(),
		"user":       emailUserData(user.Name, user.Email, string(user.Role)),
	})
}

func (am *DefaultAccountManager) notifyInviteAccepted(ctx context.Context, invite *types.UserInviteRecord, user *types.User) {
	if am.emailService == nil || invite == nil || user == nil || invite.CreatedBy == "" {
		return
	}
	inviter, err := am.Store.GetUserByUserID(ctx, store.LockingStrengthNone, invite.CreatedBy)
	if err != nil || inviter == nil || strings.TrimSpace(inviter.Email) == "" {
		return
	}
	_ = am.emailService.Notify(ctx, invite.AccountID, types.EmailTemplateInviteAccepted, emailmanager.TemplateData{
		"recipients": []string{inviter.Email},
		"account":    am.emailAccountData(ctx, invite.AccountID),
		"dashboard":  am.emailDashboardData(),
		"user":       emailUserData(user.Name, user.Email, string(user.Role)),
		"invite": map[string]any{
			"created_by_name":  inviter.Name,
			"created_by_email": inviter.Email,
			"expires_at":       formatEmailDisplayTime(invite.ExpiresAt),
			"expires_at_utc":   invite.ExpiresAt.UTC().Format(timeLayoutUTC),
		},
	})
}

func (am *DefaultAccountManager) notifyUserPendingApproval(ctx context.Context, accountID string, user *types.User) {
	if am.emailService == nil || user == nil {
		return
	}
	_ = am.emailService.Notify(ctx, accountID, types.EmailTemplateUserPendingApproval, emailmanager.TemplateData{
		"fallback_recipients": am.adminNotificationRecipients(ctx, accountID),
		"account":             am.emailAccountData(ctx, accountID),
		"dashboard":           am.emailDashboardData(),
		"user":                emailUserData(user.Name, user.Email, string(user.Role)),
		"approval": map[string]any{
			"url": am.dashboardURL("/team?status=pending"),
		},
	})
}

func (am *DefaultAccountManager) notifyDevicePendingApproval(ctx context.Context, accountID string, peer *nbpeer.Peer) {
	if am.emailService == nil || peer == nil {
		return
	}
	user := am.lookupNotificationUser(ctx, peer.UserID)
	_ = am.emailService.Notify(ctx, accountID, types.EmailTemplateDevicePendingApproval, emailmanager.TemplateData{
		"fallback_recipients": am.adminNotificationRecipients(ctx, accountID),
		"account":             am.emailAccountData(ctx, accountID),
		"dashboard":           am.emailDashboardData(),
		"device": map[string]any{
			"id":         peer.ID,
			"name":       peer.Name,
			"hostname":   peer.Meta.Hostname,
			"os":         firstNonEmpty(peer.Meta.OS, peer.Meta.GoOS),
			"user_email": user["email"],
		},
		"approval": map[string]any{
			"url": am.dashboardURL("/peers?approval_required=true"),
		},
	})
}

const (
	timeLayoutUTC         = "2006-01-02 15:04:05 UTC"
	timeLayoutDisplay     = "2006年1月2日 15:04"
	emailDisplayZoneName  = "UTC+8"
	emailDisplayZoneHours = 8
)

var emailDisplayLocation = time.FixedZone(emailDisplayZoneName, emailDisplayZoneHours*60*60)

func formatEmailDisplayTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.In(emailDisplayLocation).Format(timeLayoutDisplay) + "（" + emailDisplayZoneName + "）"
}

func (am *DefaultAccountManager) emailAccountData(ctx context.Context, accountID string) map[string]any {
	meta, err := am.Store.GetAccountMeta(ctx, store.LockingStrengthNone, accountID)
	if err != nil || meta == nil {
		return map[string]any{"name": "Cloink", "domain": ""}
	}
	name := "Cloink"
	if meta.Domain != "" {
		name = meta.Domain
	}
	return map[string]any{"name": name, "domain": meta.Domain}
}

func (am *DefaultAccountManager) emailDashboardData() map[string]any {
	return map[string]any{"url": am.dashboardURL("")}
}

func emailUserData(name, email, role string) map[string]any {
	return map[string]any{"name": name, "email": email, "role": role}
}

func (am *DefaultAccountManager) lookupNotificationUser(ctx context.Context, userID string) map[string]any {
	if userID == "" {
		return map[string]any{"name": "", "email": ""}
	}
	user, err := am.Store.GetUserByUserID(ctx, store.LockingStrengthNone, userID)
	if err != nil || user == nil {
		return map[string]any{"name": "", "email": ""}
	}
	return map[string]any{"name": user.Name, "email": user.Email}
}

func (am *DefaultAccountManager) adminNotificationRecipients(ctx context.Context, accountID string) []string {
	users, err := am.Store.GetAccountUsers(ctx, store.LockingStrengthNone, accountID)
	if err != nil {
		return nil
	}
	recipients := make([]string, 0)
	for _, user := range users {
		if user == nil || strings.TrimSpace(user.Email) == "" {
			continue
		}
		if user.Role == types.UserRoleOwner || user.Role == types.UserRoleAdmin {
			recipients = append(recipients, user.Email)
		}
	}
	return recipients
}

func (am *DefaultAccountManager) inviteURL(token string) string {
	if token == "" {
		return am.dashboardURL("/invite")
	}
	return am.dashboardURL(fmt.Sprintf("/invite?token=%s", url.QueryEscape(token)))
}

func (am *DefaultAccountManager) dashboardURL(path string) string {
	if am.config != nil && am.config.HttpConfig != nil {
		if base := publicOrigin(am.config.HttpConfig.AuthAudience); base != "" {
			return base + path
		}
		if base := publicOrigin(am.config.HttpConfig.AuthCallbackURL); base != "" {
			return base + path
		}
	}
	return path
}

func publicOrigin(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	return strings.TrimRight(parsed.Scheme+"://"+parsed.Host, "/")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
