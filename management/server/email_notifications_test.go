package server

import (
	"testing"
	"time"

	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
)

func TestInviteURLUsesPublicOriginFromCallbackURL(t *testing.T) {
	manager := &DefaultAccountManager{
		config: &nbconfig.Config{
			HttpConfig: &nbconfig.HttpServerConfig{
				AuthCallbackURL: "https://cloink.example.com/oauth2/callback",
			},
		},
	}

	got := manager.inviteURL("nbi_token")
	want := "https://cloink.example.com/invite?token=nbi_token"
	if got != want {
		t.Fatalf("inviteURL() = %q, want %q", got, want)
	}
}

func TestDashboardURLPrefersAuthAudience(t *testing.T) {
	manager := &DefaultAccountManager{
		config: &nbconfig.Config{
			HttpConfig: &nbconfig.HttpServerConfig{
				AuthAudience:    "https://dashboard.example.com",
				AuthCallbackURL: "https://management.example.com/oauth2/callback",
			},
		},
	}

	got := manager.dashboardURL("/team?status=pending")
	want := "https://dashboard.example.com/team?status=pending"
	if got != want {
		t.Fatalf("dashboardURL() = %q, want %q", got, want)
	}
}

func TestEmailDashboardDataUsesPublicOrigin(t *testing.T) {
	manager := &DefaultAccountManager{
		config: &nbconfig.Config{
			HttpConfig: &nbconfig.HttpServerConfig{
				AuthCallbackURL: "https://cloink.example.com/api/reverse-proxy/callback",
			},
		},
	}

	data := manager.emailDashboardData()
	got, _ := data["url"].(string)
	want := "https://cloink.example.com"
	if got != want {
		t.Fatalf("emailDashboardData()[url] = %q, want %q", got, want)
	}
}

func TestFormatEmailDisplayTimeUsesUTC8(t *testing.T) {
	value := time.Date(2026, 6, 15, 14, 29, 21, 0, time.UTC)

	got := formatEmailDisplayTime(value)
	want := "2026年6月15日 22:29（UTC+8）"
	if got != want {
		t.Fatalf("formatEmailDisplayTime() = %q, want %q", got, want)
	}
}

func TestDashboardURLFallsBackToRelativePath(t *testing.T) {
	manager := &DefaultAccountManager{
		config: &nbconfig.Config{
			HttpConfig: &nbconfig.HttpServerConfig{AuthAudience: "cloink-dashboard"},
		},
	}

	got := manager.dashboardURL("/invite")
	if got != "/invite" {
		t.Fatalf("dashboardURL() = %q, want %q", got, "/invite")
	}
}
