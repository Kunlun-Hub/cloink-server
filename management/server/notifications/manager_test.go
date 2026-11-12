package notifications

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
)

func TestValidateChannelWebhookTarget(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{name: "not configured", target: ""},
		{name: "legacy null target", target: "null"},
		{name: "missing url", target: "{}", wantErr: true},
		{name: "empty url", target: `{"url":""}`, wantErr: true},
		{name: "valid url", target: `{"url":"https://example.com/hook"}`},
		{name: "unsupported scheme", target: `{"url":"ftp://example.com/hook"}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateChannel(&types.NotificationChannel{
				Type:       types.NotificationChannelTypeWebhook,
				Enabled:    true,
				EventTypes: []string{types.NotificationEventPeerOnline},
				Target:     tt.target,
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
