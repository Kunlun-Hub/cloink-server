package email

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
)

func TestDefaultTemplatesCoverPasswordReset(t *testing.T) {
	templates := DefaultTemplates()

	tmpl, ok := templates[string(types.EmailTemplatePasswordReset)]
	require.True(t, ok, "the password reset template must ship with a default")
	assert.True(t, tmpl.Enabled)
	assert.NotEmpty(t, tmpl.Subject)
	assert.NotEmpty(t, tmpl.BodyHTML)
	assert.NotEmpty(t, tmpl.BodyText)
}

func TestPasswordResetTemplateRendersResetData(t *testing.T) {
	templates := DefaultTemplates()

	rendered, err := renderTemplate(types.EmailTemplatePasswordReset, templates, TemplateData{
		"user": map[string]any{"email": "user@example.com"},
		"reset": map[string]any{
			"url":        "https://cloink.example.com/oauth2/reset-password?token=nbr_abc",
			"expires_at": "2026年1月2日 15:04（UTC+8）",
		},
	})
	require.NoError(t, err)

	assert.Contains(t, rendered.Subject, "Cloink")
	assert.Contains(t, rendered.BodyHTML, "https://cloink.example.com/oauth2/reset-password?token=nbr_abc")
	assert.Contains(t, rendered.BodyHTML, "user@example.com")
	assert.Contains(t, rendered.BodyText, "https://cloink.example.com/oauth2/reset-password?token=nbr_abc")
}
