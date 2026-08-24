package server

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/management/server/types"
)

func TestValidateBrandingSettings(t *testing.T) {
	validPNG := "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte("fakepng"))
	oversized := "data:image/png;base64," + base64.StdEncoding.EncodeToString(make([]byte, maxBrandingImageBytes+1))

	tests := []struct {
		name    string
		extra   *types.ExtraSettings
		wantErr string
	}{
		{
			name:  "nil extra",
			extra: nil,
		},
		{
			name:  "empty branding",
			extra: &types.ExtraSettings{},
		},
		{
			name:  "valid png logo",
			extra: &types.ExtraSettings{BrandingLogoDataURL: validPNG},
		},
		{
			name:    "invalid data url",
			extra:   &types.ExtraSettings{BrandingLogoDataURL: "https://example.com/logo.png"},
			wantErr: "must be a valid image data URL",
		},
		{
			name:    "unsupported media type",
			extra:   &types.ExtraSettings{BrandingLogoDataURL: "data:image/gif;base64,AAAA"},
			wantErr: "supported image types",
		},
		{
			name:    "oversized logo",
			extra:   &types.ExtraSettings{BrandingLogoDataURL: oversized},
			wantErr: "can't be larger than",
		},
		{
			name:    "tab title too long",
			extra:   &types.ExtraSettings{BrandingTabTitle: strings.Repeat("a", maxBrandingTabTitleLength+1)},
			wantErr: "tab title can't be longer",
		},
		{
			name:  "valid primary color",
			extra: &types.ExtraSettings{BrandingPrimaryColor: "#A1b2C3"},
		},
		{
			name:    "invalid primary color",
			extra:   &types.ExtraSettings{BrandingPrimaryColor: "#12345"},
			wantErr: "6-digit hex color",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateBrandingSettings(test.extra)
			if test.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.wantErr)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateBrandingSVG(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{
			name: "valid fragment references",
			payload: `<svg xmlns="http://www.w3.org/2000/svg">
				<defs>
					<linearGradient id="brand-a"/>
					<g id="mark"><path fill="url(#brand-a)" d="M0 0h1v1H0z"/></g>
				</defs>
				<use href="#mark" style="stroke: url('#brand-a')"/>
			</svg>`,
		},
		{
			name:    "valid style content",
			payload: `<svg><style>.mark{fill:#123456;stroke:url(#brand-a)}</style><rect class="mark"/></svg>`,
		},
		{
			name:    "reject non svg root",
			payload: `<html></html>`,
			wantErr: "root element must be svg",
		},
		{
			name:    "reject foreign object",
			payload: `<svg><foreignObject><body>unsafe</body></foreignObject></svg>`,
			wantErr: "foreignObject elements are not allowed",
		},
		{
			name:    "reject external href",
			payload: `<svg><a href="https://example.com">link</a></svg>`,
			wantErr: "href attributes are not allowed",
		},
		{
			name:    "reject xlink data href",
			payload: `<svg xmlns:xlink="http://www.w3.org/1999/xlink"><image xlink:href="data:image/png;base64,AAAA"/></svg>`,
			wantErr: "href attributes are not allowed",
		},
		{
			name:    "reject spaced javascript href",
			payload: `<svg><a href="java&#x0a;script:alert(1)">link</a></svg>`,
			wantErr: "href attributes are not allowed",
		},
		{
			name:    "reject external style url",
			payload: `<svg><rect style="fill:url(https://example.com/paint.svg#x)"/></svg>`,
			wantErr: "style attributes are not allowed",
		},
		{
			name:    "reject external presentation url",
			payload: `<svg><rect fill="url(https://example.com/paint.svg#x)"/></svg>`,
			wantErr: "fill attributes are not allowed",
		},
		{
			name:    "reject style import",
			payload: `<svg><rect style="@import url(https://example.com/a.css); fill: #fff"/></svg>`,
			wantErr: "style attributes are not allowed",
		},
		{
			name:    "reject style element import",
			payload: `<svg><style>@import url(https://example.com/a.css); .mark{fill:#fff}</style></svg>`,
			wantErr: "style content is not allowed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateBrandingSVG([]byte(test.payload))
			if test.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.wantErr)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPreserveUnmanagedExtraSettings(t *testing.T) {
	oldExtra := &types.ExtraSettings{
		IntegratedValidator:       "validator",
		IntegratedValidatorGroups: []string{"groupA"},
		RegisteredRelays: map[string]types.RegisteredRelay{
			"relay1": {ID: "relay1", Name: "Relay One"},
		},
	}
	newExtra := &types.ExtraSettings{
		BrandingLogoDataURL: "data:image/png;base64,AAAA",
		BrandingTabTitle:    "Acme",
	}

	preserveUnmanagedExtraSettings(newExtra, oldExtra)

	assert.Equal(t, "validator", newExtra.IntegratedValidator)
	assert.Equal(t, []string{"groupA"}, newExtra.IntegratedValidatorGroups)
	assert.Contains(t, newExtra.RegisteredRelays, "relay1")
	assert.Equal(t, "data:image/png;base64,AAAA", newExtra.BrandingLogoDataURL)
	assert.Equal(t, "Acme", newExtra.BrandingTabTitle)
}
