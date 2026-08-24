package server

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"net/url"
	"regexp"
	"strings"

	"github.com/netbirdio/netbird/shared/management/status"
	"github.com/netbirdio/netbird/management/server/types"
)

const (
	maxBrandingImageBytes     = 256 * 1024
	maxBrandingTabTitleLength = 80
)

var (
	brandingPrimaryColorRegexp = regexp.MustCompile(`^#[0-9A-Fa-f]{6}$`)
	brandingSVGStyleURLRegexp  = regexp.MustCompile(`(?i)url\(([^)]*)\)`)
	allowedBrandingImageTypes  = map[string]struct{}{
		"image/png":     {},
		"image/jpeg":    {},
		"image/webp":    {},
		"image/svg+xml": {},
	}
)

// preserveUnmanagedExtraSettings copies fields that are not managed through the
// account settings API from the old extra settings so partial updates don't
// wipe them.
func preserveUnmanagedExtraSettings(newExtra, oldExtra *types.ExtraSettings) {
	if newExtra == nil || oldExtra == nil {
		return
	}

	oldExtra = oldExtra.Copy()
	newExtra.IntegratedValidator = oldExtra.IntegratedValidator
	newExtra.IntegratedValidatorGroups = oldExtra.IntegratedValidatorGroups
	newExtra.RegisteredRelays = oldExtra.RegisteredRelays
}

func validateBrandingSettings(extra *types.ExtraSettings) error {
	if extra == nil {
		return nil
	}

	if err := validateBrandingImageDataURL("branding logo", extra.BrandingLogoDataURL); err != nil {
		return err
	}
	if err := validateBrandingImageDataURL("branding dark logo", extra.BrandingLogoDarkDataURL); err != nil {
		return err
	}
	if err := validateBrandingImageDataURL("branding icon", extra.BrandingIconDataURL); err != nil {
		return err
	}

	if len([]rune(extra.BrandingTabTitle)) > maxBrandingTabTitleLength {
		return status.Errorf(status.InvalidArgument, "branding tab title can't be longer than %d characters", maxBrandingTabTitleLength)
	}

	if extra.BrandingPrimaryColor != "" && !brandingPrimaryColorRegexp.MatchString(extra.BrandingPrimaryColor) {
		return status.Errorf(status.InvalidArgument, "branding primary color must be a 6-digit hex color like #123456")
	}

	return nil
}

func validateBrandingImageDataURL(field, value string) error {
	if value == "" {
		return nil
	}

	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "data" || parsed.Opaque == "" {
		return status.Errorf(status.InvalidArgument, "%s must be a valid image data URL", field)
	}

	metadata, payload, ok := strings.Cut(parsed.Opaque, ",")
	if !ok || metadata == "" || payload == "" {
		return status.Errorf(status.InvalidArgument, "%s must be a valid image data URL", field)
	}

	mediaType, base64Encoded, err := parseBrandingDataURLMetadata(metadata)
	if err != nil {
		return status.Errorf(status.InvalidArgument, "%s must be a valid image data URL", field)
	}
	if _, ok := allowedBrandingImageTypes[mediaType]; !ok {
		return status.Errorf(status.InvalidArgument, "%s must use one of the supported image types: image/png, image/jpeg, image/webp, image/svg+xml", field)
	}

	payloadData, err := brandingDataURLPayload(payload, base64Encoded)
	if err != nil {
		return status.Errorf(status.InvalidArgument, "%s must contain valid image data", field)
	}
	if len(payloadData) > maxBrandingImageBytes {
		return status.Errorf(status.InvalidArgument, "%s can't be larger than %d KB", field, maxBrandingImageBytes/1024)
	}
	if mediaType == "image/svg+xml" {
		if err := validateBrandingSVG(payloadData); err != nil {
			return status.Errorf(status.InvalidArgument, "%s SVG is not allowed: %v", field, err)
		}
	}

	return nil
}

func parseBrandingDataURLMetadata(metadata string) (string, bool, error) {
	parts := strings.Split(metadata, ";")
	if parts[0] == "" {
		return "", false, fmt.Errorf("missing media type")
	}

	base64Encoded := false
	mediaTypeParts := []string{parts[0]}
	for _, part := range parts[1:] {
		if strings.EqualFold(part, "base64") {
			base64Encoded = true
			continue
		}
		mediaTypeParts = append(mediaTypeParts, part)
	}

	mediaType, _, err := mime.ParseMediaType(strings.Join(mediaTypeParts, ";"))
	if err != nil {
		return "", false, err
	}
	return strings.ToLower(mediaType), base64Encoded, nil
}

func brandingDataURLPayload(payload string, base64Encoded bool) ([]byte, error) {
	if len(payload) > maxBrandingImageBytes*4 {
		return make([]byte, maxBrandingImageBytes+1), nil
	}

	decodedPayload, err := url.PathUnescape(payload)
	if err != nil {
		return nil, err
	}

	if !base64Encoded {
		return []byte(decodedPayload), nil
	}

	decoded, err := base64.StdEncoding.DecodeString(decodedPayload)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(decodedPayload)
	}
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func validateBrandingSVG(payload []byte) error {
	decoder := xml.NewDecoder(bytes.NewReader(payload))
	seenRoot := false
	styleDepth := 0

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid XML")
		}

		switch typedToken := token.(type) {
		case xml.StartElement:
			start := typedToken
			elementName := strings.ToLower(start.Name.Local)
			if !seenRoot {
				if elementName != "svg" {
					return fmt.Errorf("root element must be svg")
				}
				seenRoot = true
			}

			if isUnsafeBrandingSVGElement(elementName) {
				return fmt.Errorf("%s elements are not allowed", start.Name.Local)
			}

			if elementName == "style" {
				styleDepth++
			}

			for _, attr := range start.Attr {
				if isUnsafeBrandingSVGAttribute(attr) {
					return fmt.Errorf("%s attributes are not allowed", attr.Name.Local)
				}
			}
		case xml.EndElement:
			if strings.EqualFold(typedToken.Name.Local, "style") && styleDepth > 0 {
				styleDepth--
			}
		case xml.CharData:
			if styleDepth > 0 && isUnsafeBrandingSVGStyle(string(typedToken)) {
				return fmt.Errorf("style content is not allowed")
			}
		default:
			continue
		}
	}

	if !seenRoot {
		return fmt.Errorf("missing svg root")
	}
	return nil
}

func isUnsafeBrandingSVGElement(name string) bool {
	switch name {
	case "script", "foreignobject", "iframe", "object", "embed":
		return true
	default:
		return false
	}
}

func isUnsafeBrandingSVGAttribute(attr xml.Attr) bool {
	name := strings.ToLower(attr.Name.Local)

	if strings.HasPrefix(name, "on") {
		return true
	}
	if name == "href" || name == "src" {
		return isUnsafeBrandingSVGReference(attr.Value)
	}
	if name == "style" && isUnsafeBrandingSVGStyle(attr.Value) {
		return true
	}
	if hasUnsafeBrandingSVGURLFunction(attr.Value) {
		return true
	}

	return false
}

func isUnsafeBrandingSVGStyle(style string) bool {
	normalized := normalizeBrandingSVGReference(style)
	if strings.Contains(normalized, "javascript:") ||
		strings.Contains(normalized, "data:") ||
		strings.Contains(normalized, "expression(") ||
		strings.Contains(normalized, "@import") ||
		strings.Contains(normalized, "-moz-binding") {
		return true
	}

	return hasUnsafeBrandingSVGURLFunction(style)
}

func hasUnsafeBrandingSVGURLFunction(value string) bool {
	for _, match := range brandingSVGStyleURLRegexp.FindAllStringSubmatch(value, -1) {
		if len(match) > 1 && isUnsafeBrandingSVGReference(match[1]) {
			return true
		}
	}

	return false
}

func isUnsafeBrandingSVGReference(value string) bool {
	normalized := normalizeBrandingSVGReference(value)
	if normalized == "" {
		return false
	}

	return !strings.HasPrefix(normalized, "#")
}

func normalizeBrandingSVGReference(value string) string {
	value = strings.Trim(strings.TrimSpace(value), `"'`)
	value = strings.Map(func(r rune) rune {
		if r <= ' ' || r == 0x7f {
			return -1
		}
		return r
	}, value)

	return strings.ToLower(value)
}
