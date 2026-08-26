package version

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
)

// PublicRelease is the signed installer metadata exposed by Cloink's public
// release endpoint. DownloadURL may be absolute or relative to the API host.
type PublicRelease struct {
	ID           string `json:"id"`
	Version      string `json:"version"`
	Platform     string `json:"platform"`
	Architecture string `json:"architecture"`
	Channel      string `json:"channel"`
	DownloadURL  string `json:"downloadUrl"`
	SHA256       string `json:"sha256"`
	Signature    string `json:"signature"`
	IsLatest     bool   `json:"isLatest"`
}

// EnvReleaseAPIURL configures the management endpoint used to fetch signed releases.
const EnvReleaseAPIURL = "CLOINK_RELEASE_API_URL"

const (
	DefaultReleaseAPIURL = "https://one.4w.ink/api/version-releases/public"
	releaseSignatureV1   = "cloink-release-v1"
	updatePublicKeyPEM   = `-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARwD+OWz6NI8nYVgPMyGtgsLtkqxUcb+JEu+0RK+MGj8=
-----END PUBLIC KEY-----`
)

func publicReleaseAPIURL() (string, error) {
	value := strings.TrimSpace(os.Getenv(EnvReleaseAPIURL))
	if value == "" {
		return DefaultReleaseAPIURL, nil
	}
	return value, nil
}

// FetchPublicReleases returns signed releases matching the requested target.
// A blank target asks for the latest release; callers can then select an
// installer format from the returned download URL.
func FetchPublicReleases(ctx context.Context, platform, architecture, channel, target string) ([]PublicRelease, error) {
	configuredURL, err := publicReleaseAPIURL()
	if err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(configuredURL)
	if err != nil {
		return nil, fmt.Errorf("parse release endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("platform", platform)
	query.Set("architecture", architecture)
	if channel != "" {
		query.Set("channel", channel)
	}
	if target == "" {
		query.Set("latest", "true")
	} else {
		query.Del("latest")
	}
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "cloink-client/"+NetbirdVersion())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch releases: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("release endpoint returned %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var releases []PublicRelease
	if err := json.Unmarshal(body, &releases); err != nil {
		return nil, fmt.Errorf("decode releases: %w", err)
	}
	filtered := releases[:0]
	for _, release := range releases {
		if strings.TrimSpace(release.Version) == "" || release.DownloadURL == "" || release.SHA256 == "" || release.Signature == "" {
			continue
		}
		if platform != "" && !strings.EqualFold(release.Platform, platform) ||
			architecture != "" && !strings.EqualFold(release.Architecture, architecture) ||
			channel != "" && !strings.EqualFold(release.Channel, channel) ||
			target == "" && !release.IsLatest {
			continue
		}
		if target != "" && normalizeReleaseVersion(release.Version) != normalizeReleaseVersion(target) {
			continue
		}
		artifactURL, err := url.Parse(release.DownloadURL)
		if err != nil {
			continue
		}
		release.DownloadURL = endpoint.ResolveReference(artifactURL).String()
		if err := VerifyReleaseSignature(release); err != nil {
			continue
		}
		filtered = append(filtered, release)
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("no signed %s/%s release for version %q", platform, architecture, target)
	}
	return filtered, nil
}

// ReleaseSignaturePayload returns the stable metadata representation signed by
// the offline Cloink release key. DownloadURL is intentionally excluded: the
// signed SHA256 binds any permitted URL to the exact artifact bytes.
func ReleaseSignaturePayload(release PublicRelease) []byte {
	return []byte(fmt.Sprintf(
		"%s\nversion=%s\nplatform=%s\narchitecture=%s\nchannel=%s\nsha256=%s\n",
		releaseSignatureV1,
		normalizeReleaseVersion(release.Version),
		strings.ToLower(strings.TrimSpace(release.Platform)),
		strings.ToLower(strings.TrimSpace(release.Architecture)),
		strings.ToLower(strings.TrimSpace(release.Channel)),
		strings.ToLower(strings.TrimSpace(release.SHA256)),
	))
}

// VerifyReleaseSignature verifies release metadata against the public key
// embedded in every Cloink client build.
func VerifyReleaseSignature(release PublicRelease) error {
	block, _ := pem.Decode([]byte(updatePublicKeyPEM))
	if block == nil {
		return fmt.Errorf("decode embedded update public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse embedded update public key: %w", err)
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return fmt.Errorf("embedded update public key is not Ed25519")
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(release.Signature))
	if err != nil {
		return fmt.Errorf("decode release signature: %w", err)
	}
	if !ed25519.Verify(publicKey, ReleaseSignaturePayload(release), signature) {
		return fmt.Errorf("invalid release signature")
	}
	return nil
}

func normalizeReleaseVersion(value string) string {
	return strings.TrimPrefix(strings.TrimSpace(value), "v")
}

// VerifySHA256 checks an installer against the checksum published by the
// management server before the platform installer is launched.
func VerifySHA256(file io.Reader, expected string) error {
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	got := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(got, strings.TrimSpace(expected)) {
		return fmt.Errorf("sha256 mismatch: got %s, want %s", got, expected)
	}
	return nil
}

func releasePlatformForRuntime() string {
	switch runtime.GOOS {
	case "darwin":
		return "macos"
	case "windows":
		return "windows"
	default:
		return "linux"
	}
}

func releaseArchitectureForRuntime() string {
	return runtime.GOARCH
}

func releaseFileName(downloadURL string) string {
	return path.Base(downloadURL)
}
