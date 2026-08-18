package version

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func publicReleaseAPIURL() (string, error) {
	value := strings.TrimSpace(os.Getenv(EnvReleaseAPIURL))
	if value == "" {
		return "", fmt.Errorf("%s is not configured", EnvReleaseAPIURL)
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
		if target != "" && normalizeReleaseVersion(release.Version) != normalizeReleaseVersion(target) {
			continue
		}
		artifactURL, err := url.Parse(release.DownloadURL)
		if err != nil {
			continue
		}
		release.DownloadURL = endpoint.ResolveReference(artifactURL).String()
		filtered = append(filtered, release)
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("no signed %s/%s release for version %q", platform, architecture, target)
	}
	return filtered, nil
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
