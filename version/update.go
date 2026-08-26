package version

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	goversion "github.com/hashicorp/go-version"
	log "github.com/sirupsen/logrus"
)

const (
	fetchPeriod = 30 * time.Minute

	// EnvVersionCheckURL enables version checks against an explicitly managed
	// endpoint. An empty value disables checks and prevents public fallbacks.
	EnvVersionCheckURL = "NB_VERSION_CHECK_URL"
)

var (
	versionURL = strings.TrimSpace(os.Getenv(EnvVersionCheckURL))
)

// Update fetch the version info periodically and notify the onUpdateListener in case the UI version or the
// daemon version are deprecated
type Update struct {
	httpAgent       string
	uiVersion       *goversion.Version
	daemonVersion   *goversion.Version
	latestAvailable *goversion.Version
	versionsLock    sync.Mutex
	fetchLock       sync.Mutex

	fetchTicker *time.Ticker
	fetchDone   chan struct{}

	onUpdateListener func()
	listenerLock     sync.Mutex
}

type publicRelease struct {
	Version      string `json:"version"`
	Platform     string `json:"platform"`
	Architecture string `json:"architecture"`
	IsLatest     bool   `json:"isLatest"`
}

// NewUpdate instantiate Update and start to fetch the new version information
func NewUpdate(httpAgent string) *Update {
	if versionURL == "" && httpAgent == "nb/client" {
		versionURL = DefaultReleaseAPIURL
	}
	currentVersion, err := goversion.NewVersion(version)
	if err != nil {
		currentVersion, _ = goversion.NewVersion("0.0.0")
	}

	u := &Update{
		httpAgent: httpAgent,
		uiVersion: currentVersion,
		fetchDone: make(chan struct{}),
	}

	return u
}

func NewUpdateAndStart(httpAgent string) *Update {
	u := NewUpdate(httpAgent)
	go u.StartFetcher()

	return u
}

// StopWatch stop the version info fetch loop
func (u *Update) StopWatch() {
	u.fetchLock.Lock()
	ticker := u.fetchTicker
	u.fetchLock.Unlock()
	if ticker == nil {
		return
	}

	ticker.Stop()

	select {
	case u.fetchDone <- struct{}{}:
	default:
	}
}

// SetDaemonVersion update the currently running daemon version. If new version is available it will trigger
// the onUpdateListener
func (u *Update) SetDaemonVersion(newVersion string) bool {
	daemonVersion, err := goversion.NewVersion(newVersion)
	if err != nil {
		daemonVersion, _ = goversion.NewVersion("0.0.0")
	}

	u.versionsLock.Lock()
	if u.daemonVersion != nil && u.daemonVersion.Equal(daemonVersion) {
		u.versionsLock.Unlock()
		return false
	}

	u.daemonVersion = daemonVersion
	u.versionsLock.Unlock()
	return u.checkUpdate()
}

// SetOnUpdateListener set new update listener
func (u *Update) SetOnUpdateListener(updateFn func()) {
	u.listenerLock.Lock()
	defer u.listenerLock.Unlock()

	u.onUpdateListener = updateFn
	if u.isUpdateAvailable() {
		u.onUpdateListener()
	}
}

func (u *Update) LatestVersion() *goversion.Version {
	u.versionsLock.Lock()
	defer u.versionsLock.Unlock()
	return u.latestAvailable
}

func (u *Update) StartFetcher() {
	u.fetchLock.Lock()
	if u.fetchTicker != nil {
		u.fetchLock.Unlock()
		return
	}
	if versionURL == "" {
		u.fetchLock.Unlock()
		log.Debugf("version check disabled: %s is not configured", EnvVersionCheckURL)
		return
	}
	u.fetchTicker = time.NewTicker(fetchPeriod)
	u.fetchLock.Unlock()

	if changed := u.fetchVersion(); changed {
		u.checkUpdate()
	}

	for {
		select {
		case <-u.fetchDone:
			return
		case <-u.fetchTicker.C:
			if changed := u.fetchVersion(); changed {
				u.checkUpdate()
			}
		}
	}
}

func (u *Update) fetchVersion() bool {
	log.Debugf("fetching version info from %s", versionURL)

	endpoint := versionURL
	if strings.Contains(endpoint, "/api/version-releases/public") {
		parsed, err := url.Parse(endpoint)
		if err == nil {
			query := parsed.Query()
			query.Set("platform", releasePlatform())
			query.Set("architecture", runtime.GOARCH)
			query.Set("latest", "true")
			parsed.RawQuery = query.Encode()
			endpoint = parsed.String()
		}
	}

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		log.Errorf("failed to create request for version info: %s", err)
		return false
	}

	req.Header.Set("User-Agent", u.httpAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("failed to fetch version info: %s", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("invalid status code: %d", resp.StatusCode)
		return false
	}

	if resp.ContentLength > 64*1024 {
		log.Errorf("too large response: %d", resp.ContentLength)
		return false
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("failed to read content: %s", err)
		return false
	}

	latestAvailable, err := parseLatestVersion(content)
	if err != nil {
		log.Errorf("failed to parse the version response: %s", err)
		return false
	}

	u.versionsLock.Lock()
	defer u.versionsLock.Unlock()

	if u.latestAvailable != nil && u.latestAvailable.Equal(latestAvailable) {
		return false
	}
	u.latestAvailable = latestAvailable

	return true
}

func releasePlatform() string {
	switch runtime.GOOS {
	case "darwin":
		return "macos"
	case "windows":
		return "windows"
	default:
		return "linux"
	}
}

func parseLatestVersion(content []byte) (*goversion.Version, error) {
	if parsed, err := goversion.NewVersion(strings.TrimSpace(string(content))); err == nil {
		return parsed, nil
	}

	var releases []publicRelease
	if err := json.Unmarshal(content, &releases); err != nil {
		return nil, err
	}
	var latest *goversion.Version
	for _, release := range releases {
		candidate, err := goversion.NewVersion(strings.TrimPrefix(strings.TrimSpace(release.Version), "v"))
		if err != nil {
			continue
		}
		if latest == nil || candidate.GreaterThan(latest) {
			latest = candidate
		}
	}
	if latest == nil {
		return nil, io.ErrUnexpectedEOF
	}
	return latest, nil
}

func (u *Update) checkUpdate() bool {
	if !u.isUpdateAvailable() {
		return false
	}

	u.listenerLock.Lock()
	defer u.listenerLock.Unlock()
	if u.onUpdateListener == nil {
		return true
	}

	go u.onUpdateListener()
	return true
}

func (u *Update) isUpdateAvailable() bool {
	u.versionsLock.Lock()
	defer u.versionsLock.Unlock()

	if u.latestAvailable == nil {
		return false
	}

	if u.latestAvailable.GreaterThan(u.uiVersion) {
		return true
	}

	if u.daemonVersion == nil {
		return false
	}

	if u.latestAvailable.GreaterThan(u.daemonVersion) {
		return true
	}
	return false
}
