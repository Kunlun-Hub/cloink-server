package version

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	goversion "github.com/hashicorp/go-version"
)

const httpAgent = "pkg/test"

func TestNewUpdate(t *testing.T) {
	version = "1.0.0"
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "10.0.0")
	}))
	defer svr.Close()
	versionURL = svr.URL

	wg := &sync.WaitGroup{}
	wg.Add(1)

	onUpdate := false
	u := NewUpdateAndStart(httpAgent)
	defer u.StopWatch()
	u.SetOnUpdateListener(func() {
		onUpdate = true
		wg.Done()
	})

	waitTimeout(wg)
	if onUpdate != true {
		t.Errorf("update not found")
	}
}

func TestVersionCheckUsesDefaultReleaseEndpoint(t *testing.T) {
	previousURL := versionURL
	versionURL = ""
	t.Cleanup(func() { versionURL = previousURL })

	u := NewUpdate("nb/client")
	if u == nil || versionURL != DefaultReleaseAPIURL {
		t.Fatal("default release endpoint not configured")
	}
}

func TestManagementVersionCheckStaysDisabledWithoutEndpoint(t *testing.T) {
	previousURL := versionURL
	versionURL = ""
	t.Cleanup(func() { versionURL = previousURL })

	u := NewUpdate("nb/management")
	u.StartFetcher()

	if u.fetchTicker != nil {
		t.Fatal("management version fetcher started without an endpoint")
	}
}

func TestDoNotUpdate(t *testing.T) {
	version = "11.0.0"
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "10.0.0")
	}))
	defer svr.Close()
	versionURL = svr.URL

	wg := &sync.WaitGroup{}
	wg.Add(1)

	onUpdate := false
	u := NewUpdateAndStart(httpAgent)
	defer u.StopWatch()
	u.SetOnUpdateListener(func() {
		onUpdate = true
		wg.Done()
	})

	waitTimeout(wg)
	if onUpdate == true {
		t.Errorf("invalid update")
	}
}

func TestDaemonUpdate(t *testing.T) {
	version = "11.0.0"
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "11.0.0")
	}))
	defer svr.Close()
	versionURL = svr.URL

	wg := &sync.WaitGroup{}
	wg.Add(1)

	onUpdate := false
	u := NewUpdateAndStart(httpAgent)
	defer u.StopWatch()
	u.SetOnUpdateListener(func() {
		onUpdate = true
		wg.Done()
	})

	u.SetDaemonVersion("10.0.0")

	waitTimeout(wg)
	if onUpdate != true {
		t.Errorf("invalid daemon version check")
	}
}

func TestParseLatestVersionFourPartRelease(t *testing.T) {
	latest, err := parseLatestVersion([]byte(`[{"version":"0.83.1.1","platform":"windows","architecture":"amd64","isLatest":true}]`))
	if err != nil {
		t.Fatal(err)
	}
	current, err := goversion.NewVersion("0.83.1")
	if err != nil {
		t.Fatal(err)
	}
	if !latest.GreaterThan(current) {
		t.Fatalf("four-part release %s was not considered newer than %s", latest, current)
	}
}

func waitTimeout(wg *sync.WaitGroup) {
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
		return
	case <-time.After(time.Second):
		return
	}
}
