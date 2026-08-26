package version

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicReleaseAPIURLDefaultsToCloink(t *testing.T) {
	t.Setenv(EnvReleaseAPIURL, "")

	got, err := publicReleaseAPIURL()
	if err != nil {
		t.Fatal(err)
	}
	if got != DefaultReleaseAPIURL {
		t.Fatalf("unexpected default release URL: %s", got)
	}
}

func TestVerifyReleaseSignature(t *testing.T) {
	release := signedReleaseFixture()
	if err := VerifyReleaseSignature(release); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	release.Architecture = "arm64"
	if err := VerifyReleaseSignature(release); err == nil {
		t.Fatal("signature accepted after metadata tampering")
	}
}

func TestFetchPublicReleasesRejectsInvalidSignatures(t *testing.T) {
	valid := signedReleaseFixture()
	invalid := valid
	invalid.SHA256 = strings.Repeat("1", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]PublicRelease{invalid, valid})
	}))
	defer server.Close()
	t.Setenv(EnvReleaseAPIURL, server.URL)

	releases, err := FetchPublicReleases(context.Background(), "windows", "amd64", "stable", "0.77.2")
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 || releases[0].Version != "0.77.2" {
		t.Fatalf("unexpected releases: %+v", releases)
	}
}

func signedReleaseFixture() PublicRelease {
	return PublicRelease{
		Version:      "0.77.2",
		Platform:     "windows",
		Architecture: "amd64",
		Channel:      "stable",
		DownloadURL:  "/api/version-releases/files/test",
		SHA256:       strings.Repeat("0", 64),
		Signature:    "yBTG63bjgMYpY28tsJK0keO33j04HjVKZ5MkPrTdMdqYjU+7OFHz4HrkSZAZN+YpFcR3aI1iHz5TpMYVwNoeCA==",
		IsLatest:     true,
	}
}
