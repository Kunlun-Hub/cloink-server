package version

import (
	"context"
	"testing"
)

func TestFetchPublicReleasesRequiresConfiguredEndpoint(t *testing.T) {
	t.Setenv(EnvReleaseAPIURL, "")

	_, err := FetchPublicReleases(context.Background(), "linux", "amd64", "stable", "")
	if err == nil {
		t.Fatal("expected release endpoint configuration error")
	}
}
