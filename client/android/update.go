//go:build android

package android

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/netbirdio/netbird/version"
)

// LatestSignedRelease returns verified release metadata for mobile UIs. The
// response is JSON because gomobile cannot bind the version.PublicRelease
// struct directly without exposing updater internals to Java and Swift.
func LatestSignedRelease(platform, architecture string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	releases, err := version.FetchPublicReleases(ctx, platform, architecture, "stable", "")
	if err != nil {
		return "", err
	}
	if len(releases) == 0 {
		return "", fmt.Errorf("no signed release available")
	}
	payload, err := json.Marshal(releases[0])
	if err != nil {
		return "", fmt.Errorf("encode release metadata: %w", err)
	}
	return string(payload), nil
}
