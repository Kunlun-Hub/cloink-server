package desktop

import "github.com/netbirdio/netbird/version"

// GetUIUserAgent returns the Desktop ui user agent
func GetUIUserAgent() string {
	return "cloink-desktop-ui/" + version.NetbirdVersion()
}
