//go:build !windows && !ios && !android

package daemonaddr

import (
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	scanDir           = "/var/run/netbird"
	defaultUnixSocket = UnixSocketAddr
	legacyUnixSocket  = legacyUnixSocketAddr
)

// setScanDir overrides the scan directory (used by tests).
func setScanDir(dir string) {
	scanDir = dir
}

// ResolveUnixDaemonAddr checks the requested socket, the legacy NetBird default,
// and then instance sockets under /var/run/netbird. Custom addresses never use
// the legacy default implicitly.
func ResolveUnixDaemonAddr(addr string) string {
	if !strings.HasPrefix(addr, "unix://") {
		return addr
	}

	sockPath := strings.TrimPrefix(addr, "unix://")
	if _, err := os.Stat(sockPath); err == nil {
		return addr
	}
	if addr == defaultUnixSocket {
		legacyPath := strings.TrimPrefix(legacyUnixSocket, "unix://")
		if _, err := os.Stat(legacyPath); err == nil {
			log.Infof("Cloink daemon socket is not available; using the legacy NetBird daemon socket during migration")
			return legacyUnixSocket
		}
	}

	entries, err := os.ReadDir(scanDir)
	if err != nil {
		return addr
	}

	var found []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".sock") {
			found = append(found, filepath.Join(scanDir, e.Name()))
		}
	}

	switch len(found) {
	case 1:
		resolved := "unix://" + found[0]
		log.Debugf("Default daemon socket not found, using discovered socket: %s", resolved)
		return resolved
	case 0:
		return addr
	default:
		log.Warnf("Default daemon socket not found and multiple sockets discovered in %s; pass --daemon-addr explicitly", scanDir)
		return addr
	}
}
