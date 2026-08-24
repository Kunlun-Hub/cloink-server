//go:build windows

package daemonaddr

import (
	"net"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
	log "github.com/sirupsen/logrus"
)

// probeTimeout bounds each transport probe. Both are local, so a daemon that is
// listening answers immediately and one that is not fails immediately.
const probeTimeout = 300 * time.Millisecond

// ResolveDaemonAddr prefers the Cloink named pipe and falls back to the old
// NetBird named pipe during the migration window. It never silently moves a
// client onto the legacy loopback TCP transport.
//
// Using that address automatically would be a downgrade the user never asked for:
// any local process can bind 127.0.0.1 while the daemon is not listening, and the
// transport carries no caller identity, so a client that accepted whatever answered
// would hand a setup key, a pre-shared key or an SSO prompt to a local impostor. An
// operator who needs the legacy address during the upgrade window can still pass
// --daemon-addr explicitly, which is a deliberate choice and still refuses the
// privileged operations.
//
// Only the pipe address is resolved. A custom address is left alone, though passing
// --daemon-addr npipe://netbird explicitly is indistinguishable from the default
// here, so it is treated the same way.
func ResolveDaemonAddr(addr string) string {
	if addr != WindowsPipeAddr {
		return addr
	}

	for _, path := range PipePaths(strings.TrimPrefix(WindowsPipeAddr, pipeScheme)) {
		if pipeAvailable(path) {
			return addr
		}
	}
	for _, path := range PipePaths(strings.TrimPrefix(legacyWindowsPipeAddr, pipeScheme)) {
		if pipeAvailable(path) {
			log.Infof("Cloink daemon pipe is not available; using the legacy NetBird daemon pipe during migration")
			return legacyWindowsPipeAddr
		}
	}

	if tcpAvailable(legacyWindowsAddr) {
		log.Warnf("the daemon is not serving %s, but something is listening on the legacy %s. "+
			"Restart the Cloink service so it serves the pipe. That address is not used automatically: "+
			"any local user can bind it and it carries no caller identity, so pass --daemon-addr %s "+
			"explicitly if you accept that",
			WindowsPipeAddr, legacyWindowsAddr, legacyWindowsAddr)
	}

	return addr
}

func pipeAvailable(path string) bool {
	timeout := probeTimeout
	conn, err := winio.DialPipe(path, &timeout)
	if err != nil {
		return false
	}
	if err := conn.Close(); err != nil {
		log.Debugf("close daemon pipe probe: %v", err)
	}
	return true
}

func tcpAvailable(addr string) bool {
	host := addr
	if _, after, ok := strings.Cut(addr, "://"); ok {
		host = after
	}

	conn, err := net.DialTimeout("tcp", host, probeTimeout)
	if err != nil {
		return false
	}
	if err := conn.Close(); err != nil {
		log.Debugf("close daemon TCP probe: %v", err)
	}
	return true
}
