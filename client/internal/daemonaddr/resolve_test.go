//go:build !windows && !ios && !android

package daemonaddr

import (
	"os"
	"path/filepath"
	"testing"
)

// createSockFile creates a regular file with a .sock extension.
// ResolveUnixDaemonAddr uses os.Stat (not net.Dial), so a regular file is
// sufficient and avoids Unix socket path-length limits on macOS.
func createSockFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("failed to create test sock file at %s: %v", path, err)
	}
}

func TestResolveUnixDaemonAddr_DefaultExists(t *testing.T) {
	tmp := t.TempDir()
	sock := filepath.Join(tmp, "netbird.sock")
	createSockFile(t, sock)

	addr := "unix://" + sock
	got := ResolveUnixDaemonAddr(addr)
	if got != addr {
		t.Errorf("expected %s, got %s", addr, got)
	}
}

func TestResolveUnixDaemonAddr_FallsBackToLegacySocket(t *testing.T) {
	tmp := t.TempDir()
	defaultPath := filepath.Join(tmp, "cloink.sock")
	legacyPath := filepath.Join(tmp, "netbird.sock")
	createSockFile(t, legacyPath)

	originalDefaultSocket := defaultUnixSocket
	originalLegacySocket := legacyUnixSocket
	defaultUnixSocket = "unix://" + defaultPath
	legacyUnixSocket = "unix://" + legacyPath
	t.Cleanup(func() {
		defaultUnixSocket = originalDefaultSocket
		legacyUnixSocket = originalLegacySocket
	})

	got := ResolveUnixDaemonAddr(defaultUnixSocket)
	if got != legacyUnixSocket {
		t.Errorf("expected legacy socket %s, got %s", legacyUnixSocket, got)
	}
}

func TestResolveUnixDaemonAddr_CustomSocketDoesNotUseLegacyDefault(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "netbird.sock")
	createSockFile(t, legacyPath)

	originalLegacySocket := legacyUnixSocket
	legacyUnixSocket = "unix://" + legacyPath
	t.Cleanup(func() { legacyUnixSocket = originalLegacySocket })
	originalScanDir := scanDir
	setScanDir(filepath.Join(tmp, "empty"))
	t.Cleanup(func() { setScanDir(originalScanDir) })

	customAddr := "unix://" + filepath.Join(tmp, "custom.sock")
	got := ResolveUnixDaemonAddr(customAddr)
	if got != customAddr {
		t.Errorf("expected custom socket %s, got %s", customAddr, got)
	}
}

func TestResolveUnixDaemonAddr_SingleDiscovered(t *testing.T) {
	tmp := t.TempDir()

	// Default socket does not exist
	defaultAddr := "unix://" + filepath.Join(tmp, "netbird.sock")

	// Create a scan dir with one socket
	sd := filepath.Join(tmp, "netbird")
	if err := os.MkdirAll(sd, 0o755); err != nil {
		t.Fatal(err)
	}
	instanceSock := filepath.Join(sd, "main.sock")
	createSockFile(t, instanceSock)

	origScanDir := scanDir
	setScanDir(sd)
	t.Cleanup(func() { setScanDir(origScanDir) })

	got := ResolveUnixDaemonAddr(defaultAddr)
	expected := "unix://" + instanceSock
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestResolveUnixDaemonAddr_MultipleDiscovered(t *testing.T) {
	tmp := t.TempDir()

	defaultAddr := "unix://" + filepath.Join(tmp, "netbird.sock")

	sd := filepath.Join(tmp, "netbird")
	if err := os.MkdirAll(sd, 0o755); err != nil {
		t.Fatal(err)
	}
	createSockFile(t, filepath.Join(sd, "main.sock"))
	createSockFile(t, filepath.Join(sd, "other.sock"))

	origScanDir := scanDir
	setScanDir(sd)
	t.Cleanup(func() { setScanDir(origScanDir) })

	got := ResolveUnixDaemonAddr(defaultAddr)
	if got != defaultAddr {
		t.Errorf("expected original %s, got %s", defaultAddr, got)
	}
}

func TestResolveUnixDaemonAddr_NoSocketsFound(t *testing.T) {
	tmp := t.TempDir()

	defaultAddr := "unix://" + filepath.Join(tmp, "netbird.sock")

	sd := filepath.Join(tmp, "netbird")
	if err := os.MkdirAll(sd, 0o755); err != nil {
		t.Fatal(err)
	}

	origScanDir := scanDir
	setScanDir(sd)
	t.Cleanup(func() { setScanDir(origScanDir) })

	got := ResolveUnixDaemonAddr(defaultAddr)
	if got != defaultAddr {
		t.Errorf("expected original %s, got %s", defaultAddr, got)
	}
}

func TestResolveUnixDaemonAddr_NonUnixAddr(t *testing.T) {
	addr := "tcp://127.0.0.1:41731"
	got := ResolveUnixDaemonAddr(addr)
	if got != addr {
		t.Errorf("expected %s, got %s", addr, got)
	}
}

func TestResolveUnixDaemonAddr_ScanDirMissing(t *testing.T) {
	tmp := t.TempDir()

	defaultAddr := "unix://" + filepath.Join(tmp, "netbird.sock")

	origScanDir := scanDir
	setScanDir(filepath.Join(tmp, "nonexistent"))
	t.Cleanup(func() { setScanDir(origScanDir) })

	got := ResolveUnixDaemonAddr(defaultAddr)
	if got != defaultAddr {
		t.Errorf("expected original %s, got %s", defaultAddr, got)
	}
}
