//go:build !android && !ios && !freebsd && !js

package main

import (
	"runtime"
	"testing"

	"github.com/netbirdio/netbird/client/internal/daemonaddr"
)

func TestDaemonAddrUsesCloinkDefaultSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Windows client uses a named pipe")
	}

	if got, want := DaemonAddr(), daemonaddr.UnixSocketAddr; got != want {
		t.Fatalf("DaemonAddr() = %q, want %q", got, want)
	}
}
