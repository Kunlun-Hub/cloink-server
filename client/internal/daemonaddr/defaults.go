package daemonaddr

const (
	// UnixSocketAddr is the Cloink daemon's default control socket.
	UnixSocketAddr = "unix:///var/run/cloink.sock"
	// legacyUnixSocketAddr remains readable during the desktop migration window.
	legacyUnixSocketAddr = "unix:///var/run/netbird.sock"
)
