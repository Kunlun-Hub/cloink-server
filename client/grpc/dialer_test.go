package grpc

import "testing"

func TestNormalizeTarget(t *testing.T) {
	tests := []struct {
		name       string
		addr       string
		tlsEnabled bool
		want       string
	}{
		{name: "https hostname", addr: "signal.example.com", tlsEnabled: true, want: "signal.example.com:443"},
		{name: "http hostname", addr: "signal.example.com", tlsEnabled: false, want: "signal.example.com:80"},
		{name: "explicit port", addr: "signal.example.com:8443", tlsEnabled: true, want: "signal.example.com:8443"},
		{name: "empty port", addr: "signal.example.com:", tlsEnabled: true, want: "signal.example.com:443"},
		{name: "bracketed ipv6", addr: "[2001:db8::1]", tlsEnabled: true, want: "[2001:db8::1]:443"},
		{name: "bare ipv6", addr: "2001:db8::1", tlsEnabled: false, want: "[2001:db8::1]:80"},
		{name: "resolver target", addr: "dns:///signal.example.com:443", tlsEnabled: true, want: "dns:///signal.example.com:443"},
		{name: "unix target", addr: "unix:///tmp/cloink.sock", tlsEnabled: true, want: "unix:///tmp/cloink.sock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeTarget(tt.addr, tt.tlsEnabled); got != tt.want {
				t.Fatalf("normalizeTarget(%q, %v) = %q, want %q", tt.addr, tt.tlsEnabled, got, tt.want)
			}
		})
	}
}
