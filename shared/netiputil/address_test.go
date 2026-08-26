package netiputil

import (
	"net/netip"
	"testing"
)

func TestIsSystemLocalAddress(t *testing.T) {
	tests := []struct {
		name string
		addr netip.Addr
		want bool
	}{
		{name: "invalid", want: true},
		{name: "IPv4 unspecified", addr: netip.MustParseAddr("0.0.0.0"), want: true},
		{name: "IPv6 unspecified", addr: netip.MustParseAddr("::"), want: true},
		{name: "IPv4 loopback", addr: netip.MustParseAddr("127.0.0.1"), want: true},
		{name: "IPv6 loopback", addr: netip.MustParseAddr("::1"), want: true},
		{name: "IPv4 link local", addr: netip.MustParseAddr("169.254.10.20"), want: true},
		{name: "IPv6 link local", addr: netip.MustParseAddr("fe80::1"), want: true},
		{name: "IPv4 multicast", addr: netip.MustParseAddr("224.0.0.252"), want: true},
		{name: "IPv6 multicast", addr: netip.MustParseAddr("ff02::16"), want: true},
		{name: "IPv4 limited broadcast", addr: netip.MustParseAddr("255.255.255.255"), want: true},
		{name: "mapped IPv4 multicast", addr: netip.MustParseAddr("::ffff:224.0.0.252"), want: true},
		{name: "overlay address", addr: netip.MustParseAddr("100.80.165.252"), want: false},
		{name: "private address", addr: netip.MustParseAddr("192.168.1.20"), want: false},
		{name: "public address", addr: netip.MustParseAddr("8.8.8.8"), want: false},
		{name: "global IPv6", addr: netip.MustParseAddr("2001:4860:4860::8888"), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsSystemLocalAddress(test.addr); got != test.want {
				t.Fatalf("IsSystemLocalAddress(%s) = %v, want %v", test.addr, got, test.want)
			}
		})
	}
}
