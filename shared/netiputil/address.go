package netiputil

import "net/netip"

var limitedBroadcast = netip.MustParseAddr("255.255.255.255")

// IsSystemLocalAddress reports addresses that are only meaningful to the local
// host or link and should not be persisted as network traffic events.
func IsSystemLocalAddress(addr netip.Addr) bool {
	addr = addr.Unmap()
	return !addr.IsValid() ||
		addr.IsUnspecified() ||
		addr.IsLoopback() ||
		addr.IsMulticast() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr == limitedBroadcast
}
