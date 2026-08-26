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

// IsPrefixBroadcast reports the directed broadcast address of an IPv4 prefix.
func IsPrefixBroadcast(addr netip.Addr, prefix netip.Prefix) bool {
	addr = addr.Unmap()
	if !addr.Is4() || !prefix.IsValid() || !prefix.Addr().Unmap().Is4() || prefix.Bits() >= 32 {
		return false
	}
	prefix = netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()).Masked()
	if !prefix.Contains(addr) {
		return false
	}
	base := prefix.Addr().As4()
	bits := prefix.Bits()
	for bit := bits; bit < 32; bit++ {
		base[bit/8] |= 1 << (7 - uint(bit%8))
	}
	return addr == netip.AddrFrom4(base)
}
