package wgbackend

import "github.com/netbirdio/wireguard-go/tun"

type NativeTUN = tun.NativeTun

var CreateTUNWithRequestedGUID = tun.CreateTUNWithRequestedGUID

func SetWintunTunnelType(tunnelType string) {
	tun.WintunTunnelType = tunnelType
}
