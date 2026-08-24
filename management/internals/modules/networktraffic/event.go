package networktraffic

import (
	"fmt"
	"net/netip"
	"time"

	"github.com/netbirdio/netbird/shared/management/http/api"
)

const (
	EndpointTypeUnknown      = "UNKNOWN"
	EndpointTypePeer         = "PEER"
	EndpointTypeHostResource = "HOST_RESOURCE"

	ConnectionTypeP2P    = "P2P"
	ConnectionTypeRouted = "ROUTED"
)

// Group is a read-only projection of persisted events sharing a collection window.
type Group struct {
	WindowStart time.Time
	UserID      string
	UserName    string
	UserEmail   string
	ReporterID  string
	DetailCount int64
	TotalGroups int64 `json:"-" gorm:"column:total_groups"`
	RxBytes     int64
	RxPackets   int64
	TxBytes     int64
	TxPackets   int64
	NumOfStarts int64
	NumOfEnds   int64
	NumOfDrops  int64
}

// Event is a client-reported, aggregated network flow window.
type Event struct {
	ID             string    `gorm:"primaryKey"`
	AccountID      string    `gorm:"index;index:idx_network_traffic_account_timestamp,priority:1"`
	FlowID         string    `gorm:"index"`
	Timestamp      time.Time `gorm:"index;index:idx_network_traffic_account_timestamp,priority:2"`
	WindowStart    time.Time
	WindowEnd      time.Time
	EventType      string `gorm:"index"`
	Direction      string `gorm:"index"`
	Protocol       int    `gorm:"index"`
	ConnectionType string `gorm:"index"`
	ReporterID     string `gorm:"index"`
	UserID         string `gorm:"index"`

	SourceID          string `gorm:"index"`
	SourceType        string `gorm:"index"`
	SourceName        string
	SourceAddress     string `gorm:"index"`
	SourceDNSLabel    string
	SourceOS          string
	SourceCountryCode string
	SourceCityName    string

	DestinationID          string `gorm:"index"`
	DestinationType        string `gorm:"index"`
	DestinationName        string
	DestinationAddress     string `gorm:"index"`
	DestinationDNSLabel    string
	DestinationOS          string
	DestinationCountryCode string
	DestinationCityName    string

	PolicyID   string
	PolicyName string

	ICMPType int
	ICMPCode int

	RxBytes     int64
	RxPackets   int64
	TxBytes     int64
	TxPackets   int64
	NumOfStarts int
	NumOfEnds   int
	NumOfDrops  int

	UserName  string
	UserEmail string
}

// TableName returns the dedicated table used for self-hosted flow events.
func (Event) TableName() string {
	return "network_traffic_events"
}

// FormatAddress formats an IP and optional port for the management API.
func FormatAddress(ip netip.Addr, port uint16) string {
	if !ip.IsValid() {
		return ""
	}
	if port == 0 {
		return ip.String()
	}
	return netip.AddrPortFrom(ip, port).String()
}

// ToAPIResponse converts a stored event into the public API representation.
func (e *Event) ToAPIResponse() *api.NetworkTrafficEvent {
	windowStart := e.WindowStart
	if windowStart.IsZero() {
		windowStart = e.Timestamp
	}
	windowEnd := e.WindowEnd
	if windowEnd.IsZero() {
		windowEnd = e.Timestamp
	}

	return &api.NetworkTrafficEvent{
		Id:          e.ID,
		FlowId:      e.FlowID,
		Direction:   e.Direction,
		Protocol:    e.Protocol,
		ReporterId:  e.ReporterID,
		RxBytes:     int(e.RxBytes),
		RxPackets:   int(e.RxPackets),
		TxBytes:     int(e.TxBytes),
		TxPackets:   int(e.TxPackets),
		NumOfStarts: e.NumOfStarts,
		NumOfEnds:   e.NumOfEnds,
		NumOfDrops:  e.NumOfDrops,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Source:      e.endpoint(true),
		Destination: e.endpoint(false),
		Policy:      api.NetworkTrafficPolicy{Id: e.PolicyID, Name: e.PolicyName},
		Icmp:        api.NetworkTrafficICMP{Type: e.ICMPType, Code: e.ICMPCode},
		User:        api.NetworkTrafficUser{Id: e.UserID, Name: e.UserName, Email: e.UserEmail},
		Events: []api.NetworkTrafficSubEvent{{
			Timestamp: e.Timestamp,
			Type:      e.EventType,
		}},
	}
}

func (e *Event) endpoint(source bool) api.NetworkTrafficEndpoint {
	if source {
		return api.NetworkTrafficEndpoint{
			Id:          e.SourceID,
			Type:        e.SourceType,
			Name:        e.SourceName,
			Address:     e.SourceAddress,
			DnsLabel:    optionalString(e.SourceDNSLabel),
			Os:          optionalString(e.SourceOS),
			GeoLocation: api.NetworkTrafficLocation{CountryCode: e.SourceCountryCode, CityName: e.SourceCityName},
		}
	}

	return api.NetworkTrafficEndpoint{
		Id:          e.DestinationID,
		Type:        e.DestinationType,
		Name:        e.DestinationName,
		Address:     e.DestinationAddress,
		DnsLabel:    optionalString(e.DestinationDNSLabel),
		Os:          optionalString(e.DestinationOS),
		GeoLocation: api.NetworkTrafficLocation{CountryCode: e.DestinationCountryCode, CityName: e.DestinationCityName},
	}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// InvalidCounterError reports a counter that cannot be represented by storage.
func InvalidCounterError(name string, value uint64) error {
	return fmt.Errorf("%s counter %d exceeds supported range", name, value)
}
