package grpc

import (
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nbdns "github.com/netbirdio/netbird/dns"
	"github.com/netbirdio/netbird/management/internals/controllers/network_map"
	"github.com/netbirdio/netbird/management/internals/controllers/network_map/controller/cache"
	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	relayhandler "github.com/netbirdio/netbird/management/server/http/handlers/relays"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/networkmap"
)

func TestToProtocolDNSConfigWithCache(t *testing.T) {
	var cache cache.DNSConfigCache

	// Create two different configs
	config1 := nbdns.Config{
		ServiceEnable: true,
		CustomZones: []nbdns.CustomZone{
			{
				Domain: "example.com",
				Records: []nbdns.SimpleRecord{
					{Name: "www", Type: 1, Class: "IN", TTL: 300, RData: "192.168.1.1"},
				},
			},
		},
		NameServerGroups: []*nbdns.NameServerGroup{
			{
				ID:   "group1",
				Name: "Group 1",
				NameServers: []nbdns.NameServer{
					{IP: netip.MustParseAddr("8.8.8.8"), Port: 53},
				},
			},
		},
	}

	config2 := nbdns.Config{
		ServiceEnable: true,
		CustomZones: []nbdns.CustomZone{
			{
				Domain: "example.org",
				Records: []nbdns.SimpleRecord{
					{Name: "mail", Type: 1, Class: "IN", TTL: 300, RData: "192.168.1.2"},
				},
			},
		},
		NameServerGroups: []*nbdns.NameServerGroup{
			{
				ID:   "group2",
				Name: "Group 2",
				NameServers: []nbdns.NameServer{
					{IP: netip.MustParseAddr("8.8.4.4"), Port: 53},
				},
			},
		},
	}

	// First run with config1
	result1 := networkmap.ToProtocolDNSConfig(config1, &cache, int64(network_map.DnsForwarderPort))

	// Second run with config2
	result2 := networkmap.ToProtocolDNSConfig(config2, &cache, int64(network_map.DnsForwarderPort))

	// Third run with config1 again
	result3 := networkmap.ToProtocolDNSConfig(config1, &cache, int64(network_map.DnsForwarderPort))

	// Verify that result1 and result3 are identical
	if !reflect.DeepEqual(result1, result3) {
		t.Errorf("Results are not identical when run with the same input. Expected %v, got %v", result1, result3)
	}

	// Verify that result2 is different from result1 and result3
	if reflect.DeepEqual(result1, result2) || reflect.DeepEqual(result2, result3) {
		t.Errorf("Results should be different for different inputs")
	}

	if _, exists := cache.GetNameServerGroup("group1"); !exists {
		t.Errorf("Cache should contain name server group 'group1'")
	}

	if _, exists := cache.GetNameServerGroup("group2"); !exists {
		t.Errorf("Cache should contain name server group 'group2'")
	}
}

func BenchmarkToProtocolDNSConfig(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		testData := generateTestData(size)

		b.Run(fmt.Sprintf("WithCache-Size%d", size), func(b *testing.B) {
			cache := &cache.DNSConfigCache{}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				networkmap.ToProtocolDNSConfig(testData, cache, int64(network_map.DnsForwarderPort))
			}
		})

		b.Run(fmt.Sprintf("WithoutCache-Size%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				networkmap.ToProtocolDNSConfig(testData, nil, int64(network_map.DnsForwarderPort))
			}
		})
	}
}

func generateTestData(size int) nbdns.Config {
	config := nbdns.Config{
		ServiceEnable:    true,
		CustomZones:      make([]nbdns.CustomZone, size),
		NameServerGroups: make([]*nbdns.NameServerGroup, size),
	}

	for i := 0; i < size; i++ {
		config.CustomZones[i] = nbdns.CustomZone{
			Domain: fmt.Sprintf("domain%d.com", i),
			Records: []nbdns.SimpleRecord{
				{
					Name:  fmt.Sprintf("record%d", i),
					Type:  1,
					Class: "IN",
					TTL:   3600,
					RData: "192.168.1.1",
				},
			},
		}

		config.NameServerGroups[i] = &nbdns.NameServerGroup{
			ID:                   fmt.Sprintf("group%d", i),
			Primary:              i == 0,
			Domains:              []string{fmt.Sprintf("domain%d.com", i)},
			SearchDomainsEnabled: true,
			NameServers: []nbdns.NameServer{
				{
					IP:     netip.MustParseAddr("8.8.8.8"),
					Port:   53,
					NSType: 1,
				},
			},
		}
	}

	return config
}

func TestBuildJWTConfig_Audiences(t *testing.T) {
	tests := []struct {
		name              string
		authAudience      string
		cliAuthAudience   string
		expectedAudiences []string
		expectedAudience  string
	}{
		{
			name:              "only_auth_audience",
			authAudience:      "dashboard-aud",
			cliAuthAudience:   "",
			expectedAudiences: []string{"dashboard-aud"},
			expectedAudience:  "dashboard-aud",
		},
		{
			name:              "both_audiences_different",
			authAudience:      "dashboard-aud",
			cliAuthAudience:   "cli-aud",
			expectedAudiences: []string{"dashboard-aud", "cli-aud"},
			expectedAudience:  "cli-aud",
		},
		{
			name:              "both_audiences_same",
			authAudience:      "same-aud",
			cliAuthAudience:   "same-aud",
			expectedAudiences: []string{"same-aud"},
			expectedAudience:  "same-aud",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &nbconfig.HttpServerConfig{
				AuthIssuer:      "https://issuer.example.com",
				AuthAudience:    tc.authAudience,
				CLIAuthAudience: tc.cliAuthAudience,
			}

			result := buildJWTConfig(config, nil)

			assert.NotNil(t, result)
			assert.Equal(t, tc.expectedAudiences, result.Audiences, "audiences should match expected")
			//nolint:staticcheck // SA1019: Testing backwards compatibility - Audience field must still be populated
			assert.Equal(t, tc.expectedAudience, result.Audience, "audience should match expected")
		})
	}
}

// TestShouldSkipSendingDeprecatedRemotePeers covers the version gate that
// stops populating the deprecated top-level SyncResponse.RemotePeers field for
// peers new enough to read RemotePeers off the NetworkMap. Development builds
// are treated as latest and skip the field. The gate otherwise fails safe: a
// release version older than the boundary, or one that can't be parsed (empty,
// garbage, prereleases of the boundary) still receives the deprecated field so
// older/unknown clients keep working.
func TestShouldSkipSendingDeprecatedRemotePeers(t *testing.T) {
	tests := []struct {
		name        string
		peerVersion string
		wantSkip    bool
	}{
		{"exact boundary skips", "0.29.3", true},
		{"newer patch skips", "0.29.4", true},
		{"newer minor skips", "0.30.0", true},
		{"newer major skips", "1.0.0", true},
		{"v-prefixed newer skips", "v0.30.0", true},
		{"development build skips", "development", true},
		{"development build with commit skips", "development-abc123def456-dirty", true},
		{"older patch keeps field", "0.29.2", false},
		{"older minor keeps field", "0.28.0", false},
		{"prerelease of boundary keeps field", "0.29.3-SNAPSHOT", false},
		{"tagged dev prerelease keeps field", "v0.31.1-dev", false},
		{"empty version keeps field", "", false},
		{"garbage version keeps field", "not-a-version", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldSkipSendingDeprecatedRemotePeers(tc.peerVersion)
			assert.Equal(t, tc.wantSkip, got, "skip decision for peer version %q", tc.peerVersion)
		})
	}
}

// TestEncodeSessionExpiresAt pins the wire encoding the client's
// applySessionDeadline depends on:
//
//   - zero deadline  → &Timestamp{} (seconds=0, nanos=0): the explicit
//     "expiry disabled or peer is not SSO-tracked" sentinel.
//   - non-zero       → timestamppb.New(deadline): the absolute UTC deadline.
//
// The third state (nil pointer = "no info in this snapshot") is the caller's
// responsibility on the Sync path when settings could not be resolved; the
// helper itself never returns nil.
func TestEncodeSessionExpiresAt(t *testing.T) {
	t.Run("zero deadline encodes as explicit-zero sentinel", func(t *testing.T) {
		got := encodeSessionExpiresAt(time.Time{})
		assert.NotNil(t, got, "must not return nil; nil means 'no info', not 'disabled'")
		assert.Equal(t, int64(0), got.GetSeconds())
		assert.Equal(t, int32(0), got.GetNanos())
	})

	t.Run("non-zero deadline round-trips", func(t *testing.T) {
		deadline := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		got := encodeSessionExpiresAt(deadline)
		assert.NotNil(t, got)
		assert.True(t, got.AsTime().Equal(deadline))
	})
}

// TestToNetbirdConfig_RelayInvariant guards against the v0.74.0 relay-wipe regression.
// Clients treat any non-nil NetbirdConfig as authoritative and interpret a missing relay
// section as relay disabled, wiping their relay URLs. toNetbirdConfig must therefore
// return nil when no server config is set (the fan-out network-map path) instead of a
// partial config, and a result built from a relay-enabled config must carry the relay
// section.
func TestToNetbirdConfig_RelayInvariant(t *testing.T) {
	settings := &types.Settings{MetricsPushEnabled: true}

	t.Run("nil server config returns nil config", func(t *testing.T) {
		nbCfg := toNetbirdConfig(nil, nil, nil, nil, types.TwinAccountSettings(settings), nil)
		assert.Nil(t, nbCfg, "fan-out updates must not carry a partial NetbirdConfig even when settings are present")
	})

	t.Run("relay-enabled config carries relay section", func(t *testing.T) {
		cfg := &nbconfig.Config{
			Stuns: []*nbconfig.Host{{Proto: nbconfig.UDP, URI: "stun:stun.example.com:3478"}},
			TURNConfig: &nbconfig.TURNConfig{
				Turns: []*nbconfig.Host{{Proto: nbconfig.UDP, URI: "turn:turn.example.com:3478", Username: "user", Password: "pass"}},
			},
			Relay:  &nbconfig.Relay{Addresses: []string{"rels://relay.example.com:443"}},
			Signal: &nbconfig.Host{Proto: nbconfig.HTTP, URI: "signal.example.com:10000"},
		}
		relayToken := &Token{Payload: "token-payload", Signature: "token-signature"}

		nbCfg := toNetbirdConfig(cfg, nil, relayToken, nil, types.TwinAccountSettings(settings), nil)
		require.NotNil(t, nbCfg)
		require.NotNil(t, nbCfg.Relay, "non-nil NetbirdConfig must include the relay section")
		assert.Equal(t, cfg.Relay.Addresses, nbCfg.Relay.Urls, "relay URLs should match the server config")
		require.Len(t, nbCfg.Relay.Servers, 1)
		assert.Equal(t, cfg.Relay.Addresses[0], nbCfg.Relay.Servers[0].Url)
		assert.Equal(t, int32(30), nbCfg.Relay.Servers[0].Priority)
		assert.Equal(t, relayToken.Payload, nbCfg.Relay.TokenPayload, "relay token payload should be set")
		assert.Equal(t, relayToken.Signature, nbCfg.Relay.TokenSignature, "relay token signature should be set")
		require.NotNil(t, nbCfg.Metrics)
		assert.True(t, nbCfg.Metrics.Enabled, "metrics flag should carry the settings value")
	})
}

// TestToNetbirdConfig_RelayGroupFiltering verifies that group-scoped relays are
// distributed per peer: a peer only receives global relays plus relays whose
// group IDs intersect its own. A peer matching nothing gets no relay section,
// which clients interpret as relay disabled for that peer.
func TestToNetbirdConfig_RelayGroupFiltering(t *testing.T) {
	const (
		groupEng = "group-eng-id"
		groupOps = "group-ops-id"
	)
	cfg := &nbconfig.Config{Relay: &nbconfig.Relay{}}
	extra := &types.ExtraSettings{RegisteredRelays: map[string]types.RegisteredRelay{
		"global": {ID: "global", Address: "rels://global.example.com:443", Priority: 30, LastSeen: time.Now()},
		"eng":    {ID: "eng", Address: "rels://eng.example.com:443", Priority: 30, Groups: []string{groupEng}, LastSeen: time.Now()},
		"ops":    {ID: "ops", Address: "rels://ops.example.com:443", Priority: 30, Groups: []string{groupOps}, LastSeen: time.Now()},
	}}
	settings := &types.Settings{Extra: extra}

	collect := func(t *testing.T, peerGroupIDs []string) []string {
		t.Helper()
		nbCfg := toNetbirdConfig(cfg, nil, nil, extra, types.TwinAccountSettings(settings), peerGroupIDs)
		require.NotNil(t, nbCfg)
		if nbCfg.Relay == nil {
			return nil
		}
		got := make([]string, 0, len(nbCfg.Relay.Servers))
		for _, server := range nbCfg.Relay.Servers {
			got = append(got, server.Url)
		}
		return got
	}

	engURLs := collect(t, []string{groupEng})
	require.ElementsMatch(t,
		[]string{"rels://global.example.com:443", "rels://eng.example.com:443"},
		engURLs, "eng peer must receive the global and eng relays")

	opsURLs := collect(t, []string{groupOps})
	require.ElementsMatch(t,
		[]string{"rels://global.example.com:443", "rels://ops.example.com:443"},
		opsURLs, "ops peer must receive the global and ops relays")

	plainURLs := collect(t, nil)
	require.ElementsMatch(t,
		[]string{"rels://global.example.com:443"},
		plainURLs, "peer without groups must receive only the global relay")
}

func TestToPeerConfig_RoutingPeerDNSResolution(t *testing.T) {
	network := &types.Network{Net: net.IPNet{IP: net.IPv4(100, 0, 0, 0), Mask: net.CIDRMask(8, 32)}}

	newPeer := func(embedded bool) *nbpeer.Peer {
		p := &nbpeer.Peer{IP: netip.MustParseAddr("100.0.0.1")}
		p.ProxyMeta.Embedded = embedded
		return p
	}

	tests := []struct {
		name        string
		globalFlag  bool
		embedded    bool
		forceParam  bool
		wantEnabled bool
	}{
		{name: "global off, regular peer, no force", wantEnabled: false},
		{name: "global on wins", globalFlag: true, wantEnabled: true},
		{name: "embedded proxy peer forced", embedded: true, wantEnabled: true},
		{name: "routing peer forced via param", forceParam: true, wantEnabled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := &types.Settings{RoutingPeerDNSResolutionEnabled: tt.globalFlag}
			cfg := toPeerConfig(types.TwinPeer(newPeer(tt.embedded)), types.TwinNetwork(network), "netbird.selfhosted", types.TwinAccountSettings(settings), nil, nil, false, tt.forceParam)
			assert.Equal(t, tt.wantEnabled, cfg.RoutingPeerDnsResolutionEnabled,
				"RoutingPeerDnsResolutionEnabled should reflect global || embedded || forced")
		})
	}
}

func TestRelayLoadPenalty(t *testing.T) {
	cases := []struct {
		peers int
		want  int
	}{
		{peers: 0, want: 0},
		{peers: -5, want: 0},
		{peers: 19, want: 0},
		{peers: 20, want: 1},
		{peers: 85, want: 4},
		{peers: 400, want: 20},
		{peers: 10000, want: 20},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, relayLoadPenalty(tc.peers), "peers=%d", tc.peers)
	}
}

func TestRelayConfigFromDescriptorsAppliesLoadPenalty(t *testing.T) {
	relays := []relayhandler.RelayServerDescriptor{
		{ID: "idle", Address: "rels://idle.example:443", Priority: 100},
		{ID: "busy", Address: "rels://busy.example:443", Priority: 100, ConnectedPeers: 85},
		{ID: "hammered", Address: "rels://hammered.example:443", Priority: 100, ConnectedPeers: 10000},
	}

	config := relayConfigFromDescriptors(relays)

	require.Len(t, config.Servers, 3)
	assert.Equal(t, int32(100), config.Servers[0].Priority)
	assert.Equal(t, int32(96), config.Servers[1].Priority, "85 peers subtract 4 points")
	assert.Equal(t, int32(80), config.Servers[2].Priority, "penalty is capped at 20 points")
}

func TestRelayConfigFromDescriptorsClampsNonPositivePriority(t *testing.T) {
	// A low base priority under high load must not go to zero or negative:
	// the client only honors priority > 0 and would otherwise treat the
	// relay as unprioritized with the default weight.
	relays := []relayhandler.RelayServerDescriptor{
		{ID: "low-base", Address: "rels://low.example:443", Priority: 10, ConnectedPeers: 200},
		{ID: "low-base-max", Address: "rels://lowmax.example:443", Priority: 10, ConnectedPeers: 10000},
		{ID: "tiny", Address: "rels://tiny.example:443", Priority: 1},
	}

	config := relayConfigFromDescriptors(relays)

	require.Len(t, config.Servers, 3)
	assert.Equal(t, int32(1), config.Servers[0].Priority, "10 - 10 must clamp to 1, not 0")
	assert.Equal(t, int32(1), config.Servers[1].Priority, "10 - 20 must clamp to 1, not -10")
	assert.Equal(t, int32(1), config.Servers[2].Priority, "base 1 with no load stays 1")
}
