package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type fakeRelayRegistrationSource struct {
	instanceURL    url.URL
	instanceID     string
	connectedPeers int
}

func (f fakeRelayRegistrationSource) InstanceURL() url.URL {
	return f.instanceURL
}

func (f fakeRelayRegistrationSource) InstanceID() string {
	return f.instanceID
}

func (f fakeRelayRegistrationSource) ConnectedPeerCount() int {
	return f.connectedPeers
}

func TestConfigValidateRegistration(t *testing.T) {
	base := func() Config {
		return Config{
			ListenAddress:  ":443",
			ExposedAddress: "rels://relay.example.com:443",
			AuthSecret:     "relay-secret",
			RelayPriority:  defaultRelayPriority,
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "registration disabled", mutate: func(*Config) {}},
		{name: "valid registration", mutate: func(c *Config) {
			c.RelayID = "relay-1"
			c.SetupKey = "setup-token"
			c.ManagementURL = "https://management.example.com"
		}},
		{name: "missing management URL", mutate: func(c *Config) {
			c.RelayID = "relay-1"
			c.SetupKey = "setup-token"
		}, wantErr: "configured together"},
		{name: "missing relay ID", mutate: func(c *Config) {
			c.SetupKey = "setup-token"
			c.ManagementURL = "https://management.example.com"
		}, wantErr: "relay ID is required"},
		{name: "invalid management scheme", mutate: func(c *Config) {
			c.RelayID = "relay-1"
			c.SetupKey = "setup-token"
			c.ManagementURL = "file:///tmp/management"
		}, wantErr: "invalid management URL"},
		{name: "invalid priority", mutate: func(c *Config) {
			c.RelayPriority = maxRelayPriority + 1
		}, wantErr: "relay priority"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfigValidateAppliesCloinkEnvironment(t *testing.T) {
	t.Setenv("CL_AUTH_SECRET", "relay-secret")
	t.Setenv("CL_RELAY_DOMAIN", "relay.example.com")
	t.Setenv("CL_RELAY_PORT", "8443")
	t.Setenv("CL_RELAY_SCHEME", "rels")

	cfg := Config{ListenAddress: ":443", RelayPriority: defaultRelayPriority}
	require.NoError(t, cfg.Validate())
	require.Equal(t, "relay-secret", cfg.AuthSecret)
	require.Equal(t, "rels://relay.example.com:8443", cfg.ExposedAddress)
}

func TestSetFlagsFromEnvVarsSupportsCloinkPrefix(t *testing.T) {
	t.Setenv("CL_RELAY_ID", "relay-from-cl")
	cmd := &cobra.Command{}
	var relayID string
	cmd.PersistentFlags().StringVar(&relayID, "relay-id", "", "")

	setFlagsFromEnvVars(cmd)
	require.Equal(t, "relay-from-cl", relayID)
}

func TestSetFlagsFromEnvVarsPrefersNetBirdPrefix(t *testing.T) {
	t.Setenv("NB_RELAY_ID", "relay-from-nb")
	t.Setenv("CL_RELAY_ID", "relay-from-cl")
	cmd := &cobra.Command{}
	var relayID string
	cmd.PersistentFlags().StringVar(&relayID, "relay-id", "", "")

	setFlagsFromEnvVars(cmd)
	require.Equal(t, "relay-from-nb", relayID)
}

func TestRegisterRelayReportsAuthenticatedStatus(t *testing.T) {
	var received relayRegistrationRequest
	management := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/base/api/relays/register", r.URL.Path)
		require.Empty(t, r.URL.RawQuery)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	t.Cleanup(management.Close)

	cfg := &Config{
		RelayID:       "relay-hk-1",
		RelayName:     "Hong Kong",
		RelayPriority: 70,
		SetupKey:      "setup-token",
		ManagementURL: management.URL + "/base?ignored=true",
	}
	source := fakeRelayRegistrationSource{
		instanceURL:    url.URL{Scheme: "rels", Host: "relay.example.com:443"},
		instanceID:     "source-id",
		connectedPeers: 9,
	}

	require.NoError(t, registerRelay(context.Background(), cfg, source))
	require.Equal(t, "setup-token", received.SetupKey)
	require.Equal(t, "relay-hk-1", received.ID)
	require.Equal(t, "Hong Kong", received.Name)
	require.Equal(t, "rels://relay.example.com:443", received.Address)
	require.Equal(t, 70, received.Priority)
	require.Equal(t, cfg.ManagementURL, received.ManagementURL)
	require.NotEmpty(t, received.Version)
	require.NotNil(t, received.ConnectedClients)
	require.Equal(t, 9, *received.ConnectedClients)
}

func TestRegisterRelayReturnsManagementFailure(t *testing.T) {
	management := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(management.Close)

	cfg := &Config{RelayID: "relay-1", SetupKey: "invalid", ManagementURL: management.URL}
	source := fakeRelayRegistrationSource{instanceURL: url.URL{Scheme: "rels", Host: "relay.example.com:443"}}

	err := registerRelay(context.Background(), cfg, source)
	require.ErrorContains(t, err, "401 Unauthorized")
}

func TestRelayRegistrationURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "origin", raw: "https://management.example.com", want: "https://management.example.com/api/relays/register"},
		{name: "base path", raw: "http://management.example.com/base/", want: "http://management.example.com/base/api/relays/register"},
		{name: "reject file", raw: "file:///tmp/management", wantErr: true},
		{name: "reject credentials", raw: "https://user:password@management.example.com", wantErr: true},
		{name: "reject missing host", raw: "https:///management", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := relayRegistrationURL(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
