package networktraffic

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/durationpb"

	nbconfig "github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/proto"
)

const (
	flowTokenVersion      = 1
	defaultReportInterval = 30 * time.Second
	envFlowURL            = "NB_FLOW_URL"
	envFlowTokenSecret    = "NB_FLOW_TOKEN_SECRET"
	envFlowReportInterval = "NB_FLOW_REPORT_INTERVAL"
)

var errInvalidFlowToken = errors.New("invalid flow token")

// TokenClaims identifies the account and peer allowed to submit flow events.
type TokenClaims struct {
	Version   int    `json:"v"`
	AccountID string `json:"account_id"`
	PeerID    string `json:"peer_id"`
}

// ConfigManager builds client flow configuration and validates receiver tokens.
type ConfigManager struct {
	secret         []byte
	receiverURL    string
	reportInterval time.Duration
}

// NewConfigManager creates a process-scoped flow configuration manager.
func NewConfigManager(config *nbconfig.Config) (*ConfigManager, error) {
	secret, ephemeral, err := flowTokenSecret(config)
	if err != nil {
		return nil, err
	}
	if ephemeral {
		log.Warnf("%s is not set and no management secret is configured; flow tokens will rotate on restart", envFlowTokenSecret)
	}

	receiverURL, err := flowReceiverURL()
	if err != nil {
		return nil, err
	}

	return &ConfigManager{
		secret:         secret,
		receiverURL:    receiverURL,
		reportInterval: flowReportInterval(),
	}, nil
}

// Apply adds a self-hosted FlowConfig to a management sync response.
func (m *ConfigManager) Apply(response *proto.SyncResponse, accountID, peerID string, peerGroups []string, settings *types.ExtraSettings) {
	if m == nil || response == nil {
		return
	}
	if response.NetbirdConfig == nil {
		response.NetbirdConfig = &proto.NetbirdConfig{}
	}

	enabled := flowEnabledForPeer(settings, peerGroups)
	flowConfig := &proto.FlowConfig{
		Url:      m.receiverURL,
		Interval: durationpb.New(m.reportInterval),
		Enabled:  enabled,
	}
	if settings != nil {
		flowConfig.Counters = settings.FlowPacketCounterEnabled
		flowConfig.ExitNodeCollection = settings.FlowENCollectionEnabled
		flowConfig.DnsCollection = settings.FlowDnsCollectionEnabled
	}
	if enabled {
		flowConfig.TokenPayload, flowConfig.TokenSignature = m.Sign(accountID, peerID)
	}
	response.NetbirdConfig.Flow = flowConfig
}

// Sign creates an opaque token pair understood by the official Flow client.
func (m *ConfigManager) Sign(accountID, peerID string) (payload, signature string) {
	claims := TokenClaims{Version: flowTokenVersion, AccountID: accountID, PeerID: peerID}
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", ""
	}
	payload = base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, m.secret)
	_, _ = mac.Write([]byte(payload))
	signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payload, signature
}

// Validate verifies a flow token and returns its bound account and peer.
func (m *ConfigManager) Validate(payload, signature string) (TokenClaims, error) {
	if m == nil || payload == "" || signature == "" {
		return TokenClaims{}, errInvalidFlowToken
	}

	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return TokenClaims{}, errInvalidFlowToken
	}
	mac := hmac.New(sha256.New, m.secret)
	_, _ = mac.Write([]byte(payload))
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return TokenClaims{}, errInvalidFlowToken
	}

	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return TokenClaims{}, errInvalidFlowToken
	}
	var claims TokenClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return TokenClaims{}, errInvalidFlowToken
	}
	if claims.Version != flowTokenVersion || claims.AccountID == "" || claims.PeerID == "" {
		return TokenClaims{}, errInvalidFlowToken
	}
	return claims, nil
}

// FlowEnabledForPeer reports whether the current account settings enable flow
// reporting for a peer in the supplied groups.
func FlowEnabledForPeer(settings *types.ExtraSettings, peerGroups []string) bool {
	return flowEnabledForPeer(settings, peerGroups)
}

func flowEnabledForPeer(settings *types.ExtraSettings, peerGroups []string) bool {
	if settings == nil || !settings.FlowEnabled {
		return false
	}
	if len(settings.FlowGroups) == 0 {
		return true
	}
	for _, groupID := range settings.FlowGroups {
		if slices.Contains(peerGroups, groupID) {
			return true
		}
	}
	return false
}

func flowTokenSecret(config *nbconfig.Config) ([]byte, bool, error) {
	source := os.Getenv(envFlowTokenSecret)
	if source == "" && config != nil {
		source = config.DataStoreEncryptionKey
		if source == "" && config.Relay != nil {
			source = config.Relay.Secret
		}
	}

	ephemeral := source == ""
	if ephemeral {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, false, fmt.Errorf("generate flow token secret: %w", err)
		}
		return secret, true, nil
	}

	derived := sha256.Sum256([]byte("cloink-flow-token-v1\x00" + source))
	return derived[:], false, nil
}

func flowReceiverURL() (string, error) {
	raw := strings.TrimSpace(os.Getenv(envFlowURL))
	if raw == "" {
		return "", nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("%s must be an http or https URL", envFlowURL)
	}
	return parsed.String(), nil
}

func flowReportInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envFlowReportInterval))
	if raw == "" {
		return defaultReportInterval
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval <= 0 {
		log.Warnf("invalid %s value %q; using %s", envFlowReportInterval, raw, defaultReportInterval)
		return defaultReportInterval
	}
	return interval
}
