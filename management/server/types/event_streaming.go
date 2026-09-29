package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/netbirdio/netbird/util/crypt"
)

// Event streaming integration platforms supported by the management server.
const (
	EventStreamingPlatformDatadog    = "datadog"
	EventStreamingPlatformS3         = "s3"
	EventStreamingPlatformFirehose   = "firehose"
	EventStreamingPlatformGenericHTTP = "generic_http"
)

// sensitiveConfigKeys lists config keys that hold secrets per platform.
// They are encrypted at rest and masked as **** in API responses.
var sensitiveConfigKeys = map[string][]string{
	EventStreamingPlatformDatadog:    {"api_key"},
	EventStreamingPlatformS3:         {"access_key", "secret_key"},
	EventStreamingPlatformFirehose:   {"access_key", "secret_key"},
	EventStreamingPlatformGenericHTTP: {"headers"},
}

// EventStreamingPlatforms is the set of valid platform values.
var EventStreamingPlatforms = map[string]struct{}{
	EventStreamingPlatformDatadog:     {},
	EventStreamingPlatformS3:          {},
	EventStreamingPlatformFirehose:    {},
	EventStreamingPlatformGenericHTTP: {},
}

// MaskedSecretValue is the placeholder returned in place of secret config values.
const MaskedSecretValue = "****"

// EventStreamingIntegration is a per-account event streaming destination
// (Datadog, Amazon S3, Amazon Data Firehose or a generic HTTP webhook).
// The Config map is encrypted at rest as a whole; it is only materialized
// in memory via EncryptSensitiveData/DecryptSensitiveData.
type EventStreamingIntegration struct {
	ID              uint64            `gorm:"primaryKey;autoIncrement" json:"id"`
	AccountID       string            `gorm:"index" json:"account_id"`
	Platform        string            `json:"platform"`
	Enabled         bool              `json:"enabled"`
	ConfigEncrypted string            `json:"-"`
	Config          map[string]string `gorm:"-" json:"config,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

func (EventStreamingIntegration) TableName() string {
	return "event_streaming_integrations"
}

// Copy returns a deep copy of the integration.
func (i *EventStreamingIntegration) Copy() *EventStreamingIntegration {
	if i == nil {
		return nil
	}
	c := *i
	if i.Config != nil {
		c.Config = make(map[string]string, len(i.Config))
		for k, v := range i.Config {
			c.Config[k] = v
		}
	}
	return &c
}

// EncryptSensitiveData encrypts the whole config map at rest.
func (i *EventStreamingIntegration) EncryptSensitiveData(enc *crypt.FieldEncrypt) error {
	if i == nil || len(i.Config) == 0 {
		return nil
	}
	if enc == nil {
		return fmt.Errorf("field encryption is not configured")
	}
	raw, err := json.Marshal(i.Config)
	if err != nil {
		return fmt.Errorf("marshal event streaming config: %w", err)
	}
	encrypted, err := enc.Encrypt(string(raw))
	if err != nil {
		return fmt.Errorf("encrypt event streaming config: %w", err)
	}
	i.ConfigEncrypted = encrypted
	i.Config = nil
	return nil
}

// DecryptSensitiveData materializes the config map from its encrypted form.
func (i *EventStreamingIntegration) DecryptSensitiveData(enc *crypt.FieldEncrypt) error {
	if i == nil || i.ConfigEncrypted == "" {
		return nil
	}
	if enc == nil {
		return fmt.Errorf("field encryption is not configured")
	}
	decrypted, err := enc.Decrypt(i.ConfigEncrypted)
	if err != nil {
		return fmt.Errorf("decrypt event streaming config: %w", err)
	}
	var config map[string]string
	if err := json.Unmarshal([]byte(decrypted), &config); err != nil {
		return fmt.Errorf("unmarshal event streaming config: %w", err)
	}
	i.Config = config
	return nil
}

// MaskedConfig returns a copy of the config with secret values masked.
// A masked value sent back by the client means "keep the stored value".
func (i *EventStreamingIntegration) MaskedConfig() map[string]string {
	if i.Config == nil {
		return nil
	}
	masked := make(map[string]string, len(i.Config))
	sensitive := sensitiveConfigKeys[i.Platform]
	isSensitive := func(key string) bool {
		for _, k := range sensitive {
			if k == key {
				return true
			}
		}
		return false
	}
	for k, v := range i.Config {
		if isSensitive(k) && v != "" {
			masked[k] = MaskedSecretValue
		} else {
			masked[k] = v
		}
	}
	return masked
}

// MergeMaskedConfig merges an update payload into the stored config: keys whose
// value is the mask placeholder (or empty) keep the existing stored value.
func (i *EventStreamingIntegration) MergeMaskedConfig(update map[string]string) {
	if i.Config == nil {
		i.Config = make(map[string]string, len(update))
	}
	sensitive := sensitiveConfigKeys[i.Platform]
	isSensitive := func(key string) bool {
		for _, k := range sensitive {
			if k == key {
				return true
			}
		}
		return false
	}
	for k, v := range update {
		if isSensitive(k) && (v == "" || v == MaskedSecretValue) {
			continue
		}
		i.Config[k] = v
	}
}

// EventStreamingCursor tracks per-integration forwarding watermarks so the
// forwarder can resume after a restart without re-sending or skipping events.
type EventStreamingCursor struct {
	AccountID     string    `gorm:"primaryKey" json:"account_id"`
	IntegrationID uint64    `gorm:"primaryKey" json:"integration_id"`
	ActivityID    uint64    `json:"activity_id"`
	FlowTimestamp time.Time `json:"flow_timestamp"`
	FlowID        string    `json:"flow_id"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (EventStreamingCursor) TableName() string {
	return "event_streaming_cursors"
}
