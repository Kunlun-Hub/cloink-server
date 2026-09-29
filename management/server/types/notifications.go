package types

import (
	"encoding/json"
	"time"
)

// Notification channel types supported by the management server.
const (
	NotificationChannelTypeEmail   = "email"
	NotificationChannelTypeWebhook = "webhook"
	NotificationChannelTypeSlack   = "slack"
)

// NotificationChannelTypes is the set of valid channel type values.
var NotificationChannelTypes = map[string]struct{}{
	NotificationChannelTypeEmail:   {},
	NotificationChannelTypeWebhook: {},
	NotificationChannelTypeSlack:   {},
}

// Notification event types that can trigger a notification.
// Keys match the dashboard's NotificationEventType enum values.
var NotificationEventTypes = map[string]string{
	"peer.pending.approval":  "Peer pending approval",
	"peer.add":               "Peer added",
	"routing.peer.disconnect": "Routing peer disconnected",
	"routing.peer.delete":    "Routing peer deleted",
	"user.pending.approval":  "User pending approval",
	"user.join":              "User joined",
	"service.user.create":    "Service user created",
	"idp.sync.token.expire":  "IdP sync token expiring",
	"edr.sync.token.expire":  "EDR sync token expiring",
}

// NotificationChannel is a per-account notification destination
// (email, webhook or Slack). The Target holds the type-specific
// configuration as JSON: {emails: [...]} for email,
// {url: ..., headers: {...}} for webhook and Slack.
type NotificationChannel struct {
	ID         string    `gorm:"primaryKey" json:"id"`
	AccountID  string    `gorm:"index" json:"-"`
	Type       string    `json:"type"`
	Enabled    bool      `json:"enabled"`
	EventTypes []string  `gorm:"serializer:json" json:"event_types"`
	Target     string    `json:"-"`
	CreatedAt  time.Time `json:"-"`
	UpdatedAt  time.Time `json:"-"`
}

func (NotificationChannel) TableName() string {
	return "notification_channels"
}

// ToAPI converts the stored channel to the API representation,
// decoding the target JSON into a generic map.
func (c *NotificationChannel) ToAPI() map[string]interface{} {
	var target interface{}
	if c.Target != "" {
		// Best effort decode; on failure the raw string is returned.
		var decoded interface{}
		if err := json.Unmarshal([]byte(c.Target), &decoded); err == nil {
			target = decoded
		} else {
			target = c.Target
		}
	}
	result := map[string]interface{}{
		"id":          c.ID,
		"type":        c.Type,
		"enabled":     c.Enabled,
		"event_types": c.EventTypes,
	}
	if target != nil {
		result["target"] = target
	}
	if c.EventTypes == nil {
		result["event_types"] = []string{}
	}
	return result
}
