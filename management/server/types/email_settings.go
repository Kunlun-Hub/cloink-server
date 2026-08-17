package types

import (
	"fmt"
	"slices"
	"time"

	"github.com/netbirdio/netbird/util/crypt"
)

type EmailTemplateKind string

const (
	EmailTemplateInviteUser            EmailTemplateKind = "invite_user"
	EmailTemplateCreateUser            EmailTemplateKind = "create_user"
	EmailTemplateInviteAccepted        EmailTemplateKind = "invite_accepted"
	EmailTemplateUserPendingApproval   EmailTemplateKind = "user_pending_approval"
	EmailTemplateDevicePendingApproval EmailTemplateKind = "device_pending_approval"
)

const (
	EmailEncryptionNone     = "none"
	EmailEncryptionStartTLS = "starttls"
	EmailEncryptionTLS      = "tls"
)

type EmailTemplate struct {
	Enabled  bool   `json:"enabled"`
	Subject  string `json:"subject"`
	BodyHTML string `json:"body_html"`
	BodyText string `json:"body_text"`
}

type EmailSettings struct {
	AccountID             string                   `gorm:"primaryKey" json:"account_id"`
	Enabled               bool                     `json:"enabled"`
	Host                  string                   `json:"host"`
	Port                  int                      `json:"port"`
	Username              string                   `json:"username"`
	PasswordEncrypted     string                   `json:"-"`
	FromName              string                   `json:"from_name"`
	FromEmail             string                   `json:"from_email"`
	ReplyTo               string                   `json:"reply_to"`
	Encryption            string                   `json:"encryption"`
	InsecureSkipVerify    bool                     `json:"insecure_skip_verify"`
	AdminRecipients       []string                 `gorm:"serializer:json" json:"admin_recipients"`
	Templates             map[string]EmailTemplate `gorm:"serializer:json" json:"templates"`
	CreatedAt             time.Time                `json:"created_at"`
	UpdatedAt             time.Time                `json:"updated_at"`
	PasswordConfigured    bool                     `gorm:"-" json:"password_configured"`
	Password              string                   `gorm:"-" json:"-"`
	ClearPassword         bool                     `gorm:"-" json:"-"`
	PasswordUpdatePresent bool                     `gorm:"-" json:"-"`
}

func (EmailSettings) TableName() string {
	return "email_settings"
}

func (s *EmailSettings) Copy() *EmailSettings {
	if s == nil {
		return nil
	}
	return &EmailSettings{
		AccountID:             s.AccountID,
		Enabled:               s.Enabled,
		Host:                  s.Host,
		Port:                  s.Port,
		Username:              s.Username,
		PasswordEncrypted:     s.PasswordEncrypted,
		FromName:              s.FromName,
		FromEmail:             s.FromEmail,
		ReplyTo:               s.ReplyTo,
		Encryption:            s.Encryption,
		InsecureSkipVerify:    s.InsecureSkipVerify,
		AdminRecipients:       slices.Clone(s.AdminRecipients),
		Templates:             cloneEmailTemplates(s.Templates),
		CreatedAt:             s.CreatedAt,
		UpdatedAt:             s.UpdatedAt,
		PasswordConfigured:    s.PasswordConfigured,
		Password:              s.Password,
		ClearPassword:         s.ClearPassword,
		PasswordUpdatePresent: s.PasswordUpdatePresent,
	}
}

func (s *EmailSettings) Normalize() {
	if s == nil {
		return
	}
	if s.Port == 0 {
		s.Port = 587
	}
	if s.Encryption == "" {
		s.Encryption = EmailEncryptionStartTLS
	}
	if s.AdminRecipients == nil {
		s.AdminRecipients = []string{}
	}
	if s.Templates == nil {
		s.Templates = map[string]EmailTemplate{}
	}
}

func (s *EmailSettings) EncryptSensitiveData(enc *crypt.FieldEncrypt) error {
	if s == nil || s.Password == "" {
		return nil
	}
	if enc == nil {
		return fmt.Errorf("field encryption is not configured")
	}
	encrypted, err := enc.Encrypt(s.Password)
	if err != nil {
		return fmt.Errorf("encrypt SMTP password: %w", err)
	}
	s.PasswordEncrypted = encrypted
	s.Password = ""
	return nil
}

func (s *EmailSettings) DecryptSensitiveData(enc *crypt.FieldEncrypt) error {
	if s == nil || s.PasswordEncrypted == "" {
		return nil
	}
	if enc == nil {
		return fmt.Errorf("field encryption is not configured")
	}
	decrypted, err := enc.Decrypt(s.PasswordEncrypted)
	if err != nil {
		return fmt.Errorf("decrypt SMTP password: %w", err)
	}
	s.Password = decrypted
	s.PasswordConfigured = decrypted != ""
	return nil
}

func cloneEmailTemplates(source map[string]EmailTemplate) map[string]EmailTemplate {
	if source == nil {
		return nil
	}
	result := make(map[string]EmailTemplate, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
