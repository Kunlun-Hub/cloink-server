package email

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/mail"
	"net/smtp"
	"sort"
	"strings"
	texttemplate "text/template"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/permissions/modules"
	"github.com/netbirdio/netbird/management/server/permissions/operations"
	"github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

type Store interface {
	GetEmailSettings(ctx context.Context, lockStrength store.LockingStrength, accountID string) (*types.EmailSettings, error)
	SaveEmailSettings(ctx context.Context, settings *types.EmailSettings) error
}

type Service interface {
	GetSettings(ctx context.Context, accountID, userID string) (*types.EmailSettings, error)
	UpdateSettings(ctx context.Context, accountID, userID string, update *types.EmailSettings) (*types.EmailSettings, error)
	SendTestEmail(ctx context.Context, accountID, userID, recipient string) error
	PreviewTemplate(ctx context.Context, accountID, userID string, kind types.EmailTemplateKind, data TemplateData) (*RenderedTemplate, error)
	Notify(ctx context.Context, accountID string, kind types.EmailTemplateKind, data TemplateData) error
}

// StrictNotifier is implemented by the SMTP manager for user-triggered
// sends. Lifecycle notifications use the best-effort Service.Notify method so
// a temporary mail outage does not fail account operations; explicit resend
// actions need the actual delivery error returned to the caller.
type StrictNotifier interface {
	NotifyStrict(ctx context.Context, accountID string, kind types.EmailTemplateKind, data TemplateData) error
}

type TemplateData map[string]any

type RenderedTemplate struct {
	Subject  string `json:"subject"`
	BodyHTML string `json:"body_html"`
	BodyText string `json:"body_text"`
}

type Manager struct {
	store              Store
	permissionsManager permissions.Manager
	dashboardURL       string
	sendTimeout        time.Duration
}

func NewManager(store Store, permissionsManager permissions.Manager, dashboardURL string) *Manager {
	return &Manager{
		store:              store,
		permissionsManager: permissionsManager,
		dashboardURL:       strings.TrimRight(dashboardURL, "/"),
		sendTimeout:        10 * time.Second,
	}
}

func (m *Manager) GetSettings(ctx context.Context, accountID, userID string) (*types.EmailSettings, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Read); err != nil {
		return nil, err
	}
	settings, err := m.loadSettings(ctx, accountID)
	if err != nil {
		return nil, err
	}
	return sanitizeSettings(settings), nil
}

func (m *Manager) UpdateSettings(ctx context.Context, accountID, userID string, update *types.EmailSettings) (*types.EmailSettings, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Update); err != nil {
		return nil, err
	}
	if update == nil {
		return nil, status.Errorf(status.InvalidArgument, "email settings are required")
	}

	current, err := m.loadSettings(ctx, accountID)
	if err != nil {
		return nil, err
	}

	next := update.Copy()
	next.AccountID = accountID
	next.Normalize()
	if !next.PasswordUpdatePresent && !next.ClearPassword {
		next.Password = current.Password
		next.PasswordEncrypted = current.PasswordEncrypted
	}
	if next.ClearPassword {
		next.Password = ""
		next.PasswordEncrypted = ""
	}
	if err := validateSettings(next); err != nil {
		return nil, err
	}
	if err := validateTemplates(next.Templates); err != nil {
		return nil, err
	}

	if err := m.store.SaveEmailSettings(ctx, next); err != nil {
		return nil, err
	}
	saved, err := m.loadSettings(ctx, accountID)
	if err != nil {
		return nil, err
	}
	return sanitizeSettings(saved), nil
}

func (m *Manager) SendTestEmail(ctx context.Context, accountID, userID, recipient string) error {
	if err := m.checkPermission(ctx, accountID, userID, operations.Update); err != nil {
		return err
	}
	recipient = strings.TrimSpace(recipient)
	if _, err := mail.ParseAddress(recipient); err != nil {
		return status.Errorf(status.InvalidArgument, "invalid recipient email: %v", err)
	}
	settings, err := m.loadSettings(ctx, accountID)
	if err != nil {
		return err
	}
	if !settings.Enabled {
		return status.Errorf(status.PreconditionFailed, "email notifications are disabled")
	}

	data := m.withDefaults(TemplateData{
		"account": map[string]any{"name": "Cloink", "domain": ""},
		"user":    map[string]any{"email": recipient, "name": recipient},
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
	rendered := &RenderedTemplate{
		Subject:  "Cloink SMTP 测试邮件",
		BodyHTML: "<p>这是一封 Cloink SMTP 测试邮件。</p>",
		BodyText: "这是一封 Cloink SMTP 测试邮件。",
	}
	return m.send(ctx, settings, []string{recipient}, rendered, data)
}

func (m *Manager) PreviewTemplate(ctx context.Context, accountID, userID string, kind types.EmailTemplateKind, data TemplateData) (*RenderedTemplate, error) {
	if err := m.checkPermission(ctx, accountID, userID, operations.Update); err != nil {
		return nil, err
	}
	settings, err := m.loadSettings(ctx, accountID)
	if err != nil {
		return nil, err
	}
	return renderTemplate(kind, settings.Templates, m.withDefaults(data))
}

func (m *Manager) Notify(ctx context.Context, accountID string, kind types.EmailTemplateKind, data TemplateData) error {
	return m.notify(ctx, accountID, kind, data, false)
}

// NotifyStrict sends a notification and returns configuration, rendering, or
// SMTP errors to the caller. It is intended for explicit user actions such as
// resending an invitation.
func (m *Manager) NotifyStrict(ctx context.Context, accountID string, kind types.EmailTemplateKind, data TemplateData) error {
	return m.notify(ctx, accountID, kind, data, true)
}

func (m *Manager) notify(ctx context.Context, accountID string, kind types.EmailTemplateKind, data TemplateData, strict bool) error {
	settings, err := m.loadSettings(ctx, accountID)
	if err != nil {
		if strict {
			return err
		}
		log.WithContext(ctx).WithError(err).Warnf("failed to load email settings for account %s", accountID)
		return nil
	}
	if !settings.Enabled {
		if strict {
			return status.Errorf(status.PreconditionFailed, "email notifications are disabled")
		}
		return nil
	}

	rendered, err := renderTemplate(kind, settings.Templates, m.withDefaults(data))
	if err != nil {
		if strict {
			return err
		}
		log.WithContext(ctx).WithError(err).Warnf("failed to render email template %s for account %s", kind, accountID)
		return nil
	}
	recipients := recipientsFromData(data)
	if len(recipients) == 0 {
		recipients = settings.AdminRecipients
	}
	if len(recipients) == 0 {
		recipients = fallbackRecipientsFromData(data)
	}
	recipients = normalizeEmails(recipients)
	if len(recipients) == 0 {
		if strict {
			return status.Errorf(status.InvalidArgument, "email notification has no recipients")
		}
		log.WithContext(ctx).Debugf("skipping email template %s for account %s: no recipients", kind, accountID)
		return nil
	}

	if err := m.send(ctx, settings, recipients, rendered, data); err != nil {
		if strict {
			return err
		}
		log.WithContext(ctx).WithError(err).Warnf("failed to send email template %s for account %s", kind, accountID)
		return nil
	}
	log.WithContext(ctx).Debugf("sent email template %s for account %s to %d recipient(s)", kind, accountID, len(recipients))
	return nil
}

func (m *Manager) checkPermission(ctx context.Context, accountID, userID string, operation operations.Operation) error {
	ok, _, err := m.permissionsManager.ValidateUserPermissions(ctx, accountID, userID, modules.Settings, operation)
	if err != nil {
		return status.NewPermissionValidationError(err)
	}
	if !ok {
		return status.NewPermissionDeniedError()
	}
	return nil
}

func (m *Manager) loadSettings(ctx context.Context, accountID string) (*types.EmailSettings, error) {
	settings, err := m.store.GetEmailSettings(ctx, store.LockingStrengthNone, accountID)
	if err != nil {
		return nil, err
	}
	settings.Normalize()
	settings.Templates = MergeTemplates(settings.Templates)
	settings.PasswordConfigured = settings.Password != "" || settings.PasswordEncrypted != ""
	return settings, nil
}

func (m *Manager) withDefaults(data TemplateData) TemplateData {
	if data == nil {
		data = TemplateData{}
	}
	if _, ok := data["dashboard"]; !ok {
		data["dashboard"] = map[string]any{"url": m.dashboardURL}
	}
	if _, ok := data["time"]; !ok {
		data["time"] = time.Now().UTC().Format(time.RFC3339)
	}
	return data
}

func renderTemplate(kind types.EmailTemplateKind, templates map[string]types.EmailTemplate, data TemplateData) (*RenderedTemplate, error) {
	tmpl, ok := templates[string(kind)]
	if !ok {
		return nil, status.Errorf(status.NotFound, "email template %s not found", kind)
	}
	if !tmpl.Enabled {
		return nil, status.Errorf(status.PreconditionFailed, "email template %s is disabled", kind)
	}

	subject, err := renderText("subject", tmpl.Subject, data)
	if err != nil {
		return nil, err
	}
	bodyText, err := renderText("body_text", tmpl.BodyText, data)
	if err != nil {
		return nil, err
	}
	bodyHTML, err := renderHTML("body_html", tmpl.BodyHTML, data)
	if err != nil {
		return nil, err
	}
	return &RenderedTemplate{Subject: subject, BodyHTML: bodyHTML, BodyText: bodyText}, nil
}

func renderText(name, raw string, data TemplateData) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	tmpl, err := texttemplate.New(name).Option("missingkey=zero").Parse(raw)
	if err != nil {
		return "", status.Errorf(status.InvalidArgument, "invalid %s template: %v", name, err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, data); err != nil {
		return "", status.Errorf(status.InvalidArgument, "render %s template: %v", name, err)
	}
	return output.String(), nil
}

func renderHTML(name, raw string, data TemplateData) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	tmpl, err := template.New(name).Option("missingkey=zero").Parse(raw)
	if err != nil {
		return "", status.Errorf(status.InvalidArgument, "invalid %s template: %v", name, err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, data); err != nil {
		return "", status.Errorf(status.InvalidArgument, "render %s template: %v", name, err)
	}
	return output.String(), nil
}

func validateSettings(settings *types.EmailSettings) error {
	if !settings.Enabled {
		return nil
	}
	if strings.TrimSpace(settings.Host) == "" {
		return status.Errorf(status.InvalidArgument, "SMTP host is required")
	}
	if settings.Port <= 0 || settings.Port > 65535 {
		return status.Errorf(status.InvalidArgument, "SMTP port is invalid")
	}
	if strings.TrimSpace(settings.FromEmail) == "" {
		return status.Errorf(status.InvalidArgument, "from email is required")
	}
	if _, err := mail.ParseAddress(settings.FromEmail); err != nil {
		return status.Errorf(status.InvalidArgument, "from email is invalid: %v", err)
	}
	if hasHeaderNewline(settings.FromName) {
		return status.Errorf(status.InvalidArgument, "from name contains a newline")
	}
	if strings.TrimSpace(settings.ReplyTo) != "" {
		if _, err := parseHeaderAddress("reply-to", settings.ReplyTo); err != nil {
			return status.Errorf(status.InvalidArgument, "%v", err)
		}
	}
	switch settings.Encryption {
	case types.EmailEncryptionNone, types.EmailEncryptionStartTLS, types.EmailEncryptionTLS:
	default:
		return status.Errorf(status.InvalidArgument, "SMTP encryption is invalid")
	}
	for _, recipient := range settings.AdminRecipients {
		if _, err := mail.ParseAddress(strings.TrimSpace(recipient)); err != nil {
			return status.Errorf(status.InvalidArgument, "admin recipient is invalid: %v", err)
		}
	}
	return nil
}

func validateTemplates(templates map[string]types.EmailTemplate) error {
	merged := MergeTemplates(templates)
	for key, tmpl := range merged {
		if strings.TrimSpace(tmpl.Subject) == "" {
			return status.Errorf(status.InvalidArgument, "email template %s subject is required", key)
		}
		if strings.TrimSpace(tmpl.BodyHTML) == "" && strings.TrimSpace(tmpl.BodyText) == "" {
			return status.Errorf(status.InvalidArgument, "email template %s body is required", key)
		}
		if _, err := texttemplate.New("subject").Parse(tmpl.Subject); err != nil {
			return status.Errorf(status.InvalidArgument, "invalid email template %s subject: %v", key, err)
		}
		if strings.TrimSpace(tmpl.BodyText) != "" {
			if _, err := texttemplate.New("body_text").Parse(tmpl.BodyText); err != nil {
				return status.Errorf(status.InvalidArgument, "invalid email template %s text body: %v", key, err)
			}
		}
		if strings.TrimSpace(tmpl.BodyHTML) != "" {
			if _, err := template.New("body_html").Parse(tmpl.BodyHTML); err != nil {
				return status.Errorf(status.InvalidArgument, "invalid email template %s HTML body: %v", key, err)
			}
		}
	}
	return nil
}

func sanitizeSettings(settings *types.EmailSettings) *types.EmailSettings {
	result := settings.Copy()
	result.Password = ""
	result.ClearPassword = false
	result.PasswordUpdatePresent = false
	result.PasswordConfigured = settings.Password != "" || settings.PasswordEncrypted != ""
	result.PasswordEncrypted = ""
	result.Normalize()
	return result
}

func recipientsFromData(data TemplateData) []string {
	return emailsFromDataKey(data, "recipients")
}

func fallbackRecipientsFromData(data TemplateData) []string {
	return emailsFromDataKey(data, "fallback_recipients")
}

func emailsFromDataKey(data TemplateData, key string) []string {
	if data == nil {
		return nil
	}
	raw, ok := data[key]
	if !ok {
		return nil
	}
	switch value := raw.(type) {
	case []string:
		return value
	case string:
		return []string{value}
	case []any:
		recipients := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok {
				recipients = append(recipients, text)
			}
		}
		return recipients
	default:
		return nil
	}
}

func normalizeEmails(source []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(source))
	for _, recipient := range source {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" {
			continue
		}
		address, err := mail.ParseAddress(recipient)
		if err != nil {
			continue
		}
		email := strings.ToLower(address.Address)
		if _, ok := seen[email]; ok {
			continue
		}
		seen[email] = struct{}{}
		result = append(result, address.Address)
	}
	sort.Strings(result)
	return result
}

func (m *Manager) send(ctx context.Context, settings *types.EmailSettings, recipients []string, rendered *RenderedTemplate, _ TemplateData) error {
	if settings == nil || rendered == nil {
		return errors.New("missing email settings or rendered template")
	}
	if !settings.Enabled {
		return nil
	}
	recipients = normalizeEmails(recipients)
	if len(recipients) == 0 {
		return nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, m.sendTimeout)
	defer cancel()
	return sendSMTP(sendCtx, settings, recipients, rendered)
}

func sendSMTP(ctx context.Context, settings *types.EmailSettings, recipients []string, rendered *RenderedTemplate) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	address := net.JoinHostPort(settings.Host, fmt.Sprintf("%d", settings.Port))
	if hasHeaderNewline(settings.FromName) {
		return errors.New("SMTP from name contains a newline")
	}
	fromEmail, err := parseHeaderAddress("from email", settings.FromEmail)
	if err != nil {
		return err
	}
	if hasHeaderNewline(rendered.Subject) {
		return errors.New("email subject contains a newline")
	}
	replyTo := ""
	if strings.TrimSpace(settings.ReplyTo) != "" {
		parsedReplyTo, err := parseHeaderAddress("reply-to", settings.ReplyTo)
		if err != nil {
			return err
		}
		replyTo = parsedReplyTo.String()
	}
	from := mail.Address{Name: settings.FromName, Address: fromEmail.Address}
	headers := map[string]string{
		"From":         from.String(),
		"To":           strings.Join(recipients, ", "),
		"Subject":      rendered.Subject,
		"MIME-Version": "1.0",
	}
	if replyTo != "" {
		headers["Reply-To"] = replyTo
	}
	body := rendered.BodyText
	if strings.TrimSpace(rendered.BodyHTML) != "" {
		headers["Content-Type"] = `text/html; charset="utf-8"`
		body = rendered.BodyHTML
	} else {
		headers["Content-Type"] = `text/plain; charset="utf-8"`
	}

	var message strings.Builder
	for key, value := range headers {
		message.WriteString(key)
		message.WriteString(": ")
		message.WriteString(value)
		message.WriteString("\r\n")
	}
	message.WriteString("\r\n")
	message.WriteString(body)

	auth := smtp.Auth(nil)
	if settings.Username != "" || settings.Password != "" {
		auth = smtp.PlainAuth("", settings.Username, settings.Password, settings.Host)
	}

	tlsConfig := &tls.Config{ServerName: settings.Host, InsecureSkipVerify: settings.InsecureSkipVerify}
	if settings.Encryption == types.EmailEncryptionTLS {
		conn, err := dialSMTP(ctx, address)
		if err != nil {
			return err
		}
		stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stopClose()
		defer conn.Close()
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return err
		}
		client, err := smtp.NewClient(tlsConn, settings.Host)
		if err != nil {
			return err
		}
		defer client.Close()
		return sendWithClient(client, auth, fromEmail.Address, recipients, []byte(message.String()))
	}

	conn, err := dialSMTP(ctx, address)
	if err != nil {
		return err
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	defer conn.Close()
	client, err := smtp.NewClient(conn, settings.Host)
	if err != nil {
		return err
	}
	defer client.Close()
	if settings.Encryption == types.EmailEncryptionStartTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("SMTP server does not support STARTTLS")
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			return err
		}
	}
	return sendWithClient(client, auth, fromEmail.Address, recipients, []byte(message.String()))
}

func sendWithClient(client *smtp.Client, auth smtp.Auth, from string, recipients []string, message []byte) error {
	if auth != nil {
		if ok, _ := client.Extension("AUTH"); !ok {
			return errors.New("SMTP server does not support AUTH")
		}
		if err := client.Auth(auth); err != nil {
			return err
		}
	}
	if err := client.Mail(from); err != nil {
		return err
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return err
		}
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := writer.Write(message); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func dialSMTP(ctx context.Context, address string) (net.Conn, error) {
	dialer := &net.Dialer{}
	if deadline, ok := ctx.Deadline(); ok {
		dialer.Timeout = time.Until(deadline)
		if dialer.Timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func parseHeaderAddress(field, value string) (mail.Address, error) {
	if hasHeaderNewline(value) {
		return mail.Address{}, fmt.Errorf("SMTP %s contains a newline", field)
	}
	parsed, err := mail.ParseAddress(strings.TrimSpace(value))
	if err != nil || parsed.Address == "" {
		if err == nil {
			err = errors.New("address is empty")
		}
		return mail.Address{}, fmt.Errorf("invalid SMTP %s: %w", field, err)
	}
	return *parsed, nil
}

func hasHeaderNewline(value string) bool {
	return strings.ContainsAny(value, "\r\n")
}
