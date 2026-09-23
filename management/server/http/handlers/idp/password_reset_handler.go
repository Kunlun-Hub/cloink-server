package idp

import (
	"html/template"
	"net/http"
	"net/url"
	"path"
	"strings"

	log "github.com/sirupsen/logrus"

	dexweb "github.com/netbirdio/netbird/idp/dex/web"
	"github.com/netbirdio/netbird/management/server/account"
	idpmanager "github.com/netbirdio/netbird/management/server/idp"
	"github.com/netbirdio/netbird/shared/management/status"
)

const (
	// ForgotPasswordPath serves the "send me a reset link" form.
	ForgotPasswordPath = "/oauth2/forgot-password"
	// ResetPasswordPath serves the "set a new password" form reached from email.
	ResetPasswordPath = "/oauth2/reset-password"
)

// Error keys double as the English copy rendered on the page. The sign-in
// templates translate them client-side, so the language switcher keeps working.
const (
	errKeyInvalidLink   = "This password reset link is invalid or has expired."
	errKeyMismatch      = "Passwords do not match."
	errKeyWeakPassword  = "Password must be at least 8 characters and include a digit, an uppercase letter and a special character."
	errKeyRateLimited   = "Too many password reset requests. Please try again later."
	errKeyNoEmail       = "Email delivery is not configured. Please contact your administrator."
	errKeySendFailed    = "Could not send the reset email. Please try again or contact your administrator."
	errKeyGeneric       = "Could not create the reset link. Please try again."
	errKeyUnsupported   = "This deployment does not support password reset. Please contact your administrator."
	defaultFrontendName = "Cloink"
)

// PasswordResetHandler renders the self-service password recovery pages on the
// embedded IdP.
type PasswordResetHandler struct {
	accountManager account.Manager
	issuerPath     string
	forgotTmpl     *template.Template
	resetTmpl      *template.Template
}

// NewPasswordResetHandler builds the handler and parses its templates from the
// embedded IdP web assets so the pages match the sign-in screen.
func NewPasswordResetHandler(accountManager account.Manager, embeddedIDP *idpmanager.EmbeddedIdPManager) (*PasswordResetHandler, error) {
	issuerPath := ""
	if embeddedIDP != nil {
		if parsed, err := url.Parse(embeddedIDP.GetIssuer()); err == nil && parsed.Path != "" {
			issuerPath = parsed.Path
		}
	}

	funcs := template.FuncMap{
		"issuer": func() string { return defaultFrontendName },
		"logo":   func() string { return "theme/logo.png" },
		"extra":  func(string) string { return "" },
		"url": func(reqPath, assetPath string) string {
			return relativeURL(issuerPath, reqPath, assetPath)
		},
	}

	parse := func(page string) (*template.Template, error) {
		parsed, err := template.New(page).Funcs(funcs).ParseFS(
			dexweb.FS(),
			"templates/header.html",
			"templates/footer.html",
			"templates/"+page,
		)
		if err != nil {
			return nil, err
		}
		// Execute the page itself; the shared header/footer come from the same set.
		return parsed.Lookup(page), nil
	}

	forgotTmpl, err := parse("forgot_password.html")
	if err != nil {
		return nil, err
	}
	resetTmpl, err := parse("reset_password.html")
	if err != nil {
		return nil, err
	}

	return &PasswordResetHandler{
		accountManager: accountManager,
		issuerPath:     issuerPath,
		forgotTmpl:     forgotTmpl,
		resetTmpl:      resetTmpl,
	}, nil
}

type forgotPasswordPageData struct {
	PostURL  string
	ReqPath  string
	Email    string
	ErrorKey string
	Sent     bool
}

type resetPasswordPageData struct {
	PostURL  string
	ReqPath  string
	Token    string
	ErrorKey string
	Invalid  bool
	Done     bool
}

// ForgotPassword renders and handles the reset request form.
func (h *PasswordResetHandler) ForgotPassword(w http.ResponseWriter, r *http.Request) {
	noStore(w)

	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data := forgotPasswordPageData{
		PostURL: ForgotPasswordPath,
		ReqPath: r.URL.Path,
	}

	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			data.ErrorKey = errKeyGeneric
			h.render(w, h.forgotTmpl, data)
			return
		}
		data.Email = strings.TrimSpace(r.FormValue("email"))

		if err := h.accountManager.RequestPasswordReset(r.Context(), data.Email); err != nil {
			data.ErrorKey = forgotPasswordErrorKey(err)
			h.render(w, h.forgotTmpl, data)
			return
		}
		data.Sent = true
		data.Email = ""
	}

	h.render(w, h.forgotTmpl, data)
}

// ResetPassword renders and handles the "set a new password" form.
func (h *PasswordResetHandler) ResetPassword(w http.ResponseWriter, r *http.Request) {
	noStore(w)

	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data := resetPasswordPageData{
		PostURL: ResetPasswordPath,
		ReqPath: r.URL.Path,
	}

	if r.Method == http.MethodGet {
		data.Token = r.URL.Query().Get("token")
		if err := h.accountManager.ValidatePasswordResetToken(r.Context(), data.Token); err != nil {
			data.Invalid = true
			data.ErrorKey = errKeyInvalidLink
		}
		h.render(w, h.resetTmpl, data)
		return
	}

	if err := r.ParseForm(); err != nil {
		data.Invalid = true
		data.ErrorKey = errKeyInvalidLink
		h.render(w, h.resetTmpl, data)
		return
	}
	data.Token = r.FormValue("token")

	if err := h.accountManager.ValidatePasswordResetToken(r.Context(), data.Token); err != nil {
		data.Invalid = true
		data.ErrorKey = errKeyInvalidLink
		h.render(w, h.resetTmpl, data)
		return
	}

	password, confirmation := r.FormValue("password"), r.FormValue("confirm")
	if password != confirmation {
		data.ErrorKey = errKeyMismatch
		h.render(w, h.resetTmpl, data)
		return
	}

	if err := h.accountManager.ResetPasswordWithToken(r.Context(), data.Token, password); err != nil {
		// The link was already verified above, so an invalid argument here is
		// the password policy.
		if statusErr, ok := status.FromError(err); ok && statusErr.Type() == status.InvalidArgument {
			data.ErrorKey = errKeyWeakPassword
		} else {
			data.ErrorKey = errKeyGeneric
		}
		h.render(w, h.resetTmpl, data)
		return
	}

	data.Done = true
	h.render(w, h.resetTmpl, data)
}

func (h *PasswordResetHandler) render(w http.ResponseWriter, tmpl *template.Template, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		log.WithError(err).Error("failed to render password reset page")
	}
}

func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

// forgotPasswordErrorKey maps a manager error onto the copy shown on the page.
func forgotPasswordErrorKey(err error) string {
	if statusErr, ok := status.FromError(err); ok {
		switch statusErr.Type() {
		case status.TooManyRequests:
			return errKeyRateLimited
		case status.PreconditionFailed:
			return errKeyNoEmail
		}
	}
	return errKeySendFailed
}

// relativeURL mirrors Dex's asset URL helper so the pages resolve the shared
// theme files exactly like the sign-in screen does.
func relativeURL(serverPath, reqPath, assetPath string) string {
	if u, err := url.ParseRequestURI(assetPath); err == nil && u.Scheme != "" {
		return assetPath
	}

	splitPath := func(p string) []string {
		res := []string{}
		for _, part := range strings.Split(path.Clean(p), "/") {
			if part != "" {
				res = append(res, part)
			}
		}
		return res
	}

	stripCommonParts := func(s1, s2 []string) ([]string, []string) {
		min := len(s1)
		if len(s2) < min {
			min = len(s2)
		}
		splitIndex := min
		for i := 0; i < min; i++ {
			if s1[i] != s2[i] {
				splitIndex = i
				break
			}
		}
		return s1[splitIndex:], s2[splitIndex:]
	}

	server, req, asset := splitPath(serverPath), splitPath(reqPath), splitPath(assetPath)

	_, req = stripCommonParts(server, req)
	if len(req) == 0 && len(server) > 0 {
		asset = append(server, asset...)
	}
	asset, req = stripCommonParts(asset, req)

	var result string
	for i := 0; i < len(req)-1; i++ {
		result = path.Join("..", result)
	}
	return path.Join(result, path.Join(asset...))
}
