package idp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/netbirdio/netbird/idp/dex"
	idpmanager "github.com/netbirdio/netbird/management/server/idp"
)

const (
	weChatWorkLoginURL    = "https://open.weixin.qq.com/connect/oauth2/authorize"
	weChatWorkPanelSDKURL = "https://unpkg.com/@wecom/jssdk@2.3.4/dist/wecom.global.prod.js"
)

var (
	weChatWorkTokenURL      = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
	weChatWorkUserInfoURL   = "https://qyapi.weixin.qq.com/cgi-bin/auth/getuserinfo"
	weChatWorkUserDetailURL = "https://qyapi.weixin.qq.com/cgi-bin/auth/getuserdetail"
)

type weChatWorkCallbackHandler struct {
	embeddedIDP *idpmanager.EmbeddedIdPManager
	dexHandler  http.Handler
	httpClient  *http.Client
}

type weChatWorkLoginPageData struct {
	ConnectorName string
	FallbackURL   string
	AppID         string
	AgentID       string
	RedirectURI   string
	State         string
	ScriptURL     string
}

func toJSON(v any) template.JS {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return template.JS(encoded)
}

var weChatWorkLoginPageTmpl = template.Must(template.New("wechatwork-login").Funcs(template.FuncMap{
	"toJSON": toJSON,
}).Parse(`<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{ .ConnectorName }} 登录</title>
  <style>
    *,:before,:after{box-sizing:border-box}
    body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;background:#18191d;color:#e4e7e9;font-family:ui-sans-serif,system-ui,sans-serif}
    .nb-card{width:100%;max-width:420px;background:#1b1f22;border:1px solid rgba(50,54,61,.5);border-radius:12px;padding:32px;box-shadow:0 20px 25px -5px rgba(0,0,0,.1),0 8px 10px -6px rgba(0,0,0,.1)}
    .nb-title{font-size:24px;font-weight:600;margin:0 0 12px;text-align:center}
    .nb-subtitle{font-size:14px;color:rgba(167,177,185,.8);margin:0 0 24px;text-align:center}
    .nb-panel{display:flex;justify-content:center;min-height:320px}
    .nb-fallback{margin-top:20px;text-align:center;font-size:13px;color:rgba(167,177,185,.8)}
    .nb-link{color:#f68330;text-decoration:none}
    .nb-link:hover{text-decoration:underline}
  </style>
</head>
<body>
  <div class="nb-card">
    <h1 class="nb-title">企业微信登录</h1>
    <p class="nb-subtitle">使用 {{ .ConnectorName }} 完成扫码登录</p>
    <div id="ww_login_panel" class="nb-panel"></div>
    <div class="nb-fallback">
      如果登录组件未正常加载，请<a class="nb-link" href="{{ .FallbackURL }}">点击这里继续登录</a>
    </div>
  </div>
  <script src="{{ .ScriptURL }}"></script>
  <script>
    (function() {
      if (!window.ww || typeof window.ww.createWWLoginPanel !== "function") {
        console.error("WeChat Work login panel SDK is unavailable");
        return;
      }

      try {
        window.ww.createWWLoginPanel({
          el: '#ww_login_panel',
          params: {
            login_type: 'CorpApp',
            appid: {{ toJSON .AppID }},
            agentid: {{ toJSON .AgentID }},
            redirect_uri: {{ toJSON .RedirectURI }},
            state: {{ toJSON .State }},
            redirect_type: 'callback'
          },
          onCheckWeComLogin: function (event) {
            console.info('wecom login state', event);
          },
          onLoginSuccess: function (res) {
            if (!res || !res.code) {
              return;
            }
            var callbackURL = new URL({{ toJSON .RedirectURI }});
            callbackURL.searchParams.set('code', res.code);
            callbackURL.searchParams.set('state', {{ toJSON .State }});
            window.location.replace(callbackURL.toString());
          },
          onLoginFail: function (err) {
            console.error('WeChat Work login panel failed', err);
          }
        });
      } catch (err) {
        console.error("WeChat Work login panel init failed", err);
      }
    })();
  </script>
</body>
</html>`))

type weChatWorkTokenResponse struct {
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
	AccessToken string `json:"access_token"`
}

type weChatWorkUserInfoResponse struct {
	ErrCode    int    `json:"errcode"`
	ErrMsg     string `json:"errmsg"`
	UserID     string `json:"userid"`
	OpenUserID string `json:"open_userid"`
	UserTicket string `json:"user_ticket"`
	CorpID     string `json:"corpid"`
}

type weChatWorkUserDetailRequest struct {
	UserTicket string `json:"user_ticket"`
}

type weChatWorkUserDetailResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
	Name    string `json:"name"`
	Email   string `json:"email"`
}

func NewWeChatWorkCallbackHandler(embeddedIDP *idpmanager.EmbeddedIdPManager) http.Handler {
	return &weChatWorkCallbackHandler{
		embeddedIDP: embeddedIDP,
		dexHandler:  embeddedIDP.Handler(),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (h *weChatWorkCallbackHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	connectorID := mux.Vars(r)["connector"]
	if connectorID == "" {
		h.dexHandler.ServeHTTP(w, r)
		return
	}

	connector, err := h.embeddedIDP.GetConnector(r.Context(), connectorID)
	if err != nil || connector.Type != "wechatwork" {
		h.dexHandler.ServeHTTP(w, r)
		return
	}

	state := r.URL.Query().Get("state")
	if state == "" {
		http.Error(w, "missing state parameter", http.StatusBadRequest)
		return
	}

	if errType := r.URL.Query().Get("error"); errType != "" {
		http.Error(w, r.URL.Query().Get("error_description"), http.StatusBadRequest)
		return
	}

	if r.URL.Query().Get("code") == "" {
		h.renderLoginPage(w, r, connector, state)
		return
	}

	identity, err := h.resolveWeChatWorkIdentity(r.Context(), connector, r.URL.Query().Get("code"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	proxiedReq := r.Clone(r.Context())
	proxiedReq.URL = cloneURL(r.URL)
	proxiedReq.URL.RawQuery = url.Values{"state": []string{state}}.Encode()
	proxiedReq.Header = r.Header.Clone()
	proxiedReq.Header.Set("X-NetBird-WeChatWork-User-Id", identity.UserID)
	proxiedReq.Header.Set("X-NetBird-WeChatWork-User", identity.Username)
	proxiedReq.Header.Set("X-NetBird-WeChatWork-User-Name", identity.Name)
	proxiedReq.Header.Set("X-NetBird-WeChatWork-User-Email", identity.Email)

	h.dexHandler.ServeHTTP(w, proxiedReq)
}

func (h *weChatWorkCallbackHandler) renderLoginPage(w http.ResponseWriter, r *http.Request, connector *dex.ConnectorConfig, state string) {
	if connector.ClientID == "" || connector.AgentID == "" {
		http.Error(w, "wechat work connector is missing app configuration", http.StatusBadRequest)
		return
	}

	fallbackURL, err := buildWeChatWorkLoginURL(r, connector.ClientID, connector.AgentID, state)
	if err != nil {
		http.Error(w, "failed to build WeChat Work login URL", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := weChatWorkLoginPageTmpl.Execute(w, weChatWorkLoginPageData{
		ConnectorName: connector.Name,
		FallbackURL:   fallbackURL,
		AppID:         connector.ClientID,
		AgentID:       connector.AgentID,
		RedirectURI:   currentRequestURL(r),
		State:         state,
		ScriptURL:     weChatWorkPanelSDKURL,
	}); err != nil {
		http.Error(w, "failed to render login page", http.StatusInternalServerError)
	}
}

type weChatWorkIdentity struct {
	UserID   string
	Username string
	Name     string
	Email    string
}

func (h *weChatWorkCallbackHandler) resolveWeChatWorkIdentity(ctx context.Context, connector *dex.ConnectorConfig, code string) (*weChatWorkIdentity, error) {
	if connector.ClientID == "" || connector.ClientSecret == "" {
		return nil, fmt.Errorf("wechat work connector is missing app configuration")
	}

	var tokenResp weChatWorkTokenResponse
	if err := getJSON(ctx, h.httpClient, weChatWorkTokenURL, url.Values{
		"corpid":     []string{connector.ClientID},
		"corpsecret": []string{connector.ClientSecret},
	}, &tokenResp); err != nil {
		return nil, err
	}
	if tokenResp.ErrCode != 0 || tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("failed to get access_token: %s (%d)", tokenResp.ErrMsg, tokenResp.ErrCode)
	}

	var userInfo weChatWorkUserInfoResponse
	if err := getJSON(ctx, h.httpClient, weChatWorkUserInfoURL, url.Values{
		"access_token": []string{tokenResp.AccessToken},
		"code":         []string{code},
	}, &userInfo); err != nil {
		return nil, err
	}
	if userInfo.ErrCode != 0 {
		return nil, fmt.Errorf("failed to get WeChat Work user info: %s (%d)", userInfo.ErrMsg, userInfo.ErrCode)
	}

	userID := userInfo.OpenUserID
	if userID == "" {
		userID = userInfo.UserID
	}
	if userID == "" {
		return nil, fmt.Errorf("wechat work user identity is missing userid")
	}
	if userInfo.CorpID != "" && userInfo.UserID != "" {
		userID = userInfo.CorpID + ":" + userInfo.UserID
	}

	identity := &weChatWorkIdentity{
		UserID:   userID,
		Username: userID,
		Name:     userID,
		Email:    sanitizeWeChatWorkEmail(userID),
	}

	if userInfo.UserTicket != "" {
		var detail weChatWorkUserDetailResponse
		if err := postJSON(ctx, h.httpClient, weChatWorkUserDetailURL+"?access_token="+url.QueryEscape(tokenResp.AccessToken), weChatWorkUserDetailRequest{
			UserTicket: userInfo.UserTicket,
		}, &detail); err == nil && detail.ErrCode == 0 {
			if detail.Name != "" {
				identity.Name = detail.Name
			}
			if detail.Email != "" {
				identity.Email = detail.Email
			}
		}
	}

	return identity, nil
}

func buildWeChatWorkLoginURL(r *http.Request, appID, agentID, state string) (string, error) {
	redirectURI := currentRequestURL(r)
	if redirectURI == "" {
		return "", fmt.Errorf("missing callback URL")
	}

	return buildWeChatWorkLoginURLFromCallbackURL(redirectURI, appID, agentID, state)
}

func buildWeChatWorkLoginURLFromCallbackURL(redirectURI, appID, agentID, state string) (string, error) {
	if agentID != "" {
		loginURL, err := url.Parse("https://login.work.weixin.qq.com/wwlogin/sso/login")
		if err != nil {
			return "", err
		}

		loginURL.RawQuery = url.Values{
			"login_type":    []string{"CorpApp"},
			"appid":         []string{appID},
			"agentid":       []string{agentID},
			"client_id":     []string{appID},
			"redirect_uri":  []string{redirectURI},
			"response_type": []string{"code"},
			"state":         []string{state},
		}.Encode()

		return loginURL.String(), nil
	}

	loginURL, err := url.Parse(weChatWorkLoginURL)
	if err != nil {
		return "", err
	}

	loginURL.RawQuery = url.Values{
		"appid":         []string{appID},
		"redirect_uri":  []string{redirectURI},
		"response_type": []string{"code"},
		"scope":         []string{"snsapi_privateinfo"},
		"state":         []string{state},
	}.Encode()
	loginURL.Fragment = "wechat_redirect"

	return loginURL.String(), nil
}

func currentRequestURL(r *http.Request) string {
	origin := currentRequestOrigin(r)
	if origin == "" {
		return ""
	}
	return origin + r.URL.Path
}

func currentRequestOrigin(r *http.Request) string {
	scheme := "http"
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	} else if r.TLS != nil {
		scheme = "https"
	}

	host := r.Host
	if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		host = forwardedHost
	}
	if host == "" {
		return ""
	}

	return scheme + "://" + host
}

func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return &url.URL{}
	}
	clone := *u
	return &clone
}

func postJSON[T any](ctx context.Context, client *http.Client, endpoint string, payload any, target *T) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func getJSON[T any](ctx context.Context, client *http.Client, endpoint string, query url.Values, target *T) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func sanitizeWeChatWorkEmail(id string) string {
	replacer := strings.NewReplacer(":", ".", "/", ".", "\\", ".", " ", ".")
	sanitized := replacer.Replace(id)
	if sanitized == "" {
		sanitized = "user"
	}
	return sanitized + "@wechatwork.local"
}
