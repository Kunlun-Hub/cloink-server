package idp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/idp/dex"
)

func TestBuildWeChatWorkLoginURL(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://internal/oauth2/callback/wechatwork-connector?state=auth-req", nil)
	req.Host = "sso.example.com"
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "login.example.com")

	loginURL, err := buildWeChatWorkLoginURL(req, "wwcorp", "1000002", "auth-req")
	require.NoError(t, err)

	parsed, err := url.Parse(loginURL)
	require.NoError(t, err)
	assert.Equal(t, "https://login.work.weixin.qq.com/wwlogin/sso/login", parsed.Scheme+"://"+parsed.Host+parsed.Path)
	assert.Equal(t, "CorpApp", parsed.Query().Get("login_type"))
	assert.Equal(t, "wwcorp", parsed.Query().Get("appid"))
	assert.Equal(t, "wwcorp", parsed.Query().Get("client_id"))
	assert.Equal(t, "1000002", parsed.Query().Get("agentid"))
	assert.Equal(t, "code", parsed.Query().Get("response_type"))
	assert.Equal(t, "auth-req", parsed.Query().Get("state"))
	assert.Equal(t, "https://login.example.com/oauth2/callback/wechatwork-connector", parsed.Query().Get("redirect_uri"))
}

func TestResolveWeChatWorkIdentity(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "wwcorp", r.URL.Query().Get("corpid"))
		assert.Equal(t, "corp-secret", r.URL.Query().Get("corpsecret"))
		require.NoError(t, json.NewEncoder(w).Encode(weChatWorkTokenResponse{
			ErrCode:     0,
			ErrMsg:      "ok",
			AccessToken: "corp-access-token",
		}))
	})
	mux.HandleFunc("/cgi-bin/auth/getuserinfo", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "corp-access-token", r.URL.Query().Get("access_token"))
		assert.Equal(t, "auth-code", r.URL.Query().Get("code"))
		require.NoError(t, json.NewEncoder(w).Encode(weChatWorkUserInfoResponse{
			ErrCode:    0,
			ErrMsg:     "ok",
			UserID:     "zhangsan",
			UserTicket: "user-ticket",
			CorpID:     "wwcorp",
		}))
	})
	mux.HandleFunc("/cgi-bin/auth/getuserdetail", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "corp-access-token", r.URL.Query().Get("access_token"))
		var req weChatWorkUserDetailRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "user-ticket", req.UserTicket)
		require.NoError(t, json.NewEncoder(w).Encode(weChatWorkUserDetailResponse{
			ErrCode: 0,
			ErrMsg:  "ok",
			Name:    "张三",
			Email:   "zhangsan@example.com",
		}))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	oldTokenURL := weChatWorkTokenURL
	oldUserInfoURL := weChatWorkUserInfoURL
	oldUserDetailURL := weChatWorkUserDetailURL
	weChatWorkTokenURL = server.URL + "/cgi-bin/gettoken"
	weChatWorkUserInfoURL = server.URL + "/cgi-bin/auth/getuserinfo"
	weChatWorkUserDetailURL = server.URL + "/cgi-bin/auth/getuserdetail"
	t.Cleanup(func() {
		weChatWorkTokenURL = oldTokenURL
		weChatWorkUserInfoURL = oldUserInfoURL
		weChatWorkUserDetailURL = oldUserDetailURL
	})

	handler := &weChatWorkCallbackHandler{
		httpClient: server.Client(),
	}

	identity, err := handler.resolveWeChatWorkIdentity(context.Background(), &dex.ConnectorConfig{
		ClientID:     "wwcorp",
		ClientSecret: "corp-secret",
	}, "auth-code")
	require.NoError(t, err)
	assert.Equal(t, "wwcorp:zhangsan", identity.UserID)
	assert.Equal(t, "wwcorp:zhangsan", identity.Username)
	assert.Equal(t, "张三", identity.Name)
	assert.Equal(t, "zhangsan@example.com", identity.Email)
}

func TestWeChatWorkLoginPageTemplateRendersRawURLs(t *testing.T) {
	var output bytes.Buffer
	err := weChatWorkLoginPageTmpl.Execute(&output, weChatWorkLoginPageData{
		ConnectorName: "企业微信",
		FallbackURL:   "https://login.work.weixin.qq.com/wwlogin/sso/login?appid=wwcorp",
		AppID:         "wwcorp",
		AgentID:       "1000002",
		RedirectURI:   "https://dev.cloink.4w.ink/oauth2/callback/wechatwork-connector",
		State:         "auth-state",
		ScriptURL:     "https://unpkg.com/@wecom/jssdk@2.3.4/dist/wecom.global.prod.js",
	})
	require.NoError(t, err)

	html := output.String()
	assert.Contains(t, html, `appid: "wwcorp"`)
	assert.Contains(t, html, `agentid: "1000002"`)
	assert.Contains(t, html, `redirect_uri: "https://dev.cloink.4w.ink/oauth2/callback/wechatwork-connector"`)
	assert.Contains(t, html, `state: "auth-state"`)
	assert.NotContains(t, html, `redirect_uri: "\"https://dev.cloink.4w.ink/oauth2/callback/wechatwork-connector\""`)
	assert.NotContains(t, html, `state: "\"auth-state\""`)
}
