package i18n

import (
	"testing"
)

func TestT(t *testing.T) {
	tests := []struct {
		name   string
		lang   string
		key    string
		params []string
		want   string
	}{
		{
			name: "English translation",
			lang: "en",
			key:  "error.serviceNotFound",
			want: "Service Not Found",
		},
		{
			name: "Chinese translation",
			lang: "zh-CN",
			key:  "error.serviceNotFound",
			want: "服务未找到",
		},
		{
			name: "Fallback to English for unknown language",
			lang: "fr",
			key:  "error.serviceNotFound",
			want: "Service Not Found",
		},
		{
			name: "Fallback to key for unknown key",
			lang: "en",
			key:  "error.unknownKey",
			want: "error.unknownKey",
		},
		{
			name: "Chinese language prefix match",
			lang: "zh",
			key:  "error.serviceNotFound",
			want: "服务未找到",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := T(tt.lang, tt.key, tt.params...)
			if got != tt.want {
				t.Errorf("T(%q, %q, %v) = %q, want %q", tt.lang, tt.key, tt.params, got, tt.want)
			}
		})
	}
}

func TestDetectLanguage(t *testing.T) {
	tests := []struct {
		name       string
		acceptLang string
		want       string
	}{
		{"Empty", "", "en"},
		{"English", "en-US,en;q=0.9", "en"},
		{"Chinese", "zh-CN,zh;q=0.9", "zh-CN"},
		{"Chinese simple", "zh", "zh-CN"},
		{"Mixed English first", "en-US,zh-CN;q=0.8", "en"},
		{"Mixed Chinese first", "zh-CN,en-US;q=0.8", "zh-CN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectLanguage(tt.acceptLang)
			if got != tt.want {
				t.Errorf("DetectLanguage(%q) = %q, want %q", tt.acceptLang, got, tt.want)
			}
		})
	}
}
