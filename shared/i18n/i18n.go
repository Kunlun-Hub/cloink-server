// Package i18n provides lightweight internationalization for server-side
// HTTP responses. Translations are loaded from embedded JSON locale files
// and looked up by language key with English as the default fallback.
package i18n

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

//go:embed locales/*.json
var localeFS embed.FS

var (
	translations map[string]map[string]string // lang -> key -> message
	once         sync.Once
	loadErr      error
)

// load reads all embedded locale JSON files once.
func load() {
	once.Do(func() {
		entries, err := localeFS.ReadDir("locales")
		if err != nil {
			loadErr = err
			return
		}
		translations = make(map[string]map[string]string, len(entries))
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			lang := strings.TrimSuffix(e.Name(), ".json")
			data, err := localeFS.ReadFile("locales/" + e.Name())
			if err != nil {
				loadErr = err
				return
			}
			var m map[string]string
			if err := json.Unmarshal(data, &m); err != nil {
				loadErr = err
				return
			}
			translations[lang] = m
		}
	})
}

// T returns the translation for the given language and key. If the key
// is not found in the requested language, it falls back to English.
// If still not found, the raw key is returned. Params are substituted
// for {0}, {1}, ... placeholders in the translation string.
func T(lang, key string, params ...string) string {
	load()
	if loadErr != nil {
		return key
	}

	// Try requested language
	if msg, ok := translations[lang][key]; ok {
		return applyParams(msg, params)
	}

	// Try language prefix (e.g., "zh" for "zh-CN", or "zh-CN" matching "zh")
	langPrefix := lang
	if idx := strings.Index(lang, "-"); idx > 0 {
		langPrefix = lang[:idx]
	}
	for l, msgs := range translations {
		if strings.HasPrefix(l, langPrefix) || strings.HasPrefix(lang, l) {
			if msg, ok := msgs[key]; ok {
				return applyParams(msg, params)
			}
		}
	}

	// Fallback to English
	if msg, ok := translations["en"][key]; ok {
		return applyParams(msg, params)
	}

	return key
}

// applyParams replaces {0}, {1}, ... placeholders with the given parameters.
func applyParams(msg string, params []string) string {
	for i, p := range params {
		msg = strings.ReplaceAll(msg, fmt.Sprintf("{%d}", i), p)
	}
	return msg
}

// DetectLanguage extracts the preferred language from an Accept-Language
// header. Returns "zh-CN" for Chinese preferences, "en" otherwise.
func DetectLanguage(acceptLang string) string {
	if acceptLang == "" {
		return "en"
	}
	// Parse the first quality-value pair
	for _, part := range strings.Split(acceptLang, ",") {
		lang := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		lower := strings.ToLower(lang)
		if strings.HasPrefix(lower, "zh") {
			return "zh-CN"
		}
		if strings.HasPrefix(lower, "en") {
			return "en"
		}
	}
	return "en"
}
