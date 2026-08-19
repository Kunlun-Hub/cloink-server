import { createContext, useContext, useMemo, type ReactNode } from "react";
import en, { type TranslationKey, type Translations } from "./locales/en";
import zhCN from "./locales/zh-CN";

type Locale = "en" | "zh-CN";

const messages: Record<Locale, Translations> = {
  en,
  "zh-CN": zhCN,
};

function detectLocale(): Locale {
  const lang = navigator.language || "en";
  if (lang.startsWith("zh")) return "zh-CN";
  return "en";
}

type TranslateFn = (key: TranslationKey, params?: Record<string, string | number>) => string;

const I18nContext = createContext<{
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: TranslateFn;
} | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useMemo(() => {
    const stored = localStorage.getItem("locale") as Locale | null;
    const initial = stored && messages[stored] ? stored : detectLocale();
    return [initial, (l: Locale) => {
      localStorage.setItem("locale", l);
      window.location.reload();
    }] as const;
  }, []);

  const t: TranslateFn = useMemo(() => {
    return (key: TranslationKey, params?: Record<string, string | number>) => {
      let text = messages[locale][key];
      if (params) {
        for (const [k, v] of Object.entries(params)) {
          text = text.replace(`{${k}}`, String(v));
        }
      }
      return text;
    };
  }, [locale]);

  const value = useMemo(() => ({
    locale,
    setLocale: setLocaleState,
    t,
  }), [locale, setLocaleState, t]);

  return (
    <I18nContext.Provider value={value}>
      {children}
    </I18nContext.Provider>
  );
}

export function useI18n() {
  const ctx = useContext(I18nContext);
  if (!ctx) {
    throw new Error("useI18n must be used within I18nProvider");
  }
  return ctx;
}
