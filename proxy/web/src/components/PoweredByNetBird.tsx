import { NetBirdLogo } from "./NetBirdLogo";
import { useI18n } from "@/i18n/I18nProvider";

export function PoweredByNetBird() {
  const { t } = useI18n();

  return (
    <a
      href="https://netbird.io?utm_source=netbird-proxy&utm_medium=web&utm_campaign=powered_by"
      target="_blank"
      rel="noopener noreferrer"
      className="flex items-center justify-center mt-8 gap-2 group cursor-pointer"
    >
      <span className="text-sm text-nb-gray-400 font-light text-center group-hover:opacity-80 transition-all">
        {t("common.poweredBy")}
      </span>
      <NetBirdLogo size="small" mobile={false} />
    </a>
  );
}