import { type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Browser } from "@wailsio/runtime";
import { DownloadIcon, NotepadText } from "lucide-react";
import { Update as UpdateSvc } from "@bindings/services";
import { Button } from "@/components/buttons/Button";
import { useClientVersion } from "@/contexts/ClientVersionContext";
import { cn } from "@/lib/cn";

const CLOINK_RELEASES = "https://cloink.4w.ink/api/version-releases/public?channel=stable&latest=true";

function openUrl(url: string) {
    Browser.OpenURL(url).catch(() => {
        window.open(url, "_blank");
    });
}

function openInstallerDownload() {
    UpdateSvc.DownloadURL()
        .then(openUrl)
        .catch(() => openUrl(CLOINK_RELEASES));
}

export function UpdateVersionCard() {
    const { t } = useTranslation();
    const { updateVersion, enforced, triggerUpdate } = useClientVersion();

    if (updateVersion) {
        const titleKey = enforced
            ? "update.card.versionAvailableInstall"
            : "update.card.versionAvailableDownload";
        return (
            <Card className={"max-w-lg"}>
                <div>
                    <Title>{t(titleKey, { version: updateVersion })}</Title>
                    <Link url={CLOINK_RELEASES}>
                        {t("update.card.whatsNew")}
                    </Link>
                </div>
                {enforced ? (
                    <Button variant={"primary"} size={"xs"} onClick={triggerUpdate}>
                        {t("update.card.installNow")}
                    </Button>
                ) : (
                    <Button variant={"primary"} size={"xs"} onClick={openInstallerDownload}>
                        <DownloadIcon size={14} />
                        {t("update.card.getInstaller")}
                    </Button>
                )}
            </Card>
        );
    }

    return (
        <Card className={"max-w-lg"}>
            <div>
                <Title>{t("update.card.onLatestVersion")}</Title>
                <p className={"text-sm text-nb-gray-300"}>{t("update.card.autoCheckInterval")}</p>
            </div>
            <Button variant={"primary"} size={"xs"} onClick={() => openUrl(CLOINK_RELEASES)}>
                <NotepadText size={14} />
                {t("update.card.changelog")}
            </Button>
        </Card>
    );
}

function Card({ children, className }: Readonly<{ children: ReactNode; className?: string }>) {
    return (
        <div
            className={cn(
                "flex w-full items-center justify-between gap-4 rounded-md border border-nb-gray-800 bg-nb-gray-910 px-4 py-3",
                className,
            )}
        >
            {children}
        </div>
    );
}

function Title({ children }: Readonly<{ children: ReactNode }>) {
    return <p className={"text-sm font-semibold"}>{children}</p>;
}

function Link({ url, children }: Readonly<{ url: string; children: ReactNode }>) {
    return (
        <button
            type={"button"}
            onClick={() => openUrl(url)}
            className={
                "text-sm font-medium text-netbird hover:underline hover:decoration-[0.5px] hover:underline-offset-4"
            }
        >
            {children}
        </button>
    );
}
