import { useState, useRef, useEffect } from "react";
import {Loader2, Lock, Binary, LogIn} from "lucide-react";
import { getData, type Data } from "@/data";
import Button from "@/components/Button";
import { Input } from "@/components/Input";
import PinCodeInput, { type PinCodeInputRef } from "@/components/PinCodeInput";
import { SegmentedTabs } from "@/components/SegmentedTabs";
import { PoweredByNetBird } from "@/components/PoweredByNetBird";
import { Card } from "@/components/Card";
import { Title } from "@/components/Title";
import { Description } from "@/components/Description";
import { Separator } from "@/components/Separator";
import { ErrorMessage } from "@/components/ErrorMessage";
import { Label } from "@/components/Label";
import { useI18n } from "@/i18n/I18nProvider";

const data = getData();

// For testing, show all methods if none are configured
const methods: NonNullable<Data["methods"]> =
  data.methods && Object.keys(data.methods).length > 0
    ? data.methods
    : {  password:"password", pin: "pin", oidc: "/auth/oidc" };

function App() {
  const { t } = useI18n();

  useEffect(() => {
    document.title = t("auth.pageTitle");
  }, [t]);

  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<string | null>(null);
  const [pin, setPin] = useState("");
  const [password, setPassword] = useState("");
  const passwordRef = useRef<HTMLInputElement>(null);
  const pinRef = useRef<PinCodeInputRef>(null);
  const [activeTab, setActiveTab] = useState<"password" | "pin">(
      methods.password ? "password" : "pin"
  );

  const handleAuthError = (method: "password" | "pin", message: string) => {
    setError(message);
    setSubmitting(null);
    if (method === "password") {
      setPassword("");
      setTimeout(() => passwordRef.current?.focus(), 200);
    } else {
      setPin("");
      setTimeout(() => pinRef.current?.focus(), 200);
    }
  };

  const submitCredentials = (method: "password" | "pin", value: string) => {
    setError(null);
    setSubmitting(method);

    const formData = new FormData();
    if (method === "password") {
      formData.append(methods.password!, value);
    } else {
      formData.append(methods.pin!, value);
    }

    fetch(globalThis.location.href, {
      method: "POST",
      body: formData,
      redirect: "manual",
    })
      .then((res) => {
        if (res.type === "opaqueredirect" || res.status === 0) {
          setSubmitting("redirect");
          globalThis.location.reload();
        } else {
          handleAuthError(method, t("auth.failed"));
        }
      })
      .catch(() => {
        handleAuthError(method, t("auth.error"));
      });
  };

  const handlePinChange = (value: string) => {
    setPin(value);
    if (value.length === 6) {
      submitCredentials("pin", value);
    }
  };

  const isPinComplete = pin.length === 6;
  const isPasswordEntered = password.length > 0;
  const isButtonDisabled = submitting !== null ||
    (activeTab === "password" && !isPasswordEntered) ||
    (activeTab === "pin" && !isPinComplete);

  const hasCredentialAuth = methods.password || methods.pin;
  const hasBothCredentials = methods.password && methods.pin;
  const buttonLabel = activeTab === "password" ? t("auth.signIn") : t("auth.submit");

  if (submitting === "redirect") {
    return (
      <main className="mt-20">
        <Card className="max-w-105 mx-auto">
          <Title>{t("auth.authenticated")}</Title>
          <Description>{t("auth.loadingService")}</Description>
          <div className="flex justify-center mt-7">
            <Loader2 className="animate-spin" size={24} />
          </div>
        </Card>
        <PoweredByNetBird />
      </main>
    );
  }

  return (
    <main className="mt-20">
      <Card className="max-w-105 mx-auto">
        <Title>{t("auth.title")}</Title>
        <Description>
          {t("auth.description")}
        </Description>

          <div className="flex flex-col gap-4 mt-7 z-10 relative">
            {error && <ErrorMessage error={error} />}

            {/* SSO Button */}
            {methods.oidc && (
              <Button
                variant="primary"
                className="w-full"
                onClick={() => { globalThis.location.href = methods.oidc!; }}
              >
                <LogIn size={16} />
                {t("auth.signInWithSso")}
              </Button>
            )}

            {/* Separator */}
            {methods.oidc && hasCredentialAuth && <Separator />}

            {/* Credential Authentication */}
            {hasCredentialAuth && (
              <form onSubmit={(e) => {
                e.preventDefault();
                submitCredentials(activeTab, activeTab === "password" ? password : pin);
              }}>
                {hasBothCredentials && (
                  <SegmentedTabs
                    value={activeTab}
                    onChange={(v) => {
                      setActiveTab(v as "password" | "pin");
                      setTimeout(() => {
                        if (v === "password") {
                          passwordRef.current?.focus();
                        } else {
                          pinRef.current?.focus();
                        }
                      }, 0);
                    }}
                  >
                    <SegmentedTabs.List className="rounded-lg border mb-4">
                      <SegmentedTabs.Trigger value="password">
                        <Lock size={14} />
                        {t("auth.password")}
                      </SegmentedTabs.Trigger>
                      <SegmentedTabs.Trigger value="pin">
                        <Binary size={14} />
                        {t("auth.pin")}
                      </SegmentedTabs.Trigger>
                    </SegmentedTabs.List>
                  </SegmentedTabs>
                )}

                <div className="mb-4">
                  {methods.password && (activeTab === "password" || !methods.pin) && (
                    <>
                      {!hasBothCredentials && <Label htmlFor="password">{t("auth.password")}</Label>}
                      <Input
                        ref={passwordRef}
                        type="password"
                        id="password"
                        placeholder={t("auth.enterPassword")}
                        disabled={submitting !== null}
                        showPasswordToggle
                        autoFocus
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                      />
                    </>
                  )}
                  {methods.pin && (activeTab === "pin" || !methods.password) && (
                    <>
                      {!hasBothCredentials && <Label htmlFor="pin-0">{t("auth.enterPinCode")}</Label>}
                      <PinCodeInput
                        ref={pinRef}
                        value={pin}
                        onChange={handlePinChange}
                        disabled={submitting !== null}
                        autoFocus={!methods.password}
                      />
                    </>
                  )}
                </div>

                <Button
                  type="submit"
                  disabled={isButtonDisabled}
                  variant="secondary"
                  className="w-full"
                >
                  {submitting === null ? (
                    buttonLabel
                  ) : (
                    <>
                      <Loader2 className="animate-spin" size={16} />
                      {t("auth.verifying")}
                    </>
                  )}
                </Button>
              </form>
            )}
          </div>
        </Card>

      <PoweredByNetBird />
    </main>
  );
}

export default App;
