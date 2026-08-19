const en = {
  // Page titles
  "auth.pageTitle": "Authentication Required - NetBird Service",
  "error.pageTitle": "{title} - NetBird Service",

  // App (Authentication page)
  "auth.title": "Authentication Required",
  "auth.description": "The service you are trying to access is protected. Please authenticate to continue.",
  "auth.signInWithSso": "Sign in with SSO",
  "auth.password": "Password",
  "auth.pin": "PIN",
  "auth.enterPassword": "Enter password",
  "auth.enterPinCode": "Enter PIN Code",
  "auth.signIn": "Sign in",
  "auth.submit": "Submit",
  "auth.verifying": "Verifying...",
  "auth.authenticated": "Authenticated",
  "auth.loadingService": "Loading service...",
  "auth.failed": "Authentication failed. Please try again.",
  "auth.error": "An error occurred. Please try again.",

  // Error page
  "error.code": "Error {code}",
  "error.you": "You",
  "error.proxy": "Proxy",
  "error.destination": "Destination",
  "error.connected": "Connected",
  "error.unreachable": "Unreachable",
  "error.refreshPage": "Refresh Page",
  "error.documentation": "Documentation",
  "error.requestId": "REQUEST-ID",
  "error.timestamp": "TIMESTAMP",

  // Common
  "common.poweredBy": "Powered by",
};

export type TranslationKey = keyof typeof en;
export type Translations = Record<TranslationKey, string>;
export default en;
