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

  // Proxy error messages (sent as i18n keys from Go backend)
  "proxyError.serviceNotFound": "Service Not Found",
  "proxyError.serviceNotFoundMessage": "The requested service could not be found. Please check the URL, try refreshing, or check if the peer is running. If that doesn't work, see our documentation for help.",
  "proxyError.loopDetected": "Loop Detected",
  "proxyError.loopDetectedMessage": "This peer is the target of the requested service. Reach the backend directly instead of dialing the public service URL from the same machine.",
  "proxyError.requestTimeout": "Request Timeout",
  "proxyError.requestTimeoutMessage": "The request timed out while trying to reach the service. Please refresh the page and try again.",
  "proxyError.requestCanceled": "Request Canceled",
  "proxyError.requestCanceledMessage": "The request was canceled before it could be completed. Please refresh the page and try again.",
  "proxyError.configurationError": "Configuration Error",
  "proxyError.configurationErrorMessage": "The request could not be processed due to a configuration issue. Please refresh the page and try again.",
  "proxyError.proxyNotConnected": "Proxy Not Connected",
  "proxyError.proxyNotConnectedMessage": "The proxy is not connected to the NetBird network. Please try again later or contact your administrator.",
  "proxyError.serviceOverloaded": "Service Overloaded",
  "proxyError.serviceOverloadedMessage": "The service is currently handling too many requests. Please try again shortly.",
  "proxyError.serviceUnavailable": "Service Unavailable",
  "proxyError.serviceUnavailableMessage": "The connection to the service was refused. Please verify that the service is running and try again.",
  "proxyError.peerNotConnected": "Peer Not Connected",
  "proxyError.peerNotConnectedMessage": "The connection to the peer could not be established. Please ensure the peer is running and connected to the NetBird network.",
  "proxyError.connectionError": "Connection Error",
  "proxyError.connectionErrorMessage": "An unexpected error occurred while connecting to the service. Please try again later.",

  // Access denied messages
  "proxyError.accessDenied": "Access Denied",
  "proxyError.accessDeniedMessage": "You are not authorized to access this service",
  "proxyError.authError": "An error occurred during authentication",
  "proxyError.accountPendingApproval": "Your account is pending approval by an administrator",
  "proxyError.accountBlocked": "Your account is blocked",
  "proxyError.serviceConfigError": "Service configuration error",
  "proxyError.invalidSessionToken": "invalid session token",
  "proxyError.authServiceUnavailable": "authentication service unavailable",
};

export type TranslationKey = keyof typeof en;
export type Translations = Record<TranslationKey, string>;
export default en;
