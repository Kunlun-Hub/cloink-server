// Deployment endpoints for the desktop UI.
//
// Environment-specific builds bake their own deployment in at build time
// through VITE_CLOINK_MANAGEMENT_URL and VITE_CLOINK_RELEASE_API_URL; every
// other build keeps the general defaults below, so this file is the single
// place that knows where the client's own deployment lives.
export const MANAGEMENT_URL =
    import.meta.env.VITE_CLOINK_MANAGEMENT_URL ?? "https://cloink.4w.ink:443";

export const RELEASE_API_URL =
    import.meta.env.VITE_CLOINK_RELEASE_API_URL ??
    "https://cloink.4w.ink/api/version-releases/public";

export const STABLE_RELEASES_URL = `${RELEASE_API_URL}?channel=stable&latest=true`;
export const RC_RELEASES_URL = `${RELEASE_API_URL}?channel=rc&latest=true`;
