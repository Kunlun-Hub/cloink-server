# Self-hosted runtime network policy

Cloink does not contact public announcement, release-check, or telemetry
services by default. The following endpoints are opt-in and have no public
fallback:

| Variable | Default | Expected response |
| --- | --- | --- |
| `NB_VERSION_CHECK_URL` | empty | Plain-text client/server version. Enables the shared update checker. |
| `NB_MANAGEMENT_VERSION_URL` | empty | Plain-text management version for `/api/instance/version`. |
| `NB_DASHBOARD_VERSION_URL` | empty | GitHub-release-compatible JSON for `/api/instance/version`. |
| `NB_ANONYMOUS_METRICS_ENDPOINT` | empty | Management metrics API base URL. The worker is also disabled by default. |
| `NB_METRICS_CONFIG_URL` | empty | Client metrics configuration URL. |
| `NB_METRICS_SERVER_URL` | empty | Client metrics ingest URL. |

Management anonymous metrics default to disabled in the standalone command,
the combined server, example configuration, and installation environment.
Client metric push additionally requires `NB_METRICS_PUSH_ENABLED=true`; a
management response cannot enable it on an unmanaged client.

The following upstream runtime services remain enabled intentionally:

- GeoIP database downloads from `pkgs.netbird.io` when the geolocation module
  needs or updates its database.
- Debug bundle uploads through `upload.debug.netbird.io` when a user chooses
  to upload a diagnostic bundle.
- Installer and client package links invoked by a user or by an explicitly
  configured updater.

The self-hosted browser-client image is documented in
`client/wasm/SELF_HOSTING.md`.

