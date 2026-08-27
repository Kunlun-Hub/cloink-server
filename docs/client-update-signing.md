# Cloink client update signing

Cloink update prompts accept only release metadata signed by the dedicated
Ed25519 update key. This signature is separate from Android APK signing,
Windows Authenticode, and Apple Developer ID signing.

## Key custody

- Keep the private key in the `CLOINK_UPDATE_SIGNING_KEY` GitHub Actions secret
  and mount it read-only into the management container when automatic release
  signing is enabled. Never store it in the release database, container image,
  or source repository.
- Back up the private key and its recovery instructions in a second protected
  location. Losing it prevents existing clients from trusting future updates.
- The client embeds only the public key. Its DER SHA256 fingerprint is:

  ```text
  4e304fa3373a78c3cc482bdd4edd533b26bbc1b808e9ef7399294c287ad89ff5
  ```

## Automatic signing

Set `NB_VERSION_RELEASE_SIGNING_KEY_FILE` in the management container to the
path of the PEM Ed25519 private key. The file should be mounted read-only and
accessible only to the management process. When a release has a SHA256, the
management service signs its normalized metadata automatically. The dashboard
does not request or transmit a signature.

Existing API integrations may still submit a manual signature when automatic
signing is not configured. A latest release always requires either automatic
signing or a valid manual signature so existing clients never receive metadata
they will reject.

## Manual signing

Build the final artifact first and calculate its SHA256. Sign the exact
platform, architecture, channel, version, and checksum that will be entered in
Settings > Version releases:

```bash
sha256=$(sha256sum cloink-installer.exe | awk '{print $1}')
signature=$(release_files/sign-version-release.sh \
  /secure/path/ed25519-private.pem \
  0.77.3 windows amd64 stable "$sha256")
```

Paste `signature` into the Ed25519 signature field. Upload the same artifact,
confirm the server-calculated SHA256 matches, and only then mark it as the
latest release.

The signed payload format is:

```text
cloink-release-v1
version=0.77.3
platform=windows
architecture=amd64
channel=stable
sha256=<64 lowercase hexadecimal characters>
```

Clients verify the metadata signature, download the artifact from the release
URL, verify its SHA256, and prompt the user. Management directives cannot
enable silent installation.

## Rotation

Key rotation requires a transition client that trusts both the old and new
public keys. Release that client with metadata signed by the old key before
using the new key for any latest release. Do not replace the embedded public
key and immediately sign only with the new private key.
