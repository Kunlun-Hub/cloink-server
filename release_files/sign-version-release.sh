#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: $0 PRIVATE_KEY VERSION PLATFORM ARCHITECTURE CHANNEL SHA256" >&2
  exit 2
fi

private_key=$1
version=${2#v}
platform=$(printf '%s' "$3" | tr '[:upper:]' '[:lower:]')
architecture=$(printf '%s' "$4" | tr '[:upper:]' '[:lower:]')
channel=$(printf '%s' "$5" | tr '[:upper:]' '[:lower:]')
sha256=$(printf '%s' "$6" | tr '[:upper:]' '[:lower:]')

if [[ ! -f "$private_key" ]]; then
  echo "private key not found: $private_key" >&2
  exit 2
fi
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid version: $version" >&2
  exit 2
fi
if [[ ! "$platform" =~ ^(windows|macos|linux|android)$ ]]; then
  echo "invalid platform: $platform" >&2
  exit 2
fi
if [[ ! "$architecture" =~ ^(amd64|arm64|armv7|universal)$ ]]; then
  echo "invalid architecture: $architecture" >&2
  exit 2
fi
if [[ ! "$channel" =~ ^[a-z0-9][a-z0-9._-]{0,31}$ ]]; then
  echo "invalid channel: $channel" >&2
  exit 2
fi
if [[ ! "$sha256" =~ ^[a-f0-9]{64}$ ]]; then
  echo "invalid SHA256: $sha256" >&2
  exit 2
fi

payload=$(mktemp)
trap 'rm -f "$payload"' EXIT
printf 'cloink-release-v1\nversion=%s\nplatform=%s\narchitecture=%s\nchannel=%s\nsha256=%s\n' \
  "$version" "$platform" "$architecture" "$channel" "$sha256" > "$payload"

openssl pkeyutl -sign -rawin -inkey "$private_key" -in "$payload" | base64 -w0
printf '\n'
