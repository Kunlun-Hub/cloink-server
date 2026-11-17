#!/usr/bin/env bash
#
# Resolve the release version embedded in Cloink binaries and container images.
#
# Priority:
#   1. An explicit version supplied by workflow_dispatch.
#   2. The exact tag for tag-triggered builds.
#   3. The nearest reachable vX.Y.Z tag.
#
# The upstream repository is fetched because fork checkouts do not necessarily
# contain the upstream release tags. The version is printed without the "v"
# prefix, matching goreleaser's {{.Version}} value.
set -euo pipefail

upstream_repo="${1:-}"
requested_version="${2:-}"

version="$requested_version"
if [[ -z "$version" && "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  version="${GITHUB_REF_NAME:-}"
fi

if [[ -z "$version" ]]; then
  if [[ -n "$upstream_repo" ]]; then
    if ! git fetch --force --no-tags "$upstream_repo" \
      'refs/tags/v*:refs/tags/v*' >&2; then
      echo "error: failed to fetch release tags from $upstream_repo" >&2
      exit 1
    fi
  fi
  if ! version="$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null)"; then
    echo "error: no vX.Y.Z release tag is reachable from HEAD" >&2
    exit 1
  fi
fi

printf '%s\n' "${version#v}"
