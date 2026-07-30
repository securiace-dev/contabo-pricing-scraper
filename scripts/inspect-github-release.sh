#!/usr/bin/env bash
#
# Classify a GitHub release as absent or complete. Any existing draft,
# prerelease, target mismatch, tag mismatch, or missing asset fails closed.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <tag> <required-asset> [required-asset ...]" >&2
  exit 2
fi

: "${GH_REPO:?GH_REPO must name the owner/repository}"
: "${GH_TOKEN:?GH_TOKEN must be set for GitHub API access}"

TAG="$1"
shift
GH_CLI="${GH_CLI:-gh}"
JQ_BIN="${JQ_BIN:-jq}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/securiace-release-inspect.XXXXXX")"

cleanup() {
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

RELEASE_JSON="$WORK_DIR/release.json"
RELEASE_ERROR="$WORK_DIR/release.error"

if ! "$GH_CLI" api "repos/$GH_REPO/releases/tags/$TAG" > "$RELEASE_JSON" 2> "$RELEASE_ERROR"; then
  if grep -Fq 'HTTP 404' "$RELEASE_ERROR"; then
    echo "absent"
    exit 0
  fi
  echo "Could not inspect release $TAG" >&2
  sed -n '1,3p' "$RELEASE_ERROR" >&2
  exit 1
fi

release_tag="$("$JQ_BIN" -r '.tag_name // empty' "$RELEASE_JSON")"
release_target="$("$JQ_BIN" -r '.target_commitish // empty' "$RELEASE_JSON")"
release_draft="$("$JQ_BIN" -r '.draft // false' "$RELEASE_JSON")"
release_prerelease="$("$JQ_BIN" -r '.prerelease // false' "$RELEASE_JSON")"

[ "$release_tag" = "$TAG" ] || {
  echo "Release tag mismatch for $TAG" >&2
  exit 1
}
[ "$release_draft" = "false" ] || {
  echo "Release $TAG is still a draft and requires operator recovery" >&2
  exit 1
}
[ "$release_prerelease" = "false" ] || {
  echo "Release $TAG is unexpectedly marked as a prerelease" >&2
  exit 1
}

TAG_JSON="$WORK_DIR/tag.json"
"$GH_CLI" api "repos/$GH_REPO/git/ref/tags/$TAG" > "$TAG_JSON"
tag_type="$("$JQ_BIN" -r '.object.type // empty' "$TAG_JSON")"
tag_sha="$("$JQ_BIN" -r '.object.sha // empty' "$TAG_JSON")"

while [ "$tag_type" = "tag" ]; do
  "$GH_CLI" api "repos/$GH_REPO/git/tags/$tag_sha" > "$TAG_JSON"
  tag_type="$("$JQ_BIN" -r '.object.type // empty' "$TAG_JSON")"
  tag_sha="$("$JQ_BIN" -r '.object.sha // empty' "$TAG_JSON")"
done

[ "$tag_type" = "commit" ] && [[ "$tag_sha" =~ ^[0-9a-f]{40}$ ]] || {
  echo "Release tag $TAG does not resolve to a commit" >&2
  exit 1
}
[ "$release_target" = "$tag_sha" ] || {
  echo "Release $TAG target does not match its tag commit" >&2
  exit 1
}

for asset in "$@"; do
  # shellcheck disable=SC2016 # jq expands $asset, not the shell.
  "$JQ_BIN" -e --arg asset "$asset" \
    '[.assets[]? | select(.name == $asset and .state == "uploaded")] | length == 1' \
    "$RELEASE_JSON" >/dev/null || {
      echo "Release $TAG is missing required uploaded asset: $asset" >&2
      exit 1
    }
done

echo "complete"
