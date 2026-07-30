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

resolve_tag_commit() {
  local tag_json="$WORK_DIR/tag.json"
  local tag_error="$WORK_DIR/tag.error"
  local tag_type

  if ! "$GH_CLI" api "repos/$GH_REPO/git/ref/tags/$TAG" > "$tag_json" 2> "$tag_error"; then
    if grep -Fq 'HTTP 404' "$tag_error"; then
      TAG_PRESENT=false
      TAG_SHA=""
      return
    fi
    echo "Could not inspect tag $TAG" >&2
    sed -n '1,3p' "$tag_error" >&2
    exit 1
  fi

  TAG_PRESENT=true
  tag_type="$("$JQ_BIN" -r '.object.type // empty' "$tag_json")"
  TAG_SHA="$("$JQ_BIN" -r '.object.sha // empty' "$tag_json")"

  while [ "$tag_type" = "tag" ]; do
    "$GH_CLI" api "repos/$GH_REPO/git/tags/$TAG_SHA" > "$tag_json"
    tag_type="$("$JQ_BIN" -r '.object.type // empty' "$tag_json")"
    TAG_SHA="$("$JQ_BIN" -r '.object.sha // empty' "$tag_json")"
  done

  if [ "$tag_type" != "commit" ] || [[ ! "$TAG_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    echo "Release tag $TAG does not resolve to a commit" >&2
    exit 1
  fi
}

if ! "$GH_CLI" api "repos/$GH_REPO/releases/tags/$TAG" > "$RELEASE_JSON" 2> "$RELEASE_ERROR"; then
  if grep -Fq 'HTTP 404' "$RELEASE_ERROR"; then
    resolve_tag_commit
    if [ "$TAG_PRESENT" = "true" ]; then
      : "${EXPECTED_NEW_TARGET:?EXPECTED_NEW_TARGET is required when a release tag already exists}"
      if [[ ! "$EXPECTED_NEW_TARGET" =~ ^[0-9a-f]{40}$ ]] || [ "$TAG_SHA" != "$EXPECTED_NEW_TARGET" ]; then
        echo "Existing tag $TAG does not match the intended new release commit" >&2
        exit 1
      fi
    fi
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

resolve_tag_commit
[ "$TAG_PRESENT" = "true" ] || {
  echo "Release $TAG exists without its Git tag" >&2
  exit 1
}
[ "$release_target" = "$TAG_SHA" ] || {
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
