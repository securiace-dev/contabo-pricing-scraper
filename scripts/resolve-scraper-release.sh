#!/usr/bin/env bash
set -euo pipefail

readonly required_binary="contabo-scraper-linux-x86_64"
readonly required_checksum="SHA256SUMS.txt"
readonly requested_version="${1:-latest}"
readonly scan_limit="${SCRAPER_RELEASE_SCAN_LIMIT:-100}"

if ! [[ "$scan_limit" =~ ^[1-9][0-9]*$ ]]; then
  echo "SCRAPER_RELEASE_SCAN_LIMIT must be a positive integer" >&2
  exit 2
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is required to resolve scraper releases" >&2
  exit 2
fi

repo_args=()
if [[ -n "${GH_REPO:-}" ]]; then
  repo_args=(--repo "$GH_REPO")
fi

has_required_assets() {
  local tag="$1"
  local assets

  if ! assets="$(
    gh release view "$tag" "${repo_args[@]}" \
      --json assets \
      --jq '.assets[].name'
  )"; then
    return 1
  fi

  grep -Fxq "$required_binary" <<<"$assets" &&
    grep -Fxq "$required_checksum" <<<"$assets"
}

if [[ "$requested_version" != "latest" ]]; then
  if ! has_required_assets "$requested_version"; then
    echo "Release $requested_version lacks required scraper assets" >&2
    exit 1
  fi
  printf '%s\n' "$requested_version"
  exit 0
fi

while IFS= read -r tag; do
  if ! [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
    continue
  fi
  if has_required_assets "$tag"; then
    printf '%s\n' "$tag"
    exit 0
  fi
done < <(
  gh release list "${repo_args[@]}" \
    --limit "$scan_limit" \
    --json tagName,isDraft,isPrerelease,publishedAt \
    --jq '.[] | select((.isDraft | not) and (.isPrerelease | not)) | .tagName'
)

echo "No published v* scraper release contains both required assets" >&2
exit 1
