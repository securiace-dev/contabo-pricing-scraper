#!/usr/bin/env bash
#
# Build reviewable WHMCS runtime archives. This script is local-only: it does
# not access WHMCS, Contabo, the Rust API, SSH, or any deployment target.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ADDON_DIR="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"
MODULE_DIR="$REPO_ROOT/whmcs-module/modules/servers/securiacevps"
SHIM_DIR="$REPO_ROOT/whmcs-module/modules/servers/contabo_vps"
ORDERFORM_DIR="$REPO_ROOT/whmcs-module/templates/orderforms/securiace-vps"
DIST_DIR="$REPO_ROOT/dist"
STAGE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/securiace-whmcs-package.XXXXXX")"

cleanup() {
  rm -rf "$STAGE_DIR"
}
trap cleanup EXIT

read_addon_version() {
  sed -n "s/.*const VERSION = '\\([^']*\\)'.*/\\1/p" \
    "$ADDON_DIR/lib/AdminController.php" | head -n 1
}

read_module_version() {
  sed -n "s/.*SECURIACE_VPS_VERSION', '\\([^']*\\)'.*/\\1/p" \
    "$MODULE_DIR/securiacevps.php" | head -n 1
}

copy_runtime_tree() {
  local source_dir="$1"
  local target_dir="$2"
  mkdir -p "$target_dir"
  tar -C "$source_dir" \
    --exclude='./vendor' \
    --exclude='./tests' \
    --exclude='./docs' \
    --exclude='./.phpunit.cache' \
    --exclude='./.phpunit.result.cache' \
    --exclude='./phpunit.xml' \
    --exclude='./composer.lock' \
    --exclude='./.git*' \
    --exclude='./.claude-flow' \
    --exclude='./graphify-out' \
    -cf - . | tar -C "$target_dir" -xf -
}

checksum() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file"
  else
    shasum -a 256 "$file"
  fi
}

ADDON_VERSION="$(read_addon_version)"
MODULE_VERSION="$(read_module_version)"
[ -n "$ADDON_VERSION" ] || { echo "Could not resolve addon version" >&2; exit 1; }
[ -n "$MODULE_VERSION" ] || { echo "Could not resolve module version" >&2; exit 1; }

mkdir -p "$DIST_DIR"

ADDON_STAGE="$STAGE_DIR/addon"
copy_runtime_tree "$ADDON_DIR" "$ADDON_STAGE/modules/addons/contabo_pricing"
ADDON_ZIP="$DIST_DIR/contabo_pricing-v${ADDON_VERSION}.zip"
ADDON_TMP="$STAGE_DIR/contabo_pricing-v${ADDON_VERSION}.zip"
(cd "$ADDON_STAGE" && zip -q -r "$ADDON_TMP" modules)
mv "$ADDON_TMP" "$ADDON_ZIP"
checksum "$ADDON_ZIP" > "$ADDON_ZIP.sha256"
unzip -Z1 "$ADDON_ZIP" | LC_ALL=C sort > "$ADDON_ZIP.manifest"

SUITE_STAGE="$STAGE_DIR/suite"
copy_runtime_tree "$MODULE_DIR" "$SUITE_STAGE/modules/servers/securiacevps"
copy_runtime_tree "$SHIM_DIR" "$SUITE_STAGE/modules/servers/contabo_vps"
copy_runtime_tree "$ORDERFORM_DIR" "$SUITE_STAGE/templates/orderforms/securiace-vps"
SUITE_ZIP="$DIST_DIR/securiacevps-v${MODULE_VERSION}.zip"
SUITE_TMP="$STAGE_DIR/securiacevps-v${MODULE_VERSION}.zip"
(cd "$SUITE_STAGE" && zip -q -r "$SUITE_TMP" modules templates)
mv "$SUITE_TMP" "$SUITE_ZIP"
checksum "$SUITE_ZIP" > "$SUITE_ZIP.sha256"
unzip -Z1 "$SUITE_ZIP" | LC_ALL=C sort > "$SUITE_ZIP.manifest"

for archive in "$ADDON_ZIP" "$SUITE_ZIP"; do
  if unzip -Z1 "$archive" | grep -Eq \
    '(^|/)(tests?|vendor|graphify-out|\.git|\.claude-flow)(/|$)|phpunit|composer\.lock|\.phpunit'; then
    echo "Forbidden development file found in $archive" >&2
    exit 1
  fi
done

echo "Built local release artifacts:"
echo "  $ADDON_ZIP"
echo "  $SUITE_ZIP"
