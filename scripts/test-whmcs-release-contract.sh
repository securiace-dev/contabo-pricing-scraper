#!/usr/bin/env bash
#
# Validate changelog extraction and both installable WHMCS release artifacts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/securiace-release-contract.XXXXXX")"

cleanup() {
  rm -rf "$FIXTURE_DIR"
}
trap cleanup EXIT

fail() {
  echo "Release contract failed: $*" >&2
  exit 1
}

cat > "$FIXTURE_DIR/CHANGELOG.md" <<'EOF'
## contabo_vps 1.0.0 — legacy server

Legacy server notes.

## 1x0x0 — regex collision

Must never match semantic version 1.0.0.

## 1.0.0 — addon

Exact addon notes.

## securiacevps 2.0.0 — canonical suite

Exact suite notes.
EOF

addon_notes="$("$SCRIPT_DIR/extract-changelog-section.sh" "$FIXTURE_DIR/CHANGELOG.md" "" "1.0.0")"
case "$addon_notes" in
  *"Exact addon notes."*) ;;
  *) fail "literal addon changelog section was not selected" ;;
esac
case "$addon_notes" in
  *"Legacy server notes."* | *"regex collision"*) fail "changelog namespaces crossed" ;;
esac

suite_notes="$("$SCRIPT_DIR/extract-changelog-section.sh" "$FIXTURE_DIR/CHANGELOG.md" "securiacevps " "2.0.0")"
case "$suite_notes" in
  *"Exact suite notes."*) ;;
  *) fail "canonical suite changelog section was not selected" ;;
esac

if "$SCRIPT_DIR/extract-changelog-section.sh" "$FIXTURE_DIR/CHANGELOG.md" "" "9.9.9" >/dev/null 2>&1; then
  fail "missing changelog heading did not fail closed"
fi
if "$SCRIPT_DIR/extract-changelog-section.sh" "$FIXTURE_DIR/CHANGELOG.md" "" "1x0x0" >/dev/null 2>&1; then
  fail "invalid semantic version did not fail closed"
fi

cat >> "$FIXTURE_DIR/CHANGELOG.md" <<'EOF'

## 1.0.0 — duplicate addon
EOF
if "$SCRIPT_DIR/extract-changelog-section.sh" "$FIXTURE_DIR/CHANGELOG.md" "" "1.0.0" >/dev/null 2>&1; then
  fail "duplicate changelog headings did not fail closed"
fi
if bash "$SCRIPT_DIR/package-whmcs-suite.sh" --invalid >/dev/null 2>&1; then
  fail "invalid package mode did not fail closed"
fi

PRICING_WORKFLOW="$REPO_ROOT/.github/workflows/release-contabo-pricing.yml"
SUITE_WORKFLOW="$REPO_ROOT/.github/workflows/release-contabo-vps.yml"
for required in \
  "whmcs-module/modules/addons/contabo_pricing/**" \
  "scripts/package-whmcs-suite.sh" \
  "scripts/extract-changelog-section.sh"; do
  grep -Fq "$required" "$PRICING_WORKFLOW" ||
    fail "pricing workflow does not watch required release input: $required"
done
for required in \
  "whmcs-module/modules/servers/securiacevps/**" \
  "whmcs-module/modules/servers/contabo_vps/**" \
  "whmcs-module/templates/orderforms/securiace-vps/**" \
  "scripts/package-whmcs-suite.sh" \
  "scripts/extract-changelog-section.sh"; do
  grep -Fq "$required" "$SUITE_WORKFLOW" ||
    fail "suite workflow does not watch required release input: $required"
done

cd "$REPO_ROOT"
bash scripts/package-whmcs-suite.sh --addon >/dev/null
bash scripts/package-whmcs-suite.sh --suite >/dev/null
bash scripts/package-whmcs-suite.sh --all >/dev/null

ADDON_VERSION="$(sed -n "s/.*const VERSION = '\\([^']*\\)'.*/\\1/p" \
  whmcs-module/modules/addons/contabo_pricing/lib/AdminController.php | head -n 1)"
SUITE_VERSION="$(sed -n "s/.*SECURIACE_VPS_VERSION', '\\([^']*\\)'.*/\\1/p" \
  whmcs-module/modules/servers/securiacevps/securiacevps.php | head -n 1)"
ADDON_ZIP="dist/contabo_pricing-v${ADDON_VERSION}.zip"
SUITE_ZIP="dist/securiacevps-v${SUITE_VERSION}.zip"

for archive in "$ADDON_ZIP" "$SUITE_ZIP"; do
  [ -s "$archive" ] || fail "missing archive $archive"
  [ -s "$archive.sha256" ] || fail "missing checksum $archive.sha256"
  [ -s "$archive.manifest" ] || fail "missing manifest $archive.manifest"
  checksum_target="$(awk 'NR == 1 {print $2}' "$archive.sha256")"
  [ "$checksum_target" = "$(basename "$archive")" ] ||
    fail "checksum must contain only the archive basename: $archive.sha256"
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$archive")" && sha256sum -c "$(basename "$archive").sha256" >/dev/null)
  else
    (cd "$(dirname "$archive")" && shasum -a 256 -c "$(basename "$archive").sha256" >/dev/null)
  fi
  unzip -t "$archive" >/dev/null
done

grep -Fxq 'modules/addons/contabo_pricing/contabo_pricing.php' "$ADDON_ZIP.manifest" ||
  fail "addon entrypoint missing from manifest"
grep -Fxq 'modules/addons/contabo_pricing/lib/AdminController.php' "$ADDON_ZIP.manifest" ||
  fail "addon runtime missing from manifest"
grep -Fxq 'modules/addons/contabo_pricing/templates/admin/dashboard.tpl' "$ADDON_ZIP.manifest" ||
  fail "addon template missing from manifest"
grep -Fxq 'modules/servers/securiacevps/securiacevps.php' "$SUITE_ZIP.manifest" ||
  fail "canonical module missing from suite"
grep -Fxq 'modules/servers/contabo_vps/contabo_vps.php' "$SUITE_ZIP.manifest" ||
  fail "compatibility shim missing from suite"
grep -Fxq 'templates/orderforms/securiace-vps/theme.yaml' "$SUITE_ZIP.manifest" ||
  fail "VPS order-form child missing from suite"

for manifest in "$ADDON_ZIP.manifest" "$SUITE_ZIP.manifest"; do
  if grep -Eq \
    '(^|/)(tests?|vendor|docs|graphify-out|\.git|\.claude-flow)(/|$)|(^|/)CHANGELOG\.md$|phpunit|composer\.lock|\.phpunit' \
    "$manifest"; then
    fail "development-only content present in $manifest"
  fi
done

echo "WHMCS release contract: PASS"
