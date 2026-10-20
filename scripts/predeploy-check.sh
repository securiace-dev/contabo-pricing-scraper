#!/usr/bin/env bash
#
# predeploy-check.sh — local release gate for the WHMCS-native VPS suite.
#
# Runs the full LOCAL verification battery, fail-closed. It NEVER touches
# production — only local files + the dockerised dev WHMCS (8.13 + 9.0). A prod
# release MUST NOT proceed unless this script exits 0 *without* smoke skips.
# See docs/DEPLOY_RUNBOOK.md and docs/operations/DOKPLOY_WHMCS_MODULE_DEPLOY.md.
#
# Stages (all run; gate fails if ANY stage fails):
#   1. Unit suite (PHPUnit, FakeCapsule) — addon.
#   2. Unit suite — canonical securiacevps module.
#   3. PHP 7.4 runtime/template syntax plus PHP 8.2 / 8.3 full-source syntax.
#   4. Hallmark static UI regression audit.
#   5. WHMCS changelog, checksum, manifest, and package contract.
#   6. Real-WHMCS migration/integration smoke in dev.
#   7. Read-only post-migration schema verification on WHMCS 8.13 + 9.0.
#
# Cloud / CI agents without a private whmcs-devbox checkout may set:
#   PREDEPLOY_ALLOW_MISSING_DEVBOX=1
# to SKIP stages 6–7 only when the checkout is absent. That yields a *partial*
# gate (exit 0) suitable for packaging dry-runs — NOT a production green light.
# Default remains fail-closed (missing devbox → GATE FAIL).
#
# Usage:  bash scripts/predeploy-check.sh
# Exit:   0 = gate PASS (or partial PASS with explicit skip flag);
#         non-zero = gate FAIL.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ADDON="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"
SERVER="$REPO_ROOT/whmcs-module/modules/servers/securiacevps"
SHIM="$REPO_ROOT/whmcs-module/modules/servers/contabo_vps"
RESOLVE="$SCRIPT_DIR/lib/resolve-whmcs-devbox.sh"

fail=0
skipped_smoke=0
declare -a results
record() { if [ "$2" -eq 0 ]; then results+=("[PASS] $1"); else results+=("[FAIL] $1"); fail=1; fi; }
record_skip() { results+=("[SKIP] $1"); skipped_smoke=1; }
stage()  { echo; echo "==================== $1 ===================="; }

devbox_available() {
  "$RESOLVE" >/dev/null 2>&1
}

# ── 1) addon unit suite ──────────────────────────────────────────────────────
stage "1/7 addon unit suite (phpunit)"
( cd "$ADDON" && vendor/bin/phpunit ); record "addon unit suite" $?

# ── 2) server-module unit suite ──────────────────────────────────────────────
stage "2/7 securiacevps server-module unit suite (phpunit)"
( cd "$SERVER" && "$ADDON/vendor/bin/phpunit" -c phpunit.xml ); record "server-module unit suite" $?

# ── 3) PHP syntax matrix ─────────────────────────────────────────────────────
stage "3/7 PHP 7.4 runtime/templates + PHP 8.2 / 8.3 full-source syntax lint"
lint_status=0
files=()
while IFS= read -r file; do
  files+=("$file")
done < <(
  find "$ADDON" "$SERVER" "$SHIM" "$SCRIPT_DIR" \
    -type f \( -name '*.php' -o -name '*.tpl' \) \
    -not -path '*/vendor/*' \
    -print | LC_ALL=C sort
)
rels=()
for file in "${files[@]}"; do
  rels+=("${file#"$REPO_ROOT"/}")
done
runtime_files=()
while IFS= read -r file; do
  runtime_files+=("$file")
done < <(
  find "$ADDON" "$SERVER" "$SHIM" "$SCRIPT_DIR" \
    -type f \( -name '*.php' -o -name '*.tpl' \) \
    -not -path '*/vendor/*' \
    -not -path '*/tests/*' \
    -print | LC_ALL=C sort
)
runtime_rels=()
for file in "${runtime_files[@]}"; do
  runtime_rels+=("${file#"$REPO_ROOT"/}")
done
for php_version in 7.4 8.2 8.3; do
  if docker image inspect "php:${php_version}-cli" >/dev/null 2>&1 ||
    docker pull "php:${php_version}-cli" >/dev/null 2>&1; then
    lint_rels=("${rels[@]}")
    if [ "$php_version" = "7.4" ]; then
      lint_rels=("${runtime_rels[@]}")
    fi
    docker run --rm -v "$REPO_ROOT":/app -w /app "php:${php_version}-cli" sh -c '
      st=0
      for f in "$@"; do
        php -l "$f" >/dev/null 2>&1 || { echo "  LINT FAIL: $f"; st=1; }
      done
      exit $st
    ' _ "${lint_rels[@]}" || lint_status=1
    echo "  linted with php:${php_version}-cli"
  else
    echo "  LINT MATRIX UNAVAILABLE: php:${php_version}-cli" >&2
    lint_status=1
  fi
done
record "PHP runtime/full-source lint matrix" "$lint_status"

# ── 4) Hallmark static UI audit ──────────────────────────────────────────────
stage "4/7 Hallmark static UI regression audit"
ruby "$SCRIPT_DIR/hallmark-audit.rb"; record "Hallmark UI audit" $?

# ── 5) installable release contract ──────────────────────────────────────────
stage "5/7 WHMCS changelog and installable package contract"
bash "$SCRIPT_DIR/test-whmcs-release-contract.sh"; record "WHMCS release contract" $?

# ── 6–7) real-WHMCS smokes (devbox) ──────────────────────────────────────────
allow_missing="${PREDEPLOY_ALLOW_MISSING_DEVBOX:-0}"
if ! devbox_available; then
  if [ "$allow_missing" = "1" ]; then
    stage "6/7 real-WHMCS migration and integration smoke"
    echo "  SKIP: whmcs-devbox missing and PREDEPLOY_ALLOW_MISSING_DEVBOX=1"
    echo "  Tradeoff: no live 8.13/9.0 migration/integration proof in this run."
    record_skip "integration smoke (devbox missing; explicit allow)"
    stage "7/7 post-migration schema smoke (dev 8.13 + 9.0)"
    echo "  SKIP: whmcs-devbox missing and PREDEPLOY_ALLOW_MISSING_DEVBOX=1"
    echo "  Tradeoff: no live information_schema proof on WHMCS 8.13/9.0."
    record_skip "live-schema smoke 8.13+9.0 (devbox missing; explicit allow)"
  else
    stage "6/7 real-WHMCS migration and integration smoke"
    "$RESOLVE" >/dev/null
    record "integration smoke" 1
    stage "7/7 post-migration schema smoke (dev 8.13 + 9.0)"
    echo "  FAIL: whmcs-devbox required (set PREDEPLOY_ALLOW_MISSING_DEVBOX=1 only for packaging dry-runs)" >&2
    record "live-schema smoke 8.13+9.0" 1
  fi
else
  stage "6/7 real-WHMCS migration and integration smoke"
  bash "$SCRIPT_DIR/whmcs-integration-smoke.sh"; record "integration smoke" $?

  stage "7/7 post-migration schema smoke (dev 8.13 + 9.0)"
  CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 bash "$SCRIPT_DIR/live-schema-smoke.sh" 8; s8=$?
  CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 bash "$SCRIPT_DIR/live-schema-smoke.sh" 9; s9=$?
  if [ "$s8" -eq 0 ] && [ "$s9" -eq 0 ]; then record "live-schema smoke 8.13+9.0" 0; else record "live-schema smoke 8.13+9.0" 1; fi
fi

# ── summary ──────────────────────────────────────────────────────────────────
echo; echo "==================== predeploy gate summary ===================="
for r in "${results[@]}"; do echo "  $r"; done
if [ "$fail" -eq 0 ]; then
  if [ "$skipped_smoke" -eq 1 ]; then
    echo "  GATE: PASS (PARTIAL) — stages 1–5 green; live WHMCS smoke SKIPPED"
    echo "  NOT production-ready. Re-run without PREDEPLOY_ALLOW_MISSING_DEVBOX after"
    echo "  mounting/cloning whmcs-devbox (see docs/operations/DOKPLOY_WHMCS_MODULE_DEPLOY.md)."
    exit 0
  fi
  echo "  GATE: PASS — safe to build a release artifact"
  exit 0
fi
echo "  GATE: FAIL — DO NOT DEPLOY"
exit 1
