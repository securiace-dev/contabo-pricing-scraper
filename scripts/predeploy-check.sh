#!/usr/bin/env bash
#
# predeploy-check.sh — MANDATORY pre-deploy gate for the contabo_pricing WHMCS addon.
#
# Runs the full LOCAL verification battery, fail-closed. It NEVER touches
# production — only local files + the dockerised dev WHMCS (8.13 + 9.0). A prod
# deploy MUST NOT proceed unless this script exits 0. See docs/DEPLOY_RUNBOOK.md.
#
# Stages (all run; gate fails if ANY stage fails):
#   1. Unit suite (PHPUnit, FakeCapsule).
#   2. PHP 7.4 syntax lint (the addon's polyglot floor) of lib + entrypoints +
#      admin templates + repo-root scripts.
#   3. Live-schema smoke against dev WHMCS 8.13 + 9.0 (information_schema only).
#   4. Real-WHMCS integration smoke (apply / drift / observe end-to-end on dev).
#   5. Golden API contract test (phpunit tests/GoldenApiContractTest.php).
#
# Usage:  bash scripts/predeploy-check.sh
# Exit:   0 = gate PASS (safe to deploy); non-zero = gate FAIL (do NOT deploy).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ADDON="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"

fail=0
declare -a results
record() { if [ "$2" -eq 0 ]; then results+=("[PASS] $1"); else results+=("[FAIL] $1"); fail=1; fi; }
stage()  { echo; echo "==================== $1 ===================="; }

# ── 1) unit suite ────────────────────────────────────────────────────────────
stage "1/5 unit suite (phpunit)"
( cd "$ADDON" && vendor/bin/phpunit ); record "unit suite" $?

# ── 2) PHP 7.4 syntax lint ───────────────────────────────────────────────────
stage "2/5 PHP 7.4 syntax lint"
lint_status=0
files=$( { ls "$ADDON"/lib/*.php "$ADDON"/*.php "$ADDON"/templates/admin/*.tpl "$SCRIPT_DIR"/*.php ; } 2>/dev/null )
if docker image inspect php:7.4-cli >/dev/null 2>&1 || docker pull php:7.4-cli >/dev/null 2>&1; then
  rels=""
  for f in $files; do rels="$rels ${f#"$REPO_ROOT"/}"; done
  # One container, loop inside (fast): lint every file, fail if any fails.
  docker run --rm -v "$REPO_ROOT":/app -w /app php:7.4-cli sh -c '
    st=0; for f in "$@"; do php -l "$f" >/dev/null 2>&1 || { echo "  LINT FAIL (7.4): $f"; st=1; }; done; exit $st
  ' _ $rels
  lint_status=$?
  echo "  linted with php:7.4-cli"
else
  echo "  WARNING: php:7.4-cli (docker) unavailable — falling back to local php $(php -r 'echo PHP_VERSION;' 2>/dev/null); PHP 7.4 NOT verified"
  for f in $files; do
    php -l "$f" >/dev/null 2>&1 || { echo "  LINT FAIL: $f"; lint_status=1; }
  done
fi
record "PHP 7.4 lint" "$lint_status"

# ── 3) live-schema smoke (dev 8.13 + 9.0) ────────────────────────────────────
stage "3/5 live-schema smoke (dev 8.13 + 9.0)"
CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 bash "$SCRIPT_DIR/live-schema-smoke.sh" 8; s8=$?
CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 bash "$SCRIPT_DIR/live-schema-smoke.sh" 9; s9=$?
if [ "$s8" -eq 0 ] && [ "$s9" -eq 0 ]; then record "live-schema smoke 8.13+9.0" 0; else record "live-schema smoke 8.13+9.0" 1; fi

# ── 4) real-WHMCS integration smoke (dev) ────────────────────────────────────
stage "4/5 real-WHMCS integration smoke"
bash "$SCRIPT_DIR/whmcs-integration-smoke.sh"; record "integration smoke" $?

# ── 5) golden API contract test ──────────────────────────────────────────────
stage "5/5 golden API contract test"
( cd "$ADDON" && vendor/bin/phpunit tests/GoldenApiContractTest.php ); record "golden contract" $?

# ── summary ──────────────────────────────────────────────────────────────────
echo; echo "==================== predeploy gate summary ===================="
for r in "${results[@]}"; do echo "  $r"; done
if [ "$fail" -eq 0 ]; then
  echo "  GATE: PASS — safe to proceed to deploy (see docs/DEPLOY_RUNBOOK.md)"
  exit 0
fi
echo "  GATE: FAIL — DO NOT DEPLOY"
exit 1
