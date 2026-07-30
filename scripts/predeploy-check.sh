#!/usr/bin/env bash
#
# predeploy-check.sh — local release gate for the WHMCS-native VPS suite.
#
# Runs the full LOCAL verification battery, fail-closed. It NEVER touches
# production — only local files + the dockerised dev WHMCS (8.13 + 9.0). A prod
# release MUST NOT proceed unless this script exits 0. See docs/DEPLOY_RUNBOOK.md.
#
# Stages (all run; gate fails if ANY stage fails):
#   1. Unit suite (PHPUnit, FakeCapsule) — addon.
#   2. Unit suite — canonical securiacevps module.
#   3. PHP 7.4 runtime/template syntax plus PHP 8.2 / 8.3 full-source syntax.
#   4. Rust format, test, check, and Clippy.
#   5. Rust producer ↔ WHMCS consumer schema contract.
#   6. Hallmark static UI regression audit.
#   7. Real-WHMCS migration/integration smoke in dev.
#   8. Read-only post-migration schema verification on WHMCS 8.13 + 9.0.
#
# Usage:  bash scripts/predeploy-check.sh
# Exit:   0 = gate PASS; non-zero = gate FAIL.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ADDON="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"
SERVER="$REPO_ROOT/whmcs-module/modules/servers/securiacevps"
SHIM="$REPO_ROOT/whmcs-module/modules/servers/contabo_vps"

fail=0
declare -a results
record() { if [ "$2" -eq 0 ]; then results+=("[PASS] $1"); else results+=("[FAIL] $1"); fail=1; fi; }
stage()  { echo; echo "==================== $1 ===================="; }

# ── 1) addon unit suite ──────────────────────────────────────────────────────
stage "1/8 addon unit suite (phpunit)"
( cd "$ADDON" && vendor/bin/phpunit ); record "addon unit suite" $?

# ── 2) server-module unit suite ──────────────────────────────────────────────
stage "2/8 securiacevps server-module unit suite (phpunit)"
( cd "$SERVER" && "$ADDON/vendor/bin/phpunit" -c phpunit.xml ); record "server-module unit suite" $?

# ── 3) PHP syntax matrix ─────────────────────────────────────────────────────
stage "3/8 PHP 7.4 runtime/templates + PHP 8.2 / 8.3 full-source syntax lint"
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

# ── 4) Rust verification ─────────────────────────────────────────────────────
stage "4/8 Rust format, tests, check, and Clippy"
rust_status=0
(cd "$REPO_ROOT" && cargo fmt --all -- --check) || rust_status=1
(cd "$REPO_ROOT" && cargo test --all-targets) || rust_status=1
(cd "$REPO_ROOT" && cargo check --all-targets) || rust_status=1
(cd "$REPO_ROOT" && cargo clippy --all-targets -- -D warnings) || rust_status=1
record "Rust verification" "$rust_status"

# ── 5) producer/consumer schema contract ─────────────────────────────────────
stage "5/8 Rust producer ↔ WHMCS consumer schema contract"
contract_status=0
(cd "$REPO_ROOT" && cargo test --test schema_contract) || contract_status=1
(cd "$ADDON" && vendor/bin/phpunit tests/GoldenApiContractTest.php) || contract_status=1
record "producer/consumer schema contract" "$contract_status"

# ── 6) Hallmark static UI audit ──────────────────────────────────────────────
stage "6/8 Hallmark static UI regression audit"
ruby "$SCRIPT_DIR/hallmark-audit.rb"; record "Hallmark UI audit" $?

# ── 7) real-WHMCS integration smoke (dev) ────────────────────────────────────
stage "7/8 real-WHMCS migration and integration smoke"
bash "$SCRIPT_DIR/whmcs-integration-smoke.sh"; record "integration smoke" $?

# ── 8) live-schema smoke (dev 8.13 + 9.0) ────────────────────────────────────
stage "8/8 post-migration schema smoke (dev 8.13 + 9.0)"
CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 bash "$SCRIPT_DIR/live-schema-smoke.sh" 8; s8=$?
CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 bash "$SCRIPT_DIR/live-schema-smoke.sh" 9; s9=$?
if [ "$s8" -eq 0 ] && [ "$s9" -eq 0 ]; then record "live-schema smoke 8.13+9.0" 0; else record "live-schema smoke 8.13+9.0" 1; fi

# ── summary ──────────────────────────────────────────────────────────────────
echo; echo "==================== predeploy gate summary ===================="
for r in "${results[@]}"; do echo "  $r"; done
if [ "$fail" -eq 0 ]; then
  echo "  GATE: PASS — safe to build a release artifact"
  exit 0
fi
echo "  GATE: FAIL — DO NOT DEPLOY"
exit 1
