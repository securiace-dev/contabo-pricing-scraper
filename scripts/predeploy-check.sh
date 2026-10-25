#!/usr/bin/env bash
#
# predeploy-check.sh — local release gate for the WHMCS-native VPS suite.
#
# Runs the full LOCAL verification battery, fail-closed. It NEVER touches
# production — only local files + the dockerised dev WHMCS (8.13 + 9.0). A prod
# release MUST NOT proceed unless this script exits 0. See docs/DEPLOY_RUNBOOK.md.
#
# Stages (all run; gate fails if ANY stage fails):
#   1. Unit suite (PHPUnit, FakeCapsule) — addon, on php:8.2.35-cli and php:8.3.36-cli.
#   2. Unit suite — canonical securiacevps module, on the same pinned images.
#   3. PHP 7.4 runtime/template syntax plus PHP 8.2.35 / 8.3.36 full-source syntax.
#   4. Hallmark static UI regression audit.
#   5. WHMCS changelog, checksum, manifest, and package contract.
#   6. Real-WHMCS migration/integration smoke in dev.
#   7. Read-only post-migration schema verification on WHMCS 8.13 + 9.0.
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

docker_ok=0
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker_ok=1
else
  echo "Docker is required to run php:8.2.35-cli and php:8.3.36-cli and is not usable. Host PHP is not a patched runtime." >&2
fi

# Composer runs inside the pinned image. Host PHP is not assumed to satisfy the constraint.
ensure_phpunit_vendor() {
  if [ -x "$ADDON/vendor/bin/phpunit" ]; then
    return 0
  fi
  echo "  vendor/bin/phpunit missing; composer install inside php:8.2.35-cli"
  docker run --rm \
    -v "$REPO_ROOT:/work" \
    -w /work \
    -e COMPOSER_ALLOW_SUPERUSER=1 \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -e ADDON="whmcs-module/modules/addons/contabo_pricing" \
    php:8.2.35-cli \
    bash -c '
      set -euo pipefail
      actual="$(php -r "echo PHP_VERSION;")"
      if [ "$actual" != "8.2.35" ]; then
        echo "expected PHP 8.2.35, image reports ${actual}" >&2
        exit 1
      fi
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq
      apt-get install -y -qq --no-install-recommends unzip
      curl -fsSL https://getcomposer.org/installer -o /tmp/composer-setup.php
      php /tmp/composer-setup.php --install-dir=/usr/local/bin --filename=composer
      rm -f /tmp/composer-setup.php
      composer install --no-interaction --prefer-dist --no-progress --working-dir="$ADDON"
      chown -R "${HOST_UID}:${HOST_GID}" "${ADDON}/vendor"
    '
}

run_suite_on_patched_images() {
  local workdir="$1"
  shift
  local image expected st=0
  for image in php:8.2.35-cli php:8.3.36-cli; do
    case "$image" in
      php:8.2.35-cli) expected=8.2.35 ;;
      php:8.3.36-cli) expected=8.3.36 ;;
      *) echo "  unexpected image ${image}" >&2; return 1 ;;
    esac
    echo "  phpunit on ${image}"
    if ! docker run --rm \
      -v "$REPO_ROOT:/work" \
      -w "/work/${workdir}" \
      -u "$(id -u):$(id -g)" \
      -e HOME=/tmp \
      -e EXPECTED_PHP="$expected" \
      "$image" \
      bash -c 'actual="$(php -r "echo PHP_VERSION;")"; if [ "$actual" != "$EXPECTED_PHP" ]; then echo "expected PHP ${EXPECTED_PHP}, image reports ${actual}" >&2; exit 1; fi; exec php "$@"' \
      _ "$@"
    then
      st=1
    fi
  done
  return "$st"
}

vendor_ok=0
if [ "$docker_ok" -eq 1 ]; then
  if ensure_phpunit_vendor; then
    vendor_ok=1
  else
    echo "  composer install inside php:8.2.35-cli failed" >&2
  fi
fi

# ── 1) addon unit suite ──────────────────────────────────────────────────────
stage "1/7 addon unit suite (phpunit on php:8.2.35-cli and php:8.3.36-cli)"
if [ "$vendor_ok" -eq 1 ]; then
  run_suite_on_patched_images "whmcs-module/modules/addons/contabo_pricing" vendor/bin/phpunit
  record "addon unit suite" $?
else
  record "addon unit suite" 1
fi

# ── 2) server-module unit suite ──────────────────────────────────────────────
stage "2/7 securiacevps server-module unit suite (phpunit on php:8.2.35-cli and php:8.3.36-cli)"
if [ "$vendor_ok" -eq 1 ]; then
  run_suite_on_patched_images "whmcs-module/modules/servers/securiacevps" \
    ../../addons/contabo_pricing/vendor/bin/phpunit -c phpunit.xml
  record "server-module unit suite" $?
else
  record "server-module unit suite" 1
fi

# ── 3) PHP syntax matrix ─────────────────────────────────────────────────────
stage "3/7 PHP 7.4 runtime/templates + PHP 8.2.35 / 8.3.36 full-source syntax lint"
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
if [ "$docker_ok" -ne 1 ]; then
  echo "  Docker is required for the PHP syntax matrix and is not usable. Host PHP is not a patched runtime." >&2
  lint_status=1
else
  for php_image in php:7.4-cli php:8.2.35-cli php:8.3.36-cli; do
    if docker image inspect "$php_image" >/dev/null 2>&1 ||
      docker pull "$php_image" >/dev/null 2>&1; then
      lint_rels=("${rels[@]}")
      if [ "$php_image" = "php:7.4-cli" ]; then
        lint_rels=("${runtime_rels[@]}")
      fi
      docker run --rm -v "$REPO_ROOT":/app -w /app "$php_image" sh -c '
        st=0
        for f in "$@"; do
          php -l "$f" >/dev/null 2>&1 || { echo "  LINT FAIL: $f"; st=1; }
        done
        exit $st
      ' _ "${lint_rels[@]}" || lint_status=1
      echo "  linted with ${php_image}"
    else
      echo "  LINT MATRIX UNAVAILABLE: ${php_image}" >&2
      lint_status=1
    fi
  done
fi
record "PHP runtime/full-source lint matrix" "$lint_status"

# ── 4) Hallmark static UI audit ──────────────────────────────────────────────
stage "4/7 Hallmark static UI regression audit"
ruby "$SCRIPT_DIR/hallmark-audit.rb"; record "Hallmark UI audit" $?

# ── 5) installable release contract ──────────────────────────────────────────
stage "5/7 WHMCS changelog and installable package contract"
bash "$SCRIPT_DIR/test-whmcs-release-contract.sh"; record "WHMCS release contract" $?

# ── 6) real-WHMCS integration smoke (dev) ────────────────────────────────────
stage "6/7 real-WHMCS migration and integration smoke"
bash "$SCRIPT_DIR/whmcs-integration-smoke.sh"; record "integration smoke" $?

# ── 7) live-schema smoke (dev 8.13 + 9.0) ────────────────────────────────────
stage "7/7 post-migration schema smoke (dev 8.13 + 9.0)"
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
