#!/usr/bin/env bash
#
# whmcs-integration-smoke.sh — run the real-WHMCS integration smoke for the
# contabo_pricing addon against the dockerised dev WHMCS, in one command.
#
# The unit suite runs against FakeCapsule (arrays); real WHMCS returns stdClass
# and the container can be running STALE code. This wrapper defeats the
# stale-code trap by syncing the addon source into the bind-mounted container
# source FIRST, then executes tests/integration/whmcs_smoke.php INSIDE the
# WHMCS 8.13 php container and surfaces its exit code.
#
# Usage:  bash scripts/whmcs-integration-smoke.sh
# Exit:   0 = all smoke assertions PASS; non-zero = a failure (or sync/exec error).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONTAINER="securiace-vps-platform-whmcs8-php-1"
SMOKE_IN_CONTAINER="/var/www/html/modules/addons/contabo_pricing/tests/integration/whmcs_smoke.php"

echo "==> [1/2] syncing addon into local WHMCS source (defeats the stale-code trap)"
bash "$SCRIPT_DIR/local-whmcs.sh" sync

echo
echo "==> [2/2] running integration smoke inside container [$CONTAINER]"
# local-whmcs.sh's rsync excludes tests/, so the smoke script itself is not
# copied into the bind mount. Pipe it in on stdin and execute from there, so the
# in-container PHP still loads the freshly-synced addon lib/ via init.php + the
# stub autoloader the smoke registers.
# Capture the exit code without tripping `set -e` on a smoke failure.
status=0
docker exec -i "$CONTAINER" php /dev/stdin < "$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing/tests/integration/whmcs_smoke.php" || status=$?

echo
if [ "$status" -eq 0 ]; then
  echo "==> integration smoke PASSED"
else
  echo "==> integration smoke FAILED (exit $status)"
fi
exit "$status"
