#!/usr/bin/env bash
#
# Read-only LIVE-SCHEMA smoke for the WHMCS-native contabo_pricing addon.
#
# Runs scripts/live-schema-smoke.php inside a WHMCS php container so it can read
# that install's configuration.php and SELECT from information_schema ONLY. It
# performs NO writes, migrations, syncs, or module actions.
#
# Gated by CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 — without it the check skips
# safely (exit 0). By default it targets the LOCAL DEV WHMCS containers; it never
# targets production unless you deliberately point CONTABO_WHMCS_ROOT / LIVE_DB_*
# at one.
#
# Usage:
#   CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 scripts/live-schema-smoke.sh        # local dev WHMCS 8.13
#   CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 scripts/live-schema-smoke.sh 9      # local dev WHMCS 9.0
set -euo pipefail

v="${1:-8}"
case "$v" in
  8) service="whmcs8" ;;
  9) service="whmcs9" ;;
  *) echo "usage: $0 [8|9]"; exit 2 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEVBOX_DIR="${WHMCS_DEVBOX_DIR:-$(cd "$REPO_ROOT/../whmcs-devbox" && pwd)}"
BASE_COMPOSE="$DEVBOX_DIR/docker-compose.yml"
OVERRIDE_COMPOSE="$DEVBOX_DIR/docker-compose.override.yml"

[ -f "$BASE_COMPOSE" ] || {
  echo "WHMCS devbox not found at $DEVBOX_DIR" >&2
  exit 1
}

compose_args=(-p whmcs-devbox -f "$BASE_COMPOSE")
[ -f "$OVERRIDE_COMPOSE" ] && compose_args+=(-f "$OVERRIDE_COMPOSE")
ctr="$(cd "$DEVBOX_DIR" && docker compose "${compose_args[@]}" ps -q "$service")"
if [ -z "$ctr" ]; then
  echo "No running container resolved for Compose service $service" >&2
  exit 1
fi
case "$ctr" in
  *$'\n'*) echo "More than one container resolved for Compose service $service" >&2; exit 1 ;;
esac

docker exec \
  -e CONTABO_PRICING_LIVE_SCHEMA_SMOKE="${CONTABO_PRICING_LIVE_SCHEMA_SMOKE:-}" \
  -e CONTABO_WHMCS_ROOT=/var/www/html \
  -i "$ctr" php /dev/stdin < "$SCRIPT_DIR/live-schema-smoke.php"
