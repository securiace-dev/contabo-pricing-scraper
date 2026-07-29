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
  8) ctr="whmcs-devbox-whmcs8-1" ;;
  9) ctr="whmcs-devbox-whmcs9-1" ;;
  *) echo "usage: $0 [8|9]"; exit 2 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker exec \
  -e CONTABO_PRICING_LIVE_SCHEMA_SMOKE="${CONTABO_PRICING_LIVE_SCHEMA_SMOKE:-}" \
  -e CONTABO_WHMCS_ROOT=/var/www/html \
  -i "$ctr" php /dev/stdin < "$SCRIPT_DIR/live-schema-smoke.php"
