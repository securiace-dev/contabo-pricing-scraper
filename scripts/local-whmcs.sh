#!/usr/bin/env bash
#
# Convenience wrapper for the shared, non-production whmcs-devbox.
# Source is no longer rsynced into another repository: the integration runner
# streams the current commit into a temporary container directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOLVE="$SCRIPT_DIR/lib/resolve-whmcs-devbox.sh"

case "${1:-status}" in
  status)
    if DEVBOX_DIR="$("$RESOLVE")"; then
      echo "WHMCS devbox: $DEVBOX_DIR"
    else
      echo "WHMCS devbox: MISSING (see resolve-whmcs-devbox.sh hints above)"
      exit 1
    fi
    docker ps --format '{{.Names}} {{.Status}}' |
      grep -E '^whmcs-devbox-(whmcs8|whmcs9|mariadb8|mariadb9)-1 ' || true
    ;;
  integration|smoke)
    exec bash "$SCRIPT_DIR/whmcs-integration-smoke.sh"
    ;;
  sync)
    echo "Direct rsync is retired. The smoke runner streams current source into isolated container temp paths." >&2
    exit 2
    ;;
  *)
    echo "usage: $0 [status|integration]" >&2
    exit 2
    ;;
esac
