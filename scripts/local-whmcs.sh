#!/usr/bin/env bash
#
# Convenience wrapper for the shared, non-production whmcs-devbox.
# Source is no longer rsynced into another repository: the integration runner
# streams the current commit into a temporary container directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEVBOX_DIR="${WHMCS_DEVBOX_DIR:-$(cd "$REPO_ROOT/../whmcs-devbox" && pwd)}"
BASE_COMPOSE="$DEVBOX_DIR/docker-compose.yml"
OVERRIDE_COMPOSE="$DEVBOX_DIR/docker-compose.override.yml"
compose_args=(-p whmcs-devbox -f "$BASE_COMPOSE")
[ -f "$OVERRIDE_COMPOSE" ] && compose_args+=(-f "$OVERRIDE_COMPOSE")

case "${1:-status}" in
  status)
    echo "WHMCS devbox: $DEVBOX_DIR"
    (cd "$DEVBOX_DIR" && docker compose "${compose_args[@]}" ps whmcs8 whmcs9 mariadb8 mariadb9)
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
