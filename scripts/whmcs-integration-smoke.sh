#!/usr/bin/env bash
#
# Real-WHMCS integration smoke against the local whmcs-devbox 8.13 and 9.0
# containers. Current source is streamed into a per-container temporary
# directory, so stale bind mounts cannot affect the result and no other repo is
# modified. This script never contacts production or a provider API.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEVBOX_DIR="${WHMCS_DEVBOX_DIR:-$(cd "$REPO_ROOT/../whmcs-devbox" && pwd)}"
BASE_COMPOSE="$DEVBOX_DIR/docker-compose.yml"
OVERRIDE_COMPOSE="$DEVBOX_DIR/docker-compose.override.yml"
ADDON_DIR="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"
MODULE_DIR="$REPO_ROOT/whmcs-module/modules/servers/securiacevps"
SHIM_DIR="$REPO_ROOT/whmcs-module/modules/servers/contabo_vps"
ADDON_SMOKE="$ADDON_DIR/tests/integration/whmcs_smoke.php"
NATIVE_SMOKE="$MODULE_DIR/tests/integration/native_whmcs_smoke.php"

[ -f "$BASE_COMPOSE" ] || {
  echo "WHMCS devbox not found at $DEVBOX_DIR" >&2
  exit 1
}

compose_args=(-p whmcs-devbox -f "$BASE_COMPOSE")
[ -f "$OVERRIDE_COMPOSE" ] && compose_args+=(-f "$OVERRIDE_COMPOSE")

echo "==> ensuring isolated local WHMCS web/database services are running"
(cd "$DEVBOX_DIR" && docker compose "${compose_args[@]}" up -d \
  mariadb8 whmcs8 mariadb9 whmcs9)

status=0
for service in whmcs8 whmcs9; do
  container="$(cd "$DEVBOX_DIR" && docker compose "${compose_args[@]}" ps -q "$service")"
  if [ -z "$container" ]; then
    echo "No running container resolved for Compose service $service" >&2
    status=1
    continue
  fi
  case "$container" in
    *$'\n'*)
      echo "More than one container resolved for Compose service $service" >&2
      status=1
      continue
      ;;
  esac
  echo
  echo "==> integration smoke: $service ($container)"
  stage="$(docker exec "$container" mktemp -d /tmp/securiace-native.XXXXXX)"
  case "$stage" in
    /tmp/securiace-native.*) ;;
    *) echo "Unexpected container temp path: $stage" >&2; exit 1 ;;
  esac

  COPYFILE_DISABLE=1 tar --no-xattrs -C "$REPO_ROOT/whmcs-module" -cf - \
    modules/addons/contabo_pricing/lib \
    modules/servers/securiacevps \
    modules/servers/contabo_vps |
    docker exec -i "$container" tar -C "$stage" -xf -

  addon_lib="$stage/modules/addons/contabo_pricing/lib"
  module_entry="$stage/modules/servers/securiacevps/securiacevps.php"
  shim_entry="$stage/modules/servers/contabo_vps/contabo_vps.php"

  docker exec \
    -e CONTABO_ADDON_LIB_DIR="$addon_lib" \
    -i "$container" php /dev/stdin < "$ADDON_SMOKE" || status=1

  docker exec \
    -e CONTABO_ADDON_LIB_DIR="$addon_lib" \
    -e SECURIACE_MODULE_ENTRY="$module_entry" \
    -e SECURIACE_SHIM_ENTRY="$shim_entry" \
    -i "$container" php /dev/stdin < "$NATIVE_SMOKE" || status=1

  docker exec "$container" rm -rf "$stage"
done

echo
if [ "$status" -eq 0 ]; then
  echo "==> WHMCS 8.13/9.0 integration smoke PASSED"
else
  echo "==> WHMCS 8.13/9.0 integration smoke FAILED"
fi
exit "$status"
