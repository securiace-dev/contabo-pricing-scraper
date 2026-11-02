#!/usr/bin/env bash
#
# Shared WHMCS-devbox path resolution for local-whmcs / smoke / predeploy.
# Never contacts production. Prints the resolved directory on stdout when found;
# returns non-zero with a single stderr line when missing.
#
# Resolution order:
#   1. WHMCS_DEVBOX_DIR (explicit)
#   2. $REPO_ROOT/../whmcs-devbox (sibling checkout)
#   3. $HOME/Projects/whmcs-devbox (Mac operator layout used historically)
#
# A directory counts as a usable checkout only when docker-compose.yml exists.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

candidates=()
if [ -n "${WHMCS_DEVBOX_DIR:-}" ]; then
  candidates+=("$WHMCS_DEVBOX_DIR")
fi
candidates+=("$REPO_ROOT/../whmcs-devbox")
if [ -n "${HOME:-}" ]; then
  candidates+=("$HOME/Projects/whmcs-devbox")
fi

for candidate in "${candidates[@]}"; do
  if [ -f "$candidate/docker-compose.yml" ]; then
    (cd "$candidate" && pwd)
    exit 0
  fi
done

{
  echo "WHMCS devbox not found."
  echo "  Tried:"
  for candidate in "${candidates[@]}"; do
    echo "    - $candidate"
  done
  echo "  Fix: clone/mount the private whmcs-devbox checkout, or export WHMCS_DEVBOX_DIR"
  echo "  to a directory that contains docker-compose.yml (services: mariadb8, whmcs8,"
  echo "  mariadb9, whmcs9 → containers whmcs-devbox-whmcs8-1 / whmcs-devbox-whmcs9-1)."
  echo "  Historical Mac sibling paths also include ~/Projects/securiace-vps-platform"
  echo "  (docker-compose.test-whmcs.yml) — only usable if WHMCS_DEVBOX_DIR points at a"
  echo "  tree whose compose project names match the smoke scripts."
} >&2
exit 1
