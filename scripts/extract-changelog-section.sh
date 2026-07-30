#!/usr/bin/env bash
#
# Print exactly one literal WHMCS component/version section from the shared
# changelog. Regex metacharacters in versions are intentionally not evaluated.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <changelog> <component-prefix> <version>" >&2
  exit 2
fi

CHANGELOG="$1"
COMPONENT_PREFIX="$2"
VERSION="$3"

[ -f "$CHANGELOG" ] || { echo "Changelog not found: $CHANGELOG" >&2; exit 1; }
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
  echo "Invalid version: $VERSION" >&2
  exit 1
fi

EXPECTED="## ${COMPONENT_PREFIX}${VERSION} "

awk -v expected="$EXPECTED" '
  index($0, expected) == 1 {
    matches++
    capture = 1
    section = section $0 ORS
    next
  }
  capture && /^## / {
    capture = 0
  }
  capture {
    section = section $0 ORS
  }
  END {
    if (matches != 1) {
      printf "Expected exactly one changelog heading beginning %c%s%c; found %d\n",
        34, expected, 34, matches > "/dev/stderr"
      exit 1
    }
    printf "%s", section
  }
' "$CHANGELOG"
