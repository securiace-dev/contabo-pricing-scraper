#!/usr/bin/env bash
# Usage: ./scripts/bump-version.sh 2.1.5
# Updates Cargo.toml + package.json, commits, tags, and pushes.
# The Rust binary reads its version via env!("CARGO_PKG_VERSION") at compile time.
# The Node.js scraper reads package.json at runtime.
set -euo pipefail

NEW="${1:-}"
if [ -z "$NEW" ]; then
  echo "Usage: $0 <version>   e.g. $0 2.1.5"
  exit 1
fi

# Strip leading 'v' if provided
NEW="${NEW#v}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Validate semver ────────────────────────────────────────────────────────────
if ! echo "$NEW" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "ERROR: version must be semver (MAJOR.MINOR.PATCH), got: $NEW"
  exit 1
fi

# ── Guard: no uncommitted changes ─────────────────────────────────────────────
if ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; then
  echo "ERROR: uncommitted changes present — commit or stash first"
  exit 1
fi

# ── Guard: tag must not already exist ─────────────────────────────────────────
if git -C "$ROOT" tag | grep -qx "v$NEW"; then
  echo "ERROR: tag v$NEW already exists"
  exit 1
fi

OLD_CARGO=$(grep '^version' "$ROOT/Cargo.toml" | head -1 | sed 's/.*= *"\(.*\)"/\1/')
OLD_PKG=$(node -e "process.stdout.write(require('$ROOT/package.json').version)")

echo "Bumping $OLD_CARGO → $NEW"
echo "  Cargo.toml   $OLD_CARGO → $NEW"
echo "  package.json $OLD_PKG  → $NEW"

# ── Update Cargo.toml (first version = line only) ─────────────────────────────
sed -i.bak "0,/^version = \"$OLD_CARGO\"/s/^version = \"$OLD_CARGO\"/version = \"$NEW\"/" "$ROOT/Cargo.toml"
rm "$ROOT/Cargo.toml.bak"

# ── Update package.json ────────────────────────────────────────────────────────
node -e "
  const fs = require('fs');
  const pkg = JSON.parse(fs.readFileSync('$ROOT/package.json','utf8'));
  pkg.version = '$NEW';
  fs.writeFileSync('$ROOT/package.json', JSON.stringify(pkg, null, 2) + '\n');
"

# ── Verify Rust picks up the new version ─────────────────────────────────────
NEW_CARGO=$(grep '^version' "$ROOT/Cargo.toml" | head -1 | sed 's/.*= *"\(.*\)"/\1/')
if [ "$NEW_CARGO" != "$NEW" ]; then
  echo "ERROR: Cargo.toml update failed (got $NEW_CARGO)"
  exit 1
fi

# ── Commit, tag, push ─────────────────────────────────────────────────────────
git -C "$ROOT" add Cargo.toml package.json
git -C "$ROOT" commit -m "chore: bump version to v$NEW"
git -C "$ROOT" tag "v$NEW"
git -C "$ROOT" push
git -C "$ROOT" push origin "v$NEW"

echo ""
echo "✓ Released v$NEW — CI is building binaries now."
echo "  Watch: gh run list --workflow=release.yml --limit=1"
