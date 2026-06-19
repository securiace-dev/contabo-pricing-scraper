#!/usr/bin/env bash
#
# deploy.sh — production deploy for the contabo_pricing addon + contabo_vps
#             provisioning module.
#
# Runs predeploy-check.sh first (fail-closed). Then rsyncs each module that has
# changed relative to prod (detected via --dry-run), chowns, and verifies.
# The provisioning module is deployed automatically whenever it has local
# changes — no manual step required.
#
# Usage:  bash scripts/deploy.sh [--dry-run] [--yes] [--help]
# Exit:   0 = deployed (or nothing to deploy); non-zero = gate fail or rsync error.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST='root@195.7.4.219'
WHMCS_ROOT='/var/www/my_securiace_usr/data/www/my.securiace.com'
WHMCS_OWNER='my_securiace_usr:my_securiace_usr'
SSH_OPTS='-o BatchMode=yes -o ConnectTimeout=15'

ADDON_SRC="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"
ADDON_DEST="$HOST:$WHMCS_ROOT/modules/addons/contabo_pricing/"
ADDON_EXCLUDES=(--exclude vendor/ --exclude tests/ --exclude phpunit.xml
                --exclude '.phpunit.cache' --exclude '.phpunit.result.cache'
                --exclude composer.lock --exclude '.git*' --exclude '.claude-flow/')

VPS_SRC="$REPO_ROOT/whmcs-module/modules/servers/contabo_vps"
VPS_DEST="$HOST:$WHMCS_ROOT/modules/servers/contabo_vps/"
VPS_EXCLUDES=(--exclude '.git*' --exclude '.claude-flow/')

stage() { echo; echo "==== $1 ===="; }
die()   { echo "ERROR: $1" >&2; exit 1; }

DRY_RUN=0
ASSUME_YES=0
usage() {
  cat <<'EOF'
Usage: bash scripts/deploy.sh [--dry-run] [--yes] [--help]

  --dry-run   Run the gate and show exactly what each module would transfer,
              then STOP without touching production.
  --yes, -y   Skip the interactive confirmation prompt (for automation).
  --help, -h  Show this help and exit.

With no flags: runs the gate, previews changes, asks for confirmation, then
deploys changed modules to production (my.securiace.com).
EOF
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --yes|-y)  ASSUME_YES=1 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

# ── 1) pre-deploy gate ────────────────────────────────────────────────────────
stage "1/3 pre-deploy gate"
bash "$SCRIPT_DIR/predeploy-check.sh" || die "gate FAILED — aborting deploy"

# ── 2) rsync each module (skip if no changes) ─────────────────────────────────
stage "2/3 rsync to prod"

# SSH reachability preflight (rsync — even --dry-run — needs the remote).
ssh $SSH_OPTS "$HOST" true 2>/dev/null \
  || die "cannot reach $HOST over SSH (BatchMode); check your key/agent and network"

if [ "$DRY_RUN" -ne 1 ] && [ "$ASSUME_YES" -ne 1 ]; then
  if [ -t 0 ]; then
    echo
    echo "  About to deploy changed modules to PRODUCTION: $HOST"
    printf "  Type 'deploy' to proceed: "
    read -r reply
    [ "$reply" = "deploy" ] || die "aborted by user (got '$reply')"
  else
    die "refusing to deploy non-interactively without --yes (no TTY for confirmation)"
  fi
fi

rsync_module() {
  local label="$1" src="$2" dest="$3"
  shift 3
  local excludes=("$@")

  # dry-run to detect changes
  local changes
  changes=$(rsync -rlptzc -i --dry-run --no-owner --no-group \
    "${excludes[@]}" -e "ssh $SSH_OPTS" "$src/" "$dest" 2>&1) \
    || die "rsync dry-run failed for $label"

  local file_lines
  file_lines=$(echo "$changes" | grep -c '^[<>f]' || true)

  if [ "$file_lines" -eq 0 ]; then
    echo "  $label: no changes — skipping"
    return 0
  fi

  echo "  $label: $file_lines file(s) to transfer:"
  echo "$changes" | grep '^[<>cdf]' | sed 's/^/    /'

  if [ "$DRY_RUN" -eq 1 ]; then
    echo "  $label: (dry-run) not transferring"
    return 0
  fi

  rsync -rlptzc -i --no-owner --no-group \
    "${excludes[@]}" -e "ssh $SSH_OPTS" "$src/" "$dest" \
    || die "rsync failed for $label"

  # fix ownership (rsync --no-owner leaves files root-owned)
  local remote_path="${dest#*:}"
  remote_path="${remote_path%/}"
  ssh $SSH_OPTS "$HOST" "chown -R $WHMCS_OWNER $remote_path" \
    || die "chown failed for $label"

  echo "  $label: deployed and chowned"
}

rsync_module "addon (contabo_pricing)"  "$ADDON_SRC" "$ADDON_DEST" "${ADDON_EXCLUDES[@]}"
rsync_module "server (contabo_vps)"     "$VPS_SRC"   "$VPS_DEST"   "${VPS_EXCLUDES[@]}"

# ── 3) post-deploy verification ───────────────────────────────────────────────
stage "3/3 post-deploy verification"

addon_ver=$(ssh $SSH_OPTS "$HOST" \
  "grep -m1 'const VERSION' $WHMCS_ROOT/modules/addons/contabo_pricing/lib/AdminController.php")
echo "  addon: $addon_ver"

ssh $SSH_OPTS "$HOST" "
  fail=0
  for f in $WHMCS_ROOT/modules/addons/contabo_pricing/lib/*.php \
            $WHMCS_ROOT/modules/servers/contabo_vps/lib/*.php \
            $WHMCS_ROOT/modules/servers/contabo_vps/contabo_vps.php; do
    php -l \"\$f\" >/dev/null 2>&1 || { echo '  LINT FAIL: '\$f; fail=1; }
  done
  [ \$fail -eq 0 ] && echo '  lint: ok'
  exit \$fail
" || die "post-deploy lint check failed"

echo
echo "==== deploy complete ===="
echo "  Load the addon admin page once to trigger SchemaHealth::assertOrMigrate()."
