# Plan 002: Add dry-run, confirmation, and prerequisite preflights to deploy.sh / local-whmcs.sh

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise.
>
> **Drift check (run first)**: `git diff --stat b31e458..HEAD -- scripts/deploy.sh scripts/local-whmcs.sh`
> If either in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW (adds guards/flags; the default no-arg behaviour stays equivalent except for one confirmation prompt)
- **Depends on**: none
- **Category**: dx / safety
- **Planned at**: commit `b31e458`, 2026-06-20

## Why this matters

`scripts/deploy.sh` pushes the WHMCS modules to **production** (`my.securiace.com`).
Two ergonomic/safety gaps:

1. **No preview-and-stop and no confirmation.** The internal `rsync --dry-run`
   is only used to *count* changes; the script then transfers in the same run.
   The project rule "never deploy without an explicit go" is therefore purely
   procedural — there is no `--dry-run` to review-then-approve and no
   "type to confirm" guard before mutating prod.
2. **No prerequisite preflight.** Neither `deploy.sh` (SSH reachability) nor
   `scripts/local-whmcs.sh` (docker present, the `securiace-vps-platform` source
   dir present) validates its preconditions, so failures surface as cryptic
   rsync/docker errors instead of one clear actionable message.

After this lands: `deploy.sh --dry-run` previews exactly what would transfer and
stops without touching prod; a normal `deploy.sh` run asks for explicit
confirmation before the first live transfer (skippable with `--yes` for
automation); both scripts fail fast with clear guidance when a prerequisite is
missing.

## Current state

### `scripts/deploy.sh` (full, as it exists today)

```bash
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
# Usage:  bash scripts/deploy.sh
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

# ── 1) pre-deploy gate ────────────────────────────────────────────────────────
stage "1/3 pre-deploy gate"
bash "$SCRIPT_DIR/predeploy-check.sh" || die "gate FAILED — aborting deploy"

# ── 2) rsync each module (skip if no changes) ─────────────────────────────────
stage "2/3 rsync to prod"

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
```

### `scripts/local-whmcs.sh` (relevant excerpts)

Setup + the `case` dispatcher (lines ~26–45):
```bash
set -euo pipefail

PLATFORM="${SECVPS_PLATFORM_DIR:-$HOME/Projects/securiace-vps-platform}"
ADDON_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/whmcs-module/modules/addons/contabo_pricing"

src_dir()  { echo "$PLATFORM/deploy/whmcs-test/source/$1"; }          # $1 = 8.13 | 9.0
php_ctr()  { case "$1" in 8) echo "securiace-vps-platform-whmcs8-php-1";; 9) echo "securiace-vps-platform-whmcs9-php-1";; esac; }
ver_dir()  { case "$1" in 8) echo "8.13";; 9) echo "9.0";; esac; }

sync_one() {
  local v="$1" dest; dest="$(src_dir "$(ver_dir "$v")")/modules/addons/contabo_pricing"
  rsync -a --delete \
    --exclude vendor/ --exclude tests/ --exclude phpunit.xml \
    --exclude '.phpunit.cache' --exclude composer.lock --exclude '.git*' \
    "$ADDON_SRC/" "$dest/"
  echo "  synced → $dest"
}

cmd="${1:-status}"
case "$cmd" in
  sync)
    echo "==> syncing addon into local WHMCS source (8.13 + 9.0)"
    sync_one 8; sync_one 9
    ;;
  migrate)
    ...
```
(The `migrate`, `activate`, `render`, `status` arms all shell out to `docker`;
`sync` and `status` rely on `$PLATFORM` existing. The `*)` arm prints
`unknown command` and `exit 2`.)

**Repo conventions to match**: plain bash; `deploy.sh` uses `set -uo pipefail`
+ explicit `|| die`, `stage()`/`die()` helpers, `==== … ====` banners, two-space
indent. `local-whmcs.sh` uses `set -euo pipefail` and `==>` prefixed echoes.
Match each file's own existing style.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Shell syntax check | `bash -n scripts/deploy.sh` | exit 0 |
| Shell syntax check | `bash -n scripts/local-whmcs.sh` | exit 0 |
| Help lists new flags | `bash scripts/deploy.sh --help` | prints usage incl. `--dry-run` and `--yes`; exit 0 |
| Unknown flag rejected | `bash scripts/deploy.sh --bogus; echo $?` | error message; non-zero exit |
| local preflight fires | `SECVPS_PLATFORM_DIR=/nonexistent bash scripts/local-whmcs.sh sync; echo $?` | clear "platform dir not found" message; non-zero exit |

Note: this worktree has **no docker** and **no SSH access to prod**. You cannot
run a real `deploy.sh` deploy or `--dry-run` end-to-end (it runs the gate first,
which BLOCKs without docker, and it would need SSH). That is expected — verify
via `bash -n`, `--help`, flag parsing, and reading the diff. Do NOT attempt a
real deploy or any SSH to `195.7.4.219`.

## Scope

**In scope** (the only files you may modify):
- `scripts/deploy.sh`
- `scripts/local-whmcs.sh`

**Out of scope** (do NOT touch):
- `scripts/predeploy-check.sh` and `CLAUDE.md` — handled by a separate plan.
- The rsync flag set, excludes, host, owner, and the chown dance — keep these
  byte-for-byte; they were "learned the hard way" (see runbook gotchas).
- The post-deploy verification block — leave its logic unchanged.

## Git workflow

- You are in an isolated worktree on a branch already created for you.
- Commit when done. Conventional-commit style (recent example:
  `chore(deploy): add deploy.sh for addon + provisioning module`). Suggested:
  `feat(deploy): add --dry-run/--yes, prod confirmation, and preflights`.
- Do NOT push or open a PR. Do NOT run a real deploy or SSH to prod.

## Steps

### Step 1: Parse flags + add `--help` in deploy.sh

After the variable/excludes setup and the `stage()`/`die()` helpers (i.e. just
before the `# ── 1) pre-deploy gate` block), add argument parsing that sets two
flags and supports help. Default behaviour (no args) must remain a real deploy
(plus the new confirmation from Step 3).

Target shape:
```bash
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
```
Also update the top-of-file `# Usage:` comment line to mention the flags.

**Verify**: `bash -n scripts/deploy.sh` → exit 0;
`bash scripts/deploy.sh --help` → prints usage incl. `--dry-run`, exit 0;
`bash scripts/deploy.sh --bogus; echo $?` → error + non-zero.

### Step 2: Honor `--dry-run` inside `rsync_module`

In `rsync_module`, after the change list is printed (the
`echo "$changes" | grep '^[<>cdf]' | sed 's/^/    /'` line) and **before** the
live `rsync` that transfers, add a dry-run short-circuit:
```bash
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "  $label: (dry-run) not transferring"
    return 0
  fi
```
This leaves the no-changes early-return and the real transfer/chown path intact;
it only prevents the mutation when previewing.

**Verify**: `bash -n scripts/deploy.sh` → exit 0;
`grep -n "dry-run) not transferring" scripts/deploy.sh` → 1 match.

### Step 3: Add SSH preflight + confirmation before the transfers

Between the gate stage and the two `rsync_module` calls, insert a guard block
that (a) preflights SSH reachability and (b) asks for confirmation unless
`--dry-run` or `--yes`. Skip both the SSH check and the prompt when `DRY_RUN=1`
is purely a local preview... except SSH is still needed for the dry-run (rsync
dry-run connects to the remote). So: **always** SSH-preflight before any
`rsync_module` (dry-run included, since rsync --dry-run hits the remote);
prompt only when not dry-run and not assume-yes.

Target shape (place right after the `stage "2/3 rsync to prod"` line):
```bash
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
```

**Verify**: `bash -n scripts/deploy.sh` → exit 0;
`grep -n "Type 'deploy' to proceed" scripts/deploy.sh` → 1 match;
`grep -n "cannot reach .* over SSH" scripts/deploy.sh` → 1 match.

### Step 4: Add a prerequisite preflight to local-whmcs.sh

Add two helper functions after the existing `ver_dir()` definition, and call
them at the start of the relevant `case` arms so a missing prerequisite produces
a clear message instead of a raw docker/rsync error.

Helpers:
```bash
require_platform() {
  [ -d "$PLATFORM" ] || { echo "ERROR: securiace-vps-platform dir not found at: $PLATFORM" >&2;
    echo "       set SECVPS_PLATFORM_DIR to its location." >&2; exit 2; }
  command -v rsync >/dev/null 2>&1 || { echo "ERROR: rsync not found on PATH" >&2; exit 2; }
}
require_docker() {
  command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found on PATH" >&2; exit 2; }
}
```
Wire them in:
- `sync` arm: call `require_platform` first.
- `migrate`, `activate`, `render` arms: call `require_docker` first.
- `status` arm: call `require_docker` first (it runs `docker ps`).

Insert each call as the first line inside its `case` arm, before the existing
`echo`/work.

**Verify**: `bash -n scripts/local-whmcs.sh` → exit 0;
`SECVPS_PLATFORM_DIR=/nonexistent bash scripts/local-whmcs.sh sync; echo $?` →
prints the "platform dir not found" error and exits 2.

## Test plan

Shell tooling, no unit-test harness — verification is behavioural:

- `bash -n` on both scripts exits 0.
- `deploy.sh --help` lists `--dry-run` and `--yes`; `--bogus` is rejected.
- `local-whmcs.sh sync` with a bogus `SECVPS_PLATFORM_DIR` prints the clear
  preflight error and exits 2 (does not reach rsync).
- Reading the diff confirms the rsync flag set, host, excludes, chown, and
  post-deploy verification are unchanged.
- No new test files (no shell-test harness exists in the repo).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `bash -n scripts/deploy.sh` exits 0
- [ ] `bash -n scripts/local-whmcs.sh` exits 0
- [ ] `bash scripts/deploy.sh --help` prints usage including `--dry-run` and `--yes`, exits 0
- [ ] `bash scripts/deploy.sh --bogus` exits non-zero with an error
- [ ] `grep -n "dry-run) not transferring" scripts/deploy.sh` → 1 match
- [ ] `grep -n "Type 'deploy' to proceed" scripts/deploy.sh` → 1 match
- [ ] `SECVPS_PLATFORM_DIR=/nonexistent bash scripts/local-whmcs.sh sync` exits non-zero with the platform-dir error
- [ ] `git status` shows only `scripts/deploy.sh` and `scripts/local-whmcs.sh` modified
- [ ] The rsync invocations, `$HOST`, excludes arrays, chown line, and post-deploy verification block are byte-for-byte unchanged (verify by reading the diff)
- [ ] Work committed in the worktree

## STOP conditions

Stop and report back (do not improvise) if:

- The "Current state" excerpts don't match the live files (drift since `b31e458`).
- `bash -n` fails twice after a reasonable fix attempt.
- Implementing a step appears to require touching the rsync flags, host, excludes,
  chown, or the post-deploy verification (these are out of scope).
- You are tempted to run a real deploy or SSH to `195.7.4.219` to "test" — do not;
  report that runtime verification needs the dev/prod environment instead.

## Maintenance notes

- The confirmation prompt requires a TTY; CI/automation must pass `--yes`. The
  non-interactive-without-`--yes` path deliberately `die`s rather than hanging on
  `read`.
- `--dry-run` still runs the full predeploy gate first (intentional: you preview
  the *exact* deploy that would happen, including its gate result). On a machine
  without docker the gate BLOCKs before the preview — that's expected.
- If a third module is ever added, it gets its own `rsync_module` call and
  automatically inherits the dry-run/confirm/preflight behaviour.
- Reviewer should confirm the prod-mutating rsync/chown path is unchanged and
  only newly *guarded*, not rewritten.
