# Plan 001: Harden the predeploy gate — preflight prerequisites, honest PHP-7.4 lint, and fixed runbook references

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise.
>
> **Drift check (run first)**: `git diff --stat b31e458..HEAD -- scripts/predeploy-check.sh CLAUDE.md`
> If either in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: MED (this is the production deploy gate — must stay fail-closed; never weaken it into a false PASS)
- **Depends on**: none
- **Category**: dx / correctness
- **Planned at**: commit `b31e458`, 2026-06-20

## Why this matters

`scripts/predeploy-check.sh` is the mandatory, fail-closed gate that must exit 0
before any production deploy of the WHMCS addon. Two real defects undermine it:

1. **It reports `[PASS] PHP 7.4 lint` without actually verifying PHP 7.4** when
   the `php:7.4-cli` docker image is unavailable — it silently falls back to the
   local PHP (8.5 on the maintainer's machine). The project's hard constraint is
   a **PHP 7.4 polyglot floor**; a `match`/enum/union-type/`readonly` change
   parses clean on 8.x, the gate goes green, and the floor the gate exists to
   enforce is never checked.
2. **When prerequisites are missing (no `vendor/`, no docker) it hard-FAILs
   opaquely** — `vendor/bin/phpunit: No such file or directory`, `docker:
   command not found` — indistinguishable from a real test/smoke failure, with
   no guidance. On any machine without the full dev stack the gate is
   unconditionally red and the operator can't tell "set up the tooling" from
   "the code is broken."

Plus, every reference to the runbook points at `docs/DEPLOY_RUNBOOK.md`, but
there is no repo-root `docs/` directory — the file lives at
`whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md`. Anyone
following the pointer hits a dead path.

After this lands: the gate (a) refuses to run with a clear, distinct **BLOCKED**
status when prerequisites are missing (never masquerading as PASS or as a code
failure), (b) never reports the 7.4 lint as PASS unless it actually ran under
PHP 7.4, and (c) points at the real runbook path.

## Current state

- `scripts/predeploy-check.sh` — the gate. Relevant excerpts as they exist today:

Line 7 (header comment):
```
# deploy MUST NOT proceed unless this script exits 0. See docs/DEPLOY_RUNBOOK.md.
```

Lines 20–26 (setup — `ADDON` and helpers; do not change, shown for context):
```
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ADDON="$REPO_ROOT/whmcs-module/modules/addons/contabo_pricing"

fail=0
declare -a results
record() { if [ "$2" -eq 0 ]; then results+=("[PASS] $1"); else results+=("[FAIL] $1"); fail=1; fi; }
stage()  { echo; echo "==================== $1 ===================="; }
```

Lines 29–31 (stage 1 — unit suite; the `vendor/bin/phpunit` invocation):
```
# ── 1) unit suite ────────────────────────────────────────────────────────────
stage "1/4 unit suite (phpunit)"
( cd "$ADDON" && vendor/bin/phpunit ); record "unit suite" $?
```

Lines 33–52 (stage 2 — the PHP 7.4 lint with the silent fallback). **This is the bug in defect #1**:
```
# ── 2) PHP 7.4 syntax lint ───────────────────────────────────────────────────
stage "2/4 PHP 7.4 syntax lint"
lint_status=0
files=$( { ls "$ADDON"/lib/*.php "$ADDON"/*.php "$ADDON"/templates/admin/*.tpl "$SCRIPT_DIR"/*.php ; } 2>/dev/null )
if docker image inspect php:7.4-cli >/dev/null 2>&1 || docker pull php:7.4-cli >/dev/null 2>&1; then
  rels=""
  for f in $files; do rels="$rels ${f#"$REPO_ROOT"/}"; done
  # One container, loop inside (fast): lint every file, fail if any fails.
  docker run --rm -v "$REPO_ROOT":/app -w /app php:7.4-cli sh -c '
    st=0; for f in "$@"; do php -l "$f" >/dev/null 2>&1 || { echo "  LINT FAIL (7.4): $f"; st=1; }; done; exit $st
  ' _ $rels
  lint_status=$?
  echo "  linted with php:7.4-cli"
else
  echo "  WARNING: php:7.4-cli (docker) unavailable — falling back to local php $(php -r 'echo PHP_VERSION;' 2>/dev/null); PHP 7.4 NOT verified"
  for f in $files; do
    php -l "$f" >/dev/null 2>&1 || { echo "  LINT FAIL: $f"; lint_status=1; }
  done
fi
record "PHP 7.4 lint" "$lint_status"
```

Lines 64–72 (summary block):
```
# ── summary ──────────────────────────────────────────────────────────────────
echo; echo "==================== predeploy gate summary ===================="
for r in "${results[@]}"; do echo "  $r"; done
if [ "$fail" -eq 0 ]; then
  echo "  GATE: PASS — safe to proceed to deploy (see docs/DEPLOY_RUNBOOK.md)"
  exit 0
fi
echo "  GATE: FAIL — DO NOT DEPLOY"
exit 1
```

- `CLAUDE.md` — repo-root project guide. Line 113 (inside the Deploy section):
```
3. Follow `docs/DEPLOY_RUNBOOK.md`: rsync to `root@195.7.4.219`, `--exclude '.claude-flow/'`,
```

**Repo conventions to match** (from the existing script): plain bash, `set -uo
pipefail` already at the top, two-space indentation, the `stage`/`record`
helpers, and box-style `==== … ====` stage banners. Match that style exactly.
The canonical runbook path (verified via `find`) is
`whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md`.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Shell syntax check | `bash -n scripts/predeploy-check.sh` | exit 0, no output |
| Run the gate here (no docker) | `bash scripts/predeploy-check.sh; echo "exit=$?"` | prints the BLOCKED summary, `exit=2` |
| Confirm refs fixed | `grep -rn "docs/DEPLOY_RUNBOOK.md" scripts/predeploy-check.sh CLAUDE.md` | no matches (exit 1) |
| Confirm new ref present | `grep -c "whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md" scripts/predeploy-check.sh` | ≥ 1 |

Note: this worktree has **no docker** and likely **no `vendor/bin/phpunit`** —
that is expected and is exactly the BLOCKED path you are adding. You cannot run
the full green gate here; that is fine and not a deviation.

## Scope

**In scope** (the only files you may modify):
- `scripts/predeploy-check.sh`
- `CLAUDE.md`

**Out of scope** (do NOT touch):
- `scripts/deploy.sh`, `scripts/local-whmcs.sh` — handled by a separate plan.
- `scripts/live-schema-smoke.sh`, `scripts/whmcs-integration-smoke.sh` — the
  smoke scripts themselves are fine; only the orchestrating gate changes.
- Any PHP under `whmcs-module/` — no module code changes here.

## Git workflow

- You are in an isolated worktree on a branch already created for you.
- Commit your work in this worktree when done. Message style — match the repo's
  conventional-commit style (recent example from `git log`:
  `chore(deploy): add deploy.sh for addon + provisioning module`). Suggested:
  `fix(gate): preflight prerequisites, enforce real 7.4 lint, fix runbook refs`.
- Do NOT push or open a PR.

## Steps

### Step 1: Add a preflight stage that blocks (not fails) on missing prerequisites

Insert a new **preflight** block immediately after the helper definitions
(after the `stage()` definition line, i.e. after current line 27) and **before**
stage 1. It must:

- Check `[ -x "$ADDON/vendor/bin/phpunit" ]`; if missing, print a remediation
  line naming the exact command `(cd "$ADDON" && composer install)`.
- Check `command -v docker >/dev/null 2>&1`; if missing, print a remediation
  line explaining the live-schema + integration smokes and the 7.4 lint all run
  inside docker, and to run the gate on a machine with the dockerised dev WHMCS
  stack (reference the runbook at its correct path).
- If any prerequisite is missing, print a clearly-labelled summary and
  `exit 2` — a distinct code from the FAIL path (`exit 1`) so callers and humans
  can tell "blocked / cannot run here" from "ran and failed."

Target shape (match surrounding style):
```bash
# ── 0) preflight — gate prerequisites ────────────────────────────────────────
stage "0/4 preflight — verifying gate prerequisites"
preflight_ok=1
if [ ! -x "$ADDON/vendor/bin/phpunit" ]; then
  echo "  MISSING: $ADDON/vendor/bin/phpunit"
  echo "           fix: (cd \"$ADDON\" && composer install)"
  preflight_ok=0
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "  MISSING: docker — the live-schema + integration smokes and the PHP 7.4 lint run inside docker."
  echo "           Run this gate on a machine with the dockerised dev WHMCS stack."
  echo "           See whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md"
  preflight_ok=0
fi
if [ "$preflight_ok" -ne 1 ]; then
  echo
  echo "==================== predeploy gate summary ===================="
  echo "  GATE: BLOCKED — prerequisites missing; cannot run the gate here."
  echo "  This is NOT a code failure. Resolve the items above, then re-run. DO NOT DEPLOY."
  exit 2
fi
```

**Verify**: `bash -n scripts/predeploy-check.sh` → exit 0.

### Step 2: Make the PHP 7.4 lint fail closed when it cannot run under 7.4

In the stage-2 `else` branch (the docker-unavailable fallback), the gate must
**not** record a PASS. With Step 1 in place this branch is unreachable in normal
runs, but keep it defensive: if execution ever reaches it, set
`lint_status=1` so the floor is treated as unverified → gate FAIL. Change the
`else` branch so it prints that 7.4 could NOT be verified and forces failure:

```bash
else
  echo "  ERROR: php:7.4-cli (docker) unavailable — cannot verify the PHP 7.4 polyglot floor."
  echo "         The gate will FAIL closed rather than report an unverified PASS."
  lint_status=1
fi
```

Leave the docker (`if`) branch exactly as-is. Leave `record "PHP 7.4 lint" "$lint_status"` as-is.

**Verify**: `bash -n scripts/predeploy-check.sh` → exit 0; and
`grep -n "PHP 7.4 NOT verified" scripts/predeploy-check.sh` → no matches (the
old silent-PASS warning string is gone).

### Step 3: Fix the runbook path references

Replace `docs/DEPLOY_RUNBOOK.md` with
`whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md` in:
- `scripts/predeploy-check.sh` line 7 (header comment)
- `scripts/predeploy-check.sh` line ~68 (the `GATE: PASS` echo)
- `CLAUDE.md` line 113 (the `3. Follow ...` deploy step)

Change only the path string; leave surrounding text intact.

**Verify**:
- `grep -rn "docs/DEPLOY_RUNBOOK.md" scripts/predeploy-check.sh CLAUDE.md` → no
  matches (i.e. every remaining occurrence is the full corrected path).
- `grep -c "whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md" scripts/predeploy-check.sh` → `2`.

### Step 4: Confirm the BLOCKED path works end-to-end in this worktree

This worktree has no docker (and probably no vendor), so the gate should now
take the BLOCKED path.

**Verify**: `bash scripts/predeploy-check.sh; echo "exit=$?"` →
output contains `GATE: BLOCKED` and ends with `exit=2`. (It must NOT print
`GATE: PASS` and must NOT reach the unit/lint/smoke stages.)

## Test plan

This is shell tooling with no unit-test harness; verification is behavioural:

- `bash -n scripts/predeploy-check.sh` exits 0 (no syntax regressions).
- On a machine without docker (this worktree): the gate prints `GATE: BLOCKED`
  and exits 2 (Step 4).
- Static greps in Step 3 confirm the path fix.
- No new test files are created. If a `tests/` harness for shell existed we'd
  extend it; none does (confirmed: tooling is verified by `bash -n` + the gate's
  own run).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `bash -n scripts/predeploy-check.sh` exits 0
- [ ] `bash scripts/predeploy-check.sh; echo $?` prints `GATE: BLOCKED` and exits `2` (in this docker-less worktree)
- [ ] `grep -rn "docs/DEPLOY_RUNBOOK.md" scripts/predeploy-check.sh CLAUDE.md` returns no matches
- [ ] `grep -n "PHP 7.4 NOT verified" scripts/predeploy-check.sh` returns no matches
- [ ] `git status` shows only `scripts/predeploy-check.sh` and `CLAUDE.md` modified
- [ ] Work committed in the worktree

## STOP conditions

Stop and report back (do not improvise) if:

- The "Current state" excerpts don't match the live file (drift since `b31e458`).
- `bash -n` fails twice after a reasonable fix attempt.
- The change appears to require editing any out-of-scope file.
- You find that `docker` *is* available in the worktree — then Step 4's expected
  `exit=2` won't hold; report this rather than forcing it (the BLOCKED path is
  only taken when a prerequisite is genuinely missing).

## Maintenance notes

- The preflight currently checks `vendor/bin/phpunit` and `docker`. If a future
  gate stage needs another tool (e.g. a specific CLI), add it to the same
  preflight block so prerequisites stay declared in one place.
- `exit 2` = BLOCKED, `exit 1` = FAIL, `exit 0` = PASS. `deploy.sh` aborts on any
  non-zero, so BLOCKED still correctly prevents deploy. Keep this code mapping if
  `deploy.sh` is ever changed to branch on exit codes.
- Reviewer should scrutinise that the 7.4 `else` branch can never silently pass.
