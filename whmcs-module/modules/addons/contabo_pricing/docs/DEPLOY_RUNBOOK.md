# contabo_pricing — Deployment Runbook

Production target: **my.securiace.com** — addon at
`/var/www/my_securiace_usr/data/www/my.securiace.com/modules/addons/contabo_pricing`,
provisioning module at `…/modules/servers/contabo_vps/`,
owned by `my_securiace_usr:my_securiace_usr`, served on PHP 8.1.x.

> **Scope rule (hard):** every production action is confined to the
> `contabo_pricing` and `contabo_vps` module directories. Never touch other
> admin pages, clients, invoices, services, products, or server config. Prod
> deploy is a **manual rsync** — there is intentionally **no CI/CD workflow
> that deploys WHMCS modules** (the GitHub `Release` workflow only builds the
> Rust scraper's Docker image on `v*` tags).

---

## 1. MANDATORY pre-deploy gate

A prod deploy MUST NOT proceed unless this exits `0`:

```bash
bash scripts/predeploy-check.sh        # run from the repo root
```

It runs, fail-closed (gate fails if any stage fails), entirely against **local**
files + the **dev** WHMCS — never prod:

| Stage | What |
|------|------|
| 1 | Unit suite (PHPUnit, FakeCapsule) |
| 2 | PHP **7.4** syntax lint (the polyglot floor) of `lib/*.php`, the entrypoints, `templates/admin/*.tpl`, and repo-root `scripts/*.php` |
| 3 | Live-schema smoke vs dev WHMCS **8.13 + 9.0** (`information_schema` only) |
| 4 | Real-WHMCS integration smoke (apply / drift / observe end-to-end on dev) |

The smoke scripts live at **repo-root `scripts/`** (alongside `local-whmcs.sh`),
**not** under the addon directory:
- `scripts/predeploy-check.sh` — the gate.
- `scripts/live-schema-smoke.{php,sh}` — env-gated (`CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1`), read-only `information_schema` check (asserts `tblhosting.amount`/`firstpaymentamount` exist, the v6 `expected_hash` columns exist; skips safely without the flag/credentials).
- `scripts/whmcs-integration-smoke.sh` — syncs the addon into dev WHMCS then runs `tests/integration/whmcs_smoke.php` inside the 8.13 container.

---

## 2. Production deploy

**Normal workflow — use `deploy.sh`** (runs the gate, detects changes, deploys
both modules, chowns, verifies):

```bash
bash scripts/deploy.sh        # run from the repo root
```

`deploy.sh` covers both the addon and the provisioning module automatically:
- Runs `predeploy-check.sh` first — aborts on failure.
- Detects per-module changes via `rsync --dry-run`; skips a module if nothing changed.
- Deploys only what changed, then chowns both destinations.
- Verifies `AdminController::VERSION` and PHP lint on prod for both modules.

After the script finishes, **load the addon admin page once** so
`SchemaHealth::assertOrMigrate()` runs the schema migration (if any).

### Manual rsync (fallback / reference)

Use this only if `deploy.sh` is unavailable or you need to deploy a single
module in isolation.

**Addon (`contabo_pricing`):**
```bash
ADDON=whmcs-module/modules/addons/contabo_pricing
DEST='root@195.7.4.219:/var/www/my_securiace_usr/data/www/my.securiace.com/modules/addons/contabo_pricing/'
rsync -rlptzc -i --no-owner --no-group \
  --exclude vendor/ --exclude tests/ --exclude phpunit.xml \
  --exclude '.phpunit.cache' --exclude '.phpunit.result.cache' \
  --exclude composer.lock --exclude '.git*' --exclude '.claude-flow/' \
  -e 'ssh -o BatchMode=yes -o ConnectTimeout=15' \
  "$ADDON/" "$DEST"
ssh -o BatchMode=yes root@195.7.4.219 \
  "chown -R my_securiace_usr:my_securiace_usr ${DEST#*:}"
```

**Provisioning module (`contabo_vps`):**
```bash
VPS=whmcs-module/modules/servers/contabo_vps
VDEST='root@195.7.4.219:/var/www/my_securiace_usr/data/www/my.securiace.com/modules/servers/contabo_vps/'
rsync -rlptzc -i --no-owner --no-group \
  --exclude '.git*' --exclude '.claude-flow/' \
  -e 'ssh -o BatchMode=yes -o ConnectTimeout=15' \
  "$VPS/" "$VDEST"
ssh -o BatchMode=yes root@195.7.4.219 \
  "chown -R my_securiace_usr:my_securiace_usr ${VDEST#*:}"
```

**Gotchas (learned the hard way):**
- **`--no-owner --no-group`** — the source is a macOS uid (501); without these,
  rsync-as-root maps that uid onto the prod box. Always chown afterwards.
- **`vendor/` is excluded** — prod has no `vendor/autoload.php`; the addon's
  stub autoloader (`ContaboPricing\` → `lib/`) loads every class, so new `lib/`
  files work with no `composer dump-autoload`.
- **`tests/` excluded** — never ship tests to prod.
- **`.claude-flow/` excluded** — local tool-state directory; must never reach prod.
- **zsh word-splitting** — an unquoted `$SSH` variable is NOT word-split in zsh
  (`command not found: ssh -o …`). Write the `ssh` command **inline**, not via a
  variable, or run the deploy steps under bash.
- **No `--delete`** — never remove prod files not in the source.

### Post-deploy verification (manual)
```bash
ssh root@195.7.4.219 "D=/var/www/my_securiace_usr/data/www/my.securiace.com; \
  grep -m1 'const VERSION' \$D/modules/addons/contabo_pricing/lib/AdminController.php; \
  for f in \$D/modules/addons/contabo_pricing/lib/*.php \
            \$D/modules/servers/contabo_vps/lib/*.php \
            \$D/modules/servers/contabo_vps/contabo_vps.php; do \
    php -l \"\$f\" >/dev/null || echo LINT_FAIL \$f; done; echo lint_ok"
```
Then load the addon admin page once so `SchemaHealth::assertOrMigrate()` runs.

---

## 3. Database & migrations

- The addon's schema migration runs automatically via
  `SchemaHealth::assertOrMigrate()` on the next admin page load (and on activate
  / upgrade). It is **idempotent** and version-gated (`Installer::SCHEMA_VERSION`).
- **No manual SQL on prod.** No deploy step writes the WHMCS DB.
- A deploy that does not bump `Installer::SCHEMA_VERSION` performs **no migration**
  (assertOrMigrate is a no-op when already current).
- **0.6.0 / Phase C** bumps the schema to **v7** — `migrateTo7` adds the
  idempotent `mod_contabo_profile.expose_configurable_options` column
  (`TINYINT DEFAULT 1`). On an existing prod (currently at v6) the first admin
  page load after deploy runs it; confirm with
  `SELECT value FROM mod_contabo_settings WHERE \`key\`='schema_version'` → `7`.

## 4. Rollback

- **Code:** redeploy the previous tag's addon dir (same rsync + chown).
- **Addon-created WHMCS config objects:** use the maintenance page's
  config-object-aware purge (`ConfigPurgeService`, scoped to the link tables) —
  never hand-delete WHMCS config objects.

## 5. Release-gate checklist (before tagging a release)

1. `scripts/predeploy-check.sh` → green.
2. CHANGELOG entry finalized; version bumped in `AdminController::VERSION` (+ the
   `Installer::SCHEMA_VERSION` only if the schema changed).
3. Commit + push to `origin/main`.
4. `bash scripts/deploy.sh` (explicit approval required). Deploys both modules if changed.
5. Load the addon admin page once — `SchemaHealth::assertOrMigrate()` runs the migration.

## 6. Contabo VPS provisioning module (`modules/servers/contabo_vps/`)

A separate WHMCS server/provisioning module. `deploy.sh` deploys it automatically
alongside the addon whenever it has local changes — no separate step needed.
`predeploy-check.sh` covers it too: its PHPUnit suite (stage 2) and the PHP 7.4
lint of its lib/entrypoints/tests (stage 3) gate every deploy. Full contract:
`docs/PROVISIONING_CONTRACT.md`.

- **No DB schema of its own.** The created instance id lives in the service
  custom field `contabo_instance_id` — **auto-created on the product at first
  provision** (no manual step). The instance's Contabo displayName carries a
  `whmcs-{serviceid}` tag; destructive actions verify it and sync restores it.
- **Credentials** live in WHMCS server config (Setup → Servers), encrypted at
  rest by WHMCS: Username = OAuth2 `client_id`, Password = `client_secret`,
  Access Hash = `apiUser:apiPassword`. They are redacted from `logModuleCall`
  (plus a recursive sanitizer for secret-bearing payload keys) and never logged
  in plaintext; SSL verification is enforced on every call.
- **Activation:** link a WHMCS product to the `contabo_vps` server module and
  set config options 1–6 (image id + region fallbacks, SSH secret id, Contabo
  product id, optional cloud-init, optional add-ons JSON). On configurable
  products, curated customer selections (Image/Region/Private Networking)
  override the fallbacks via the addon link tables. Use the module's
  **Test Connection** before ordering.
- **Passwords:** the service password is pushed to the Contabo vault
  (`whmcs-svc-{id}-root`) and rides in as `rootPassword`, so what WHMCS shows
  the customer works on the server. Reset Password (admin + client) rotates it.
- **Billing:** the WHMCS cycle maps to Contabo's contract `period`
  (Monthly→1 … Annually→12; bi/triennial→12, logged).
- Lifecycle: Contabo has no native suspend — Suspend/Unsuspend map to power
  stop/start; Terminate issues a cancel (`{}` body, cancelDate logged) and
  best-effort vault cleanup; ChangePackage is a manual-intervention message
  (no live resize). IP/status sync happens on admin/client page views, the
  "Sync from Contabo" admin button, and a bounded DailyCronJob sweep.
