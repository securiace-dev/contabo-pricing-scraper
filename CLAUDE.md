# Contabo Pricing — project guide for Claude Code

This repo is a **WHMCS addon** (`whmcs-module/modules/addons/contabo_pricing`) that
scrapes Contabo VPS/VDS pricing and syncs it into WHMCS products via versioned
profiles, plus a companion provisioning module (`modules/servers/contabo_vps`).
There is **no Node build** and **no ruflo/swarm tooling** — ignore any generic
agent-swarm instructions. Work the code directly.

## Where things live

```
whmcs-module/modules/addons/contabo_pricing/
  lib/            PHP classes (PSR-4 ContaboPricing\, autoload via composer)
  templates/admin/*.tpl   plain-PHP admin views (NOT Smarty; require _layout_open.tpl)
  assets/app.js   vanilla JS for the admin UI (no framework, no bundler)
  tests/          PHPUnit; FakeCapsule stubs WHMCS\Database\Capsule (no real DB)
  docs/           DEPLOY_RUNBOOK.md, PHASE_*_*.md design specs
scripts/          predeploy-check.sh (the gate), local-whmcs.sh (docker dev render)
data/output/      scraped Contabo dataset (contabo_pricing_dataset.json, …)
```

## Pricing architecture (read before touching pricing) — see docs/PHASE_D_PRICING_SPEC.md

Two layers. Keep them separate.

- **Profile = SOURCE.** What we buy from Contabo and what it costs us. A profile
  sources a price for **every** cycle via `period_prices_json` on each
  `profile_version`: scraped 1/3/6/12 (EUR `effective_monthly`) plus projected
  24/36. `published_cycles_mask` is the offered superset (default 63 = all six).
- **Mapping = CUSTOMER.** What a customer pays on a specific WHMCS product.
  `mod_contabo_mapping.catalog_cycles_mask` is the **customer-facing gate** (which
  cycles reach `tblpricing`/checkout); `markup_overrides_json` + `rounding_mode` set
  the price; optional `source_overrides_json` pins a per-product cost basis.

**Fallback rule (one rule, everywhere):** `Source(M)` = `effective_monthly` of the
**longest scraped period whose months ≤ M**. So 24/36 → the 12-mo rate; a *missing*
quarterly → the 1-mo rate (never a deeper longer-cycle discount). Implemented in
`SyncEngine::periodPriceVectorFromPlan()` / `nearestSourceRate()`, mirrored in
`RenewalEngine::resolveCycleEurMonthly()` and `assets/app.js` (`sourceRateForMonths`).

- **Catalog** writes go through `SyncEngine` → `tblpricing` ONLY (a static-grep test
  forbids `tblhosting` writes from there). New orders get the latest price.
- **Renewal** writes go through `RenewalEngine`/`ScheduledChangeProcessor` →
  `tblhosting.recurringamount`, gated per-service by `mod_contabo_service_policy` +
  the global `repricing_phase`. Existing customers are **grandfathered** until their
  own cycle boundary; only push-enabled services move. Never touch an open invoice.
- EUR→local conversion lives in **one** place: `ProfileVersionInput::toLocalMonthly()`
  (GST then FX). Don't reimplement it. GST placement is documented there
  (`GST-PLACEMENT:` note) — currently on the cost basis, by owner decision.
- The shared cost basis means catalog and renewal stay consistent; if you change one
  engine's basis, change the other in the same commit.

## Modes

- **Fixed (`fixed_admin_profile`)** — pre-packaged SKU. Admin pins every configurator
  dimension; **no** configurable/add-on options are exposed to customers. Save is
  rejected unless every required dimension has a concrete value
  (`AdminController::fixedCompletenessError`).
- **Configurable (`customer_configurable_product`)** — customer picks exposed options;
  curate exposure via the Exposure editor / `ConfigOptionLinkRepository`.
- Contabo's API takes exactly **one** image (OS/Apps/Panels/Blockchain are categories
  of a single choice). `ImageOptionNormalizer` collapses them — never split.

## Delete is recoverable; purge is guarded

- Profiles soft-delete (`deleted_at`) → Trash → Restore (Undo). Default listings
  exclude trashed rows (`ProfileManager::listProfiles` uses `whereNull('deleted_at')`).
- **Never hard-delete a profile without `ProfilePurgeService`.** It refuses while an
  active mapping or a live `tblhosting` service references the profile, requires the
  typed phrase (`SchemaHealth::isPurgeConfirmed`), cascades **only that profile's**
  rows + the WHMCS config objects it created, and writes a `logActivity` audit.

## Schema migrations

- `lib/Installer.php`: `SCHEMA_VERSION` + one `migrateToN()` per bump. Every change is
  **additive + idempotent** (`hasColumn`/`hasTable`-guarded). `SchemaHealth::assertOrMigrate`
  runs on admin page load. Add new required columns to `SchemaHealth::REQUIRED_COLUMNS`.
- LONGTEXT for JSON blobs — **never** the native JSON column type (FastPanel PHP 7.4 +
  unknown MySQL/MariaDB JSON support).

## Coding constraints

- **PHP 7.4 polyglot floor.** No `match`, no enums, no `readonly`, no constructor
  promotion, no union types, no `str_starts_with`. Typed properties are fine.
  `composer lint` (`php -l` over lib + templates) enforces parse-level; the gate lints
  under php:7.4-cli.
- Templates are plain PHP (`<?= $esc(...) ?>`), start with `require _layout_open.tpl`,
  end by closing the `.cb-wrap` div. There is **no** `_layout_close.tpl`.
- `assets/app.js` is ES5-ish vanilla JS (no modules); keep `node --check` clean.

## Tests

- `cd whmcs-module/modules/addons/contabo_pricing && vendor/bin/phpunit` (PHPUnit 10,
  runs on PHP 8.x but code must stay 7.4-compatible).
- `tests/FakeCapsule.php` is an in-memory `Capsule` stub: seed `Capsule::$tables[...]`,
  assert via `Capsule::$calls` / `Capsule::$inserts`. It supports `whereNull`/
  `whereNotNull` (NULL semantics matter — real `where('col', null)` is `= NULL`, which
  never matches; use `whereNull`).
- Always run the full suite after a change; keep it green before moving on.

## Coordinating parallel edits

`lib/AdminController.php` (~3k lines) and `templates/admin/profiles.tpl` are the
collision points — **serialize** edits to each (one change at a time), and only
parallelize work on genuinely disjoint files. `mappings.tpl` and `profiles.tpl` are
disjoint.

## Deploy (SSH is permission-gated; never deploy without an explicit go)

1. `bash scripts/predeploy-check.sh` must be **GREEN** (unit + PHP 7.4 lint +
   live-schema smoke 8.13/9.0 + integration smoke). Fail-closed; no deploy on red.
2. Dev render: the canonical local WHMCS stack is now **`~/Projects/whmcs-devbox`**
   (`whmcs-devbox use contabo-pricing` → `up` → `render 8 dashboard`); it mounts
   both modules live into WHMCS 8.13 + 9.0. `scripts/local-whmcs.sh render 8 …`
   is the legacy in-repo equivalent (kept working). NOTE: a fresh devbox DB needs
   the one-time WHMCS install wizard before the live-schema/integration smokes run.
3. Follow `whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md`: rsync to `root@195.7.4.219`, `--exclude '.claude-flow/'`,
   chown, verify `AdminController::VERSION` + lint. Prod only on green gate + approval.

## Git

- Commit/push only when asked. Branch off if on the default branch.
- Do not add a `Co-Authored-By` trailer unless `.claude/settings.json` enables it.
- Do not put model identifiers in commits, PRs, code, or comments.

## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `/opt/homebrew/opt/python@3.14/bin/python3.14 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current (system python3 is 3.10 and lacks the graphify module)
