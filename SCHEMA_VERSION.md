# Schema versions

This project carries independent schema versions. Consumers pin to whichever one
is relevant to their use.

| Contract | Defined in | Scope |
|---|---|---|
| API 1.1 plan shape | `tests/fixtures/api/v1.1/` (golden fixtures) | Plan/catalog/quote JSON shape the addon reads and produces |
| Catalog exchange 1.0 | `CatalogImportService::SUPPORTED_SCHEMA_VERSION` | Catalog envelope the addon imports and publishes |
| `Installer::SCHEMA_VERSION` | `whmcs-module/.../lib/Installer.php` | `mod_contabo_*` database tables |

> **Historical note:** until 2026-10-01 (last commit `aa275b6`) the first two
> contracts were also emitted by a Rust service (`SCHEMA_VERSION` in
> `src/main.rs`, `CATALOG_SCHEMA_VERSION` in `src/api/catalog.rs`) over
> `/api/v1/*` and `data/output/*` files. That service is removed; the addon now
> owns both contracts, and the `*.rust-generated.json` fixtures are frozen parity
> vectors from that service.

When making a schema-touching change, **bump the relevant version and add an
entry below in the same commit**. The pre-commit hook will remind you.

Format: most-recent version first. Always include date, scope (API vs DB),
sections (Added / Renamed / Removed / Migration).

---

## API 1.1 — current

### Added
- `contabo_view_model.json` (historical) — flat row-per-period view with `options_summary` per dimension, formerly emitted by the Rust scraper. It is no longer produced at runtime.
- Historical: `/api/v1/*` REST routes (meta, plans, catalog, quote, openapi) introduced in 2.3.0-dev; frozen as golden fixtures under `tests/fixtures/api/v1.1/`. The addon implements the same shapes in PHP.
- Golden contracts under `tests/fixtures/api/v1.1/` pin the plan shape.

### Contract fixtures

The addon's PHP tests load the required fixtures `meta`, `plans`, `catalog`,
`quote`, and `openapi`, plus the frozen `*.rust-generated.json` parity vectors.
Missing fixtures, nested type drift, hash drift, or an undocumented version
mismatch fails the release gate. Fixture changes must update this document in
the same pull request, even when the change is additive and does not require a
version bump.

### Renamed / Removed
(none since 1.0)

### Migration
None required. Pin to schema_version `1.x` to remain stable across additive changes.

---

## Catalog exchange 1.0 — current

The catalog envelope (formerly served at `/api/v1/catalog`) is an independently versioned contract consumed
by `CatalogImportService::SUPPORTED_SCHEMA_VERSION`. It includes immutable
catalog and item hashes, stable machine/provider identifiers, observation and
effective timestamps, compatibility metadata, and the source plan payload.

The catalog version is intentionally independent from API/output schema 1.1.
An additive API change therefore cannot silently change the catalog importer
contract.

---

## API 1.0 — initial

Historical. Initial JSON/CSV file emission schema (data/output artefacts of the removed Rust/Node scraper) covering `contabo_base_plans.json`,
`contabo_configs.json`, `contabo_pricing_dataset.json`,
`contabo_quick_reference.json`, `contabo_base_plans.csv`,
`contabo_option_catalog.csv`, `contabo_gap_report.json`,
`contabo_gap_summary.json`.

---

## WHMCS DB 16 — current

`Installer::SCHEMA_VERSION = 16`.

`install()` creates the v1 tables, stamps `schema_version = 1`, then runs the
idempotent `migrateTo2..16` chain — so both fresh installs and step-by-step
upgrades converge to the current shape. Each `migrateToN` is guarded by
`hasTable`/`hasColumn`.

Highlights by version:
- **v2** — Renewal Pricing Policy Engine: `mod_contabo_service_policy`,
  `mod_contabo_price_decision`, `mod_contabo_pricing_action`,
  `mod_contabo_price_change_schedule`, `mod_contabo_price_notice`,
  `mod_contabo_repricing_lock`; additive policy/markup columns on
  `mod_contabo_profile` + `mod_contabo_profile_version`.
- **v3–v5** — mapping refits, catalog audit, profile identity, and the
  configurable-options link tables (`mod_contabo_config_*`).
- **v6** — `expected_hash` on the config-option link tables.
- **v7 (Phase C)** — `mod_contabo_profile.expose_configurable_options`
  (`TINYINT NOT NULL DEFAULT 1`). Master switch: when 0,
  `ConfigurableOptionsSyncer::apply()` skips WHMCS config-option group creation.
- **v8** — source/customer pricing separation, per-period source vectors,
  recoverable profile deletion, and mapping source overrides.
- **v9** — WHMCS-native SecuriAce VPS schema: sealed order snapshots,
  resources, durable operations/attempts/provider requests, leases,
  capabilities, reconciliation, adoption, billing sagas, audit events, and
  operator commands.
- **v10** — versioned catalog imports/publications, approval history,
  one-time secrets, communications, and additive provisioning controls.
- **v11** — operation-bound encrypted one-time reveal tokens.
- **v12** — idempotent customer lifecycle email-template installation.
- **v13** — WHMCS-owned provider snapshot inventory projection.
- **v14** — expiring fenced claims for operator-command and communication
  workers plus explicit lease defaults.
- **v15** — WHMCS-native catalog scraping: `mod_contabo_scrape_sources`
  (provider registry, 4 seeded disabled rows), `mod_contabo_scrape_runs`,
  `mod_contabo_scrape_run_attempts`, `mod_contabo_decisions`, and
  `mod_contabo_catalog_versions.envelope_json` (`LONGTEXT NULL`).
- **v16** — self-learning family registry: `mod_contabo_scrape_families`
  (one row per upstream `categories[].id`: slug, title, nav title/href/position,
  `status` active|hidden|retired|new, first/last seen, `last_plan_count`,
  `typical_plan_count` (median of `plan_count_history_json`, last 5 accepted
  runs), `title_history_json`, `plan_slugs_json`, `sample_product_url`,
  `approved`, `admin_hidden`, `display_name`, `successor_of`, `notes`) and
  `mod_contabo_scrape_run_attempts.nav_titles_json` (the category titles the
  fetched page rendered). `scrape.plan_urls_json` becomes an explicit override
  only; the migration clears the old 16-URL default so it cannot pin a stale list.

### Migration

Every migration remains additive and idempotent. Fresh installs and upgrades
must both finish at addon schema 16 and VPS suite schema 5. Rollback of
application code does not drop columns or tables; it requires a compatible
previous release artifact and the documented deployment runbook.

---

## WHMCS DB 1 — initial

Tables created by the original `Installer::install()` (v1 shape):

- `mod_contabo_profile`
- `mod_contabo_profile_version`
- `mod_contabo_mapping`
- `mod_contabo_sync_log`
- `mod_contabo_settings`

---

## Rules

1. **Adding a field**: minor bump. Document it under "Added".
2. **Renaming a field**: major bump. Document old → new mapping under "Renamed". Provide a migration block.
3. **Removing a field**: major bump. Document the removal and the replacement under "Removed". Provide a migration block.
4. **Reusing a field name for a different type or meaning**: forbidden. Pick a new name.
5. **Bumping an addon or module release version** does NOT imply a schema bump. Schema bumps are independent of release version, but schema majors should be timed to coincide with release majors.
6. **WHMCS DB schema changes** must also add a `migrateToN()` method on `Installer` so upgrades from older addon versions apply cleanly.
