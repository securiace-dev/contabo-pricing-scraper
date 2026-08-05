# Schema versions

This project exposes two independent schema constants. Consumers (the WHMCS
addon, dashboards, billing tools) pin to whichever one is relevant to their use.

| Constant | Defined in | Scope |
|---|---|---|
| `SCHEMA_VERSION` | `src/main.rs` (const) | JSON/CSV output files + REST API payloads |
| `CATALOG_SCHEMA_VERSION` | `src/api/catalog.rs` (const) | Rust → WHMCS catalog exchange envelope |
| `Installer::SCHEMA_VERSION` | `whmcs-module/.../lib/Installer.php` | `mod_contabo_*` database tables |

When making a schema-touching change, **bump the relevant constant and add an
entry below in the same commit**. The `contabo-pricing-schema-guard` skill
walks you through the decision; the pre-commit hook will remind you.

Format: most-recent version first. Always include date, scope (API vs DB),
sections (Added / Renamed / Removed / Migration).

---

## API 1.2 — current

### Added
- Canonical active product families: `Core VPS`, `Performance VPS`,
  `Max Performance VPS`, and `Storage VPS`.
- `family_aliases` on plan, configurator, view-model, and catalog compatibility
  records so `Cloud VPS`, `Cloud VPS Plus`, `Cloud VDS`, and `VDS` remain
  queryable without replacing the current product-line label.
- The active discovery catalog now records the pricing-page source URL,
  observation timestamp, verification counts, and per-family storage policy.

### Changed
- Default discovery follows the 22 active VPS/VDS/Storage pricing-card links
  observed on 2026-08-05. Legacy SKUs hydrated inside checkout payloads are no
  longer treated as active products.
- Hydrated products are selected by exact slug before title fallback, and
  impossible legacy storage options are filtered by product-line policy.

### Migration
Consumers displaying or grouping plan families should use `family` as the
current label and may match `family_aliases` for legacy filters. Consumers that
pin fixtures must move from `tests/fixtures/api/v1.1` to `v1.2`.

---

## API 1.1 — previous

### Added
- `contabo_view_model.json` — flat row-per-period view with `options_summary` per dimension. Emitted natively by the Rust scraper; synthesized by `enrich_output.js` (STEP 7) when the Node scraper is used. Consumers should treat this as the canonical analytics surface.
- API endpoints under `/api/v1/*` introduced in 2.3.0-dev. See [README.md](README.md#api-quick-start) for the inventory.
- Complete OpenAPI 3.0.3 route inventory at `/api/v1/openapi.json`.
- Required producer/consumer golden contracts under `tests/fixtures/api/v1.1/`.

### Contract fixtures

The Rust producer test and WHMCS PHP consumer test load the same five required
fixtures: `meta`, `plans`, `catalog`, `quote`, and `openapi`. Missing fixtures,
nested type drift, route drift, hash drift, or an undocumented version mismatch
fails the release gate. Fixture changes must update this document in the same
pull request, even when the change is additive and does not require a version
bump.

### Renamed / Removed
(none since 1.0)

### Migration
None required. Pin to schema_version `1.x` to remain stable across additive changes.

---

## Catalog exchange 1.0 — current

The `/api/v1/catalog` envelope is an independently versioned contract consumed
by `CatalogImportService::SUPPORTED_SCHEMA_VERSION`. It includes immutable
catalog and item hashes, stable machine/provider identifiers, observation and
effective timestamps, compatibility metadata, and the source plan payload.

The catalog version is intentionally independent from API/output schema 1.1.
An additive API change therefore cannot silently change the catalog importer
contract.

---

## API 1.0 — initial

Initial JSON/CSV emission schema covering `contabo_base_plans.json`,
`contabo_configs.json`, `contabo_pricing_dataset.json`,
`contabo_quick_reference.json`, `contabo_base_plans.csv`,
`contabo_option_catalog.csv`, `contabo_gap_report.json`,
`contabo_gap_summary.json`.

---

## WHMCS DB 14 — current

`Installer::SCHEMA_VERSION = 14`.

`install()` creates the v1 tables, stamps `schema_version = 1`, then runs the
idempotent `migrateTo2..14` chain — so both fresh installs and step-by-step
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

### Migration

Every migration remains additive and idempotent. Fresh installs and upgrades
must both finish at addon schema 14 and VPS suite schema 5. Rollback of
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
5. **Bumping `Cargo.toml` version**: does NOT imply a schema bump. Schema bumps are independent of project version, but schema majors should be timed to coincide with project majors.
6. **WHMCS DB schema changes** must also add a `migrateToN()` method on `Installer` so upgrades from older addon versions apply cleanly.
