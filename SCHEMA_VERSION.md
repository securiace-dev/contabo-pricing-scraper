# Schema versions

This project exposes two independent schema constants. Consumers (the WHMCS
addon, dashboards, billing tools) pin to whichever one is relevant to their use.

| Constant | Defined in | Scope |
|---|---|---|
| `SCHEMA_VERSION` | `src/main.rs` (const) | JSON/CSV output files + REST API payloads |
| `Installer::SCHEMA_VERSION` | `whmcs-module/.../lib/Installer.php` | `mod_contabo_*` database tables |

When making a schema-touching change, **bump the relevant constant and add an
entry below in the same commit**. The `contabo-pricing-schema-guard` skill
walks you through the decision; the pre-commit hook will remind you.

Format: most-recent version first. Always include date, scope (API vs DB),
sections (Added / Renamed / Removed / Migration).

---

## API 1.1 — current

### Added
- `contabo_view_model.json` — flat row-per-period view with `options_summary` per dimension. Emitted natively by the Rust scraper; synthesized by `enrich_output.js` (STEP 7) when the Node scraper is used. Consumers should treat this as the canonical analytics surface.
- API endpoints under `/api/v1/*` introduced in 2.3.0-dev. See [README.md](README.md#api-quick-start) for the inventory.

### Renamed / Removed
(none since 1.0)

### Migration
None required. Pin to schema_version `1.x` to remain stable across additive changes.

---

## API 1.0 — initial

Initial JSON/CSV emission schema covering `contabo_base_plans.json`,
`contabo_configs.json`, `contabo_pricing_dataset.json`,
`contabo_quick_reference.json`, `contabo_base_plans.csv`,
`contabo_option_catalog.csv`, `contabo_gap_report.json`,
`contabo_gap_summary.json`.

---

## WHMCS DB 1 — current

Tables created by `Installer::install()`:

- `mod_contabo_profile`
- `mod_contabo_profile_version`
- `mod_contabo_mapping`
- `mod_contabo_sync_log`
- `mod_contabo_settings`

### Migration

None required for fresh installs. Future schemas should add a `migrateToN()`
method on the installer; `Installer::upgrade()` runs them in sequence.

---

## Rules

1. **Adding a field**: minor bump. Document it under "Added".
2. **Renaming a field**: major bump. Document old → new mapping under "Renamed". Provide a migration block.
3. **Removing a field**: major bump. Document the removal and the replacement under "Removed". Provide a migration block.
4. **Reusing a field name for a different type or meaning**: forbidden. Pick a new name.
5. **Bumping `Cargo.toml` version**: does NOT imply a schema bump. Schema bumps are independent of project version, but schema majors should be timed to coincide with project majors.
6. **WHMCS DB schema changes** must also add a `migrateToN()` method on `Installer` so upgrades from older addon versions apply cleanly.
