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

## API 1.2 — current

Adds two new product families and per-family typing so the dataset is unified across
Cloud VPS / VDS / Storage VPS **plus Dedicated Servers and Object Storage**.

### Added
- **New families** in `plans[].family`: `"Dedicated Server"` (Contabo `type:"ds"`) and
  `"Object Storage"` (`type:"object-storage"`).
- `plans[].pricing_model` — `"fixed"` (VPS/VDS/Storage), `"fixed_plus_setup"` (Dedicated),
  `"metered_capacity"` (Object Storage). Consumers branch instead of assuming fixed recurring.
- `plans[].availability` — `{ out_of_stock, unavailable }` (drives WHMCS Stock Control).
- `specs_parsed.setup_fee_eur` — for all plans (0 where none).
- **Dedicated** options: OS images → `Image`; the paid `addons` catalog → `Add-on` rows (Yes/No,
  `category` ∈ Control Panel/RAM/Storage/GPU/Security/Networking/Management/Software/Misc);
  datacenter choices → `Region`. `specs_parsed` now parses the "N x F GHz" / "N x SIZE" forms.
- **Object Storage** `specs_parsed`: `{ capacity_tb: 0.25, price_per_tb_eur, region,
  included_traffic: "free_egress", setup_fee_eur }` (base = 250 GB tier; per-TB = base × 4).

### Migration
None — additive. Consumers pinned to `1.x` stay stable. Object-Storage products are matched via a
`#slug` URL fragment (they share one catalog page and have no per-product URL).

---

## API 1.1

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

## WHMCS DB 7 — current (`Installer::SCHEMA_VERSION = 7`)

`install()` creates the v1 tables, stamps `schema_version = 1`, then runs the
idempotent `migrateTo2..7` chain — so both fresh installs and step-by-step
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

### Added (v7)
- `mod_contabo_profile.expose_configurable_options` — via `migrateTo7`.

### Migration
`migrateTo7` is idempotent (`hasColumn` guard). It is a **separate** migration
on purpose: installs already at v6 never re-run earlier migrations, so folding
the column into `migrateTo2` would have left upgraded installs without it.

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
