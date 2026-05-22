# WHMCS Contabo Pricing Addon

Syncs Contabo VPS / Storage VPS / Cloud VDS pricing into WHMCS products via versioned profiles. Talks to the [`contabo-pricing` API server](../) — never scrapes Contabo directly.

## Requirements

- WHMCS **8.0+**  (uses Capsule, AbstractWidget, modern hook signatures)
- PHP **8.1+**
- A running `contabo-pricing` API server reachable from the WHMCS host (set the URL in addon Settings)

## Concepts

- **Profile** — a named template: `(plan_slug, period_months, region, OS, options[])`. Stored in `mod_contabo_profile`.
- **Profile Version** — immutable pricing snapshot of a profile at a point in time. Every sync that detects a change creates a new version. Old versions retained for diff + rollback. Stored in `mod_contabo_profile_version`.
- **Mapping** — links a Contabo Profile to a WHMCS product (`tblproducts.id`) and chooses which billing cycles (monthly / semi-annually / annually) receive the synced price. One profile can drive many products.
- **Sync Run** — execution of `SyncEngine::run()`: pulls fresh `/api/v1/plans`, diffs against the latest version of each profile, optionally pushes new prices to mapped products. Writes a row to `mod_contabo_sync_log` summarising what changed.

## Install

```bash
# In your WHMCS install:
cd /path/to/whmcs/modules/addons/
git clone <repo> contabo_pricing_src
mv contabo_pricing_src/whmcs-module/modules/addons/contabo_pricing .
rm -rf contabo_pricing_src

cd contabo_pricing
composer install --no-dev --optimize-autoloader

# Then in WHMCS:
#   1. Setup → Addon Modules → Contabo Pricing → Activate
#   2. Click Configure → fill in API base URL + bearer token
#   3. Tick the admin role(s) that should see the addon → Save
#   4. Addons → Contabo Pricing → opens the dashboard
```

The Composer step is optional — the addon ships with a stub autoloader that works without `vendor/`. Composer is only required if you want PHPUnit for the test suite.

### Bearer token encryption at rest

The bearer token is stored encrypted in `tbladdonmodules` via WHMCS's native `encrypt()` helper — no third-party crypto library is introduced and no separate key store is required. The first time you save the token in the WHMCS Settings form the addon detects the plaintext row on its next read, calls `encrypt()`, and writes the value back with an `ENC:` prefix. From that point on every read decrypts via `decrypt()` transparently; no manual migration step is required.

Because encryption is keyed off the WHMCS install's encryption key from `configuration.php`, that key must be preserved across backup/restore. If the encryption key is regenerated, the stored ciphertext is unrecoverable — re-enter the bearer token in the addon Settings form and the auto-migration will re-encrypt it under the new key.

## First-time workflow

```
Activate addon                                    (creates mod_contabo_* tables)
   ↓
Open addon → Settings tab                         (confirm API base URL works)
   ↓
Open addon → Profiles → Create a new profile      (one per plan/period combo)
   ↓
Open addon → Mappings → link profile to a WHMCS product + tick cycles
   ↓
Open addon → Run sync now                         (first sync creates Version 1)
   ↓
Repeat                                            (DailyCronJob hook keeps it fresh)
```

## Sync strategies (per-profile)

| Strategy | Behaviour |
|---|---|
| `manual` | New versions are recorded but no products are touched. Admin reviews diffs in the UI and applies manually. |
| `notify` (default) | New versions are recorded + admin gets an email summary, products are NOT auto-updated. |
| `auto-apply` | New versions are recorded AND the mapped WHMCS products' prices are updated in `tblpricing` per the configured cycles. |

Mix strategies per profile: keep production products on `notify` (so a human approves before invoices change) and dev products on `auto-apply`.

## Pricing math

The addon mirrors the API `/api/v1/quote` formula exactly, so the "Final / mo" column in the Profile History matches what an admin sees in the report UI:

```
final_monthly = base_eur × (1 + gst_pct) × fx_rate × (1 + fx_markup_pct/100)
```

`gst_pct`, `fx_rate`, `fx_markup_pct`, and `currency_iso` are all controlled from the addon Settings page; they're snapshotted into each `ProfileVersion` row so historical prices remain reproducible even if you later change the addon's global FX or GST settings.

## Tax rules

If you prefer to let WHMCS apply GST instead of baking it into the product price, click **Settings → Apply GST: no** AND run the one-time tax-rule installer (`TaxRuleManager::ensure()`) to create the 18% rule under Configuration → Tax Rules. WHMCS will then add GST on top of the synced product price at invoice time.

## Database schema

- `mod_contabo_profile` — id, slug (unique), name, plan_slug, period_months, region, os, options (json), tags, sync_strategy, active, latest_version_id, timestamps
- `mod_contabo_profile_version` — id, profile_id, version (sequential int per profile), base/configured/setup EUR, options_snapshot (json), specs_snapshot (json), fx_rate, fx_source, fx_markup_pct, gst_pct, currency_iso, final_monthly, final_setup, snapshot_generated_at, timestamps
- `mod_contabo_mapping` — id, profile_id, product_id (tblproducts.id), product_group_id, apply_to_monthly/annually/semiannually, active, timestamps
- `mod_contabo_sync_log` — id, trigger ('cron'|'manual'|'webhook'), status ('running'|'succeeded'|'failed'|'no-change'), started_at, finished_at, profiles_checked, profiles_changed, products_updated, error_message, summary (json)
- `mod_contabo_settings` — key, value, updated_at  (currently holds only `schema_version`)

Deactivating the addon **retains** all these tables so you don't lose history. Drop them manually if you really want a clean removal.

## Cron

Registered as `DailyCronJob` hook (runs once per WHMCS daily cron pass). Idempotent: short-circuits as `no-change` when the API's `/meta.snapshot_generated_at` is unchanged from the previous successful run. Admin notifications fire on `succeeded` or `failed`, never on `no-change`.

## Roadmap

- Configurator selection deltas (currently `configured == base`; will apply options{} via API `/quote`)
- Per-profile FX overrides (e.g. fixed cost-plus-margin instead of live ECB)
- WebSocket subscription to `/ws/changes` for near-real-time updates
- Client-area cart hook that re-prices on the fly for prospects who change region/OS
