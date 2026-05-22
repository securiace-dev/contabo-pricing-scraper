# Phase A.5.2 — Design Impact Report (mode-aware profiles + configurable options)

> **Design-only. No implementation code.** Approved 2026-05-22 (with 10 amendments,
> below). Gates A.6 implementation. Sequencing: A.5.1 (shipped, v0.4.1/schema v4) →
> **A.5.2 (this report)** → A.6.

## Context

A.5.1 stabilised the addon and laid the `profile_mode` foundation. Before building A.6
(WHMCS configurable-options sync + provisioning metadata), this report settles how the
two product models — `fixed_admin_profile` and `customer_configurable_product` — affect
identity, pricing, lifecycle, drift, and the future provisioning module. Grounded in
**verified** Contabo configurator data + WHMCS schema.

## Verified facts (live data + production DB, 2026-05-22)

**Contabo `options` model** (`data/output/contabo_configs.json`, cloud-vps-10): per-plan
`options` object keyed by 5 dimensions, each a flat array of `{dimension, category,
option_label, monthly_price_delta, setup_fee_delta, …}`:

| Dimension | #opts | Categories | Selection model |
|---|---|---|---|
| **Image** | 34 | OS (17), Apps (8), Panels (6), Blockchain (3) | **mutually exclusive — ONE per server**; category = visual grouping only |
| **Networking** | 7 | Bandwidth, IPv4, Private Networking | **3 independent concerns — must split** |
| **Region** | 9 | America/Europe/Asia (region_group, country, country_code, subregion) | mutually exclusive — one |
| **Storage Type** | 4 | NVMe tiers | mutually exclusive — one |
| **Data Protection** | 2 | Auto Backup / none | mutually exclusive — one |

**Decisive:** cPanel/Plesk are in **Image → Panels**, i.e. complete image choices (a Panel
image IS the image), not add-ons. OS/App/Panel/Blockchain are categories of the single
Image dimension. **→ Image becomes ONE WHMCS option, never four.** Networking is the only
dimension that splits (Bandwidth / IPv4 / Private Networking).

**WHMCS configurable-options schema** (verified on `my.securiace.com`):
`tblproductconfiggroups`(id,name,description) → `tblproductconfigoptions`(id,gid,
optionname,optiontype,qtyminimum,qtymaximum,order,hidden) → `tblproductconfigoptionssub`
(id,configid,optionname,sortorder,hidden); `tblproductconfiglinks`(id,gid,pid);
`tblhostingconfigoptions`(id,relid,configid,optionid,qty); `tblpricing` rows
`type='configoptions'` keyed by `relid = tblproductconfigoptionssub.id`, 6 cycle + 6
setup-fee columns. `optiontype`: **1=dropdown, 2=radio, 3=yes/no, 4=quantity**
(empirically corrected in the A.6 preflight — WHMCS has no type 0 and no text
type; an earlier draft of this line was wrong. A type-3 yes/no charges only at
qty=1; a type-4 quantity multiplies the unit price by qty). Currencies: id 1 is
the install's **base** currency (INR on prod my.securiace.com; USD on the local
dev WHMCS) — the syncer must key off the base currency, not assume id 1 == INR.

---

## 1. Mode-aware profile model

### `fixed_admin_profile`
Admin builds a fixed SKU; customer does NOT choose build options; provisioning passes the
exact preselected values. Fixed selections are part of identity.
Identity: profile_mode, plan_slug, period_months, fixed image (single value), fixed
region, fixed storage, fixed data-protection, fixed bandwidth, fixed IPv4/private-net
defaults, pricing-strategy version, tax-strategy, provisioning-schema version.

### `customer_configurable_product`
Admin maps a Contabo plan to ONE WHMCS product; customer chooses admin-exposed options.
plan_slug is identity; period_months is NOT (cycles handle it); image/region/storage are
NOT identity unless admin locks them.
Identity: profile_mode, plan_slug, product scope, exposed-option-schema hash,
default-values hash, pricing-strategy hash, tax-strategy, provisioning-metadata-schema
version.

| Aspect | fixed_admin_profile | customer_configurable_product |
|---|---|---|
| plan_slug in identity | yes | yes |
| period_months in identity | yes | no (cycles handle it) |
| image in identity | yes (locked value) | no, unless locked |
| region/storage in identity | yes | no, unless locked |
| exposed-option schema in identity | n/a | yes (hash) |
| default values in identity | n/a | yes (hash) |
| one row maps to | one fixed SKU | one WHMCS product (1:1 plan default) |

Slug examples — fixed: `cloud-vps-10-12mo-eu-ubuntu-2404-nvme200-backup`; configurable:
`cloud-vps-10-configurable` (option choices NOT in slug).

Fingerprint — fixed: hash of locked values; configurable: hash of SHAPE
(exposed_schema_hash + defaults_hash + markup_hash), concrete values excluded.

Duplicate handling extends A.5.1: fixed → same slug+fingerprint loads existing, different
→ conflict chooser. Configurable → one product per plan default; a second is a conflict
unless an explicit `product_scope_key` differs (see amendment 7).

---

## 2. Image option model (headline rule)

ONE WHMCS option `Image` per profile. `optiontype` = 1 (dropdown — 34 values) with
**prefixed labels + sortorder** for grouping (see amendment 2; do NOT assume real
optgroups). Sub-options = all 34 image values; `category` drives sortorder + label prefix
(`[OS] Ubuntu 24.04`, `[Panel] cPanel`, `[App] Docker`, `[Blockchain] Geth`). Exactly one
selectable. New class `ImageOptionNormalizer` collapses 34 rows → one option + ordered
sub-values, attaches category metadata, and emits a single `image_value` for provisioning.
**NEVER four options.** Region / Storage Type / Data Protection = one option each;
Networking splits into 3 (Bandwidth dropdown, IPv4 qty, Private Networking yes/no).

---

## 3. Admin-curated option exposure

Per dimension/value flags (in `mod_contabo_config_option_link` / `…_value_link`):
expose_to_customer, default_value, hidden, deprecated, allowed_for_new_orders,
allowed_on_create, allowed_post_provision, allowed_on_reinstall, allowed_on_upgrade,
allowed_on_downgrade, pass_to_provisioning, destructive_if_changed, requires_confirmation,
requires_admin_approval. **Default: preview-first; production exposure is admin-curated
(nothing exposed until ticked).** Default seed preset = "Retail VPS Minimal" (amendment 8).

| Preset | Image | Region | Storage | Backup | IPv4 | Bandwidth | Private Net |
|---|---|---|---|---|---|---|---|
| Retail VPS | OS only | exposed | exposed | exposed | qty | hidden | hidden |
| Managed VPS | OS + Panels | locked | exposed | forced-on | qty | exposed | exposed |
| Fixed SKU | locked one | locked | locked | locked | locked | locked | locked |

---

## 4. Capability matrix — `mod_contabo_option_capability`

Fields: contabo_plan_slug, dimension_key, value_key, allowed_on_create/_reinstall/
_post_provision/_upgrade/_downgrade, requires_reinstall, requires_recreate,
destructive_change, data_loss_expected, requires_backup_warning, requires_admin_approval,
billing_change_possible, provisioning_action, **capability_source** (api_verified |
scrape_verified | manual_assumption | admin_override | unknown — amendment 6),
last_verified_at.

| Change | post-provision | destructive | provisioning_action |
|---|---|---|---|
| Image/OS change | yes (reinstall) | yes — data loss | reinstall |
| Region change | assume recreate | yes — recreate | recreate |
| Storage change | plan-dependent | assume destructive | reinstall/recreate |
| IPv4 qty | yes | no | add/remove IP |
| Backup toggle | yes | no | enable/disable |
| Bandwidth tier | yes | no | adjust (billing) |
| Private net toggle | yes | no | attach/detach |
| Plan upgrade | yes (ChangePackage) | usually safe | resize |
| Plan downgrade | conditional | assume destructive | admin-only |

Only `api_verified` capabilities may auto-apply destructive/in-place changes; weaker
sources require admin approval (amendment 6).

---

## 5. Compatibility matrix — `mod_contabo_option_compatibility`

Fields: plan_slug, dimension_key, value_key, compatible_with_json, incompatible_with_json,
required_values_json, min_value, max_value, source_snapshot_id, last_verified_at. Prevents
unsupported region/image/storage-for-plan, image-unavailable-in-region, IPv4 over limit,
backup-unavailable-in-region. Validation on every selection change (AJAX) → invalid combos
greyed + reasoned; provisioning blocks invalid orders; admin per-order force-allow with
audit.

---

## 6. Base price vs option-delta strategy

`baseline_strategy` = **`admin_selected_default`** (recommended). Base WHMCS product price
= admin default config; configurable-option `tblpricing` rows are DELTAS vs default.
Avoids double-charging / undercharging / renewal-margin miscalc. **Negative-delta clamp:
delta ≥ 0 in v1** (amendment 1).

Worked example (cloud-vps-10, INR, FX=90, vendor tax 18% non-recoverable, buffer 4%,
markup cost+15%): base landed ≈ ₹447.85 → sell ≈ ₹515/mo. Each non-default option priced
independently across all 6 cycles via the same `MarginCalculator::landedCostMonthly`
(the `monthly_price_delta` is the EUR marginal cost). Auto Backup €1.50 → +₹191/mo;
US-Central region €0.95 → +₹121/mo; 150 GB NVMe €1.85 → +₹235/mo; IPv4 per-unit ×
chargeable_qty. Annual = (515 + Σ deltas) × 12, each rounded per `mapping.rounding_mode`.

---

## 7. Pricing override cascade (most specific wins)

1 option-value → 2 dimension → 3 cycle → 4 mapping → 5 profile → 6 global. Top-level markup
cascades down unless a narrower scope overrides; a fixed-price override wins within its
scope. (See plan for the conflict-resolution worked example.)

---

## 8. Configurable-options architecture (addon-owned link tables)

`mod_contabo_config_group_link` (**UNIQUE(profile_id, whmcs_product_id, group_key)** — supports multiple groups per profile across products / scopes; not one-group-per-profile-forever), `mod_contabo_config_option_link`
((profile_id, dimension_key) ↔ option + exposure flags), `mod_contabo_config_option_value_link`
((option_link_id, contabo_value_key) ↔ sub + `contabo_label` round-trip key +
capability/compat refs), `mod_contabo_config_option_audit` (append-only). Idempotent
upsert, ownership tracking, manual-drift detection, preview-first, rollback/unlink/hide.
Image → 1 option-link + 34 value-links; Networking → 3 option-links. **All WHMCS-table
writes go through `WhmcsConfigOptionsAdapter`** (amendment 3).

---

## 9. Config options vs product addons

Config options (build/provision the server): Image, Region, Storage, IPv4 qty, Bandwidth,
Data Protection (Contabo Auto Backup), Private Networking. Product addons (commercial
services): managed support, monitoring, managed-backup service, security, migration, admin
services. **Rule:** changes the Contabo deploy payload → config option; a service you
perform → addon. Backup edge case resolved this way (Contabo Auto Backup = option; your
managed-backup service = addon).

---

## 10. Quantity option model (IPv4)

min_qty, max_qty, included_qty, unit_cost (EUR), unit_price (sell), allowed_increment,
provisioning_payload_rule. `chargeable_qty = max(0, selected − included)`; line =
unit_price × chargeable_qty per cycle. Affects pricing, renewal margin (snapshot qty),
upgrade/downgrade, provisioning payload (absolute count), drift (WHMCS qty vs Contabo IPs).

---

## 11. Post-provision change workflow

A. Safe in-place (IPv4/backup/bandwidth/private): invoice→pay→provision→audit. B.
Destructive reinstall (Image/OS, Panel↔Linux, App/Blockchain): warnings + typed
confirmation `I UNDERSTAND THIS WILL REINSTALL OR RECREATE THE SERVER AND DATA MAY BE
LOST` → config-change order → provision after pay/approval. C. Region/disk: assume
destructive unless API proves in-place; confirm + admin approval. D. Upgrade/downgrade:
WHMCS flow; ChangePackage in Phase C; preserve compatible options; downgrade admin-only.
_Lifecycle tree is provisional — gated by amendment 4._

---

## 12. Selected service snapshot — `mod_contabo_service_config_snapshot`

Captured at order/provision: service_id, profile_id, profile_mode, plan_slug,
whmcs_product_id, selected_image, selected_region, selected_options_json,
contabo_payload_json, base_price_snapshot, config_option_price_snapshot,
landed_cost_snapshot, tax_mode_snapshot, pricing_version_snapshot,
provisioning_metadata_version, timestamps. Source of truth for renewal margin, deprecation,
disputes, reinstall, drift, audit.

---

## 13. Active service config price-locking (Phase B)

Selected-option prices follow the same no-silent-reprice principle. RenewalEngine uses
**`ServiceRevenueResolver`** (amendment 5): base recurring + selected config-option
recurrings + addons + discounts — NOT bare `recurringamount`. `service_config_price_policy`
mirrors the base policy set (grandfather / current-term / margin-floor / scheduled /
manual). `MarginCalculator::landedCostWithSelections($service, $snapshot)` prices the whole
configuration. Vendor increases for Windows/cPanel/backup/IPv4 flow through the guarded
renewal decision — never a silent bump.

---

## 14. Manual edit / drift policy

Ownership: addon_owned_strict | addon_owned_soft | admin_owned | ignored. Each object
tracks expected_hash, last_synced_hash, current_whmcs_hash, drift_status. **Default: detect,
flag, do NOT overwrite.** Admin actions: accept WHMCS edit / overwrite / create override /
ignore / unlink. Audit every transition.

---

## 15. Hidden / deprecated / orphan values

Contabo removes/renames → existing services keep mapping (round-trip via contabo_label),
renewal margin stays computable, value never deleted. New orders hide/deprecate + admin
alert. **Default: hide_missing_values_after_90_days. Never hard-delete.**

---

## 16. Upgrade path policy

`upgrade_path_policy` = `upgrade_only` (recommended) | upgrade_and_downgrade | admin_only |
disabled. Suggested paths: Cloud VPS 10→20→30→40→50→60; VDS S→M→L→XL→XXL; Storage VPS
10→…→50. Downgrade never assumed safe.

---

## 17. Provisioning contract (Phase C) — `docs/PROVISIONING_CONTRACT.md` (authored in A.6)

Defines how the provisioning module reads selected config options, maps WHMCS sub-option →
Contabo value via `contabo_label`, builds CreateAccount payload from the snapshot,
ChangePackage / Reinstall / Terminate / Suspend / Unsuspend behaviour, instance-id
storage, drift detection, destructive vs payment-first vs approval-gated actions. **A.6
prepares metadata; Phase C implements lifecycle.**

---

## 18. Contabo ↔ WHMCS drift reconciliation (Phase C)

Expected (module) vs actual (Contabo deploy API) state → drift report → safe remediation
(adopt actual / re-push expected / flag manual). Designed now; built in Phase C.

---

## 19. Dev purge / reset controls (extends A.5.1 maintenance page)

Independently-gated toggles: purge mod_contabo_* / unlink config groups / hide config
values / delete addon-created config groups / delete addon-created products (only if
explicitly marked) / reset schema. All require backup + checkbox + typed phrase
`PURGE CONTABO PRICING DATA` + admin log. Never deletes clients/invoices/transactions/real
services. A.6 adds the config-object-aware toggles once link tables exist.

---

## 10 amendments (review round 2 — binding before A.6 coding)

Most load-bearing: #1, #3, #5, #6, #4.

1. **Negative-delta policy.** Don't assume WHMCS handles negative configurable-option
   pricing. A.6 preflight: test end-to-end. **v1 default: avoid negative deltas** (clamp
   ≥ 0; rebase default to cheapest, or mark cheaper value admin-only) until tested green.
   Fallbacks: cheapest_supported_combination / zero_base_all_costs_as_options / disallow
   cheaper-than-default / convert to base adjustment with warning.
2. **Label grouping ≠ optgroups.** `ImageOptionNormalizer` uses prefixed labels +
   sortorder, not HTML optgroups, unless the order-form template proves support.
3. **`WhmcsConfigOptionsAdapter` = sole write chokepoint** for tblproductconfig* +
   tblpricing(configoptions). Verifies schema, transactions, audit rows, dry-run preview.
   Static-grep gate. Syncer never raw-writes.
4. **Upgrade/downgrade preflight (no theory).** Dev product, test each option type through
   checkout + renewal + upgrade/downgrade incl. setup fees + proration + client-area path.
   Gates A.6.3 apply mode.
5. **`ServiceRevenueResolver`.** Don't equate `tblhosting.recurringamount` with full
   revenue. Resolve base + selected config-option amounts + addons + discounts; snapshot at
   order time; Phase B renewal uses it.
6. **Capability-source confidence.** api_verified | scrape_verified | manual_assumption |
   admin_override | unknown. Only api_verified auto-applies destructive/in-place; weaker →
   admin approval.
7. **Multiple configurable products per plan.** Identity adds `product_scope_key`,
   `commercial_variant`, `audience_segment` (e.g. `-retail`, `-managed`, `-developer`,
   `-private-india`). Default scope `default`; conflict only when scope keys match.
8. **Default exposure = "Retail VPS Minimal".** Image=OS only; Region limited; Storage
   hidden unless meaningful; Backup optional; IPv4 qty (if unit price known); Bandwidth /
   Private / Panels / Apps / Blockchain hidden. Advanced presets opt-in.
9. **Stricter idempotency/drift gate.** 2nd no-change sync → zero new rows, zero price
   changes. After manual edit → drift record, not overwrite. After upstream price change →
   preview diff, not write, unless apply-mode AND ownership=addon_owned_strict.
10. **INR-only v1 currency guard.** Sync currency 1 (INR) only; mark USD/EUR/GBP explicitly
    unsupported/not-synced (no silent stale tblpricing rows); UI/logs state "only INR synced".

---

## A.6 implementation sequence (after this report)

1. **A.6.1** — schema v5 (config link + capability(+capability_source) + compatibility +
   snapshot + product_scope_key/commercial_variant/audience_segment), `ImageOptionNormalizer`
   (prefixed labels), `DimensionParser` update (Image=1, Networking=3), `OptionTypeMapper`,
   `WhmcsConfigOptionsAdapter`, `ServiceRevenueResolver`.
2. **A.6.2** — `ConfigurableOptionsSyncer` (observe mode), per-value pricing via
   MarginCalculator + override cascade + negative-delta clamp + INR-only guard,
   `OptionAuditLog`.
3. **A.6.3** — apply mode + admin UI (mode toggle, exposure matrix incl. Retail-VPS-Minimal
   seed, capability/compat editors, sync button), preview-first. **Gated by amendment-4
   preflight.**
4. **A.6.4** — snapshot capture wiring, orphan/drift handling, `docs/PROVISIONING_CONTRACT.md`.
5. **A.6.5** — config-object-aware purge toggles, **INR-only guard + multi-currency readiness** (v1 syncs INR only; USD/EUR/GBP explicitly marked unsupported/not-synced; schema + code shaped so multi-currency is a later flip, not a rewrite), docs, CHANGELOG 0.5.0.
6. **Phase B** consumes selected-option deltas in renewal margin (via ServiceRevenueResolver);
   **Phase C** builds the provisioning module against the contract.

## Acceptance + test plan
See plan (`harmonic-popping-hollerith.md`) — ≥30 A.6 tests covering identity/fingerprint per
mode, Image 34→1 collapse, Networking 3-way split, capability/compat, baseline+delta across
6 cycles, override cascade, quantity math, snapshot, renewal-with-deltas, drift-no-overwrite,
orphan-90d, idempotent resync (zero new rows), purge typed-phrase, negative-delta clamp,
INR-only guard.
