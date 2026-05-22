# Phase A.6 — WHMCS Configurable Options sync

> **⚠️ SUPERSEDED IN PART BY `docs/PHASE_A52_DESIGN_IMPACT.md` (approved 2026-05-22).**
> Read A.5.2 first — it carries the binding design decisions + 10 amendments and corrects
> several assumptions in this draft. Key corrections to apply when implementing:
> - **Image is ONE mutually-exclusive dimension** (verified: 34 options, categories OS/Apps/
>   Panels/Blockchain are visual grouping only). Earlier this doc's mapping table implied
>   four separate option groups (Image:OS, Image:Apps, Image:Control Panel, Image:Blockchain)
>   — that is WRONG. One `Image` option, never four. cPanel/Plesk are Image→Panels values.
> - **Networking splits into 3** options (Bandwidth / IPv4 / Private Networking).
> - **Two product modes**: `fixed_admin_profile` + `customer_configurable_product` (A.5.1
>   schema v4 laid `profile_mode`).
> - **10 amendments** are binding: negative-delta clamp, prefixed labels (not optgroups),
>   `WhmcsConfigOptionsAdapter` write chokepoint, upgrade/downgrade preflight,
>   `ServiceRevenueResolver`, capability_source confidence levels, product_scope_key for
>   multiple products per plan, Retail-VPS-Minimal default preset, stricter idempotency/drift
>   gate, INR-only v1 currency guard.
> - **Schema is v5** for A.6 (A.5.1 already took v4), not v4 as this draft says.
>
> **Status:** superseded draft. Implement per A.5.2.
>
> **Purpose:** stop baking the OS / App / Control Panel / Blockchain / Region / Storage / Networking / Data Protection selections into a profile as fixed templates. Instead, sync them into WHMCS's native **product configurable-options** system so the customer picks at checkout, the choice flows through ordering, and the provisioning module sends it to Contabo's deploy API.

---

## Context

Phase A.5 made the addon 6-cycle aware on the catalog side and dual-mask aware (catalog vs renewal). Profiles still bake exactly ONE option per dimension via `mod_contabo_profile.options` JSON. WHMCS sees one price; customer never picks anything.

The user (2026-05-22): *"options OS / APP / CONTROL PANEL / BLOCKCHAIN should be made available to users as addon in whmcs standard way and its one group out of which only one option can be selected and that option is passed with contabo provisioning api request to deploy/install/reinstall."*

That's the WHMCS-native model: per-product **configurable option groups**, single-select (radio) where appropriate, dropdown or quantity where not, with per-option price modifiers. Phase A.6 builds the syncer that populates them.

Important distinctions vs Phase A.5:
- Phase A.5 wrote the **base product price** per cycle (`tblpricing` type=`product`).
- Phase A.6 writes **per-option price modifiers** per cycle (`tblpricing` type=`configoptions`).
- A customer's invoice line at checkout = base price + sum of selected option modifiers.
- The profile's "baked" selection becomes a **default** (used as the pricing baseline + the manual-deploy fallback), not the only allowed config.

---

## WHMCS configurable options schema (verified on production 2026-05-22)

| Table | Columns | Role |
|---|---|---|
| `tblproductconfiggroups` | `id`, `name`, `description` | A named bundle of options (e.g. "Contabo cloud-vps-10") |
| `tblproductconfigoptions` | `id`, `gid` (→ groups.id), `optionname`, `optiontype`, `qtyminimum`, `qtymaximum`, `order`, `hidden` | One row per OPTION within a group (e.g. "Operating System") |
| `tblproductconfigoptionssub` | `id`, `configid` (→ options.id), `optionname`, `sortorder`, `hidden` | One row per VALUE the customer can pick (e.g. "Ubuntu 24.04") |
| `tblproductconfiglinks` | `id`, `gid`, `pid` (→ tblproducts.id) | Many-to-many: which groups apply to which products |
| `tblhostingconfigoptions` | `id`, `relid` (→ tblhosting.id), `configid`, `optionid` (→ sub.id), `qty` | The customer's actual chosen values per service |
| `tblpricing` (rows where `type='configoptions'`) | `id`, `currency`, `relid` (→ `tblproductconfigoptionssub.id`), `monthly`, `quarterly`, `semiannually`, `annually`, `biennially`, `triennially`, `msetupfee`, `qsetupfee`, `ssetupfee`, `asetupfee`, `bsetupfee`, `tsetupfee` | Per-sub-option price MODIFIER. Same 6-cycle schema as products. Same `-1` / `0` / `>0` sentinel meaning. |

**`optiontype` values** (`tblproductconfigoptions.optiontype` — stored as TEXT but values are integer-like):

| Value | Behaviour | Use for Contabo |
|---|---|---|
| `0` | Dropdown — customer picks one from a list of sub-values | Image:Apps (many values), Networking:Bandwidth |
| `1` | Radio buttons — customer picks one | Image:OS, Image:Control Panel, Image:Blockchain, Region, Storage Type, Data Protection |
| `2` | Yes/No checkbox | Networking:Private Networking |
| `3` | Quantity — customer enters a number | Networking:IPv4 (additional IPs) |
| `4` | Free-text — customer types | Not used by Contabo |

Currency IDs on this install (`tblcurrencies`): **1 = INR (base)**, 2 = USD, 3 = EUR, 4 = GBP. Phase A.6 writes per-option pricing in INR first; multi-currency follows the existing pattern.

---

## Mapping Contabo dimensions → WHMCS option groups

Each Contabo plan has ONE WHMCS option group containing N options. Group name format: `Contabo <plan-slug>`. Option order matches Contabo's own configurator order (so admins recognise it).

| Contabo dimension | WHMCS option name | optiontype | Typical sub-options | Pricing pattern |
|---|---|---|---|---|
| Image:OS | Operating System | 1 (radio) | Ubuntu 24.04 / Debian 12 / AlmaLinux 9 / Rocky 9 / CentOS Stream | Most free; some paid (Windows is the obvious one when available) |
| Image:Apps | Pre-installed App | 0 (dropdown) | None / WordPress / LAMP / Docker / etc. | Mostly free; ML-stack types may carry a fee |
| Image:Control Panel | Control Panel | 1 (radio) | None / cPanel / Plesk / Webmin | License-based per-cycle fee |
| Image:Blockchain | Blockchain Image | 1 (radio) | None / Ethereum / Geth / Bitcoin / Solana | Most free |
| Region | Region | 1 (radio) | EU / US-East / US-West / Asia | Per-region delta (US/Asia premium over EU) |
| Storage Type | Storage Type | 1 (radio) | SSD / NVMe | NVMe premium |
| Data Protection | Backup | 1 (radio) | None / Auto Backup | Per-cycle fee for backup |
| Networking:IPv4 | Additional IPv4 | 3 (qty) | 0..N | Per-IPv4 per-cycle fee |
| Networking:Bandwidth | Bandwidth | 0 (dropdown) | Standard / Extended tiers | Tier-based |
| Networking:Private Networking | Private Networking | 2 (yes/no) | on / off | Per-cycle fee when on |

If a dimension only has one value in Contabo's configurator (and that's the default), it's silently omitted — no point exposing a single-choice radio.

---

## Pricing math worked example

**Plan:** `cloud-vps-10`. Anchor (no-option) monthly = €4.05 (vendor EUR). FX = 90.0 (illustrative). vendor_tax_rate = 18% non-recoverable. payment+fx buffer = 4%.

```
landed_monthly_anchor = 4.05 × 90 × (1.18) × (1.04) = ₹447.85 (rounded)
sell_monthly_anchor (cost+pct 15% markup) = ₹515 (rounded nearest_5 or whatever rule)
```

**Per-option delta**: Contabo's configurator returns `monthly` for each option (the marginal cost vs the default). Example:

| Option (sub-value) | Contabo monthly EUR delta | Landed local-currency delta (per month) | Markup-applied modifier (per month) | × 12 for Annually | × 24 for Biennially |
|---|---|---|---|---|---|
| OS: Ubuntu 24 (default) | 0.00 | ₹0 | ₹0 | ₹0 | ₹0 |
| OS: Windows Server 2022 | 9.99 | ₹1,104 | ₹1,270 | ₹15,240 | ₹30,480 |
| Backup: Auto Backup | 1.50 | ₹166 | ₹191 | ₹2,292 | ₹4,584 |
| Region: US-East | 1.40 | ₹155 | ₹178 | ₹2,136 | ₹4,272 |
| IPv4: per additional IP | 1.00 | ₹110 | ₹127 | ₹1,524 | ₹3,048 |

**At checkout** (customer picks Windows + Auto Backup + 2 extra IPv4s, Annually):

```
base annual price = 515 × 12 = ₹6,180   (from tblpricing.type=product)
+ OS Windows mod  = ₹15,240             (from tblpricing.type=configoptions, sub.id=windows)
+ Backup mod      = ₹2,292
+ IPv4 × 2        = ₹1,524 × 2 = ₹3,048
─────────────────
Total annual      = ₹26,760
```

WHMCS displays each line; the customer sees both base and add-ons clearly. The renewal engine (Phase B) recomputes ALL of these at the next renewal evaluation.

Rounding mode (per Phase A.5) applies to each modifier independently — so a customer's invoice sums clean rupee values, not mid-rounding fragments.

---

## Schema additions (Phase A.6 / `Installer::SCHEMA_VERSION = 4`)

### New table — `mod_contabo_option_group_link`

Owns the link between a Contabo profile and the WHMCS group it manages.

| col | type | notes |
|---|---|---|
| `id` | bigint PK | |
| `profile_id` | bigint FK `mod_contabo_profile.id` | |
| `whmcs_group_id` | int FK `tblproductconfiggroups.id` | the group this addon created/manages |
| `enabled` | tinyint(1) default 1 | when 0, syncer leaves the group alone |
| `created_at`, `updated_at` | timestamps | |
| UNIQUE | `profile_id` | one group per profile |

### New table — `mod_contabo_option_link`

Per (profile, dimension) → which `tblproductconfigoptions` row the addon owns. Maps Contabo's dimension key to WHMCS's option id so re-syncs don't duplicate.

| col | type | notes |
|---|---|---|
| `id` | bigint PK | |
| `profile_id` | bigint | |
| `dimension_key` | varchar(60) | e.g. `Image:OS`, `Image:Apps`, `Region`, `Networking:IPv4` |
| `whmcs_option_id` | int FK `tblproductconfigoptions.id` | |
| `optiontype` | tinyint | 0/1/2/3 — what we set this option to |
| `enabled` | tinyint(1) default 1 | admin can disable a dimension without uninstalling the option |
| `created_at`, `updated_at` | timestamps | |
| UNIQUE | `(profile_id, dimension_key)` | |

### New table — `mod_contabo_option_sub_link`

Per (option, contabo_value_idx) → which `tblproductconfigoptionssub` row the addon owns. Needed because Contabo identifies a value by `(dimension, category, idx)` but WHMCS identifies it by its own sub.id; we need a stable round-trip.

| col | type | notes |
|---|---|---|
| `id` | bigint PK | |
| `option_link_id` | bigint FK `mod_contabo_option_link.id` | |
| `contabo_idx` | int | the configurator's index for this option value (matches the report.html configurator's `data-cb-opt-idx`) |
| `contabo_label` | varchar(160) | the human label as Contabo returned it (used by provisioning module to map back) |
| `whmcs_sub_id` | int FK `tblproductconfigoptionssub.id` | |
| `is_default` | tinyint(1) | the default flagged by Contabo |
| `monthly_eur_delta` | decimal(12,4) | Contabo's stated EUR delta per month |
| `created_at`, `updated_at` | timestamps | |
| UNIQUE | `(option_link_id, contabo_idx)` | |
| UNIQUE | `whmcs_sub_id` | |
| INDEX | `contabo_label` | for provisioning module label-lookup |

### New table — `mod_contabo_option_audit`

Append-only ledger for every configurable-options sync write (groups, options, subs, pricing rows). Same shape as `mod_contabo_catalog_audit` (Phase A.5).

| col | type | notes |
|---|---|---|
| `id` | bigint PK | |
| `sync_batch_id` | char(36) | UUID per `ConfigurableOptionsSyncer::run()` pass |
| `profile_id` | bigint | |
| `dimension_key` | varchar(60) nullable | |
| `target_table` | varchar(40) | `tblproductconfiggroups` / `tblproductconfigoptions` / `tblproductconfigoptionssub` / `tblpricing` |
| `target_id` | int nullable | the WHMCS row id touched, or null for inserts not yet committed |
| `action` | enum | `insert`, `update`, `delete`, `skip_no_change`, `skip_disabled`, `error` |
| `old_value_json` | longtext nullable | the previous row state |
| `new_value_json` | longtext nullable | the post-write state |
| `note` | varchar(255) nullable | human-readable explanation |
| `created_at` | timestamp | |
| INDEX | `sync_batch_id` | |
| INDEX | `profile_id, created_at` | |

### Column additions

**`mod_contabo_profile`** — 2 columns:
- `expose_configurable_options` tinyint(1) default 0 — when 0, Phase A.6 syncer skips this profile entirely (admin opt-in per profile)
- `default_selections_json` longtext nullable — the "default" selection per dimension. Used by repricing engine as the pricing baseline. Replaces the v0.2 `options` column's role over time (kept side-by-side until next major).

**`mod_contabo_settings`** — 2 keys:
- `option_sync_phase` ∈ `observe` / `apply` (default `observe`) — same observe-mode pattern as repricing. In `observe`, syncer writes audit rows but no actual `tblproduct*` changes.
- `option_sync_batch_max_writes` (default `200`) — safety cap; if a single sync would mutate more rows, abort + alert admin.

---

## Syncer algorithm — `lib/ConfigurableOptionsSyncer.php`

```
function syncProfile(profile):
    if (!profile.expose_configurable_options) return SKIP('profile_not_opted_in')

    cfg = ApiClient::configurator(profile.plan_slug)
    if (cfg == null) return SKIP('configurator_unavailable')

    syncBatchId = uuid()

    // 1. Ensure group exists (one per profile)
    groupLink = mod_contabo_option_group_link.where('profile_id', profile.id).first()
    if (!groupLink) {
        groupId = whmcs_groups_insert(['name' => "Contabo {profile.plan_slug}", 'description' => "Managed by contabo_pricing addon"])
        groupLink = mod_contabo_option_group_link.insert([profile_id, whmcs_group_id=groupId])
        audit('tblproductconfiggroups', groupId, 'insert')
    }
    if (groupLink.enabled == 0) return SKIP('group_disabled')

    // 2. Link group to mapped products (idempotent on tblproductconfiglinks)
    mappings = mod_contabo_mapping.where('profile_id', profile.id).where('active', 1).get()
    for mapping in mappings:
        ensureProductLink(groupLink.whmcs_group_id, mapping.product_id)

    // 3. For each dimension that Contabo exposes, upsert the option
    dimensions = parseConfiguratorDimensions(cfg)
    // {Image:OS, Image:Apps, Image:Panels, Image:Blockchain, Region, Storage, Networking:IPv4, ...}
    for dimensionKey, contaboValues in dimensions:
        if (len(contaboValues) <= 1) {
            audit(null, null, 'skip_no_change', note: "$dimensionKey has only 1 value")
            continue
        }
        optionLink = mod_contabo_option_link.where(profile_id, dimensionKey).first()
        optiontype = pickOptionType(dimensionKey, contaboValues)  // 0/1/2/3 per the mapping table above
        if (!optionLink) {
            optionId = whmcs_options_insert([
                'gid' => groupLink.whmcs_group_id,
                'optionname' => humanizedNameFor(dimensionKey),
                'optiontype' => optiontype,
                'qtyminimum' => (optiontype==3 ? 0 : null),
                'qtymaximum' => (optiontype==3 ? maxIpv4 : null),
                'order' => orderForDimension(dimensionKey),
                'hidden' => 0,
            ])
            optionLink = mod_contabo_option_link.insert([profile_id, dimensionKey, whmcs_option_id=optionId, optiontype])
            audit('tblproductconfigoptions', optionId, 'insert')
        }
        if (optionLink.enabled == 0) continue

        // 4. Upsert sub-values + pricing
        for contaboValue in contaboValues:
            subLink = mod_contabo_option_sub_link
                .where(option_link_id, optionLink.id)
                .where(contabo_idx, contaboValue.idx)
                .first()
            if (!subLink) {
                subId = whmcs_subs_insert([
                    'configid' => optionLink.whmcs_option_id,
                    'optionname' => contaboValue.label,
                    'sortorder' => contaboValue.idx,
                    'hidden' => 0,
                ])
                subLink = mod_contabo_option_sub_link.insert([
                    option_link_id, contabo_idx, contabo_label,
                    whmcs_sub_id=subId,
                    is_default=contaboValue.is_default,
                    monthly_eur_delta=contaboValue.monthly,
                ])
                audit('tblproductconfigoptionssub', subId, 'insert')
            } else if (subLink.contabo_label != contaboValue.label OR subLink.monthly_eur_delta != contaboValue.monthly) {
                whmcs_subs_update(subLink.whmcs_sub_id, ['optionname' => contaboValue.label])
                mod_contabo_option_sub_link.update(subLink.id, [contabo_label, monthly_eur_delta])
                audit('tblproductconfigoptionssub', subLink.whmcs_sub_id, 'update')
            }

            // 5. Per-cycle pricing modifier for this sub-value
            cyclePrices = computeOptionPricing(contaboValue, profile, mapping_settings_per_currency)
            for currency in active_currencies:
                upsertConfigOptionPricing(subLink.whmcs_sub_id, currency, cyclePrices[currency])

    // 6. Detect orphans (sub-values, options, or links that no longer exist in Contabo's config)
    detectAndAuditOrphans(profile, dimensions)
    // Phase A.6 default: AUDIT ONLY. Phase B can flip to delete-after-grace-period.
```

### `computeOptionPricing(value, profile, mapping)` reuses Phase A.5's MarginCalculator

```
landedDelta_monthly = value.monthly × fx × (1 + fxBufferPct/100) × (1 + paymentBufferPct/100)
                                          × (vendorTaxRecoverable ? 1 : 1 + vendorTaxRatePct/100)
markupResolved = resolveMarkup(mapping, value.dimension)  // per-dimension override or inherit
sellDelta_monthly = applyMarkup(landedDelta_monthly, markupResolved)
for each cycle in active 6 cycles enabled for this mapping:
    raw = sellDelta_monthly × cycleMonths
    rounded = applyRounding(raw, mapping.rounding_mode)
    cyclePrices[cycle] = rounded
```

If `monthly_eur_delta == 0` (the default option), all cycle modifiers = 0.00 (free per Phase A.5 semantics). If a cycle is `respect_disabled` and the product's base price for that cycle is `-1`, the option modifier defaults to `-1` to match — admin doesn't see options for a cycle the product doesn't offer.

### `upsertConfigOptionPricing(subId, currency, prices)`

For each cycle column, look up the existing `tblpricing` row (`type='configoptions'`, `relid=$subId`, `currency=$currency`). If absent, INSERT with all 6 cycle columns. If present, UPDATE only the cycle columns that differ. Skip if `respect_disabled_cycles=1` and the target column is `-1`. Audit each write.

---

## Idempotent re-sync semantics

Re-running `syncProfile` is safe:
- Existing groups/options/subs detected via `mod_contabo_option_*_link` tables → updated in place, not duplicated.
- Sub-options Contabo NO LONGER offers → audit row, `action='skip_no_change'`, note="orphaned in Contabo configurator". Admin reviews. Optional cron flips them to `hidden=1` after grace period.
- New options added by Contabo (e.g. new OS released) → INSERT + audit.
- `monthly_eur_delta` change → triggers per-cycle pricing re-upsert.

---

## UI changes

### Profile edit modal (rework)

Two panels:
1. **Defaults & pricing baseline** (the existing configurator UI, repurposed): admin sets the default selection used by the repricing engine for margin math. NOT the only allowed config.
2. **Configurable options sync** (new): a checkbox `Expose configurable options in WHMCS` (= `profile.expose_configurable_options`). When checked, shows a per-dimension matrix:
   - One row per dimension Contabo offers
   - Columns: dimension name, suggested optiontype (radio/dropdown/qty/yes-no), how many sub-values, default value (highlighted), per-cycle modifier sample (computed from current FX + markup)
   - Per-dimension Enable/Disable toggle (writes `mod_contabo_option_link.enabled`)
   - "Sync now" button per profile (or "Sync all" on the dashboard)

### Mappings page

Added column: **Configurable options synced?** — pill showing yes/no/partial/disabled per mapped product. Hover shows the WHMCS group id + last sync timestamp.

### Per-mapping audit log

New page `?action=option-audit` — filterable by profile, dimension_key, action, batch_id. CSV export same shape as the catalog audit.

### Dashboard KPI tile (new)

"Configurable option groups managed" — count of groups owned by this addon, count of active links, count of orphaned sub-values pending review.

---

## Phase B integration (the renewal engine)

Phase B reads `tblhostingconfigoptions` to compute the customer's TRUE current sell price, not just the base product price. The MarginCalculator already understands per-cycle modifiers from Phase A.5; it just needs to know "this service has selections X, Y, Z that add to the base." Implementation: a new `MarginCalculator::landedCostWithSelections($service, $selections)` that sums base + per-selection deltas before margin math.

This is the reason Phase A.6 must land before Phase B: if Phase B repriced a Windows-with-backup VPS using only the base price, the margin math would be wildly wrong (Windows + backup can double the bill in EUR terms).

---

## Provisioning module integration (Phase C — separate plan)

`tblhostingconfigoptions` rows are READ by the linked provisioning module's `CreateAccount` / `Reinstall` hook. We don't own that today. Two paths:
- **(a) third-party Contabo provisioning module** — admin installs one from the WHMCS marketplace; we just populate the data it expects.
- **(b) own provisioning module** — separate `modules/servers/contabo/contabo.php` with `CreateAccount`/`SuspendAccount`/`UnsuspendAccount`/`TerminateAccount`/`Reinstall` calling Contabo's deploy API and using `tblproductconfigoptionssub.optionname` ↔ Contabo's `contabo_label` round-trip via `mod_contabo_option_sub_link`.

Phase A.6 makes BOTH paths possible. Phase C builds (b) if we choose to.

---

## Edge cases

| Scenario | Phase A.6 behaviour |
|---|---|
| Plan has only one OS available | Dimension omitted entirely (no point exposing 1-radio) — audit row notes the skip |
| Contabo adds a new sub-value (e.g. Ubuntu 25 releases) | Insert + audit; existing customers unaffected (their tblhostingconfigoptions still point at Ubuntu 24) |
| Contabo removes a sub-value | Audit `orphaned`. Don't delete from WHMCS. Hide it (set `hidden=1`) after configurable grace period (default 90 days). Admin override available. |
| Contabo silently changes a sub-value's monthly delta | Audit `update` on the sub-link + pricing re-upsert. Phase B's renewal engine sees the new delta on next decision. |
| Admin manually edits a `tblproductconfigoptionssub` row | Next sync detects drift (`whmcs_sub_id` row's optionname ≠ `mod_contabo_option_sub_link.contabo_label`); logs `manual_edit_detected`, leaves admin's edit intact. |
| Customer already has a service with a now-orphaned sub_id selected | Their tblhostingconfigoptions row points at the hidden sub. Provisioning still works (the round-trip via `contabo_label` still maps). Renewal engine prices it as zero-delta (legacy "grandfathered choice") and flags admin. |
| Multi-currency | Per-currency pricing rows in `tblpricing`. INR is base; USD/EUR/GBP rates need FX. Phase A.6 v1 only writes INR; multi-currency follows the existing tblpricing pattern. |
| Disabled cycle on the base product | Option modifiers for that cycle inherit `-1` so WHMCS hides the option entirely for that cycle. |
| `option_sync_phase = observe` | Syncer iterates as normal but every write is `action='observed'` in the audit log; no actual WHMCS table touched. |
| Profile not opted in (`expose_configurable_options=0`) | Syncer skips with audit row `profile_not_opted_in`. |
| Group disabled (`mod_contabo_option_group_link.enabled=0`) | Syncer skips with audit row `group_disabled`. WHMCS group + options stay in DB (admin can re-enable). |
| Two profiles map to the same product | Each owns its own group; both groups are linked to the product. WHMCS shows both groups during checkout. Documented limitation — admin should not double-link in practice. |

---

## Acceptance criteria

1. Schema v4 migrates idempotently. Re-running is a no-op.
2. `ConfigurableOptionsSyncer::syncProfile($profileId)` is idempotent. Two consecutive runs produce zero new WHMCS rows on the second pass.
3. Customer at checkout sees one option group per profile, with dimensions exposed as radio/dropdown/qty/yes-no per the mapping table.
4. Each sub-option carries 6 cycle modifiers in `tblpricing` (type=`configoptions`), respecting per-cycle `-1` from the base product.
5. Audit log records EVERY write or deliberate skip. CSV export works.
6. Renewal engine (when Phase B lands) sees the full customer price (base + selections) via `MarginCalculator::landedCostWithSelections`.
7. Manual admin edit to a WHMCS sub-option row is preserved on re-sync (drift detection logs but does not overwrite).
8. `option_sync_phase = observe` causes ZERO writes to WHMCS tables (asserted by snapshot fixture test).
9. PHP 7.4 + 8.x polyglot maintained.
10. Static grep: writes to `tblproduct*` only inside `ConfigurableOptionsSyncer.php` (new chokepoint, parallel to ServicePriceWriter's role for `tblhosting`).
11. All existing Phase A.5 tests still pass.

---

## Test cases (≥ 20 new tests)

1. `OptionGroupCreationTest::testFreshProfileCreatesOneGroup`
2. `OptionGroupCreationTest::testReSyncDoesNotCreateDuplicateGroup`
3. `OptionGroupCreationTest::testDisabledGroupIsSkipped`
4. `DimensionParsingTest::testImageDimensionSplitByCategory_OS_Apps_Panels_Blockchain`
5. `DimensionParsingTest::testNetworkingDimensionSplitByIPv4_Bandwidth_Private`
6. `DimensionParsingTest::testSingleValueDimensionOmitted`
7. `OptionTypeMappingTest::testOSIsRadio`
8. `OptionTypeMappingTest::testAppsIsDropdown`
9. `OptionTypeMappingTest::testIPv4IsQuantity`
10. `OptionTypeMappingTest::testPrivateNetworkingIsYesNo`
11. `SubOptionLifecycleTest::testNewSubOptionInserted`
12. `SubOptionLifecycleTest::testRenamedLabelUpdated`
13. `SubOptionLifecycleTest::testOrphanedSubOptionHiddenAfterGracePeriod`
14. `SubOptionLifecycleTest::testManualEditedSubOptionPreserved`
15. `OptionPricingTest::testEachSubOptionHasSixCycleModifiers`
16. `OptionPricingTest::testDefaultSubHasZeroDelta`
17. `OptionPricingTest::testWindowsOSPositiveDelta`
18. `OptionPricingTest::testDisabledBaseCycleInheritsDisabledModifier`
19. `OptionPricingTest::testPerCycleMarkupOverrideRespected`
20. `ProductLinkTest::testGroupLinkedToAllMappedProducts`
21. `ProductLinkTest::testReSyncDoesNotDuplicateProductLinks`
22. `AuditLogTest::testEveryWriteEmitsAuditRow`
23. `AuditLogTest::testObservePhaseWritesAuditNoMutation`
24. `IdempotencyTest::testTwoConsecutiveSyncsZeroNewRows`
25. `StaticGrepTest::testTblproductWritesOnlyInSyncer`

---

## Migration path

### Phase A.6.1 — schema + classes (~1.5 d)

- `Installer::migrateTo4()` — new tables + columns
- `lib/ConfigurableOptionsSyncer.php` (skeleton, observe-only)
- `lib/OptionAuditLog.php` (mirror of CatalogAuditLog)
- `lib/DimensionParser.php` (configurator JSON → per-dimension structures)
- `lib/OptionTypeMapper.php` (canonical Contabo dimension → WHMCS optiontype map)

### Phase A.6.2 — pricing math + observe-mode syncer (~1 d)

- Extend `MarginCalculator` with per-option-delta helper
- Wire syncer in `option_sync_phase = observe` (writes audit, no WHMCS writes)
- Tests 1–15

### Phase A.6.3 — apply mode + UI (~1.25 d)

- Flip `option_sync_phase = apply` (still admin opt-in per profile)
- Profile-edit modal rework (defaults + configurable-options matrix + Sync button)
- Mappings page "configured?" column
- Dashboard tile
- Tests 16–25

### Phase A.6.4 — orphan handling + multi-currency (~0.5 d)

- Orphan grace-period logic
- USD/EUR/GBP pricing rows (read FX, write all 4 currencies in active set)

### Phase A.6.5 — docs + handoff (~0.25 d)

- This file becomes spec history
- New `docs/CONFIGURABLE_OPTIONS.md` is end-user-facing
- `CHANGELOG.md` 0.5.0 entry

**Total ≈ 4.5 days**. Same parallel-agent pattern as A.5.

---

## Files to create / modify

### Modified

- `lib/Installer.php` — `SCHEMA_VERSION = 4`, `migrateTo4()` added
- `lib/AdminController.php` — 5 new actions: `option-audit`, `option-sync-trigger`, `option-link-toggle`, `option-link-detail`, `ajax-option-preview`
- `lib/MarginCalculator.php` — `landedCostWithSelections()` + `optionDeltaForCycle()` helpers
- `lib/ApiClient.php` — `configurator($planSlug)` already exists; no change
- `templates/admin/profiles.tpl` — modal rework (split into Defaults pane + Configurable options pane)
- `templates/admin/mappings.tpl` — add "Options synced?" column
- `templates/admin/repricing.tpl` — add KPI tile
- `assets/app.js` — wire the new Configurable options matrix + sync button + ajax-option-preview
- `contabo_pricing.php` — version → `0.5.0`
- `CHANGELOG.md` — 0.5.0 entry
- `hooks.php` — `DailyCronJob` adds a Phase-A.6 sync call (observe mode by default)

### New

- `lib/ConfigurableOptionsSyncer.php` — the chokepoint for all tblproduct* writes
- `lib/OptionAuditLog.php`
- `lib/DimensionParser.php`
- `lib/OptionTypeMapper.php`
- `templates/admin/option_audit.tpl`
- `tests/OptionGroupCreationTest.php`
- `tests/DimensionParsingTest.php`
- `tests/OptionTypeMappingTest.php`
- `tests/SubOptionLifecycleTest.php`
- `tests/OptionPricingTest.php`
- `tests/ProductLinkTest.php`
- `tests/AuditLogTest.php`
- `tests/IdempotencyTest.php`
- `tests/StaticGrepTest.php`
- `docs/CONFIGURABLE_OPTIONS.md` (end-user)

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Syncer accidentally deletes a customer's chosen sub-option | Default behaviour is NEVER delete; orphan → hide. Admin must explicitly approve delete. |
| Two profiles fight over the same group | UNIQUE `(profile_id)` on `mod_contabo_option_group_link`. One profile = one group, always. |
| Configurator API returns different shape post-Contabo redesign | `DimensionParser` is isolated; one class to update. Add a parser version test. |
| WHMCS schema changes in next major | We touch only documented WHMCS tables; mocked in tests. |
| Sync runs amid a customer checkout, mid-write | All writes per-profile wrapped in a transaction. WHMCS reads see consistent state or pre-write state. |
| Massive option count (e.g. 50 apps × 6 cycles × 4 currencies = 1200 pricing rows per option) | `option_sync_batch_max_writes` cap (default 200). Above cap → abort + admin alert with row count. |
| Provisioning module mismatch on label round-trip | `mod_contabo_option_sub_link.contabo_label` is the authoritative key. Provisioning module reads label, not WHMCS optionname. |

---

## Out of scope (deferred)

- The actual Contabo provisioning module (Phase C, separate plan).
- Customer-area UI changes (we don't touch the cart — WHMCS renders configurable options natively).
- Promo/discount interactions with configurable options.
- Configurable-options-aware reports / MRR projections (Phase D).

---

## Decision points for review

1. **Orphan policy default**: hide after 90 days vs. immediate hide vs. never hide. Plan defaults to 90-day hide. Confirm or change.
2. **Pricing currency scope**: INR-only in v1 vs. all 4 currencies (INR/USD/EUR/GBP) in v1. Plan defaults to INR-only in v1, multi-currency in v1.1. Confirm.
3. **Per-profile opt-in default**: `expose_configurable_options=0` by default (must explicitly enable per profile). Confirm.
4. **Phase A.6 enforce vs observe**: ship in observe mode (recommended) vs. ship in apply mode directly. Plan defaults to observe-first. Confirm.

When you've reviewed: say "ship A.6 with options 1=X, 2=Y, 3=Z, 4=W" and I'll dispatch the parallel agents the same way I did for A.5.
