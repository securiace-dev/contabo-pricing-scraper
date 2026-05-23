# Changelog

## 0.5.0 — 2026-05-23 (A.6 complete — configurable options: observe → apply → curate → safe rollback)

The **A.6 milestone**: an admin maps a Contabo plan to a WHMCS product, previews the
curated configurable-option catalog, applies it idempotently with drift protection and
a seeded capability matrix, and can now cleanly reverse it. Cut after a three-lens review
(architect / edge-case / security) whose release-gating conditions are all addressed below.

### Added — A.6.5
- **`ConfigPurgeService`** (design §19) — config-object-aware purge. Deletes ONLY the
  WHMCS configurable options the addon **created** (groups / options / sub-options /
  `tblpricing(configoptions)` rows / product links), scoped strictly to the ids recorded
  in the `mod_contabo_config_*_link` tables. Never touches a config object the addon
  didn't create, nor any client / service / invoice / order. Idempotent. Closes the
  prod-rollback gap: a mis-applied catalog now has a clean, ownership-scoped reversal
  instead of leaving orphaned objects on the live product.
- **Maintenance purge toggle** — opt-in `purge_config_objects` checkbox on the
  maintenance page. Runs the config-object purge **before** the `mod_contabo_*` truncate
  (so the ownership link tables are still readable), behind the existing typed-phrase +
  CSRF + confirmation gating. Reports per-table delete counts; logs to the activity log.

### Fixed — review-gated correctness on the revenue path
- **`addon_price_snapshot` phantom column** (architect condition #1) —
  `ServiceRevenueResolver::resolveFromSnapshot()` no longer reads a column the snapshot
  table doesn't have. Addons are explicitly **not** part of the v1 snapshot revenue path
  (the live `resolveForService` path is the one that sums `tblhostingaddons`); the result
  carries `addons_in_snapshot => false` so the snapshot-vs-live difference is documented,
  not a silent zero.
- **Multi-currency guard on the resolver/snapshot side** (architect condition #2,
  amendment #10) — `ServiceRevenueResolver` now resolves the service's billing currency
  (via the client) and surfaces `currency_id` + `currency_supported`. A non-INR service
  is flagged (never silently priced off INR catalog rows), and `ServiceConfigSnapshot`
  logs a loud activity-log warning when capturing one. The adapter already refused
  non-INR writes; this completes the half-built guard on the read side.

### Tests
- `ConfigPurgeServiceTest` (ownership scope held: addon objects deleted, non-addon
  objects + clients untouched; idempotent; no-links no-op), multi-currency guard tests
  (INR supported / non-INR flagged), and the corrected snapshot contract. Added
  `delete()` to the `FakeCapsule` test double. Suite: 367 tests.

### Known deferrals (scoped out of 0.5.0, tracked for 0.5.x / Phase C)
- Sub-option + pricing + group **drift** coverage (v1 is option-level only).
- An outer transaction around `apply()` (each upsert is its own transaction today; the
  new purge toggle provides the reversal path in the meantime).
- Per-image-category / per-sub-value **exposure** curation; pre-apply "diff against this
  live product" screen; `addon_price_snapshot` column + capture; per-cycle option price
  detail in the snapshot.
- **Do NOT** wire the recorded `whole_config_below_floor` margin signal to drive
  repricing without first backing config revenue out of the base candidate (billing-
  critical, Phase C).
- Compatibility-rule seeding (the matrix is wired into `SelectionValidator` but inert);
  capability auto-apply gate stays dormant (all entries are `manual_assumption` — correct
  for a no-provisioning release).

## 0.4.12 — 2026-05-23 (schema v6 — drift detection: re-apply won't clobber admin edits)

### Added
- **Schema v6** (`migrateTo6`) — adds a nullable `expected_hash` column to the three
  config link tables (idempotent, hasColumn-guarded). The `upgrade()` chain runs it
  automatically on the next admin load / activation.
- **`DriftHasher`** — pure, deterministic canonical hash of an object's addon-controlled
  fields (order-independent, control-byte separators, sha1). 15 tests.
- **Drift guard in `apply` (amendment #14)** — before overwriting a WHMCS option the
  addon previously created, it re-hashes the live object and compares to the recorded
  baseline. On a mismatch (an admin hand-edited it out of band) the option is **flagged
  (`drift_skip` audit, `summary.drift_skipped`) and skipped — never clobbered**. After a
  clean write it records the new baseline. `WhmcsConfigOptionsAdapter::fetchOption()` +
  `OPTION_DRIFT_COLUMNS` back the check; `ConfigOptionLinkRepository::upsertOptionLink`
  stores `expected_hash`.

### Notes
- v1 covers **option-level** drift (the structural/visibility object). Sub-option +
  pricing + group drift are a follow-up (the pattern + hash column are in place).
  Verified end-to-end on the local dev WHMCS: a hand-edited live option survives a
  re-apply (drift_skipped=1, edit intact). 16 new tests; suite 358.

## 0.4.11 — 2026-05-23 (exposure curation — apply produces a curated catalog)

### Fixed / Added — the configurable-product feature is now usable end-to-end
Before this, `apply` exposed **every** option (no `hidden` control), so applying a
profile flooded the customer order form with all 34 images + every dimension.
Amendment #8 ("preview-first, nothing exposed until ticked") was not enforced.

- **`WhmcsConfigOptionsAdapter::upsertOption`** gained an optional `$hidden` arg —
  controls WHMCS option visibility (null = unchanged, so `observe()` is untouched).
- **`ExposureResolver`** — bridges DimensionParser keys → `RetailVpsMinimalPreset`
  exposure decisions (`decideForDimension`, `decideForImageCategory`).
- **`ConfigurableOptionsSyncer::apply`** now sets WHMCS visibility from the exposure
  decision: an existing option-link's curated flags win (admin edits), else the
  Retail-Minimal default. Image sub-values are hidden per category (OS shown;
  Panels/Apps/Blockchain hidden). The chosen exposure is recorded on the option-link.
- **Exposure editor** — new `config-exposure` admin screen (+ `config-exposure-save`)
  to toggle each dimension's expose/hidden flags per profile; re-apply pushes them.
  `ConfigOptionLinkRepository::listOptionLinksForProfile()` backs it.

Verified on the local dev WHMCS: applying a sample profile leaves Image + Region +
Data Protection exposed, hides Bandwidth/Private-Networking/Storage, and hides the
Panel/App/Blockchain image sub-values while keeping OS visible. 23 new tests; suite 343.

## 0.4.10 — 2026-05-23 (wire capability + compatibility into the live flow)

### Added
- **`CapabilityDefaultsProvider`** — supplies the §4 default capability classification
  per dimension (Image→reinstall/destructive, Region→recreate, Storage→reinstall,
  Data-Protection/IPv4/Bandwidth/Private-Net→in-place; all `capability_source=manual_assumption`)
  and `seedForPlan()` to populate the capability table.
- **`SelectionValidator`** — single facade composing the two matrices: hard compatibility
  violations (`validateCombination`) + non-blocking capability warnings (destructive options).
- **Wired into the flow:**
  - `config-apply` now **seeds the §4 capability defaults** for the plan's
    dimensions/values after writing the WHMCS config options (count shown in the flash).
  - `config-preview` runs `SelectionValidator` on the default selection and **surfaces
    compatibility violations + destructive-option warnings** (with backup/admin-approval
    flags + capability source) in a "Default-configuration checks" panel.

### Notes
- Capability defaults are `manual_assumption` until a Phase C deploy-API check upgrades
  them to `api_verified` (only `api_verified` may auto-apply a destructive change). The
  compatibility matrix has no automatic data source yet, so its `validateCombination`
  surfaces nothing until rules are added — the hook is in place. Verified end-to-end on
  the local dev WHMCS. 20 new tests; suite 320.

## 0.4.9 — 2026-05-23 (capability + compatibility matrix repositories)

### Added
- **`ConfigOptionCapabilityRepository`** (§4 + amendment #6) — sole chokepoint for the
  `mod_contabo_option_capability` table (existed since schema v5, was unused). upsert/find/
  listForPlan + `canAutoApply(row, isDestructive)` enforcing the capability-source gate:
  only `api_verified` may auto-apply a destructive/in-place change; `scrape_verified`/
  `manual_assumption`/`admin_override`/`unknown`/missing all require admin approval. 13 tests.
- **`ConfigOptionCompatibilityRepository`** (§5) — sole chokepoint for the
  `mod_contabo_option_compatibility` table. upsertRule/find + `validateCombination(plan,
  selections)` rejecting incompatible pairs, missing required values, and qty out of
  [min,max]; returns `{valid, violations[]}`. 12 tests.

### Notes
- Foundation chokepoints only — **not yet wired** into the order/selection validation
  flow or the apply/lifecycle path (that wiring is the next step). The tables now have
  guarded read/write access. Both mirror the `ConfigOptionLinkRepository` pattern
  (incl. the stdClass-cast discipline). Suite 300.

## 0.4.8 — 2026-05-23 (Phase B: landedCostWithSelections — whole-config margin)

### Added
- **`MarginCalculator::landedCostWithSelections(baseEur, selections[], fx…)`** — landed
  monthly cost of a whole configured service (base + every selected option). Each
  option's EUR delta is clamped ≥ 0 (a cheaper-than-default option never reduces cost)
  and multiplied by quantity (matches the ServiceRevenueResolver qty convention so
  revenue and cost stay aligned). Linear in EUR, so it equals the single landed-cost of
  the summed EUR.
- **Snapshot now carries per-option EUR deltas.** `ServiceConfigSnapshot` enriches
  `selected_options_json` with each selection's `monthly_eur_delta` (from its value link),
  making the snapshot self-contained for the whole-config cost calc.
- **RenewalEngine records the whole-config margin.** When a snapshot supplies the
  selections, the engine computes the whole-config landed cost + margin ratio
  (`landedCostWithSelections` vs the resolved revenue) and records `margin_basis`,
  `whole_config_landed_for_cycle`, `whole_config_margin_ratio`, `whole_config_below_floor`
  in `metadata_json` — an **accurate signal for config-driven undercharging**.

### Notes
- This is recorded only; it does not yet *drive* the base candidate/floor decision.
  Driving repricing off the whole config requires the candidate to back out config
  revenue (so the base bump restores the whole-config floor) — a separate, careful step
  in the billing-critical candidate path, deliberately not bundled here. No billing-math
  regression: existing renewal behaviour is byte-identical without an injected snapshot.
  11 new tests; suite 275.

## 0.4.7 — 2026-05-22 (A.6.4 snapshot capture + ServiceRevenueResolver wiring)

### Added
- **A.6.4 service config snapshot** (`ServiceConfigSnapshot`, design §12) — captures
  a point-in-time record of a service's configuration + agreed-upon prices into
  `mod_contabo_service_config_snapshot` (the table existed but was unused). Captured
  on provision via a new `AfterModuleCreate` hook; the stable basis for renewal margin
  (immune to later catalog drift). Recovers the selected image/region by round-tripping
  each WHMCS sub-option id back to its Contabo value link (§17).
- **ServiceRevenueResolver wired into RenewalEngine** (amendment 5, opt-in) — when a
  resolver (+ optional snapshot reader) is injected, the engine records each service's
  TRUE revenue (snapshot-preferred) and the drift from the stale
  `tblhosting.recurringamount` in `metadata_json`. Default (not injected) = unchanged.

### Fixed
- **ServiceRevenueResolver was both buggy and dormant.** (1) It read `Capsule::first()`
  results as arrays — real WHMCS returns stdClass (same trap as the adapter), so it would
  have crashed on a live read. (2) It treated `tblhosting.recurringamount` as the base and
  added config options on top — but `recurringamount` already includes config options and
  drifts (preflight §5), so it double-counted. Base now comes from the product catalog
  (`tblpricing` type=product); `recurringamount` is exposed only as `current_charge`.
  Class is no longer `final` (matches its "overridable for tests" design).

### Notes
- The renewal engine **records** true revenue + drift but does NOT yet let it drive the
  margin/floor decision: that needs the matching cost side (`landedCostWithSelections`),
  which §13 designates a Phase B step. Wiring it half-way would inflate the (base-only)
  margin. 6 new tests (snapshot + revenue wiring); suite 268.

## 0.4.6 — 2026-05-22 (A.6.3: configurable-options apply path)

### Added
- **Apply path** — writes a profile's previewed configurable options to a mapped
  WHMCS product. New `ConfigOptionLinkRepository` (sole chokepoint for the
  `mod_contabo_config_*_link` ownership tables), `ConfigurableOptionsSyncer::apply()`
  (real-write traversal that records each WHMCS id in the link tables + writes real
  `OptionAuditLog` rows), and a confirmed `config-apply` admin action (POST,
  CSRF-protected, requires the profile mapped to a product + an explicit
  confirmation checkbox). The preview screen's Apply button is now a real form
  (product select + confirm) or a "map a product first" notice.
- **Idempotent + ownership-scoped**: re-applying reuses the recorded WHMCS ids
  (no duplicates) and only ever touches addon-created objects; base-currency only.
  Drift detection (manual-edit guard) is deferred — it needs hash columns the v5
  link tables don't carry yet. 10 new tests (link repo + apply mode); suite 259.

## 0.4.5 — 2026-05-22 (A.6.3: configurable-options preview screen)

### Added
- **Configurable-options preview** (`&action=config-preview&id=N`, "Config preview"
  in each profile row's actions). Read-only/dry-run: fetches the plan's live Contabo
  options from the API, runs them through `DimensionParser` →
  `ConfigurableOptionsSyncer::observe()`, and renders the exact WHMCS configurable
  options that would be created — group → options (Image as one dropdown; Networking
  split into Bandwidth/IPv4/Private; Region/Storage/Data-Protection) → sub-values with
  per-cycle pricing across all 6 billing cycles. Pricing reuses the profile version's
  landed-cost basis + markup; cheaper-than-default values clamp to 0. Nothing is
  written; the apply path is the next, gated step. Shows skipped/omitted dimensions
  and the pricing assumptions used.

## 0.4.4 — 2026-05-22 (profiles toolbar + bulk-bar fixes)

### Fixed
- **Toolbar labels overlapped their controls.** The "Search" label sat inside
  `.cb-search`, whose `::before` search icon is absolutely positioned at the
  left — so the icon rendered on top of the label ("SE⎄RCH"). The "Sort" label
  sat inside the bordered `.cb-seg` segment box, cramped against the first
  button. Both labels are now siblings of their controls, separated by the
  toolbar's gap. (profiles.tpl)
- **Bulk-action bar never tracked the selection.** `refreshBulkToolbar()` looked
  for the bar via `table.closest('.cb-card')`, but the bar is a separate sibling
  card — so it was never found: the "N selected" count stayed at 0 and the bar's
  visibility never toggled. It now resolves the bar at document scope, updates
  the count, and toggles the `hidden` attribute. (assets/app.js)
- **Bulk bar was always visible.** Its `hidden` attribute was defeated by an
  inline `display:flex`; the inline display is removed so `hidden` hides it until
  a row is selected. (profiles.tpl)

## 0.4.3 — 2026-05-22 (visual-QA gap fixes: drawer, sidebar, flash, asset cache-buster)

Found during a browser-driven visual walk of every admin page against the local
dual-stack WHMCS (8.13 + 9.0) with the Contabo API served locally in docker.

### Fixed
- **Profile detail drawer was invisible.** `#cb-drawer-profile` ships with the HTML
  `hidden` attribute (so it doesn't flash on load); the `.cb-drawer.open` CSS only set
  the slide-in `transform` and never cleared `hidden`/`display:none`, so clicking a
  profile row did nothing visible. `openDrawer()` now removes `hidden` + sets
  `aria-hidden="false"`; a new `closeDrawerEl()` re-applies both on close. (assets/app.js)
- **Five admin pages had no sidebar link.** `contabo_pricing_sidebar()` listed only
  Dashboard/Profiles/Mappings/Sync history/Settings; Repricing, Price decisions, Skipped
  report, Tax settings and Maintenance were reachable by direct URL only. Added them under
  a "Repricing" divider. (contabo_pricing.php)
- **Dashboard silently dropped flash messages.** Redirects from "Run sync" / "Trigger API
  refresh" pass `?flash=…`, but `dashboard.tpl` had no flash block (profiles/mappings did),
  so the admin saw no success/failure feedback. Added a flash block that styles failures
  as `cb-error`. (dashboard.tpl + AdminController::dashboard now passes `flash`)
- **Asset cache-buster was frozen.** `app.js?v=…` fell back to a hardcoded `0.2.0` because
  `cb_addon_version` was never passed to the layout — so a JS change never reached browsers
  that had cached the old file. The version is now a single source of truth
  (`AdminController::VERSION`), consumed by both `contabo_pricing_config()` and the layout,
  so every release invalidates the cached asset.

## 0.4.2 — 2026-05-22 (fresh-install schema fix + local dev/test harness)

### Fixed
- **Fresh addon install left a broken schema.** `Installer::install()` created the
  v1-shaped tables but stamped `schema_version = SCHEMA_VERSION`, so `SchemaHealth`
  saw "already current" and never ran the migrations that add `catalog_cycles_mask`,
  `profile_mode`, the `config_*` tables, etc. — leaving 14 required columns missing on
  any clean activate. (Production was unaffected because it activated at v1 and upgraded
  step-by-step; only a brand-new install hit it.) `install()` now records v1 then runs
  the idempotent upgrade chain, so a fresh activate ends fully current. Verified on the
  WHMCS 8.13 + 9.0 local matrix. Regression: `tests/FreshInstallSchemaTest.php`.

### Added
- **Local dev/test harness** — `scripts/local-whmcs.sh` installs + exercises the addon
  against the dockerised dual-stack WHMCS dev environment (WHMCS 8.13 @ :8013 + 9.0 @ :8090)
  from the securiace-vps-platform project. Commands: `sync`, `migrate`, `activate`,
  `render`, `status`. Never touches production. Lets us reproduce the WHMCS CI matrix
  (PHP 7.4/8.x × WHMCS 8.13/9.0) locally and run the amendment-4 preflight without prod.

## 0.4.1 — 2026-05-22 (Phase A.5.1: bugfix & stabilisation)

### Fixed
- **Mapping save crashed** with `SQLSTATE[42S22] Unknown column 'apply_to_monthly'`. `migrateTo3` dropped the legacy `apply_to_*` columns but `AdminController::mappingSave()` still wrote them. All runtime references removed; mapping writes now go through a single guarded `MappingRepository`.
- **Profile create crashed** with `SQLSTATE[23000] Duplicate entry … for key 'mod_contabo_profile_slug_unique'` when an admin created the same profile twice. Now handled gracefully via `ProfileRepository::createOrResolve()`: same slug + same config loads the existing profile; same slug + different config shows a conflict chooser (open existing / create with suffix / update existing / cancel). No raw SQL ever reaches the admin UI.

### Added
- **`MappingRepository`** — the single write path for `mod_contabo_mapping`. Whitelist-filters to schema-v3 columns; any stray legacy `apply_to_*` key is dropped + logged.
- **`ProfileRepository` + `ProfileIdentityResolver`** — slug + mode-aware fingerprint. `profile_mode` (`fixed_admin_profile` default, `customer_configurable_product` reserved for A.6). Fingerprint treats OS/App/Control-Panel/Blockchain as ONE mutually-exclusive Image choice, never four independent fields.
- **Schema v4**: `mod_contabo_profile` gains `profile_mode` (VARCHAR 40), `profile_fingerprint_hash` (CHAR 64), `profile_identity_json` (LONGTEXT). Idempotent `migrateTo4()`.
- **`SchemaHealth::assertOrMigrate()`** — auto-runs pending migrations at the top of dashboard load, profile create/save, mapping save. Stale schemas self-heal; failures show a friendly prompt, never a stack trace.
- **Maintenance page** (`?action=maintenance`) — schema health panel, "Run migrations now", and a guarded "Purge module data" (requires ticking a checkbox + typing `PURGE CONTABO PRICING DATA`; backs up every `mod_contabo_*` table to a `_purgebackup_<ts>` table before truncating; never touches WHMCS clients/services/invoices/transactions).
- **Friendly SQL error translator** — `23000` → "entry already exists", `42S22` → auto-repair + retry prompt; full technical detail logged via `logActivity`.

### Tests
- 13 new tests (mapping legacy-rejection, profile duplicate/conflict/suffix, fingerprint identity, schema-health auto-migrate, purge confirmation) + a `LegacyFieldGrepTest` regression gate asserting `apply_to_*` appears only in `Installer.php`.

## 0.4.0 — 2026-05-22 (Phase A.5: WHMCS 6-cycle pricing support)

### Added
- **Full WHMCS 6-cycle support**: Monthly / Quarterly / Semi-Annually / Annually / Biennially / Triennially. Previously only 3 cycles were addressable; the live install had 23 services on cycles the addon couldn't even map.
- **Schema v3** with two separate bitmasks on `mod_contabo_mapping`:
  - `catalog_cycles_mask` (which cycles SyncEngine may write to `tblpricing` for new orders)
  - `renewal_cycles_mask` (which cycles RenewalEngine may evaluate for existing-service repricing)
- **Per-cycle markup overrides** (`markup_overrides_json` LONGTEXT). Strategies: `inherit` / `cost_plus_pct` / `cost_plus_amount` / `fixed`. Enables commit-discount UX (e.g. lock Annual at fixed ₹9,200 while Monthly inherits cost-plus-15%).
- **Sentinel-aware catalog writes**: respects WHMCS's `-1.00 = disabled`, `0.00 = free`, `>0 = priced` convention. Two opt-in flags: `respect_disabled_cycles` (default ON) and `overwrite_free_cycles` (default OFF).
- **Setup-fee sync opt-in** (`sync_setup_fees`, default OFF) with `setup_fee_overrides_json`. Setup fees affect new orders only; never touched by RenewalEngine.
- **Rounding modes**: `exact_2_decimals` (default), `nearest_rupee`, `nearest_9`, `nearest_99`, `nearest_100`, `custom`. Audit row stores both `pre_round_price` and `rounded_price`.
- **New `mod_contabo_catalog_audit` table** — append-only ledger of every tblpricing write or deliberate skip; one row per (sync_batch, product, currency, cycle) combo.
- **Two new skip-reason values** distinguishing the cases that previously collided:
  - `cycle_unsupported` — Free Account / One Time / unknown billing cycle
  - `cycle_not_mapped` — recurring cycle that is absent from the mapping's `renewal_cycles_mask`
- **Cycle-aware scheduled changes**: `mod_contabo_price_change_schedule` gains `cycles_mask`, `applies_to_catalog`, `applies_to_renewals`. Same `applyWithGuards()` write path as cron decisions — no second unsafe write path.
- **Mapping form rewrite** — 6-row cycle table with per-cycle current price + status (priced/free/disabled/absent) + catalog-sync checkbox + renewal-repricing checkbox + per-cycle markup override editor + rounding mode + three guard checkboxes.
- **`ajax-product-cycles` endpoint** — when admin picks a product+currency in the mapping form, returns the full `tblpricing` cycle row so disabled/free states are surfaced as info (not silently silently allowed).
- **Dashboard cycle tiles** — "Services per cycle" breakdown + "Cycle exposure" (count of services on recurring cycles missing from the renewal mask). Audit log gains cycle filter + CSV column.
- **Helper classes**: `CycleSet` (bitmask helper with 6 bits 0..5 + `MASK_MAX = 63`), `CyclePricingMap` (canonical cycle → tblpricing column + setup-fee column + months), `Rounding` (shared across SyncEngine and RenewalEngine).
- **Migration safety net**: timestamped backup table `mod_contabo_mapping_backup_v3_YmdHis` created via `CREATE TABLE LIKE` + `INSERT SELECT` before legacy columns are dropped; row-count parity asserted; per-row validation gate must pass or legacy columns are retained.

### Changed
- `SyncEngine` rewritten to be 6-cycle aware. Walks every (mapping × currency × cycle) combo. Honours the sentinel rules. Skips with explicit audit row instead of silent no-op. Still NEVER writes to `tblhosting`.
- `RenewalEngine` gates by `renewal_cycles_mask` BEFORE policy hierarchy. Per-cycle markup override resolved before margin math. Rounding applied with both pre/post values stored in decision metadata.
- `ScheduledChangeProcessor` (was scaffold-only in Phase A) — now real: walks due rows, iterates `CycleSet::fromMask($cyclesMask)->enabledCycles()`, delegates to `RenewalEngine::decideForScheduledChange()`.

### Migration
- Legacy `apply_to_monthly`/`apply_to_semiannually`/`apply_to_annually` boolean columns dropped after validation passes. Backfilled to bitmask: monthly→bit 0, semiannually→bit 2, annually→bit 3. Backup table retained for rollback.
- New columns guarded by `hasColumn`; re-running `migrateTo3()` is safe.

### Tests
- 52 new tests across `CycleSetTest`, `MigrationV3Test`, `SyncEngine6CycleTest`, `RenewalEngineCycleTest`, `ScheduledChangeCycleTest`, `MappingFormTest`, `DashboardCycleTilesTest`.
- Full suite: **117 tests, 394 assertions, 0 failures**.
- Static grep verified: zero `tblhosting` writes outside `ServicePriceWriter.php`.

### Phase status
- Phase A.5 ships in `observe` mode (inherited from Phase A). `ServicePriceWriter` and `Notifier` still gated to `enabled=false`. No service price moves until you explicitly opt into Phase B.

### Known follow-ups (Phase B)
- Extract `MarkupResolver` helper so `SyncEngine` and `RenewalEngine` share one resolution path instead of two parallel ones.
- Adopt the shared `Rounding` helper in SyncEngine (currently SyncEngine has its own implementation).

## 0.3.0 — 2026-05-22 (Renewal Pricing Policy Engine — Phase A: Observe)

### Added
- **Renewal Pricing Policy Engine** — schema v2 with 6 new tables (`mod_contabo_service_policy`, `mod_contabo_price_decision`, `mod_contabo_pricing_action`, `mod_contabo_price_change_schedule`, `mod_contabo_price_notice`, `mod_contabo_repricing_lock`) and additive columns on `mod_contabo_profile` + `mod_contabo_profile_version`. All migrations are idempotent.
- **8 engine classes**: `RenewalEngine`, `PolicyResolver`, `MarginCalculator` (with floor-ratio conversion centralised), `TaxModeEngine` (8 modes; default = `unregistered_no_output_tax` for non-GST-registered resellers), `CycleNormalizer`, `Lock` (GET_LOCK with mod_contabo_repricing_lock fallback), `DecisionLog` (append-only), `PricingActionLog`.
- **`ServicePriceWriter`** — the single chokepoint for `tblhosting.recurringamount` writes. Prefers WHMCS LocalAPI `UpdateClientProduct`; falls back to raw update with logActivity. Transactionally bracketed. Phase A: gated to inert via constructor `enabled=false`.
- **`Notifier`** — durable + idempotent. `upsertNotice()` computes `sha1(serviceId|targetPrice|effective_at|noticeType)` and inserts on conflict-ignore. Phase A: `send()` is inert; notice rows accumulate in `mod_contabo_price_notice` with `status=pending`.
- **`Watchdog`** — `InvoiceCreation` (pre-final, log-only) + `InvoiceCreated` (post-final, log + admin alert) compare each invoice line to the latest applied decision.
- **`CronDriver`** — Phase A observe-only sweep. Walks mapped services, calls `RenewalEngine::decide()`, processes scheduled changes, prunes old decisions.
- **`BackfillCommand`** — sets every existing mapped service to `policy=lifetime` with `locked_price=current recurringamount`. Idempotent.
- **`EmailTemplateSeeder`** — installs 4 WHMCS email templates on activation: Notice / Reminder / Confirmation / Force-Approve Alert.
- **5 new admin pages**: Repricing dashboard, Audit log (with CSV export), Skipped report, Tax mode settings, embedded per-service "Contabo Pricing" tab in WHMCS service profile.
- **AJAX endpoint** `ajax-policy-preview` — what-would-happen-at-next-renewal calculation for any hypothetical policy.

### Changed
- `SyncEngine` no longer writes to `tblhosting.recurringamount`. Catalog updates only.
- Default profile policy is `current_term` (not `reprice_renewal`). Reprice-every-renewal is explicit opt-in.

### Phase gate
- `repricing_phase = observe` (default). Engine evaluates every cron pass and writes audit rows, but `ServicePriceWriter.enabled = false` so no service price ever moves. Run for ≥ 7 days before opting in to Phase B.

### Verified
- 65/65 PHPUnit tests green (8 new MarginMath + 4 new NoticeIdempotency on top of pre-existing).
- Static grep: zero `tblhosting` writes outside `ServicePriceWriter.php`.
- PHP 7.4 + 8.x polyglot maintained.

## 0.2.0 — 2026-05-21 (UI/UX rebuild)

### Added
- Redesigned admin UI using a refined editorial palette (IBM Plex Sans/Mono, Instrument Serif accent, burnt-orange dominant on cream).
- Status strip (4-tile KPI row) on every page: API health, plan count, last sync, active profile count.
- Sortable, filterable profiles table with inline sparklines of recent prices.
- Filter pills (All / Active / Drifted / Inactive) + search + bulk select with multi-profile actions.
- Profile create as a modal with live `/api/v1/quote` price preview.
- Drawer for profile detail + version history.
- Sync history with status / trigger / date filters.
- Settings page sectioned into API / Sync / Currency & Tax / FX with a live FX rate preview.
- Keyboard shortcuts: `/` focus search, `n` new profile, `r` test API, `Esc` close.
- Toast feedback for AJAX actions.
- AJAX endpoints in AdminController: `ajax-quote`, `ajax-fx`, `ajax-meta-probe`, `ajax-profile-versions`, `ajax-profile`. Read endpoints are CSRF-open; mutators require `check_token()`.
- `CHANGELOG.md` (this file) and `docs/UI_ARCHITECTURE.md`.

### Changed
- All admin templates rewritten under a shared design system in `_layout_open.tpl`.
- Buttons are now `cb-btn` / `cb-btn.ghost` / `cb-btn.danger` / `cb-btn.subtle` with consistent affordance.
- "Run sync now" + "Trigger API refresh" + "Toggle profile" converted from `<a href>` GET links to POST forms with CSRF tokens (security fix for v0.1.x).

### Fixed
- `contabo_pricing_sidebar()` now returns a string (HTML) instead of an array, so WHMCS no longer prints the literal "Array" in the sidebar slot.
- PHP 7.4 polyglot — all 8.x-only syntax removed so the addon runs on FastPanel 7.4 sites as well as 8.x.

## 0.1.0 — 2026-05-21 (initial)

- First production release: profile manager, sync engine, mappings, daily cron, admin home widget, WHMCS-native CSRF + encrypted bearer token storage.
