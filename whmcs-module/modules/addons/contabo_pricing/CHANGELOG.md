# Changelog

## 1.1.0 — 2026-08-05 (Proposal Studio preview boundary)

- Adds an admin-first, deterministic Proposal Studio with separate client and
  internal artifacts; delivery remains hard-blocked until immutable approval,
  durable outbox/idempotency, and attachment-token persistence are available.
- Separates provider tax cash/recoverability, FX/card markup, owner adjustment,
  and fail-closed Securiace output GST; tax-inclusive provider quotes are
  decomposed before landed-cost recovery logic.
- Adds Founder Managed quantities from 1–99, scaling annual fees and included
  monthly Founder minutes while preserving per-server and total evidence.
- Adds up to four authoritative same-term plan comparisons with non-billing
  totals, field-specific visibility, and cross-family warnings.
- Adds bounded OpenAI-compatible narrative assistance with deterministic
  fallback, advisory cost metadata, redirect/endpoint safeguards, and strict
  client-projection leakage checks.

## securiacevps 2.0.0 — 2026-07-30 (WHMCS-native lifecycle and customer experience)

This release replaces direct request/response provisioning with the canonical
`securiacevps` WHMCS provisioning module. The former `contabo_vps` entrypoint is
retained only as a delegation shim for phased service reassignment.

### Provisioning integrity

- Requires an immutable paid-order snapshot containing the published mapping,
  catalog, configuration, price, cart, and compatibility identities.
- Persists deterministic operations, attempts, provider requests, leases,
  fencing tokens, reconciliation findings, and correlation IDs in module-owned
  WHMCS MySQL tables.
- Treats accepted requests followed by timeouts as unknown outcomes and
  reconciles before retry; duplicate callbacks return the original operation.
- Retries transient responses only for reads. A mutation that receives a
  transport, rate-limit, or 5xx ambiguity returns immediately to durable
  reconciliation instead of being replayed inside the HTTP client.
- Completes provider ownership and request preflight before persisting the
  provider-effect marker, and renews a ten-minute minimum fenced lease
  immediately before each effect so an expired worker cannot submit.
- Keeps WHMCS Pending until create readiness is read back and verified; changes
  commercial lifecycle state only after the provider effect is verified.
- Serializes commercial intents on the WHMCS service row, fences and supersedes
  older in-flight intents, and projects only from explicit predecessor states,
  so delayed callbacks cannot reactivate Cancelled or Terminated services.
- Routes create, suspend, unsuspend, terminate, power, reinstall, and password
  reset through the same durable engine. Direct provider-mutation helpers now
  fail closed.
- Adds certified snapshot inventory, create, delete, and rollback workflows.
  Provider requests use stable UUID audit identities; ambiguous outcomes are
  reconciled without replay, and rollback warns that newer snapshots are
  removed.

### Ownership, recovery, and billing

- Adds exact-tag ownership verification, read-only existing-service adoption,
  provider orphan inventory, and destructive-action guards.
- Adds billing-saga projections for provider success paired with failed WHMCS
  persistence, exposing repair state without repeating the provider mutation.
- Adds retry classification, crash recovery, expired-lock handling, manual
  review, safe operator commands, and global/per-capability write switches.
- Adds expiring fenced claims to operation, operator-command, and communication
  queues so cron interruption is recoverable and an expired worker cannot
  overwrite its replacement.
- Adds lifecycle communications with delivery state and customer-safe error
  references.

### Credential lifecycle

- Replaces persistent plaintext service-password delivery with encrypted
  temporary secrets and owner-bound, short-lived, one-time reveal tokens.
- Prevents provider credentials and raw response payloads from entering the
  customer UI, operation timeline, audit metadata, or module logs.

### Customer and administrator UX

- Adds a local-projection client service experience with truthful provisioning
  progress, networking, recovery, billing, and capability-derived actions.
- Enforces POST, WHMCS CSRF, service ownership, capability certification, and
  typed confirmation for customer mutations.
- Expands the addon operations workbench with queue health, attempts, adoption,
  reconciliation, billing-repair, communications, and safe recovery controls.
- Ships a VPS-specific Standard Cart child template. Product discovery and
  configuration gain a responsive, accessible visual layer while WHMCS retains
  cart, session, coupons, tax, invoice, payment, and shared checkout behavior.

## 1.0.0 — 2026-07-30 (native catalog, pricing, mapping, and operations workbench)

Schema **v14** is additive and idempotent.

### Catalog and order contracts

- Imports versioned Rust `/api/v1/catalog` payloads with stable machine IDs,
  provider IDs, observation timestamps, availability/deprecation state, and
  canonical hashes.
- Publishes immutable mapping versions and validates hash integrity before use.
- Adds checkout-time validation of catalog availability, mapping publication,
  configuration compatibility, option exposure, and management-tier rules.
- Seals paid-order snapshots only after WHMCS payment/fraud eligibility and
  rejects unmapped or mutable selections. Money remains decimal strings.
- Keeps the Rust API outside existing-service lifecycle operations; an outage
  may pause catalog refresh or new quotation but cannot block an existing VPS.

### Operations and security

- Adds the WHMCS-native operation, adoption, reconciliation, capability,
  one-time-secret, billing-saga, communication, and audit schemas consumed by
  `securiacevps`.
- Adds the minimum operator recovery workbench required before provider writes.
- Adds provider-write kill switches, credential lifecycle management,
  authorization/method enforcement, redaction, and safe error references.
- Seeds customer lifecycle email templates without embedding secret material.

### Compatibility

- Validates the addon and provisioning module on PHP 7.4-compatible syntax and
  the WHMCS 8.13.x/9.x module, hook, client-area, order-form, and cron contracts.
- Uses explicit sub-64-character MariaDB index names and repairs interrupted
  non-transactional DDL by matching index column sequences on migration retry.
- Adds a staged `contabo_vps` compatibility shim and a release package containing
  the canonical module, shim, and VPS order-form child template.

## contabo_vps 1.0.0 — 2026-07-07 (provisioning module production hardening)

Full audit + rework of the `modules/servers/contabo_vps` provisioning module
against the official Contabo REST API contract. No addon schema change. New
contract doc: `docs/PROVISIONING_CONTRACT.md`.

### Correctness (bugs fixed)
- **Contract `period` now sent** (required by the create API): WHMCS billing
  cycle maps Monthly→1 … Annually→12 (bi/triennial→12, logged). Previously every
  order silently became a 1-month Contabo contract.
- **Cancel body corrected** to the documented `{}` (was an undocumented
  `terminationDate`); the returned `cancelDate` is logged. Empty bodies now
  encode as JSON objects (`{}`), never `[]`.
- **Root passwords work now**: the service password is vaulted as
  `whmcs-svc-{id}-root` and passed as `rootPassword` secretId on create;
  Reset Password (admin + new client button) rotates it and updates
  `tblhosting.password` only after the API accepts. Previously the password
  WHMCS displayed never matched the server.
- **Create is idempotent**: instance-id custom field is auto-created *before*
  the API call; a retry with a stored id is a verified no-op; an interrupted
  create is recovered by displayName-tag adoption (exact single match only).
- SSH secret id (config option 3) validated numeric (was silently cast to 0);
  custom-field lookups tolerate the `name|Friendly` form.

### Safety (wrong-server protection)
- Instances are tagged `whmcs-{serviceid}` in their Contabo displayName;
  destructive actions (terminate, reset password) refuse on tag mismatch, and a
  different stored instance id is never silently overwritten. Sync restores a
  drifted tag. Ambiguous tag matches always error — never guess.
- Secret values and passwords are masked from `logModuleCall` by a recursive
  sanitizer (enforced by `LogRedactionTest`).

### UX / sync
- Client area: live status panel (status badge, IPv4 **and IPv6**,
  region/image/created, graceful stale fallback) + Start/Stop/Restart/Reset
  Root Password client buttons. Admin: Start/Stop/Restart/Reset Password/
  **Reinstall**/Sync from Contabo buttons.
- **Reinstall** rebuilds the OS to the product/selection image with a fresh
  vaulted root password (tag-verified, destructive — via the reinstall `PUT`).
- Dual-stack aware: `tblhosting.dedicatedip` ← primary IPv4, `assignedips` ←
  extra IPv4s + all IPv6s. Backfilled on admin/client views, the Sync button,
  and a bounded `DailyCronJob` sweep (new `hooks.php`).
- Configurable products provision what the customer picked: selections
  round-trip via the addon link tables; Image resolves against
  `GET /v1/compute/images` and Region via a label→slug map — both fail-closed;
  unmappable dimensions are acknowledged in the activity log.
- New config options 5 (cloud-init userData) and 6 (add-ons JSON);
  TestConnection distinguishes auth vs API-reachability failures.

### Reliability
- Instance recovery/adoption (`findByTag`) uses Contabo's server-side `search`
  filter first, so it no longer depends on scanning the whole account — safe
  for large/reseller fleets. A truncated fallback scan is logged rather than
  silently returning a partial result (which could let a duplicate tag slip
  through). Matches are deduped by instance id.
- The daily sync sweep authenticates **once per server** (grouped) and reuses
  the client across that server's services, with a small inter-call pause —
  roughly halving call volume and staying under Contabo's rate limit.

### Engineering
- HTTP transport extracted behind `HttpExecutor` (mirrors the addon's
  RequestExecutor seam); PUT/DELETE added; 401 refresh no longer consumes the
  retry budget; total backoff capped at 6s; 10s timeouts on view paths with
  cached-IP degradation.
- New PHPUnit suite (117 tests) under `modules/servers/contabo_vps/tests`
  (FakeCapsule + scripted FakeHttpExecutor, real entry functions via a Runtime
  factory seam). `scripts/predeploy-check.sh` now gates the server module
  (unit suite + PHP 7.4 lint). Two per-module GitHub Actions release workflows
  publish versioned, installable ZIPs on version bumps.

## 0.7.0 — 2026-05-29 (Phase D — two-layer pricing, mode-aware profiles, recoverable delete)

Schema **v8** (additive + idempotent). The profile becomes the SOURCE authority and
the mapping the CUSTOMER pricing layer; per-cycle pricing is fixed; profiles are
deletable with undo. See `docs/PHASE_D_PRICING_SPEC.md`.

### Pricing — per-cycle source, corrected
- Each published cycle now prices off **its own** scraped discount tier, not the
  single profile period. `profile_version.period_prices_json` stores the EUR source
  vector ({1,3,6,12} scraped + {24,36} projected).
- **Fallback rule:** `Source(M)` = `effective_monthly` of the longest scraped period
  whose months ≤ M. 24/36 → the 12-mo rate; a *missing* quarterly → the 1-mo rate.
  `SyncEngine::periodPriceVectorFromPlan()` / `nearestSourceRate()`.
- EUR→local extracted to the single shared `ProfileVersionInput::toLocalMonthly()`
  (GST then FX), used by both `computed()` and the engine. GST placement documented
  + commented for a future move (kept on the cost basis per owner decision).
- `RenewalEngine` uses the same per-cycle source basis (`resolveCycleEurMonthly`),
  keeping catalog and renewal consistent; markup still read from the mapping.

### Profile = SOURCE
- `mod_contabo_profile.published_cycles_mask` (default 63 = all six) — offered
  cycles; the mapping narrows to customer-facing.
- Create/edit form: **Publish cycles** control + per-cycle source-price preview
  replaces the single Period dropdown; `period_months` is derived server-side from
  the longest published cycle (slug/identity unchanged).
- **Fixed mode** = pre-packaged SKU: server rejects save unless every required
  configurator dimension is pinned (`fixedCompletenessError`). Mode-aware form
  (fixed hides the exposure control; configurable shows it).

### Mapping = CUSTOMER
- New `mod_contabo_mapping.source_overrides_json` — optional per-product per-cycle
  cost-basis pin (falls back to the profile vector); whitelisted in
  `MappingRepository`.
- Mapping cycle table gains a **Source (from profile)** column; `ajax-product-cycles`
  returns the profile's source vector.

### Recoverable delete
- Profiles **soft-delete** (`deleted_at`) → Trash → Restore (Undo); default listings
  exclude trashed rows.
- New `ProfilePurgeService`: guarded permanent purge (blocked by active mapping or
  live service; typed phrase; **per-profile** cascade of addon rows + addon-created
  WHMCS config objects; audited). New `profile-delete` / `profile-restore` /
  `profile-purge` / `profiles-trash` actions.

### Schema v8 (idempotent)
- `+profile.published_cycles_mask`, `+profile.deleted_at`,
  `+profile_version.period_prices_json`, `+mapping.source_overrides_json`.
- `SchemaHealth` required-column set updated; `SCHEMA_VERSION = 8`.

### Tests
- `PerCycleSourcePricingTest` (fallback rule incl. 24/36→12-mo and missing-3→1-mo,
  per-cycle pricing, publish gate, source override, legacy fallback),
  `ProfilePurgeServiceTest` (soft-delete lifecycle + guard + scoped cascade),
  renewal per-cycle-basis cases, mapping source-override persistence. FakeCapsule
  gains `whereNull`/`whereNotNull`.

## 0.6.1 — 2026-05-29 (A.6.3 admin UI — mode, exposure gate, capability/compat editors)

Wires the create/edit profile form and admin UI up to features whose backend
already shipped in 0.6.0. **No schema change** (`SCHEMA_VERSION` stays 7).

### Added — profile create/edit form
- **Profile mode selector** (`fixed_admin_profile` | `customer_configurable_product`).
  `profile_mode` now flows through `profileCreate`/`profileSave` →
  `ProfileRepository`/`ProfileManager` and feeds the identity fingerprint
  (fixed vs configurable hash differently). Defaults to fixed — fully
  backward-compatible.
- **“Expose configurable options” switch** (`expose_configurable_options`). Wired
  through the form, handlers and `ProfileRepository::insert` (default on = prior
  behavior). Closes the dead-end where `config-apply` told admins to “enable it on
  the profile” with no UI control to do so.

### Added — A.6.3 capability + compatibility editors
- `capability-editor` / `capability-editor-save` — edit the per-(plan,dimension,
  value) capability matrix (allowed-on-*, destructive, `capability_source`).
- `compatibility-editor` / `compatibility-editor-save` — author
  incompatible/required/min-max rules feeding `validateCombination()`.
- Both enumerate the plan's dimensions from the live configurator and overlay
  saved rows; reachable from `config-preview` and the profile row actions. New
  `ConfigOptionCompatibilityRepository::listForPlan()`.

### Fixed / hardened
- **Drift indicator now works**: the profiles list computes `drifted` server-side
  (orphaned-plan or stored-vs-current price mismatch) — previously dead UI.
- **`ProfileManager::update()` column whitelist** — drops unknown keys
  (mass-assignment guard).
- **Region/OS dedupe** — removed the redundant JS-mirrored hidden inputs; OS/Region
  are now derived server-side from the options selection only.
- Refreshed `docs/UI_ARCHITECTURE.md` (0.2.0 → 0.6.1; the 5 missing AJAX
  endpoints; the new pages, mode selector and expose switch).

## 0.6.0 — 2026-05-28 (Phase C — approval workflow, true revenue, multi-currency, provisioning)

Phase C closes the remaining gaps after Phase B (price-write activation): an
admin approval workflow, discount-aware revenue, a completed multi-currency
report, a configurable-options master switch, and a first-class Contabo VPS
provisioning module. **Schema bumps to DB v7** (`migrateTo7`).

### Added — Approval Queue UI
- New admin page `action=approval-queue` listing renewal decisions parked for
  sign-off (`requires_admin_approval=1, applied=0` with no resolution child).
  Handlers `approval-approve` / `approval-reject` (CSRF via `generate_token()`),
  plus `ajax-approval-count` for a badge. Reachable from the Repricing dashboard.
- **Append-only resolution**: approve/reject INSERT a child
  `mod_contabo_price_decision` row (`parent_decision_id` lineage); the original
  is never UPDATEd. The queue excludes already-resolved parents via
  `whereNotExists`. Reuses the existing `mod_contabo_pricing_action.admin_id` /
  `action_type` columns — **no schema change for the queue**.
- **Concurrency-safe**: approve/reject run inside a transaction with
  `lockForUpdate` + a child-exists re-check, so a decision can't be double-applied.
- **Phase-aware**: the write goes through `ServicePriceWriter` only for
  `opt_in`/`enforce`; under `observe` the approval is recorded but the write is
  suppressed (surfaced in the flash + the queue's phase banner).

### Added — ServiceRevenueResolver discounts
- `fetchDiscounts()` now folds recurring promos (`tblorders`→`tblpromotions`,
  honouring `recurnextcycle`) and client discounts (`tblclientdiscounts`),
  greater-of (not additive). Schema-guarded (`hasColumn`) with a `partial=true`
  fallback, and skipped entirely for non-INR services. New
  `ServiceRevenueResolverDiscountTest` (7 cases).

### Added — Multi-currency report
- `action=currency-report` gains a live FX-rates panel (from the pricing API),
  per-service INR equivalents for at-risk non-base services, a remediation
  callout, and a `currency-report-csv` export.

### Added — expose_configurable_options gate (schema v7)
- Profile-level `expose_configurable_options` (TINYINT default 1) added to
  `mod_contabo_profile` via **`migrateTo7`**. When 0,
  `ConfigurableOptionsSyncer::apply()` returns early
  (`skip_reason=expose_gate_disabled`) and `config-apply` shows a clear message
  instead of creating WHMCS option groups.

### Added — Contabo VPS provisioning module
- New WHMCS server module `modules/servers/contabo_vps/` (separate from this
  addon): OAuth2 password-grant auth (Keycloak) with in-memory token cache + 401
  refresh, a curl API client (429/5xx backoff, `x-request-id`), instance mapper,
  and `CreateAccount` / `SuspendAccount`(stop) / `UnsuspendAccount`(start) /
  `TerminateAccount`(cancel) / restart / reset-password, admin tab + client area.
  Credentials are redacted from `logModuleCall` and SSL verification is enforced.

### Fixed
- **Migration correctness**: `expose_configurable_options` is added in its own
  `migrateTo7` (not folded into `migrateTo2`), so installs already at schema v6
  receive it on upgrade.

## 0.5.1 — 2026-05-24 (hardening — real-WHMCS schema parity + operational safety)

Closes the **complete** raw-`tblhosting.recurringamount` parity defect class
surfaced by the production currency audit. `recurringamount` is a WHMCS API/model
*field name* (the `UpdateClientProduct` LocalAPI param + the `WHMCS\Service\Service`
accessor) — it is **not** a raw `tblhosting` column. The real recurring-charge
column is **`amount`**. Raw Capsule reads/writes of `recurringamount` silently
returned `0.0` or errored "Unknown column" on a live install; the schemaless test
double (FakeCapsule) hid all of it.

### Fixed — every raw `recurringamount` site → `tblhosting.amount`
- **`ServiceRevenueResolver::fetchBase()`** reads `tblhosting.amount`. A missing
  `amount` column now **throws `SchemaMismatchException`** (it is mandatory WHMCS
  schema — fail loud, never mask a monetary value as `0.0`). `current_charge` is
  documented + aliased (`service_amount`) as the WHMCS service amount — explicitly
  **not** the pricing base (which comes from `tblpricing`).
- **`CronDriver`** — candidate query now filters `where('amount','>',0)`. The
  broken `new RenewalEngine()` (no-args) + `decideForCron`/`decide((int)…)` calls
  (constructor/signature TypeErrors — pre-existing scaffold drift) are removed:
  candidate loading is isolated into the testable `loadActiveMappedServiceIds()`,
  and per-service RenewalEngine evaluation is **explicitly deferred to Phase B with
  a clear log — no fake success, no broken call**.
- **`BackfillCommand`** — replaced the JOIN that selected `h.recurringamount` with
  two portable reads selecting `tblhosting.amount` (internal key renamed to
  `service_amount`). Candidate loading is now wrapped in command-level error
  handling (logs + exits safely `errors=1` instead of throwing uncaught before the
  per-service try/catch).
- **`ServicePriceWriter`** — the raw fallback `update(['recurringamount'=>…])` →
  `update(['amount'=>…])`. The LocalAPI `UpdateClientProduct` **param stays
  `recurringamount`** (correct API field) — documented inline so a future sed-style
  edit can't conflate the two.
- **`RenewalEngine`** — `$service['recurringamount']` reads now prefer a canonical
  `service_amount` key with `recurringamount` kept as a backward-compatible alias;
  commented as a normalized service-row key (NOT a raw column). DB callers must
  alias `amount AS recurringamount` (or set `service_amount`).
- **Test-harness parity**: resolver-exercising tests now seed `amount`
  (+ `firstpaymentamount`). New regression
  `testCurrentChargeComesFromTblhostingAmountNotRecurringamount` +
  `testMissingAmountColumnThrowsSchemaMismatch`. **New suites**
  `CronDriverTest`, `BackfillCommandTest`, `ServicePriceWriterTest` (the latter
  proves LocalAPI uses `recurringamount` while the raw fallback writes `amount`).
  `FakeCapsule` gained `whereIn`/`whereNotIn`/`select` so these paths are testable.

### Added
- **`CurrencySupportReport`** + read-only admin diagnostic (`action=currency-report`,
  linked from Maintenance) — in-addon equivalent of the production currency audit.
  Verdict: `no_non_inr` / `non_inr_unmapped` / `non_inr_mapped_active_risk`. Counts
  meaningful (Active/Suspended/Pending) services by client currency; flags non-INR
  services on mapped products. Zero writes; reads are column-projected; amounts are
  labelled as the WHMCS service amount, not the pricing base.
- **Live-schema smoke guard** at **`scripts/live-schema-smoke.{php,sh}`**
  (repo-root `scripts/`, alongside `local-whmcs.sh` — NOT under the addon dir),
  env-gated (`CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1`), `information_schema`-only,
  read-only. Asserts `tblhosting.amount`/`firstpaymentamount` exist + the v6
  `expected_hash` columns; skips safely without the flag/credentials. Verified
  green on local dev WHMCS 8.13 + 9.0.

### Hardening (operational safety — every billing-impacting write now has preview/dry-run/diff coverage)
- **Mandatory pre-deploy gate** `scripts/predeploy-check.sh` (fail-closed: unit suite
  + PHP 7.4 lint + live-schema smoke 8.13/9.0 + real-WHMCS integration smoke; never
  touches prod) + `docs/DEPLOY_RUNBOOK.md` (addon-scoped rsync/chown procedure +
  gotchas). The integration smoke gained a real-schema parity section.
- **ConfigPurgeService dry-run** — `previewRemoval()` counts exactly what the purge
  would delete, writing nothing; a one-click "Preview purge (dry-run)" on the
  maintenance page (no confirmation phrase required, read-only).
- **Pre-apply live diff** — `ConfigurableOptionsSyncer::diff()` + the `config-diff`
  screen show create/update/noop/drift_skip per dimension against the live product
  BEFORE any write; the preview's Apply routes through the diff (CSRF + confirm live
  on the diff screen).
- **Drift extended to the value level** — sub-option + recurring pricing drift via
  `value_link.expected_hash`; a hand-edited live sub-option/price is flagged
  (`drift_skipped`) and skipped on re-apply, never clobbered.
- **Flash/error hardening** — admin error paths show a generic message + log full
  detail (no raw exception/SQLSTATE leak; security review S2). config-apply gains an
  audit-trail log (admin id + product + outcome), consistent with maintenance-purge.

### Migration
- **No migration required** for any 0.5.1 work — parity is a code/test fix, the
  diagnostics + diff are read-only, and value-level drift reuses the existing v6
  `expected_hash` columns. Schema stays **v6**.

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
