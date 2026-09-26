# Plan 003 — Audit findings + New categories (Dedicated, Object Storage) + Scraper resilience

Status: DRAFT for owner review · Author: audit pass 2026-07-01 · Supersedes nothing

### 2026-08-05 taxonomy amendment

The live pricing catalogue now has three compute labels that must remain distinct
in the canonical render model:

- **Core VPS** — `cloud-vps-core-*`; SSD storage only (`storage_policy=ssd_only`).
- **Performance VPS** — `cloud-vps-plus-*`; NVMe storage only
  (`storage_policy=nvme_only`).
- **Max Performance VPS** — `vds-*`; this is the current label for the former
  VDS category. Existing `Cloud VDS` labels and `vds-*` slugs remain as legacy
  aliases for WHMCS and historical snapshots.
- Generic historical `cloud-vps-*` plans remain `Cloud VPS`; they are not
  inferred to be Core VPS. Only the explicit `cloud-vps-core-*` and
  `cloud-vps-plus-*` slugs receive the new storage policies.

The additive fields are `canonical_family`, `legacy_family`, and
`storage_policy`; consumers must not infer the storage type from a plan name or
silently permit an SSD/NVMe option that violates the policy. The scraper records
a `storage_policy_violation` gap when the upstream payload contradicts the
catalogue rule. See API schema 1.3 and
[`docs/PROPOSAL-GENERATION.md`](../docs/PROPOSAL-GENERATION.md) for the report
margin/proposal contract.

The 2026-08-05 direct release scrape fetched all 36 current active URLs
successfully through the automatic request/CloakBrowser fallback. It produced
1,975 option rows and six explicit Core-VPS `storage_policy_violation` gaps
(NVMe add-ons on SSD-only plans); localized/wrapped region labels and
fallback-success noise are normalized rather than silently reported as
unclassified or failed. The three historical Object Storage fragment URLs now
redirect to `/en/storage-vps/` and expose no product payload, so they are
retained as discontinued catalog history rather than counted as active plans.

This document is (1) a consolidated audit of the current scraper + WHMCS module, and
(2) a research-grounded plan to add **Dedicated Servers** and **Object Storage** as new
priced categories, unified into the existing dataset/schema and WHMCS ingestion path.

Every design choice below is grounded in the **actual** Contabo API/CLI surface and WHMCS
standards (see §5 "Evidence"), not assumptions.

---

## 1. Audit — current failures & bugs (severity-ranked)

### Live-blocking
- **[CRIT] Scraper is dead outside CI.** `scrape.err` (2026-06-30): all 16 fetches → HTTP 403.
  Default `FetchMode::Reqwest` (`src/main.rs:245`) does a direct rustls request with no
  fallback; Contabo now Cloudflare-403s default-library TLS fingerprints. CI survives only
  because it sets `SCRAPER_PROXY` (`scrape.yml:157-161`). The failing run passed neither a
  proxy nor `--fetch-mode auto` → guaranteed total failure. Data is stale since 2026-06-20.
- **[CRIT] macOS fallback is broken.** `cloak.err` = `(eval):4: command not found: timeout`.
  macOS has no `timeout` binary (it's `gtimeout`). The CloakBrowser escape hatch was invoked
  through a `timeout` wrapper that no-ops here → no working fetch path on this machine.

### Data-integrity (from deep bug-sweep)
- **[HIGH] Partial-failure overwrites good data.** `src/main.rs:~1955` — snapshot-preserve
  only triggers when `base_plans.is_empty()`. If 8/16 succeed, all outputs are rewritten with
  only 8; the other 8 vanish from every consumer. Fix: merge missing plans from the previous
  on-disk snapshot; never drop plans that weren't re-scraped this run.
- **[HIGH] Silent plan loss with no gap trace.** `product_not_found` path (`main.rs:971-979`)
  returns `None` via `?` with **no `GapEntry`** (JS records one — `contabo_scraper.js:548`).
  A Contabo markup change silently drops a plan. Fix: push a `product_not_found` gap w/ slug.
- **[MED] Fetch-failure gaps lose plan identity** (`main.rs:1796-1801`, `plan_sku:None`).
- **[MED] INR quote returns ₹0.** `handlers.rs:219-237` — `fx_rate.unwrap_or(0.0)` yields a
  valid-looking zero-priced quote. Fix: reject non-EUR w/o fx_rate (400) or use `/fx` cache.
- **[MED] Retries burn budget on 403/404; ignores `Retry-After`** (`main.rs:314-334`).
- **[MED] `panic="abort"` (Cargo.toml) nullifies the spawn_blocking panic-recovery** arms
  (`main.rs:1828,1675`) — a parse panic aborts the whole process. Fix: `unwind`, or remove
  the recovery pretense.

### Structural
- **[HIGH] Dual scraper implementations** — Rust `src/main.rs` and `scripts/contabo_scraper.js`
  parse the same payload into (nominally) the same schema. The bug-sweep found **real
  divergences**: CSV column set/order (`main.rs:2031-2065` vs `contabo_scraper.js:888-895`),
  quick_reference fields, and the JS scraper never emits `contabo_view_model.json` (which the
  API `/quote` handler 503s without — `handlers.rs:195`). Every new category doubles this work
  and this divergence surface. **Decision needed: designate ONE canonical producer.**
- **[MED] `src/main.rs` health 2.0/10** — 2200-line God-file. Adding categories is a good
  moment to extract a `fetch`, `parse`, `emit`, `model` module split.
- **[LOW] auth.rs length-leak timing side-channel; swallowed CSV write errors; NaN/Inf
  coercion to null in savings math.** (See bug-sweep detail.)

### Existing-build audit (what's already there)
- `modules/servers/contabo_vps` is a **real, working** provisioning module on the Contabo REST
  API (`/v1/compute/instances`, OAuth2 password-grant) — **Cloud-VPS-only**, **no** usage/metrics.
- **Setup-fee billing is already wired end-to-end** (`SyncEngine.php:391-399`,
  `CyclePricingMap::getSetupFeeColumn`, `sync_setup_fees` flag) — **gated, default OFF**.
  Dedicated servers inherit this for free.
- **Metered/usage billing = greenfield** (zero code, zero spec repo-wide).
- **`family` flows through cleanly** as a filter/pass-through (`ApiClient.php:33`), but the
  **dimension layer is hardcoded VPS-shaped**: `DimensionParser` (`:63-67`),
  `CapabilityDefaultsProvider` (`:34-40`), `ProfileIdentityResolver::fixedProjection`
  (`:128-133`), `OptionTypeMapper`. Unknown dimensions **degrade gracefully** (surface as
  specs, conservative capability defaults) but with VPS assumptions.

---

## 2. Grounded design conclusions (the "don't default to simple tiers" answer)

### 2a. Object Storage — committed-capacity config option, NOT consumption-metered. Here's why.
Research into the live Contabo API (OpenAPI spec) + pricing establishes hard constraints:

1. **Contabo bills the reseller for _purchased_ space, not consumed.** Create takes
   `totalPurchasedSpaceTB`; price is flat **€9.96/TB-month**, 250 GB increments, no setup fee,
   1-month minimum. So our **cost basis is committed TB**, not usage.
2. **Egress is free** (Contabo now markets "S3-Compatible & Free Egress"). There is no
   per-TB-traffic charge to pass through.
3. **The API exposes NO traffic/egress/bandwidth metering anywhere** — the object-storage
   `stats` endpoint returns only `usedSpaceTB`, `usedSpacePercentage`, `numberOfObjects`, and
   is explicitly "not live". A traffic-metered WHMCS product is **unbuildable** (no data) and
   **commercially wrong** (nothing to bill).
4. **Multi-tenant caveat:** as a reseller with ONE Contabo account, `usedSpaceTB` is aggregate
   **per your object-storage instance per region**, not per customer. Per-customer *consumption*
   billing would require S3-bucket-level accounting via the S3 API, not the management API.

**Conclusion:** model Object Storage as a **committed-capacity product** — a `Quantity` (per-TB)
or tiered `Drop Down` **configurable option** priced per-cycle in `tblpricing` (fully
scraper/SyncEngine-syncable, mirrors Contabo's own purchase model, no free-egress mismatch).
WHMCS **metrics** (`TYPE_SNAPSHOT`, storage-used, rate set manually) are an **optional Phase-2
overlay** for usage *visibility*/soft-overage only — not the billing basis. This is the
best-practice answer that fits Contabo reality; naive per-GB metered billing is rejected on
evidence (no traffic data, cost basis is purchased-not-used, reseller aggregation problem).

*(Provisioning fork to decide at build time: one Contabo object-storage instance per region
shared across customers + per-customer buckets + S3 accounting, vs. one instance per customer.
The API supports create/resize/cancel either way; this is an ops decision, flagged not blocked.)*

### 2b. Dedicated Servers — fixed price + setup fee, MANUAL provisioning.
- **No self-serve provisioning API** for bare metal — the Contabo API only assigns VIPs to an
  *existing* bare-metal box. So the WHMCS side = **manual/email fulfilment** + **native Stock
  Control** (qty 0 = out-of-stock gate) + **setup fees** (already-wired `*setupfee` columns,
  flip `sync_setup_fees` on for this family) + **configurable options** for RAID/drives/IPs.
- Pricing still must be **scraped** (no catalog API).

### 2c. Unified schema (satisfies "unified or properly formatted for WHMCS consumption").
One dataset, one schema, add discriminators — additive, so schema minor-bump (API 1.2):
- `family` gains `"Dedicated Server"`, `"Object Storage"`.
- Add `pricing_model: "fixed" | "fixed_plus_setup" | "metered_capacity"` per plan so the module
  branches instead of assuming fixed.
- `specs_parsed` becomes a **per-family typed sub-object** (VPS keeps its shape; Dedicated adds
  `raid`, `drive_count`, `drive_type`, `uplink_gbps`, `ipmi`, `setup_fee_eur`; Object Storage
  adds `capacity_tb`, `price_per_tb_eur`, `regions[]`, `included_traffic: "free_egress"`).
- Dimension vocabulary becomes **data-driven per family** rather than the hardcoded VPS set.

### 2d. Scraper fetch resilience — EMPIRICALLY VERIFIED (2026-07-01 live tests).
The research hypothesis ("plain fingerprint/IP 403, no JS challenge") was **tested against live
Contabo and proven WRONG.** Actual behaviour from a flagged (India residential) IP:
- Contabo serves a **Cloudflare Turnstile managed challenge** (`cf-mitigated: challenge`,
  `<title>Just a moment...</title>`, `cf_chl`/`turnstile` tokens). It is **IP/geo-reputation
  triggered** — a clean/whitelisted egress IP is served 200 + `__SAPPER__` with **no challenge**.
- ❌ wreq (Chrome-137 TLS impersonation), ❌ curl HTTP/2 + browser headers, ❌ cloakbrowser free
  0.3.31 **and** 0.4.5 all fail from a flagged IP. ✅ `SCRAPER_PROXY` (clean IP) works — this is
  why plain reqwest-through-proxy already succeeds in CI.

**Decided architecture (stable-ops):**
- **PRIMARY bypass = `SCRAPER_PROXY` (clean residential/whitelisted IP).** Sidesteps the
  challenge entirely — no Turnstile to solve, no browser, fast, proven. This is mandatory, not
  optional, from any flagged IP.
- **DONE — `wreq` + `wreq-util` replace the rustls `reqwest` fetch layer** (BoringSSL, Chrome137
  emulation). Built green 2026-07-01. Value even though it doesn't alone beat Turnstile: authentic
  fingerprint (fewer challenges from marginal IPs), and the rewrite added **fast-fail on non-
  retryable 4xx** (0.6 s vs ~15 s wasted retries), **`Retry-After` honouring**, and **soft-block/
  decoy `__SAPPER__` detection**. Runs through the proxy in-process; **no Node/`timeout`/macOS
  breakage.**
- **`cloak-fetch.mjs` browser fallback** — fixed to wait for the challenge to clear (poll for
  `__SAPPER__`) + `humanize:true`, but the **free** cloakbrowser tier does NOT solve Turnstile.
  Keep it as an emergency lever; it only helps with a paid Turnstile-solver tier
  (cloakbrowser Pro / Scrapling+CapSolver / Byparr) — slower + more fragile than a clean proxy.
- **Reject** `bypass-agent` (abandoned 1-star repo, paid Claude-Vision calls).
- BoringSSL build dep (cmake/perl/clang) now required for the Docker image + predeploy gate.

---

## 3. Work plan (phased)

**Phase 0 — Unblock + de-risk (independent of category work):**
1. Land data-integrity fixes: partial-failure merge (HIGH), `product_not_found`/fetch gaps with
   slug, INR-fx guard, retry error-classification + `Retry-After`, panic strategy.
2. Fix the fetch layer per §2d (owner picks proxy-only vs `wreq`). Verify a **green live scrape**.
3. Fix the local runner invocation (proxy or `--fetch-mode auto`); replace any `timeout` usage
   with `gtimeout`/tokio timeout.

**Phase 1 — Canonicalize the pipeline:** pick ONE producer (recommend Rust; make JS a thin
parity-tested fallback or retire it). Extract `main.rs` into fetch/parse/emit/model modules.

**Phase 2 — Schema 1.2 + Dedicated Servers:** add discover URLs (`/en/dedicated-servers/…`),
per-family `specs_parsed`, `pricing_model`, setup-fee capture, availability flag. WHMCS: new
dimension keys + capability defaults for RAID/drives; flip `sync_setup_fees`; native stock
control; manual-provisioning product. (Setup-fee plumbing already exists.)

**Phase 3 — Object Storage (committed-capacity):** scrape per-TB rate + regions; emit as a
capacity config-option family; WHMCS: capacity `Quantity`/tier option priced in `tblpricing`,
region option, `contabo_vps`-style server module extended with `POST /v1/object-storages`
create/resize/cancel. Decide the multi-tenant provisioning fork (§2a).

**Phase 4 (optional) — Usage visibility overlay:** `MetricProvider` (`TYPE_SNAPSHOT`,
`usedSpaceTB`) for display/soft-overage only. Requires WHMCS 7.9+; metric rates are admin-UI
configured (not table-writable) — so this is display/ops, not scraper-driven billing.

---

## 4. Decisions still open for the owner
1. **Fetch fix strategy:** proxy-only (zero code, works now) vs `wreq` in-process (proxy-
   optional, adds BoringSSL build dep) vs both (belt-and-suspenders). *Recommend: `wreq` + proxy.*
2. **Canonical producer:** Rust-only (retire JS) vs keep both under parity. *Recommend: Rust.*
3. **Object Storage confirmation:** committed-capacity config option (recommended, evidence in
   §2a) — confirm you accept this over consumption-metered.
4. **Object Storage provisioning fork:** shared per-region instance + per-customer buckets/S3
   accounting, vs per-customer instance. (Build-time, not blocking the scraper/pricing work.)

## 4b. Progress log (2026-07-01)

**Landed in working tree (Lane 1, unverified-against-live pending proxy):**
- Fetch layer: rustls `reqwest` → `wreq` + `wreq-util` Chrome137 emulation (`Cargo.toml`,
  `src/main.rs` fetch_html/fetch_with_retry/client builder). Builds green.
- Fetch hardening: `FetchErr` classification (fast-fail on non-retryable 4xx — verified 0.6 s vs
  ~15 s), `Retry-After` honouring, soft-block/decoy `__SAPPER__` warn.
- `scripts/cloak-fetch.mjs`: challenge-aware wait (poll for `__SAPPER__`) + `humanize:true`.
- Data-integrity fixes: **#1 partial-failure preservation merge** (carries unfetched plans +
  their configs + option rows from the previous snapshot; `option_count`/`failed` accounting
  fixed against underflow); **#2 `product_not_found`/`products_missing` gaps now recorded with
  slug** (JS parity); **#3 `fetch_failed` gap tagged with slug**; **#4 INR quote returns 400 on
  missing/≤0 fx_rate** (was a valid-looking ₹0 quote) + reject unknown currency.
- `cargo test`: 14/16 pass. The 2 failures (`quote_endpoint_*`) are **pre-existing** (verified at
  HEAD) — `tests/api_smoke.rs` hard-codes quote expectations from the committed `data/output`, so
  any price drift breaks them. **NEW finding → fix: derive expectations from the loaded dataset,
  or pin a test fixture.**

**Landed in worktree `worktree-agent-a74a12925aed49b6d` (Lane 2, Dedicated — review before merge):**
- Dedicated family end-to-end + setup-fee flow into `tblpricing.*setupfee` (gated) + new
  dimensions RAID/Extra Drives/Additional IP across DimensionParser/OptionTypeMapper/
  CapabilityDefaultsProvider/ExposureResolver + `docs/DEDICATED_SERVERS.md`. Tests 419→440 green.
- Open Q to confirm against real scraper output: exact dimension string casing/nesting; the
  ≤1-value omission rule for quantity dims; exposed-by-default policy for the 3 dims.

**Blocked / pending owner:** `SCRAPER_PROXY` value to verify a full green live scrape (Turnstile
can't be solved free from this IP). Not started: module split (C), Object Storage scraper + WHMCS
(H/I), CSV/JSON Rust↔JS parity reconciliation, `panic=abort` decision (#6), auth timing side-channel.

## 4c. Schema 1.2 GROUND TRUTH (live payload inspection, 2026-07-01, via proxy)

Verified end-to-end: **wreq+proxy full scrape = 16/16 plans, 940 options, 0 gaps**; partial-merge
**carried 15 plans forward** on a 1-plan re-scrape. Dedicated + Object Storage pages return 200
with the same `__SAPPER__`/`preloaded[0].products` structure — extraction extends cleanly.

**The `products` map is the ENTIRE catalog (138 products) on every page**, discriminated by `type`:
`vps:90, storage-vps:17, vds:6, ds:18, outletServer:4, object-storage:3` (+ gpu-vps seen inline).
→ A single fetch can replace the 16 per-URL fetches (optional simplification).

**Type → family mapping to add** (`family_from_type` in `src/main.rs` today only maps vps/
storage-vps/vds): `ds`→"Dedicated Server", `object-storage`→"Object Storage". Decide: `outletServer`
(price:null → **exclude**, quote-only), `gpu-vps` (out of scope for now).

**Dedicated (`type:"ds"`, e.g. `ds-40` "AMD Turin 32 Cores"):**
- `price:{EUR:249,GBP,USD}` monthly. **Pricing shape differs from VPS**: periods carry a
  `discount:{EUR,..}` (total over the term), NOT `effective_monthly`. Effective monthly must be
  derived (`price - discount/length`). The current `monthly_price_for_period` assumes the VPS shape
  → needs a `ds`-aware branch.
- Setup fee via `oneMonthSetupFeeDisplaySettings` (string `"hide"` when none; a fee object when set).
- `specs`: typed array of `{title, subtitle, type: cpu|ram|storage|traffic|port}` — richer than VPS;
  map to a Dedicated `specs_parsed` (cpu/ram/storage drives/traffic/uplink).
- **Config options live in `addons`** (hash-keyed: `{title, price, setupPrice, groupId, osId?}`),
  NOT `options`. So RAID/RAM/storage/OS upgrades come from `addons`. **This corrects the Lane 2
  worktree**, which assumed top-level `dimension` strings "RAID"/"Extra Drives"/"Additional IP" —
  those must be derived from `addons` groups instead.
- Availability: `outOfStock`, `unavailable` fields present → feed WHMCS Stock Control.

**Object Storage (`type:"object-storage"`, 3 regional: `european-union`/US/SIN):**
- `price:{EUR:2.49}` = the **250 GB base tier** (@ €9.96/TB), `periods` 1/3/6/12 all `setup:0`,
  `specs:[]`, `options:[]`, `addons:{}`. Capacity scaling is API-only (`resize`), not in the payload.
- → Emit as `family:"Object Storage"`, `pricing_model:"metered_capacity"`, `specs_parsed:{capacity_tb:0.25,
  price_per_tb_eur:9.96, region, included_traffic:"free_egress"}`. WHMCS: committed-capacity Quantity
  option (per-250GB or per-TB) in `tblpricing` + optional metric-visibility overlay (per §2a).

**Also newly available (not yet captured):** multi-currency `price:{EUR,GBP,USD}` and per-period
`discount` in all currencies — the scraper extracts EUR only today; schema 1.2 could carry all three.

**Next-phase task list (scraper):** add `ds`/`object-storage` to type→family + plan-URL discovery;
add a `ds` period/spec/addons parser + an `object-storage` parser; extend `specs_parsed` per family;
add `pricing_model`; mirror in `scripts/contabo_scraper.js` (or retire it per §1) + parity; bump
`SCHEMA_VERSION`→1.2 with a SCHEMA_VERSION.md entry.

## 4d. Fleet progress (2026-07-01, later)

**Verified (main tree):** `data/output` refreshed via wreq+proxy (16 plans, 940 opts, PRICES.md +
report.html regenerated). `family_from_type` now maps `ds`→"Dedicated Server",
`object-storage`→"Object Storage".

**Ops lane — DONE (green):**
- No host scheduler exists — the 2026-06-30 failure was an **ad-hoc manual run** (no `SCRAPER_PROXY`,
  a `timeout` wrapper). CI (`scrape.yml`) is already correct. Proposed (not installed) a launchd plist
  that sources the proxy from a `chmod 600` `~/.config/contabo-pricing/proxy.env` + uses `gtimeout`.
- `Dockerfile` builder stage: added `cmake perl clang libclang-dev pkg-config build-essential` for the
  wreq/BoringSSL compile. `deploy/` wiring was already correct (clarifying comments added).
- **Fixed the 2 fragile quote tests** — expectations now derive from the response's own
  `base_monthly_eur` (immune to price drift). `cargo test --test api_smoke` → **16/16 green.**

**Dedicated PHP lane — reworking** (agent resumed with the corrected `addons`/Yes-No contract).

**Empirical new-category extraction test (existing parser, per-product URL):**
- `ds-40` via `/en/dedicated-servers/ds-40/` → **200, extracts ~80% correctly**: family, base €249,
  and period **discounts parse right** (existing period math already handles the `price - discount`
  shape). Gaps: `cpu_count` null ("32 x 3.55 GHz" vs VPS "4 vCPU Cores"), `storage_primary_gb` null
  ("2 x 1 TB NVMe"), and 15 addon-parse gaps (VPS option parser doesn't understand `addons`).
- `object-storage` (catalog page) → **"plan not found"**: needs custom handling (slug match fails;
  no per-product URL — products live only in `/en/object-storage/` payload).

**Precise remaining scraper work (Lane 1 D/E/F):**
1. Add 18 `ds-*` URLs to `data/plan_urls.json` (`/en/dedicated-servers/{slug}/`).
2. `ds` spec parser: `cpu` "N x F GHz" → cpu_count N; `storage` "N x SIZE TYPE" → total/type; traffic;
   uplink from port. Emit per-family `specs_parsed`; add `pricing_model:"fixed_plus_setup"`; capture
   setup fee from `oneMonthSetupFeeDisplaySettings`; availability from `outOfStock`/`unavailable`.
3. `ds` options: OS images → `Image`; the ~67 paid `addons` → `Add-on` rows (Yes/No, category by
   title keyword) with monthly + setup deltas — matches the PHP contract just sent.
4. Object Storage: special-case extraction of the 3 `object-storage`-typed products from the
   `/en/object-storage/` payload → `family:"Object Storage"`, `pricing_model:"metered_capacity"`,
   `specs_parsed:{capacity_tb:0.25, price_per_tb_eur:9.96, region, included_traffic:"free_egress"}`.
5. Bump `SCHEMA_VERSION`→1.2 + SCHEMA_VERSION.md entry; mirror in `contabo_scraper.js` (or retire it).

## 4e. COMPLETED — unified schema-1.2 extraction (2026-07-01, verified live)

**`data/output` now holds a clean unified v1.2 dataset: 27 plans, 1300 options, 0 gaps**, families
Cloud VPS 6 / Cloud VDS 5 / Storage VPS 5 / **Dedicated Server 8 / Object Storage 3**. PRICES.md +
report.html regenerated. `SCHEMA_VERSION`→1.2 + SCHEMA_VERSION.md entry added. `cargo test` 16/16+3/3.

- **Dedicated (`ds`)** — 8 live servers (the 7 EOL slugs correctly excluded: they 301-redirect).
  Per-product URLs `/en/dedicated-servers/{slug}/`. Emits family/pricing_model=`fixed_plus_setup`/
  availability/specs(cpu·ram·storage·uplink·setup)/periods-with-discounts, and options: `Image` (OS) +
  `Add-on` (Yes/No, categorised) + `Region`. 0 gaps.
- **Object Storage (`object-storage`)** — 3 regional products via `#slug` fragment URLs on the shared
  `/en/object-storage/` page. family/pricing_model=`metered_capacity`/specs{capacity_tb, price_per_tb_eur,
  region, included_traffic:free_egress}. 0 options (API-scaled). 0 gaps.
- **Fetch robustness fix** — 403 is now retryable **when a proxy is set** (rotating residential IP →
  a retry lands on a fresh, unchallenged IP). Recovered the transient ds failures.
- `data/plan_urls.json` updated to 27 entries (16 + 8 ds + 3 os).

**Remaining follow-ups (not blocking the dataset):**
- **JS parity** (`scripts/contabo_scraper.js`) now diverges (no 1.2 fields) — update or retire + adjust
  `parity.yml`.
- **Object Storage WHMCS side** — capacity Quantity option + `contabo_vps` provisioning
  (`POST /v1/object-storages`) + optional metric-visibility overlay (the PHP lane did Dedicated only).
- **Integrate the Dedicated PHP worktree** (`worktree-agent-a74a12925aed49b6d`, 439 tests green) — review + merge.
- report.html reconciliation shows 60 mismatches (new families not understood by the reconciler) —
  report-level follow-up, not a data error.
- `discover_plans.js` (CI) is unaware of ds/object-storage — ensure it doesn't drop the manual entries.
- Object Storage sells only the 250 GB base tier today; per-TB capacity tiers for WHMCS are a follow-up.

## 5. Evidence (research citations)
- Contabo API: no catalog/pricing endpoint; object-storage `stats` = `usedSpaceTB` only, no
  traffic field anywhere in the 30k-line OpenAPI spec; dedicated = no create API (VIP-assign
  only); `POST/PATCH /v1/object-storages` create/resize/cancel; OAuth2 password-grant;
  `x-request-id` required. Object storage €9.96/TB-mo, 250 GB steps, free egress.
- WHMCS: `MetricProvider`/Usage Billing (v7.9+), `TYPE_SNAPSHOT` vs `TYPE_PERIOD_*`, metric
  pricing is admin-UI-only (not bulk-writable); setup fee = per-cycle `*setupfee` columns in
  `tblpricing`; configurable options for hardware/capacity (cycle must match parent); native
  Stock Control for dedicated; metrics require a server object.
- Scraper: Contabo block is TLS-fingerprint + IP-reputation (plain 403, SSR pages, no JS
  needed); `wreq`/`wreq-util` in-process impersonation recommended over browser solvers;
  `bypass-agent` rejected (abandoned, paid vision calls).
