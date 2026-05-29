# Phase D — Two-Layer Pricing, Mode-Aware Profiles, Renewal Push & Recoverable Delete

Status: specification (in build). Supersedes the cycle/markup assumptions in the
A.5 mapping design. Owner-confirmed decisions are marked **[OWNER]**.

---

## 0. Mental model (the one paragraph)

A **profile** is *what we buy from Contabo and what it costs us* — the SOURCE.
A **mapping** is *what we sell to a customer on a specific WHMCS product* — the
CUSTOMER layer. The profile sources a price for **every** billing cycle it can
derive; the mapping decides **which** of those cycles a customer actually sees at
checkout and **how much** they pay (markup, rounding). New orders always price off
the latest source; existing customers are **grandfathered** until their own billing
cycle rolls over, and only move if a **price-push** policy is enabled for them.

```
Contabo scrape (1/3/6/12 mo, EUR)
        │  build per-cycle SOURCE vector (fill 24/36 + any gaps)
        ▼
PROFILE (SOURCE)  ── period_prices_json {1,3,6,12,24,36 → EUR/mo}
   • mode: fixed (pre-packaged) | configurable
   • published_cycles_mask = cycles we *can* offer (default all 6)
   • auto SKU slug + human name
        │  EUR→local (FX + GST), × cycleMonths, × markup, round
        ▼
MAPPING (CUSTOMER, per profile × product × currency)
   • catalog_cycles_mask  = cycles the CUSTOMER sees at checkout  ◀ the real gate
   • markup_overrides_json + rounding_mode  = customer price
   • source_overrides_json (optional per-product source pin)
        │
        ├─ catalog  → tblpricing            (NEW orders / checkout)
        └─ renewal  → tblhosting.recurring  (EXISTING services, cycle-boundary, opt-in)
```

---

## 1. Image / OS is ONE choice **[OWNER]**

Contabo's API accepts exactly **one** image identifier at provision/reprovision.
OS, Apps, Control Panels, Blockchain are *categories of the same single
mutually-exclusive choice*, not independent add-ons. We mirror this exactly:

- `ImageOptionNormalizer` already collapses all Image categories into **one** spec
  with category-prefixed labels — keep as-is.
- A fixed profile pins exactly one image; a configurable profile exposes the image
  list as one single-select dimension. Never more than one image value reaches
  provisioning.

No code change; this confirms the existing `DimensionParser` Image handling is
correct and must not be "split" into multiple add-on choices.

---

## 2. Source pricing & fallbacks **[OWNER]**

### 2.1 What Contabo exposes
Public scrape yields periods **1, 3, 6, 12** months (`effective_monthly`, EUR).
**24 and 36 months are never public** — they appear only as post-provision upgrade
options. We therefore *project* them.

### 2.2 The fallback rule (single rule, covers every gap)

> **Source(M) = `effective_monthly` of the longest scraped period whose months ≤ M.**

- `24 mo`, `36 mo` → **12-mo** rate (deepest public tier). e.g. 12mo=€3.60 →
  24mo source total = 3.60 × 24, 36mo = 3.60 × 36.
- A **missing 3-mo** (quarterly) → **1-mo** rate (×3) — *not* a longer-cycle
  discount the customer never qualified for.
- A missing 6-mo → 3-mo if present else 1-mo; missing 12-mo → 6 → 3 → 1.
- 1-mo is always present, so a basis always exists.

These projected prices are **estimates of our purchase cost** (24/36) — clearly
labelled as such in the UI so the admin knows they're derived, not scraped.

Implemented: `SyncEngine::buildPeriodPriceVector()` + `nearestSourceRate()` produce
the full six-cycle EUR vector; stored in `mod_contabo_profile_version.period_prices_json`.

### 2.3 EUR → local → customer
Per cycle: `Source(M)` (EUR/mo) → `ProfileVersionInput::toLocalMonthly()` (GST then
FX, the single shared converter) → × cycleMonths → mapping markup → mapping
rounding → `tblpricing`. Sentinels (-1 disabled / 0 free) and the 50%
suspicious-change guard are unchanged.

### 2.4 GST placement — kept as-is, flagged for future **[OWNER]**
`toLocalMonthly()` applies GST(18%) to the **cost basis** exactly as the legacy
`computed()` did (no behaviour change). Under a strict source/customer split GST is
arguably a customer-price concern, not a cost concern. **Decision: leave as-is for
now**; a `// GST-PLACEMENT:` note + commented alternative is left in
`ProfileVersionInput::toLocalMonthly()` for a future revisit.

---

## 3. Modes — primary decision, drives the form **[OWNER]**

Mode is chosen **first** and the create/edit form reshapes to match.

| | **Fixed (pre-packaged)** | **Configurable** |
|---|---|---|
| Customer choice | **None** — admin locks one image + every dimension; one SKU | Customer picks exposed dimensions at order time |
| Configurable/add-on options | **Not exposed at all** | Exposed per the exposure curation |
| Form | Full configurator, every dimension required to a concrete value | Plan + per-dimension *expose* toggles + defaults |
| WHMCS config options | None created | Created on Apply for exposed dimensions only |
| Identity | concrete pinned values | exposed-schema + defaults shape |

- **Fixed completeness:** save is rejected unless every non-optional configurator
  control (`buildConfiguratorControls`, Image:OS required, etc.) has a concrete
  (non-"None") value. The locked set is the SKU and feeds provisioning verbatim.
- **Profile name** is a human-readable label only (backend mapping, dashboards).
- **Slug / SKU** is **auto-generated** (`ProfileIdentityResolver::buildSlug`) and is
  the stable key everything maps to. Admin never hand-types it.
- `period_months` (legacy single period) is **kept** but auto-derived = longest
  *published* cycle, purely to keep slug + identity fingerprint stable for existing
  rows (no re-hash). It is no longer an admin input; the Period dropdown is removed.

---

## 4. Mapping = customer-facing cycle gate **[OWNER]**

The admin sees **all** source prices on the profile, then in the **mapping** picks
which cycles to actually sell on a given WHMCS product:

- `mod_contabo_mapping.catalog_cycles_mask` is the **customer-facing gate** — it
  already drives `tblpricing` writes, i.e. exactly what the customer sees in
  checkout. *This is the real toggle the owner described* ("customer will see 1mo /
  3mo / 12mo depending on what I chose in the mapping UI"). **Unchanged.**
- Example: a fixed server whose source has all six cycles, but the admin enables
  only **Quarterly + Annually** in the mapping → the customer checkout shows only
  those two; the other four are not written / shown.
- `published_cycles_mask` on the **profile** is the *offered* set (which cycles we
  bother to source), **defaults to all six (63)**. It is a soft profile-level
  superset; the mapping narrows within it. Default-all means current customer-facing
  behaviour is byte-for-byte preserved (the mapping mask still decides).
- Per-cycle **markup** + **rounding** stay on the mapping (`markup_overrides_json`,
  `rounding_mode`) — the customer price.
- Optional `source_overrides_json` (new, per mapping) lets one product pin its own
  source basis per cycle, falling back to the profile vector.

The mapping cycle table gains two read-only columns: **Source (from profile)** and
the resulting **Customer price** preview, so the admin sees cost → sell at a glance.

---

## 5. Renewal & price-push — grandfathering model **[OWNER]**

This is the area most exposed to WHMCS mechanics; spelled out to close gaps.

### 5.1 Principles
1. **New orders / checkout** always use the latest source → latest `tblpricing`
   (catalog sync, SyncEngine). No grandfathering on new sales.
2. **Existing services** keep their signed-up price (`tblhosting.recurringamount`)
   — **grandfathered** — and only change at a **cycle boundary**, and only if a
   **price-push policy** is active for that service.
3. The **billing cycle the customer signed up on** determines *when* a new price
   lands: a quarterly customer sees the new price on the **next quarterly invoice**,
   not mid-term. A monthly customer rolls sooner; an annual customer up to a year
   later. This is intrinsic to WHMCS renewal invoicing and we align to it rather
   than fight it.

### 5.2 How WHMCS actually applies it (the mechanism, no gaps)
- WHMCS generates a renewal invoice `X` days before `tblhosting.nextduedate`
  (the **Invoice generation lookahead**, a WHMCS setting; mirrored in addon
  `invoice_generation_lookahead_days`). The invoice amount is taken from
  `tblhosting.recurringamount` **at generation time**.
- Therefore the **only safe moment** to write a new renewal price is **after** the
  current cycle's invoice exists and **before** the next is generated — i.e. we
  update `recurringamount` so the *next* generated invoice picks it up. We never
  touch an already-generated/unpaid invoice (a `safety_window_days` guard +
  `unpaid_invoice` flag already exist for this).
- The addon **never writes `tblhosting` from the catalog path** (SyncEngine is
  catalog-only, enforced by a static-grep test). Renewal writes go solely through
  the renewal path (RenewalEngine / ScheduledChangeProcessor).

### 5.3 Per-service policy (already modelled, reused)
`mod_contabo_service_policy.policy` governs each service:
- `lifetime` / `frozen_until` → **grandfathered**, push never applies (or only after
  a date).
- `current_term` → keep this term's price; recompute at the next renewal.
- `reprice_renewal` → **opt-in push**: at the next cycle boundary, set
  `recurringamount` to the latest derived sell price for that service's cycle.
- `margin_floor` → push only enough to hold the configured margin floor.
- `manual` → admin-only.

A **global push toggle** (`repricing_phase`: `observe` → compute + log only;
`notify` → email admin; `enforce` → actually write) gates the whole engine, so
"push not configured/accepted" cleanly degrades to grandfathering — the customer
keeps the old price indefinitely.

### 5.4 Source-change → who gets repriced
When the scraper reports a Contabo price change:
- SyncEngine appends a new profile **version** (the per-cycle source vector changed)
  and updates `tblpricing` for mapped cycles → **new orders** get it immediately.
- RenewalEngine, on its cron pass, evaluates **existing** services against their
  policy + the new version, and **only** schedules/writes renewal changes for
  services whose policy permits, at their next cycle boundary, with the configured
  customer notice (`notice_days_default`, pre-change email). Everyone else stays
  grandfathered.

### 5.5 Gap checks (must hold; covered by tests)
- A service mid-cycle with an unpaid current invoice → **skip** (don't reprice under
  an open invoice).
- Cycle not in the mapping's `renewal_cycles_mask` → **skip** (`cycle_not_mapped`).
- Free/disabled sentinel cycles → never overwritten.
- `period_prices_json` per-cycle basis is used for the renewal cost too (so a
  quarterly renewal prices off the quarterly source tier, not the monthly), keeping
  catalog and renewal **consistent**. RenewalEngine keeps reading the customer
  markup from the mapping (the "byte-identical resolveMarkup" contract holds; only
  the *cost basis* now comes from the version vector).

---

## 6. Delete / Trash / Undo / Purge **[OWNER: per-profile, not global]**

- **Delete** → soft: set `mod_contabo_profile.deleted_at`; excluded from all default
  listings; inline "Deleted — Undo" flash.
- **Restore (Undo)** → clear `deleted_at`.
- **Purge** (permanent, per profile): guarded — **blocked** if an *active* mapping
  or a *live `tblhosting` service* references the profile's product; requires the
  typed confirmation phrase (`SchemaHealth::isPurgeConfirmed`); cascades **only that
  profile's** owned rows (its mappings, versions, config-option link rows, and the
  WHMCS config objects created *for it* — scoped via the link tables, the
  `ConfigPurgeService` pattern but profile-scoped); writes a `logActivity` audit.
  Never touches another profile's data, clients, invoices, or services.

---

## 7. Smaller decisions (brainstormed) **[OWNER: brainstorm each]**

1. **Mapping cycle list = profile's published set.** The mapping UI offers only
   cycles the profile publishes (default all six); engine enforces regardless. Keeps
   the admin from enabling a cycle with no source.
2. **`source_overrides_json` shape:** keyed by WHMCS cycle literal →
   `{ "monthly_eur": <float> }`. Absent/invalid → fall back to the profile vector.
   Validated + coerced in `MappingRepository` like the existing JSON columns.
3. **No surprise price moves on deploy.** Existing `profile_version` rows have
   `period_prices_json = NULL` → engine falls back to the legacy single-`finalMonthly`
   basis until the next sync repopulates the vector. First post-v8 sync rewrites the
   vector and *may* change catalog prices for cycles that previously inherited the
   single period — this is the intended correction, surfaced in the sync diff +
   guarded by the 50% suspicious-change block. Admin reviews before enforce.
4. **Suspicious-change 50% guard stays** for both catalog and renewal.
5. **Currency:** v1 still syncs the base/INR currency for catalog; the source vector
   is currency-agnostic (EUR) and converted per active currency exactly as today.
6. **Exposure UI = light (option a) [OWNER].** Configurable-mode save seeds
   `ConfigOptionLink` rows with expose-defaults; fine-grained curation stays in the
   existing dedicated **Exposure editor** (already linked per profile row). The modal
   shows the dimension list + expose checkboxes + default value, not a second full
   matrix. Fixed mode shows **no** exposure UI (no configurable options by design).

---

## 8. Schema delta (v8) — implemented

| Table | Column | Type | Purpose |
|---|---|---|---|
| `mod_contabo_profile` | `published_cycles_mask` | uint, default **63** | offered cycles (superset; mapping narrows) |
| `mod_contabo_profile` | `deleted_at` | timestamp null | soft-delete / Trash |
| `mod_contabo_profile_version` | `period_prices_json` | longtext null | per-cycle EUR source vector |
| `mod_contabo_mapping` | `source_overrides_json` | longtext null | per-product source basis pin |

Additive + idempotent (`migrateTo8`, `hasColumn`-guarded). `SchemaHealth`
required-column set updated. No column drops.

---

## 9. Build order & status

1. **WS1 schema v8** — ✅ done (migration, SchemaHealth, version-coupled tests).
2. **WS2 engine** — ✅ done (catalog vector build + corrected nearest-≤ fallback +
   per-cycle source + publish gate + shared EUR→local; RenewalEngine cost-basis
   switched to the same vector via `resolveCycleEurMonthly`).
3. **WS3 profile form** — ✅ done (Publish-cycles control + per-cycle source preview
   replaces the Period dropdown; `period_months` derived server-side; fixed-mode
   completeness validation; mode-aware form hides exposure for fixed).
4. **WS4 mapping UI** — ✅ done (Source column in the cycle table;
   `source_overrides_json` field + `MappingRepository` whitelist +
   `ajax-product-cycles` returns the profile source vector).
5. **WS5 delete/trash/undo/purge** — ✅ done (soft-delete + Trash view + Undo +
   per-profile guarded `ProfilePurgeService`; `profile-delete`/`-restore`/`-purge`/
   `profiles-trash` actions).
6. **WS7 CLAUDE.md** — ✅ rewritten (project-real guidance; ruflo content removed).
7. **WS8 tests** — ✅ `PerCycleSourcePricingTest`, `ProfilePurgeServiceTest`, renewal
   per-cycle basis cases, mapping source-override; FakeCapsule `whereNull`/`whereNotNull`.
8. **WS6 provisioning** — ◻ deferred (separate `contabo_vps` deploy; out of this sweep).

Version bumped to **0.7.0**; CHANGELOG updated.

Verification status: **419 PHPUnit tests green**; PHP 8.4 `php -l` clean on all
changed files; no PHP 7.4-incompatible syntax; templates render-smoked (profiles
normal + trash, mappings). The full gate (`bash scripts/predeploy-check.sh`:
php:7.4 lint + live-schema 8.13/9.0 + integration smoke) + dev renders run before
deploy — the docker daemon is unavailable in this build session. Deploy only on a
green gate + explicit approval.
