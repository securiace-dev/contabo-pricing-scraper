# Contabo Pricing — UI Architecture (v0.6.0)

The admin UI is a self-contained design system rendered via PHP templates inside the WHMCS admin chrome. No framework, no jQuery — vanilla JS (ES2017+) + hand-written CSS variables.

## Design tokens

All tokens live as CSS custom properties in `templates/admin/_layout_open.tpl` under `:root { … }`. Single source of truth. Touch nothing else if you want to retheme.

- **Color** — burnt-orange accent (`--accent: #b45309`) on cream (`--bg: #faf7f1`). Semantic tones `--good / --warn / --bad`. Border tones are warm-grey so the editorial feel survives.
- **Typography** — IBM Plex Sans (body), IBM Plex Mono (numbers, tabular-nums), Instrument Serif (display headings only).
- **Spacing** — implicit via padding/margin scale in components (no global spacing tokens — keeps the system small).
- **Radius** — `--radius: 10px` (cards), `--radius-sm: 6px` (buttons, inputs).
- **Shadows** — `--shadow-sm` for cards, `--shadow-lg` for overlays.

## Component inventory

| Component | Class | Purpose |
|-----------|-------|---------|
| Status strip | `.cb-strip > .cb-stat` | 4 KPI tiles at the top of every page |
| Card | `.cb-card` | Panel with optional `.cb-card-title` + `.cb-card-sub` |
| Table | `.cb-table` | Sortable, with `.sparkline` cells |
| Pill | `.cb-pill[.good/.warn/.bad/.grey]` | Status badge |
| Filter pills | `.cb-filter-pills > button[aria-pressed]` | Mutually-exclusive filters |
| Segmented control | `.cb-seg` | Inline two-or-three-way toggle |
| Button | `.cb-btn[.ghost/.danger/.subtle]` | Primary / secondary / destructive |
| Form field | `.cb-field > label + input` | Vertical label-over-control pattern |
| Search | `.cb-search` | Pill-shaped search input with leading icon |
| Toolbar | `.cb-toolbar` | Row of filters + actions atop a table |
| Drawer | `.cb-drawer[.open]` | Right-side slide-in for detail views |
| Modal | `.cb-modal[.open]` | Centred overlay sheet |
| Sparkline | `svg.sparkline` | Inline trend, currentColor stroke |
| Toast | `.cb-toast.cb-pill` | Transient (3 s) feedback messages |

## Asset versioning

`_layout_open.tpl` builds a **host-absolute** JS bundle URL and emits it as:

```php
$cb_assets_url = '/modules/addons/contabo_pricing/assets/app.js?v='
    . rawurlencode(isset($cb_addon_version) ? (string) $cb_addon_version : '0.2.0');
```
```html
<script src="<?= $esc($cb_assets_url) ?>" defer></script>
```

The `?v=` query string forces the browser to cache-bust whenever the addon version bumps. `render()` (in `AdminController`) injects `$cb_addon_version = AdminController::VERSION` (`0.6.0`) on every render unless a view overrides it; the literal `0.2.0` is only the template-side fallback when the variable is somehow unset. The host-absolute path (leading `/…`, not under the WHMCS admin slug) is deliberate — module static files live at the install root regardless of a custom admin slug. Same pattern can be applied to external `<link>` stylesheets in the future.

## AJAX endpoint catalogue

All endpoints sit under `addonmodules.php?module=contabo_pricing&action=<action>` and return `application/json`.

| Action | Method | Auth | Request | Response (200) |
|--------|--------|------|---------|----------------|
| `ajax-quote` | POST | CSRF token | `plan_slug`, `period_months` | `{ base_monthly_eur, final_monthly, currency_iso, gst_pct, fx_markup_pct, … }` |
| `ajax-fx` | GET | — | — | `{ rate, source, age_minutes?, … }` |
| `ajax-meta-probe` | POST | CSRF token | — | `{ ok: true, scraper_version, snapshot_at }` (200) or `{ ok: false, error }` (200, soft-fail) |
| `ajax-profile-versions` | GET | — | `id` | `{ profile_id, versions: [{ version, final_monthly, currency_iso, snapshot_generated_at }, …] }` (newest first, up to 50) |
| `ajax-profile` | GET | — | `id` | `{ profile, latest_version }` |
| `ajax-configurator` | GET | — | `plan_slug` | `{ plan_slug, controls: [{ key, label, optional, defaultIdx, options: [{ label, monthly, setup }, …] }, …], default_monthly_by_period, default_setup_by_period, periods, title, family }` — reshapes the upstream configurator into the report-style `controls[]` model used by the create/edit modal |
| `ajax-profile-edit-form` | GET | — | `id` | `{ profile, selections }` — `selections` is the decoded `options` map (`{}` for legacy rows); prefills the edit modal |
| `ajax-product-cycles` | GET | — | `product_id`, `currency_id?` | `{ product_id, currency_id, currency_code, cycles: [{ cycle, months, recurring_column, setup_fee_column, current_price, current_setup_fee, status, can_catalog_sync, can_renewal_sync }, …] }` — current WHMCS catalog prices per billing cycle (drives the mappings cycle picker) |
| `ajax-policy-preview` | GET | — | `service_id`, `policy?` | `{ service_id, available, preview }` (or `{ available: false, reason }` when `RenewalEngine` isn't deployed) — dry-run repricing decision for a service under a hypothetical policy |
| `ajax-approval-count` | GET | — | — | `{ count }` — number of pending approval-queue decisions (badge poll; soft-fails to `{ count: 0 }`) |

All endpoints emit `{ "error": "<msg>" }` with HTTP 500 (or 400/404 for input errors) on failure. The mutating endpoints — `ajax-quote` (hits a paid upstream API) and `ajax-meta-probe` (can leak server reachability) — call `check_token()` and require a CSRF token. The read-only endpoints (`ajax-fx`, `ajax-profile-versions`, `ajax-profile`, `ajax-configurator`, `ajax-profile-edit-form`, `ajax-product-cycles`, `ajax-policy-preview`, `ajax-approval-count`) are side-effect-free and intentionally do NOT call `check_token()`, so a stale token can't break a drawer/modal fetch (see the dispatch-comment around `AdminController` line ~88). `ajax-meta-probe` is the only soft-fail endpoint — it returns 200 with `ok: false` so the UI can render a red pill without treating it as a network error (`ajax-approval-count` similarly degrades to `count: 0` rather than erroring).

## Page actions

Full-page views and form posts dispatched by `AdminController::dispatch()` (these render a `.tpl`/redirect rather than JSON). Mutating actions (form posts) call `verifyToken()` (CSRF) before any write.

| Action | Method | Auth | Renders / does |
|--------|--------|------|----------------|
| `dashboard` (default) | GET | — | KPI strip + last-sync summary |
| `profiles` | GET | — | Profile list + create/edit modal |
| `profile-create` | POST | CSRF | Create-or-resolve a profile (conflict chooser on slug clash) |
| `profile-save` | POST | CSRF | Patch an existing profile (only submitted fields) |
| `profile-toggle` | POST | CSRF | Flip a profile's active flag |
| `profile-diff` | GET | — | Version diff for a profile |
| `config-preview` | GET | — | Read-only dry-run of the WHMCS configurable options a profile *would* create |
| `config-diff` | GET | — | Read-only pre-apply diff against a live mapped product |
| `config-apply` | POST | CSRF | Write configurable options to a mapped product (honours the expose gate) |
| `config-exposure` | GET | — | Exposure editor — curate which option dimensions are exposed/hidden |
| `config-exposure-save` | POST | CSRF | Persist the `expose_to_customer` / `hidden` flags (effective on next Apply) |
| `mappings` / `mapping-save` | GET / POST | — / CSRF | Profile↔product mapping list + save |
| `sync-history` / `sync-run` / `refresh-api` | GET / POST / POST | — / CSRF / CSRF | Sync log, run a manual sync, queue an upstream refresh |
| `repricing`, `price-decisions`, `skipped-report` | GET | — | Renewal Pricing Policy Engine read-mostly views (Phase A) |
| `approval-queue` | GET | — | Pending repricing decisions awaiting admin/force approval |
| `approval-approve` / `approval-reject` | POST | CSRF | Approve / reject a queued decision |
| `currency-report` / `currency-report-csv` | GET | — | Multi-currency exposure diagnostic + CSV export |
| `tax-settings` / `tax-settings-save`, `maintenance*` | GET / POST | — / CSRF | Tax config + schema maintenance |
| `capability-editor` | GET | — | A.6.3 capability-matrix editor — per (plan, dimension, value) change classification (allowed-on-*, destructive, `capability_source`). Rows enumerated from the live configurator, overlaid with saved `mod_contabo_option_capability` rows. Reached from `config-preview` + a profile row action. |
| `capability-editor-save` | POST | CSRF | Upsert capability rows via `ConfigOptionCapabilityRepository` (re-whitelists every column). |
| `compatibility-editor` | GET | — | A.6.3 compatibility-matrix editor — author `incompatible_with` / `required_values` / min-max-qty per (plan, dimension, value); feeds `validateCombination()`. |
| `compatibility-editor-save` | POST | CSRF | Upsert compatibility rules via `ConfigOptionCompatibilityRepository`; blank+never-saved rows are skipped. |

## Create / edit profile modal

`templates/admin/profiles.tpl` carries a single `profile-create` modal that doubles as the **edit** modal: `assets/app.js` switches it into edit mode when the trigger button has `data-cb-profile-edit-id`, repointing the hidden `action` to `profile-save` and prefilling from `ajax-profile-edit-form`. Two profile-level controls beyond the legacy fields:

- **Profile mode** — a `<select name="profile_mode">` carrying `data-cb-profile-mode`, with two options:
  - `fixed_admin_profile` (default) — admin locks every build option into one SKU; the customer cannot choose.
  - `customer_configurable_product` — the plan maps to one WHMCS product and the customer picks from the exposed options at order time.

  The value feeds the profile identity fingerprint server-side (fixed vs configurable hash differently), so it's whitelisted by `AdminController::normalizeProfileMode()` — anything unrecognised collapses back to `fixed_admin_profile`. `app.js` resets it to `fixed_admin_profile` on a fresh create and restores `p.profile_mode` on edit.

- **Expose configurable options** — a checkbox `<input name="expose_configurable_options" value="1">` carrying `data-cb-expose-config`, inside a `[data-cb-expose-field]` field, defaulting **checked**. Because an unchecked checkbox doesn't POST, a paired hidden `<input name="expose_configurable_options" value="0">` precedes it as the off fallback. This is the master switch the Apply path honours: when off, `config-apply` skips creating WHMCS configurable-option groups for the profile (the flash says so). `AdminController::normalizeExposeFlag()` maps it to `0`/`1`, defaulting absent → `1` to preserve pre-v7 behaviour. On edit, `app.js` sets `checked = String(p.expose_configurable_options) !== '0'` so legacy `null` rows render as exposed.

Each profile row also exposes a new **Exposure** action: an `<a class="cb-btn ghost">` linking to `…&action=config-exposure&id=<pid>`, which opens the exposure editor to curate exactly which option dimensions customers see.

## Keyboard shortcuts

Only active when focus is NOT in an `<input>` / `<textarea>` / `<select>` and no modifier key is held.

| Key | Action |
|-----|--------|
| `/` | Focus the first visible `[data-cb-search]` |
| `n` | Click any visible `[data-cb-open-modal="profile-create"]` |
| `r` | Click any visible `[data-cb-action="test-api-connection"]` |
| `Esc` | Close any open drawer + modal |

## `data-cb-*` attribute reference

The JS module reads only these attributes. Templates must keep them stable.

| Attribute | On | Used by |
|-----------|----|---------|
| `data-cb-filter="<value>"` | filter-pill button | filter pills |
| `data-cb-sort="<key>"` | `<th>` | sort |
| `data-cb-sort-value="<v>"` | `<td>` | sort (per-cell override) |
| `data-cb-search` | `<input>` | search + `/` shortcut |
| `data-cb-profile-row`, `data-cb-profile-id`, `data-cb-profile-name`, `data-cb-active`, `data-cb-drifted` | `<tr>` | filter pills + search |
| `data-cb-bulk` | row `<input type=checkbox>` | bulk select |
| `data-cb-bulk-all` | header `<input type=checkbox>` | bulk select master |
| `data-cb-bulk-toolbar` | bulk action bar | bulk select |
| `data-cb-open-modal="<id>"` | trigger button | modals |
| `data-cb-close-modal` | cancel button inside `.cb-modal` | modals |
| `data-cb-open-drawer="profile\|log"` + `data-cb-profile-id` / `data-cb-log-id` | row | drawer |
| `data-cb-close-drawer` | drawer close button | drawer |
| `data-cb-drawer-body` | element inside drawer | drawer fetch target |
| `data-cb-sparkline`, `data-cb-sparkline-large` + `data-cb-profile-id` | `<svg>` | sparkline render |
| `data-cb-quote-plan`, `data-cb-quote-period`, `data-cb-preview-price` | inside profile-create modal | live quote preview |
| `data-cb-profile-mode` | `<select>` in profile-create modal | profile mode (`fixed_admin_profile` \| `customer_configurable_product`) — reset on create, restored on edit |
| `data-cb-expose-config` | checkbox in profile-create modal | "Expose configurable options" gate (within `data-cb-expose-field`) |
| `data-cb-expose-field` | `.cb-field` wrapping the expose checkbox | groups the exposure control |
| `data-cb-profile-edit-id` | row "Edit" button | switches the create modal into edit mode (→ `profile-save` + `ajax-profile-edit-form` prefill) |
| `data-cb-modal-title`, `data-cb-form-action`, `data-cb-form-id`, `data-cb-submit-label` | inside profile-create modal | create⇄edit mode swap targets |
| `data-cb-configurator-form` | the modal `<form>` | configurator wiring root |
| `data-cb-configurator`, `data-cb-summary`, `data-cb-cfg-reset` | elements in modal | configurator render targets |
| `data-cb-cfg-plan`, `data-cb-cfg-period` | `<select>` in modal | configurator inputs (drive `ajax-configurator`) |
| `data-cb-cfg-control`, `data-cb-cfg-control-idx`, `data-cb-cfg-default-idx` | per-dimension `<select>` | configurator controls |
| `data-cb-options-json` | hidden `<input name="options">` | serialised configurator selections (server derives OS/Region) |
| `data-cb-fx-preview` | element in settings | FX preview |
| `data-cb-action="test-api-connection"` | settings page button | API probe |
| `data-cb-log-row`, `data-cb-status`, `data-cb-trigger`, `data-cb-started` | sync-history `<tr>` | filter + search |
| `data-cb-date-from`, `data-cb-date-to` | sync-history filter inputs | (template-side filter UI) |
| `data-cb-respect-disabled` | mappings cycle picker checkbox | filters `ajax-product-cycles` cycles |

## Idempotency

The JS bundle guards against double-initialisation via `window.__cbPricingInit`. If WHMCS evaluates the page twice in one request (e.g. via sidebar widget callbacks), the second run is a no-op.
