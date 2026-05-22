# Contabo Pricing — UI Architecture (v0.2.0)

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

`_layout_open.tpl` emits the JS bundle as:

```html
<script src="modules/addons/contabo_pricing/assets/app.js?v={$cb_addon_version|0.2.0}" defer></script>
```

The `?v=` query string forces the browser to cache-bust whenever the addon version bumps. The controller can override `$cb_addon_version` per-render; otherwise the literal default kicks in. Same pattern can be applied to external `<link>` stylesheets in the future.

## AJAX endpoint catalogue

All endpoints sit under `addonmodules.php?module=contabo_pricing&action=<action>` and return `application/json`.

| Action | Method | Auth | Request | Response (200) |
|--------|--------|------|---------|----------------|
| `ajax-quote` | POST | CSRF token | `plan_slug`, `period_months` | `{ base_monthly_eur, final_monthly, currency_iso, gst_pct, fx_markup_pct, … }` |
| `ajax-fx` | GET | — | — | `{ rate, source, age_minutes?, … }` |
| `ajax-meta-probe` | POST | CSRF token | — | `{ ok: true, scraper_version, snapshot_at }` (200) or `{ ok: false, error }` (200, soft-fail) |
| `ajax-profile-versions` | GET | — | `id` | `{ profile_id, versions: [{ version, final_monthly, currency_iso, snapshot_generated_at }, …] }` (newest first) |
| `ajax-profile` | GET | — | `id` | `{ profile, latest_version }` |

All endpoints emit `{ "error": "<msg>" }` with HTTP 500 (or 400/404 for input errors) on failure. `ajax-meta-probe` is the only soft-fail endpoint — it returns 200 with `ok: false` so the UI can render a red pill without treating it as a network error.

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
| `data-cb-fx-preview` | element in settings | FX preview |
| `data-cb-action="test-api-connection"` | settings page button | API probe |
| `data-cb-log-row`, `data-cb-status`, `data-cb-trigger`, `data-cb-started` | sync-history `<tr>` | filter + search |
| `data-cb-date-from`, `data-cb-date-to` | sync-history filter inputs | (template-side filter UI) |

## Idempotency

The JS bundle guards against double-initialisation via `window.__cbPricingInit`. If WHMCS evaluates the page twice in one request (e.g. via sidebar widget callbacks), the second run is a no-op.
