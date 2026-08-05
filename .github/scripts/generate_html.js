#!/usr/bin/env node
'use strict';

// Renders the interactive, self-contained report.html from the Rust-emitted
// contabo_view_model.json (canonical) plus the enriched option_catalog (so the
// detail panel matches PRICES.md exactly), then reconciles the view model
// against contabo_pricing_dataset.json and writes a consistency report.

const fs   = require('fs');
const path = require('path');
const { loadManagedCatalog } = require('./managed_services_model');
const proposalModel = require('./proposal_model');

const OUTPUT_DIR    = path.resolve(__dirname, '../../data/output');
const VM_PATH       = path.join(OUTPUT_DIR, 'contabo_view_model.json');
const DATASET_PATH  = path.join(OUTPUT_DIR, 'contabo_pricing_dataset.json');
const CONFIGS_PATH  = path.join(OUTPUT_DIR, 'contabo_configs.json');
const QR_PATH       = path.join(OUTPUT_DIR, 'contabo_quick_reference.json');
const HTML_PATH     = path.resolve(__dirname, '../../report.html');
const RECON_PATH    = path.join(OUTPUT_DIR, 'contabo_consistency_report.json');
const MANAGED_CATALOG_PATH = path.resolve(__dirname, '../../data/managed_services_catalog.json');
const PROPOSAL_MODEL_PATH = path.resolve(__dirname, './proposal_model.js');
const PROPOSAL_MODEL_SOURCE = fs.readFileSync(PROPOSAL_MODEL_PATH, 'utf8')
  .replace(/<\/script/gi, '<\\/script');

const round2 = (x) => Math.round((Number(x) + Number.EPSILON) * 100) / 100;

// Pricing model:
//   Contabo lists prices in EUR excluding VAT/GST.
//   For Indian buyers Contabo charges 18% GST on the EUR amount.
//   Card networks apply a forex markup (~3.5% default) on top of the mid-market
//   EUR→INR rate. GST and FX markup are *user-controllable* in the report UI
//   (defaults below); they are never baked into the JSON/CSV/PRICES.md.
const PROVIDER_PRICES_INCLUDE_TAX = false;
const PROVIDER_TAX_RATE   = 0.18;
const OUTPUT_TAX_RATE     = 0.18;
const FX_MARKUP_DEFAULT   = 0.035;  // typical card forex markup
const GST_REGISTRATION_VERIFIED = process.env.SECURIACE_GST_REGISTRATION_VERIFIED === 'true';

async function fetchFx() {
  const fx = {
    eurInr: null,
    at: null,
    source: 'frankfurter.app (ECB mid-market)',
    providerPricesIncludeTax: PROVIDER_PRICES_INCLUDE_TAX,
    providerTaxRate: PROVIDER_TAX_RATE,
    outputTaxRate: OUTPUT_TAX_RATE,
    outputTaxRegistrationVerified: GST_REGISTRATION_VERIFIED,
    outputTaxSource: GST_REGISTRATION_VERIFIED
      ? 'verified build gate: SECURIACE_GST_REGISTRATION_VERIFIED'
      : 'default disabled: registration evidence not verified',
    fxMarkupDefault: FX_MARKUP_DEFAULT,
  };
  try {
    const ac = new AbortController();
    const to = setTimeout(() => ac.abort(), 4000);
    const res = await fetch('https://api.frankfurter.app/latest?from=EUR&to=INR', { signal: ac.signal });
    clearTimeout(to);
    if (res.ok) {
      const j = await res.json();
      const rt = j && j.rates && j.rates.INR;
      if (typeof rt === 'number' && rt > 0) {
        fx.eurInr = rt;
        fx.at = new Date().toISOString();
        fx.rateDate = j.date || null; // ECB publication date
      }
    }
  } catch { /* non-fatal: offline/firewalled CI — the browser retries live, else INR is omitted */ }
  return fx;
}

if (!fs.existsSync(VM_PATH)) {
  console.log(`No view model at ${VM_PATH} — skipping report.html generation.`);
  process.exit(0);
}

const vm    = JSON.parse(fs.readFileSync(VM_PATH, 'utf8'));
const rows  = Array.isArray(vm.rows) ? vm.rows : [];
const genAt = vm.meta?.generated_at || '';
const managedServices = loadManagedCatalog(MANAGED_CATALOG_PATH);
const legacyTaxonomyFamilies = [...new Set(rows.map(row => row.family))]
  .filter(family => family === 'Cloud VPS' || family === 'Cloud VDS');

let dataset = null;
if (fs.existsSync(DATASET_PATH)) {
  try { dataset = JSON.parse(fs.readFileSync(DATASET_PATH, 'utf8')); }
  catch { /* graceful: no add-ons / reconciliation if malformed */ }
}
const optionCatalog = dataset?.option_catalog || [];

// ── Per-plan metadata straight from the dataset (the view_model carries only
// VPS-shaped specs, so pull the real per-family specs_parsed + pricing_model +
// availability here and join by slug on the client). ────────────────────────
const planMeta = {};
if (dataset && Array.isArray(dataset.plans)) {
  for (const p of dataset.plans) {
    planMeta[p.product_slug] = {
      pricing_model: p.pricing_model || 'fixed',
      availability:  p.availability || null,
      specs:         p.specs_parsed || {},
      canonical_family: p.canonical_family || p.family || null,
      legacy_family: p.legacy_family || p.family || null,
      storage_policy: p.storage_policy || 'not_applicable',
    };
  }
}

// ── Dedicated add-ons: independent optional line-items grouped by category,
// rendered as a toggle checklist in the detail modal. ────────────────────────
const ADDON_CAT_ORDER = ['Control Panel', 'RAM', 'Storage', 'GPU', 'Networking', 'Security', 'Management', 'Software', 'Misc'];
const planExtras = {};
for (const opt of optionCatalog) {
  if (opt.dimension !== 'Add-on') continue;
  (planExtras[opt.plan_sku] ??= []).push({
    label:    opt.option_label,
    category: opt.category || 'Misc',
    monthly:  Number(opt.monthly_price_delta) || 0,
    setup:    Number(opt.setup_fee_delta) || 0,
  });
}
for (const slug of Object.keys(planExtras)) {
  planExtras[slug].sort((a, b) => {
    const ci = ADDON_CAT_ORDER.indexOf(a.category), cj = ADDON_CAT_ORDER.indexOf(b.category);
    if (ci !== cj) return (ci < 0 ? 99 : ci) - (cj < 0 ? 99 : cj);
    if (a.monthly !== b.monthly) return a.monthly - b.monthly;
    return a.label.localeCompare(b.label);
  });
}

let configsRoot = null;
if (fs.existsSync(CONFIGS_PATH)) {
  try {
    const raw = JSON.parse(fs.readFileSync(CONFIGS_PATH, 'utf8'));
    const inner = raw?.plans ?? raw;          // enrich_output.js wraps it
    configsRoot = {};
    for (const v of Object.values(inner)) {
      if (v && typeof v === 'object' && v.slug) configsRoot[v.slug] = v;
    }
    if (Object.keys(configsRoot).length === 0) configsRoot = null;
  } catch { configsRoot = null; }
}

// ── Calculator model: slug → { controls[], defaultMonthlyByPeriod, … } ────────
const NETWORK_CATS = ['Bandwidth', 'IPv4', 'Private Networking'];
const IMAGE_CATS   = ['OS', 'Apps', 'Panels', 'Blockchain'];
const IMAGE_LABEL  = { OS: 'OS', Apps: 'App', Panels: 'Control Panel', Blockchain: 'Blockchain' };

function mapOpt(o) {
  return {
    label:   o.option_label,
    monthly: Number(o.monthly_price_delta) || 0,
    setup:   Number(o.setup_fee_delta) || 0,
    isDef:   !!o.is_default,
  };
}
function finalizeControl(key, label, optional, opts) {
  opts.sort((a, b) =>
    (a.isDef !== b.isDef) ? (a.isDef ? -1 : 1)
    : (a.monthly !== b.monthly) ? a.monthly - b.monthly
    : String(a.label).localeCompare(String(b.label)));
  if (optional && !opts.some((o) => o.label === 'None'))
    opts.unshift({ label: 'None', monthly: 0, setup: 0, isDef: false });
  let defaultIdx = opts.findIndex((o) => o.isDef);
  if (defaultIdx < 0) defaultIdx = optional ? opts.findIndex((o) => o.label === 'None') : 0;
  if (defaultIdx < 0) defaultIdx = 0;
  return { key, label, optional, defaultIdx, options: opts.map(({ label, monthly, setup }) => ({ label, monthly, setup })) };
}

const planConfig = {};
if (configsRoot) {
  for (const [slug, cfg] of Object.entries(configsRoot)) {
    const controls = [];
    const opts = cfg.options || {};
    for (const dim of Object.keys(opts)) {
      // Dedicated `Add-on`s are independent optional line-items, not one
      // mutually-exclusive choice — rendered as a checklist (planExtras), not a
      // 50-option dropdown. Skip them here.
      if (dim === 'Add-on') continue;
      const list = (opts[dim] || []).map(mapOpt);
      if (list.length === 0) continue;
      if (dim === 'Networking') {
        for (const cat of NETWORK_CATS) {
          const sub = list.filter((_, i) => (opts[dim][i].category) === cat);
          if (sub.length) controls.push(finalizeControl('Networking:' + cat, cat, false, sub));
        }
      } else if (dim === 'Image') {
        const cats = [...IMAGE_CATS, ...new Set(opts[dim].map((o) => o.category).filter((c) => !IMAGE_CATS.includes(c)))];
        for (const cat of cats) {
          const sub = list.filter((_, i) => opts[dim][i].category === cat);
          if (sub.length) controls.push(finalizeControl('Image:' + cat, IMAGE_LABEL[cat] || cat, cat !== 'OS', sub));
        }
      } else {
        const optional = dim === 'Data Protection';
        controls.push(finalizeControl(dim, dim, optional, list));
      }
    }
    planConfig[slug] = {
      controls,
      defaultMonthlyByPeriod: cfg.default_monthly_by_period || {},
      defaultSetupByPeriod:   cfg.default_setup_by_period   || {},
    };
  }
}

// ── Fallback only (no configs.json): static add-on tag map ───────────────────
const NEGATION_DEFAULTS = new Set([
  'No Data Protection', 'No Backup Space', 'No Private Networking',
]);
const planAddons = {};
if (!configsRoot) {
  for (const opt of optionCatalog) {
    if (opt.dimension === 'Storage Type') continue;
    if (NEGATION_DEFAULTS.has(opt.option_label)) continue;
    const slug = opt.plan_sku;
    planAddons[slug] ??= {};
    planAddons[slug][opt.dimension] ??= [];
    planAddons[slug][opt.dimension].push({
      label:       opt.option_label,
      category:    opt.category,
      delta:       opt.monthly_price_delta ?? 0,
      isDefault:   !!opt.is_default,
      regionGroup: opt.region_group || null,
    });
  }
  for (const slug of Object.keys(planAddons)) {
    for (const dim of Object.keys(planAddons[slug])) {
      planAddons[slug][dim].sort((a, b) => {
        if (a.isDefault !== b.isDefault) return a.isDefault ? -1 : 1;
        if (a.delta !== b.delta)         return a.delta - b.delta;
        return a.label.localeCompare(b.label);
      });
    }
  }
}

// ── Reconciliation: view_model rows vs pricing_dataset.plans ──────────────────
const mismatches = [];
let checked = 0;
if (dataset && Array.isArray(dataset.plans)) {
  const FIELDS = [
    ['effective_monthly', 'effective_monthly'],
    ['setup_fee',         'setup_fee'],
    ['discount_total',    'discount_total'],
    ['total_period_cost', 'total_period_cost'],
  ];
  const bySlug = new Map();
  for (const p of dataset.plans) bySlug.set(p.product_slug, p);

  for (const r of rows) {
    const p = bySlug.get(r.plan_slug);
    if (!p) {
      mismatches.push({ plan_slug: r.plan_slug, period: r.period_months, field: '*plan*',
        view_model_value: 'present', dataset_value: 'missing' });
      continue;
    }
    const per = (p.periods || []).find((x) => x.months === r.period_months);
    if (!per) {
      mismatches.push({ plan_slug: r.plan_slug, period: r.period_months, field: '*period*',
        view_model_value: 'present', dataset_value: 'missing' });
      continue;
    }
    checked++;
    for (const [vmK, dsK] of FIELDS) {
      const a = Number(r[vmK]), b = Number(per[dsK]);
      if (Math.abs(a - b) > 0.005) {
        mismatches.push({ plan_slug: r.plan_slug, period: r.period_months, field: vmK,
          view_model_value: a, dataset_value: b });
      }
    }
    // Key-aware spec reconciliation: only compare a spec the family actually
    // uses (present in dataset specs_parsed), coalescing null/undefined so
    // Object Storage — which has capacity specs, not cpu/ram — isn't flagged.
    const specKeys = ['cpu_count', 'ram_gb', 'storage_primary_gb', 'storage_primary_type', 'port_speed_mbps'];
    for (const k of specKeys) {
      const b = p.specs_parsed?.[k];
      if (b === undefined) continue;
      const a = r.specs?.[k];
      if ((a ?? null) !== (b ?? null)) {
        mismatches.push({ plan_slug: r.plan_slug, period: r.period_months, field: `specs.${k}`,
          view_model_value: a, dataset_value: b });
      }
    }
  }
}
// ── Reconcile the scraper's own default-config arithmetic ────────────────────
let defaultChecks = 0;
if (configsRoot) {
  for (const r of rows) {
    const cfg = configsRoot[r.plan_slug];
    if (!cfg || !cfg.options) continue;
    const pk = String(r.period_months);
    let dM = 0, dS = 0;
    for (const list of Object.values(cfg.options)) {
      for (const o of list) {
        if (o.is_default) {
          dM += Number(o.monthly_price_delta) || 0;
          dS += Number(o.setup_fee_delta) || 0;
        }
      }
    }
    const expM = round2(Number(r.effective_monthly) + dM);
    const gotM = cfg.default_monthly_by_period?.[pk];
    if (gotM != null && Math.abs(expM - Number(gotM)) > 0.01) {
      mismatches.push({ plan_slug: r.plan_slug, period: r.period_months,
        field: 'default_config_monthly', view_model_value: expM, dataset_value: Number(gotM) });
    }
    const expS = round2(Number(r.setup_fee) + dS);
    const gotS = cfg.default_setup_by_period?.[pk];
    if (gotS != null && Math.abs(expS - Number(gotS)) > 0.01) {
      mismatches.push({ plan_slug: r.plan_slug, period: r.period_months,
        field: 'default_config_setup', view_model_value: expS, dataset_value: Number(gotS) });
    }
    defaultChecks += 2;
  }
}

const recon = {
  generated_at: new Date().toISOString(),
  view_model_generated_at: genAt,
  source: 'contabo_view_model.json vs contabo_pricing_dataset.json + contabo_configs.json default-config arithmetic',
  checked,
  default_config_checks: defaultChecks,
  mismatch_count: mismatches.length,
  mismatches,
};
fs.writeFileSync(RECON_PATH, JSON.stringify(recon, null, 2) + '\n');

// ── Build the HTML ────────────────────────────────────────────────────────────
const payload = {
  meta: vm.meta || {},
  rows,
  consistency: { checked, mismatch_count: mismatches.length },
  taxonomy: {
    status: legacyTaxonomyFamilies.length ? 'legacy_flattened' : 'canonical',
    legacy_families: legacyTaxonomyFamilies,
    canonical_contract: {
      'Core VPS': 'SSD only',
      'Performance VPS': 'NVMe only',
      'Max Performance VPS': 'legacy Cloud VDS alias',
    },
  },
};
if (configsRoot) {
  payload.planConfig     = planConfig;
  payload.dimensionSchema = vm.dimension_schema || null;
} else {
  payload.planAddons = planAddons;
}
payload.planMeta   = planMeta;
payload.planExtras = planExtras;
payload.managedServices = managedServices;
payload.proposal = {
  snapshot_schema_version: proposalModel.SCHEMA_VERSION,
  document_schema_version: proposalModel.DOCUMENT_SCHEMA_VERSION,
  profiles: proposalModel.PROFILE_DEFAULTS,
};
;(async () => {
payload.fx = await fetchFx();
const dataJson = JSON.stringify(payload, null, 1)
  .replace(/</g, '\\u003c')
  .replace(/>/g, '\\u003e');

const html = `<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark light">
<title>Contabo Pricing — Interactive Report</title>
<link rel="icon" href="data:,">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Instrument+Serif&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#0b0d10;--bg-elev:#11141a;--panel:#161a22;--panel2:#1d222c;
    --border:#262d3a;--border-soft:#1f2530;
    --fg:#e8edf3;--fg-strong:#ffffff;--muted:#8b95a7;--muted-soft:#5b6478;
    --accent:#f0a91a;--accent-soft:rgba(240,169,26,.14);
    --accent2:#2dd4bf;--good:#34d399;--bad:#f87171;--warn:#fbbf24;
    --chip:#1a1f29;--row-hover:#181d27;
    --price:#ffd57a;
    --shadow-lg:0 20px 40px -16px rgba(0,0,0,.6),0 6px 20px -8px rgba(0,0,0,.4);
    --radius:10px;--radius-sm:6px;
  }
  html[data-theme=light]{
    --bg:#faf7f1;--bg-elev:#fefcf7;--panel:#fffaf1;--panel2:#f5efe2;
    --border:#e6dfd0;--border-soft:#efe9d9;
    --fg:#1a1d24;--fg-strong:#000;--muted:#5c6373;--muted-soft:#8b91a0;
    --accent:#b45309;--accent-soft:rgba(180,83,9,.10);
    --accent2:#0d9488;--good:#15803d;--bad:#b91c1c;--warn:#a16207;
    --chip:#f0e9d8;--row-hover:#f5efe2;
    --price:#b45309;
    --shadow-lg:0 14px 30px -16px rgba(63,42,0,.18),0 4px 12px -6px rgba(63,42,0,.08);
  }
  *{box-sizing:border-box}
  html,body{margin:0}
  body{background:var(--bg);color:var(--fg);
    font:14px/1.55 "IBM Plex Sans","SF Pro Text",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    -webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;
    font-feature-settings:"cv11","ss01","ss02";
    background-image:radial-gradient(800px 400px at 80% -10%,var(--accent-soft),transparent 70%);}
  html[data-theme=light] body{background-image:radial-gradient(900px 500px at 90% -20%,var(--accent-soft),transparent 70%);}
  .mono{font-family:"IBM Plex Mono",ui-monospace,Menlo,monospace;font-variant-numeric:tabular-nums}
  a{color:var(--accent);text-decoration:none}
  a:hover{text-decoration:underline}
  button{font:inherit}
  :focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px}

  /* ── Header ─────────────────────────────────────────────────────────────── */
  header.app{padding:22px 28px 18px;display:grid;
    grid-template-columns:auto 1fr auto;gap:16px;align-items:center;
    border-bottom:1px solid var(--border-soft)}
  .brand{display:flex;align-items:baseline;gap:12px}
  .brand .logo{font-family:"Instrument Serif",Georgia,serif;font-size:30px;
    line-height:1;color:var(--fg-strong);letter-spacing:-.01em}
  .brand .logo .dot{color:var(--accent)}
  .brand .sub{color:var(--muted);font-size:11.5px;letter-spacing:.04em;
    text-transform:uppercase}
  .head-meta{display:flex;gap:10px;align-items:center;flex-wrap:wrap;
    justify-content:flex-end}
  .badge{font-size:11px;color:var(--muted);background:var(--panel);
    border:1px solid var(--border);border-radius:999px;padding:4px 10px;
    display:inline-flex;gap:6px;align-items:center;line-height:1}
  .badge .dot-i{width:6px;height:6px;border-radius:50%;display:inline-block}
  .badge.ok .dot-i{background:var(--good)}
  .badge.warn .dot-i{background:var(--warn)}
  .badge.bad .dot-i{background:var(--bad)}
  .badge strong{color:var(--fg);font-weight:500}
  .iconbtn{background:var(--panel);border:1px solid var(--border);
    color:var(--fg);border-radius:var(--radius-sm);padding:6px 10px;
    cursor:pointer;font-size:12px}
  .iconbtn:hover{border-color:var(--accent);color:var(--accent)}

  /* ── Hero strip ─────────────────────────────────────────────────────────── */
  .hero{padding:14px 28px 18px;border-bottom:1px solid var(--border-soft);
    display:flex;flex-wrap:wrap;gap:8px 22px;align-items:center}
  .hero h2{margin:0;font-size:13px;color:var(--muted);font-weight:500;
    letter-spacing:.05em;text-transform:uppercase}
  .hero .pill-row{display:flex;gap:6px;flex-wrap:wrap}
  .hero-card{background:var(--panel);border:1px solid var(--border);
    border-radius:var(--radius-sm);padding:8px 14px;display:flex;gap:10px;
    align-items:baseline}
  .hero-card .lbl{font-size:11px;color:var(--muted);letter-spacing:.03em;
    text-transform:uppercase}
  .hero-card .v{font-family:"IBM Plex Mono",monospace;font-size:14px;
    font-weight:600;color:var(--price);font-variant-numeric:tabular-nums}
  .hero-card .nm{font-size:12px;color:var(--fg)}

  /* ── Toolbar ───────────────────────────────────────────────────────────── */
  .toolbar{padding:14px 28px;display:flex;flex-wrap:wrap;gap:14px 22px;
    align-items:center;border-bottom:1px solid var(--border-soft);
    position:sticky;top:0;background:var(--bg);z-index:8;backdrop-filter:blur(8px)}
  .tb-group{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  .tb-group .glabel{font-size:10.5px;color:var(--muted-soft);
    text-transform:uppercase;letter-spacing:.08em;font-weight:600;margin-right:2px}
  .pill{padding:5px 12px;border:1px solid var(--border);border-radius:999px;
    background:var(--chip);color:var(--fg);cursor:pointer;font-size:12px;
    transition:all .15s ease}
  .pill:hover{border-color:var(--accent)}
  .pill[aria-pressed=true]{background:var(--accent);border-color:var(--accent);
    color:#0b0d10}
  html[data-theme=light] .pill[aria-pressed=true]{color:#fff}
  .seg{display:inline-flex;border:1px solid var(--border);border-radius:8px;
    overflow:hidden;background:var(--panel)}
  .seg button{background:transparent;color:var(--muted);border:0;padding:6px 12px;
    font-size:12px;cursor:pointer;transition:all .15s ease;font-weight:500}
  .seg button+button{border-left:1px solid var(--border)}
  .seg button:hover{color:var(--fg)}
  .seg button[aria-pressed=true]{background:var(--accent);color:#0b0d10}
  html[data-theme=light] .seg button[aria-pressed=true]{color:#fff}
  .check{display:inline-flex;gap:7px;align-items:center;font-size:12px;
    color:var(--fg);cursor:pointer;user-select:none;padding:5px 10px;
    border:1px solid var(--border);border-radius:8px;background:var(--panel);
    transition:all .15s ease}
  .check:hover{border-color:var(--accent)}
  .check input{accent-color:var(--accent);margin:0}
  .check .pct{color:var(--accent);font-weight:600;margin-left:2px}
  .field{display:inline-flex;gap:6px;align-items:center;font-size:11.5px;
    color:var(--muted)}
  .field input,.field select{background:var(--panel);border:1px solid var(--border);
    color:var(--fg);border-radius:var(--radius-sm);padding:5px 8px;
    font-size:12px;font-family:inherit;width:70px}
  .field select{width:auto;min-width:142px}
  .field input[type=text]{width:180px}
  .field input:focus{outline:none;border-color:var(--accent)}
  .toolbar .spacer{flex:1}
  .count{font-size:12px;color:var(--muted)}

  /* ── Table ─────────────────────────────────────────────────────────────── */
  .wrap{padding:0 0 160px}
  @media (max-width:900px){ .wrap{overflow-x:auto} }
  table.grid{border-collapse:separate;border-spacing:0;width:100%;min-width:900px}
  table.grid thead th{position:sticky;top:var(--thead-top,0);background:var(--bg);
    border-bottom:1px solid var(--border);padding:11px 14px;text-align:right;
    font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;
    font-weight:600;white-space:nowrap;cursor:pointer;user-select:none;z-index:4}
  table.grid thead th.l{text-align:left}
  table.grid thead th:hover{color:var(--accent)}
  table.grid thead th .arr{color:var(--accent);font-size:9px;margin-left:3px}
  table.grid tbody td{padding:12px 14px;text-align:right;
    border-bottom:1px solid var(--border-soft);white-space:nowrap;
    font-variant-numeric:tabular-nums}
  table.grid tbody td.l{text-align:left}
  table.grid tbody tr{cursor:pointer;transition:background .12s ease}
  table.grid tbody tr:hover{background:var(--row-hover)}
  .plan-cell{position:sticky;left:0;background:var(--bg);font-weight:600;z-index:3}
  table.grid tbody tr:hover .plan-cell{background:var(--row-hover)}
  .plan-cell .pn{color:var(--fg-strong);font-size:13.5px}
  .plan-cell .fm{display:block;font-size:10.5px;color:var(--muted);
    font-weight:400;text-transform:uppercase;letter-spacing:.04em;margin-top:2px}
  .price{font-family:"IBM Plex Mono",monospace;font-weight:500}
  .price.best{color:var(--price);font-weight:600}
  .save{color:var(--good);font-weight:600;font-size:12.5px}
  .save .bar{display:inline-block;width:24px;height:3px;border-radius:2px;
    background:var(--good);vertical-align:middle;margin-right:5px;opacity:.7}
  .muted{color:var(--muted)}
  .inr{color:var(--muted);font-size:.85em;display:block;margin-top:2px;
    font-family:"IBM Plex Mono",monospace}
  .empty{padding:60px;text-align:center;color:var(--muted)}
  .gst-mark{color:var(--warn);font-size:9.5px;margin-left:4px;font-weight:600;
    letter-spacing:.04em;vertical-align:super}

  /* compare drawer */
  #drawer{position:fixed;left:0;right:0;bottom:0;background:var(--panel);
    border-top:2px solid var(--accent);max-height:50vh;overflow:auto;
    transform:translateY(105%);transition:transform .25s cubic-bezier(.2,.8,.2,1);
    z-index:20;padding:18px 28px;box-shadow:var(--shadow-lg)}
  #drawer.open{transform:translateY(0)}
  #drawer .head{display:flex;align-items:center;gap:12px;margin-bottom:12px}
  #drawer h3{margin:0;font-size:15px;font-weight:600}
  .cmp{border-collapse:collapse;width:100%;min-width:520px}
  .cmp th,.cmp td{padding:8px 14px;border-bottom:1px solid var(--border-soft);
    text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
  .cmp th.l,.cmp td.l{text-align:left;color:var(--muted);font-weight:500}
  .cmp .delta{font-size:11px;margin-left:6px;font-family:"IBM Plex Mono",monospace}
  .delta.up{color:var(--bad)}.delta.down{color:var(--good)}
  .cmp th{font-weight:600;color:var(--fg-strong)}

  /* detail modal */
  #modal{position:fixed;inset:0;background:rgba(8,10,14,.6);display:none;
    align-items:flex-start;justify-content:center;z-index:30;padding:48px 20px;
    overflow:auto;backdrop-filter:blur(4px)}
  #modal.open{display:flex}
  .sheet{background:var(--panel);border:1px solid var(--border);
    border-radius:14px;max-width:820px;width:100%;padding:28px 32px;
    box-shadow:var(--shadow-lg);max-height:calc(100vh - 48px);overflow:auto}
  .sheet .top{display:flex;justify-content:space-between;align-items:flex-start;
    gap:12px;margin-bottom:14px}
  .sheet h2{margin:0;font-size:22px;font-weight:600;letter-spacing:-.01em}
  .sheet h2 a{color:var(--fg-strong);border-bottom:1px solid transparent;
    transition:border-color .15s}
  .sheet h2 a:hover{border-color:var(--accent);text-decoration:none}
  .sheet .specs{color:var(--muted);font-size:13px;margin:2px 0 18px;
    font-family:"IBM Plex Mono",monospace}
  .sheet .specs b{color:var(--fg);font-weight:500}
  .sheet h4{margin:22px 0 8px;font-size:11px;text-transform:uppercase;
    letter-spacing:.08em;color:var(--muted);font-weight:600}
  .sheet table{min-width:auto;margin:6px 0;width:100%;border-collapse:collapse}
  .sheet td,.sheet th{padding:8px 12px;font-size:13px;text-align:right;
    border-bottom:1px solid var(--border-soft);font-variant-numeric:tabular-nums}
  .sheet th.l,.sheet td.l{text-align:left;color:var(--muted);font-weight:500}
  .sheet th{font-weight:600;color:var(--fg-strong)}
  .pp{font-size:13px;margin:4px 0;line-height:1.7}
  .pp b{color:var(--fg);font-weight:600;margin-right:4px}
  .tag{display:inline-block;background:var(--chip);border:1px solid var(--border);
    border-radius:6px;padding:2px 8px;font-size:11.5px;margin:2px 4px 2px 0;
    transition:all .12s}
  .tag.def{border-color:var(--good);color:var(--good);background:rgba(52,211,153,.08)}
  .tag.paid{border-color:var(--accent);color:var(--accent);background:var(--accent-soft)}
  .tag code{background:transparent;color:inherit;padding:0;margin-left:4px;
    font-family:"IBM Plex Mono",monospace;font-size:11px}
  code{background:var(--chip);padding:2px 6px;border-radius:4px;
    font-family:"IBM Plex Mono",monospace;font-size:12px}

  /* configurator */
  .cfg-bar{display:flex;flex-wrap:wrap;gap:10px 16px;align-items:center;
    margin:10px 0 14px}
  .cfg-bar select,.cfg-grid select{background:var(--panel2);color:var(--fg);
    border:1px solid var(--border);border-radius:var(--radius-sm);
    padding:6px 10px;font:inherit;font-size:13px}
  .cfg-grid{display:grid;grid-template-columns:130px 1fr;gap:8px 14px;
    align-items:center;margin:8px 0 4px}
  .cfg-grid label{color:var(--muted);font-size:12px;text-align:right}
  .cfg-grid select{width:100%}
  .osum{margin-top:16px;border-top:1px solid var(--border);padding-top:14px}
  .osum .ln{display:flex;justify-content:space-between;font-size:13px;
    padding:4px 0;align-items:baseline;gap:12px}
  .osum .ln.muted{color:var(--muted);font-size:12px}
  .osum .ln.tot{font-weight:700;font-size:15px;border-top:1px solid var(--border);
    margin-top:8px;padding-top:8px}
  .osum .ln.tot .v{color:var(--price);font-family:"IBM Plex Mono",monospace;
    font-size:18px}
  .osum .chg .v{color:var(--good);font-family:"IBM Plex Mono",monospace}
  .osum .chg.up .v{color:var(--bad)}
  .osum .sel{font-size:12px;color:var(--muted);margin-top:10px;line-height:1.65;
    padding:8px 10px;background:var(--bg-elev);border-radius:var(--radius-sm)}
  .osum .sel b{color:var(--fg)}
  .breakdown{font-size:11px;color:var(--muted);margin-top:4px;
    font-family:"IBM Plex Mono",monospace}

  /* ── Pricing-model chip ──────────────────────────────────────────────────── */
  .mchip{display:inline-block;margin-left:8px;font-size:9.5px;font-weight:600;
    letter-spacing:.05em;text-transform:uppercase;padding:1px 6px;border-radius:999px;
    vertical-align:middle;line-height:1.6;font-family:"IBM Plex Mono",monospace}
  .mchip.setup{color:var(--accent);background:var(--accent-soft)}
  .mchip.metered{color:var(--accent2);background:rgba(45,212,191,.12)}
  html[data-theme=light] .mchip.metered{background:rgba(13,148,136,.10)}
  .pn .mchip{margin-left:6px}

  /* ── Add-on checklist (Dedicated) ────────────────────────────────────────── */
  .chk-grid{display:flex;flex-direction:column;gap:1px;margin:4px 0 2px;
    max-height:320px;overflow:auto}
  .chk-cat{font-size:10px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;
    color:var(--muted-soft);margin:14px 0 4px;padding-bottom:3px;
    border-bottom:1px solid var(--border-soft)}
  .chk-cat:first-child{margin-top:2px}
  .chk{display:flex;align-items:center;gap:10px;padding:6px 8px;border-radius:var(--radius-sm);
    cursor:pointer;transition:background .12s ease}
  .chk:hover{background:var(--row-hover)}
  .chk input{margin:0;accent-color:var(--accent);width:15px;height:15px;flex:none;cursor:pointer}
  .chk-l{flex:1;font-size:13px;color:var(--fg)}
  .chk-p{font-size:11.5px;color:var(--muted);font-family:"IBM Plex Mono",monospace;
    font-variant-numeric:tabular-nums;white-space:nowrap}
  .chk-setup{color:var(--muted-soft)}
  .chk input:checked~.chk-l{color:var(--fg-strong);font-weight:500}

  /* ── Founder Managed Track ──────────────────────────────────────────────── */
  .managed-track{margin-top:24px;border-top:1px solid var(--border);padding-top:2px}
  .managed-note{font-size:11.5px;color:var(--muted);line-height:1.6;padding:10px 12px;
    margin:8px 0 12px;background:var(--bg-elev);border-radius:var(--radius-sm);
    border-left:2px solid var(--accent)}
  .managed-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;
    align-items:stretch}
  .managed-card{display:flex;position:relative;min-width:0;padding:12px;
    border:1px solid var(--border);border-radius:var(--radius-sm);cursor:pointer;
    background:var(--panel2);transition:border-color .12s,background .12s}
  .managed-card:hover{border-color:var(--accent);background:var(--row-hover)}
  .managed-card.selected{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent-soft) inset}
  .managed-card input{position:absolute;opacity:0;pointer-events:none}
  .managed-card-body{display:flex;flex-direction:column;gap:6px;width:100%}
  .managed-card-top{display:flex;justify-content:space-between;gap:8px;align-items:flex-start}
  .managed-card-name{font-weight:600;color:var(--fg-strong);font-size:13px}
  .managed-card-price{font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--accent);
    white-space:nowrap;text-align:right}
  .managed-card-price small{display:block;color:var(--muted);font-family:inherit;font-size:10px;
    margin-top:2px}
  .managed-card-time{font-size:11.5px;color:var(--accent2);font-weight:600}
  .managed-card ul{margin:2px 0 0;padding-left:16px;color:var(--muted);font-size:11px;line-height:1.5}
  .managed-card li+li{margin-top:2px}
  .managed-card-status{color:var(--warn);font-size:10px;line-height:1.4}
  .managed-controls{display:flex;flex-wrap:wrap;gap:10px 16px;align-items:center;margin:12px 0 2px}
  .managed-controls label{font-size:12px;color:var(--muted)}
  .managed-controls input{width:68px;margin-left:6px;background:var(--panel2);color:var(--fg);
    border:1px solid var(--border);border-radius:var(--radius-sm);padding:6px 8px;font:inherit}
  .managed-summary{margin-top:12px;border-top:1px solid var(--border);padding-top:10px}
  .managed-summary .ln{display:flex;justify-content:space-between;gap:12px;align-items:baseline;
    padding:4px 0;font-size:13px}
  .managed-summary .ln.muted{color:var(--muted);font-size:11.5px}
  .managed-summary .ln.tot{font-weight:700;font-size:15px;border-top:1px solid var(--border);
    margin-top:6px;padding-top:8px}
  .managed-summary .ln.tot .v{color:var(--price);font-family:"IBM Plex Mono",monospace;font-size:17px}
  .managed-summary .pending{color:var(--warn);font-size:11px;line-height:1.5;margin-top:8px}
  .managed-review{margin-top:12px;color:var(--muted);font-size:11px;line-height:1.55}
  .managed-review summary{cursor:pointer;color:var(--muted-soft)}
  .managed-review ul{margin:6px 0 0;padding-left:18px}
  .managed-review li+li{margin-top:3px}

  /* ── Proposal workspace ─────────────────────────────────────────────────── */
  .proposal-launch{border-color:var(--accent2);color:var(--accent2);font-weight:600}
  .proposal-launch:hover{border-color:var(--accent);color:var(--accent)}
  .proposal-intro{color:var(--muted);font-size:12px;line-height:1.6;margin:0 0 16px}
  .proposal-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px 16px}
  .proposal-field{display:flex;flex-direction:column;gap:5px;color:var(--muted);font-size:11px}
  .proposal-field.wide{grid-column:1/-1}
  .proposal-field input,.proposal-field select,.proposal-field textarea{width:100%;background:var(--panel2);color:var(--fg);border:1px solid var(--border);border-radius:var(--radius-sm);padding:8px 9px;font:inherit;font-size:13px}
  .proposal-field textarea{min-height:72px;resize:vertical;line-height:1.5}
  .proposal-controls{display:flex;flex-wrap:wrap;gap:8px;margin:12px -8px 16px;align-items:center;
    position:sticky;top:-28px;z-index:4;padding:12px 8px;background:color-mix(in srgb,var(--panel) 96%,transparent);
    border-bottom:1px solid var(--border);box-shadow:0 8px 14px rgba(0,0,0,.12)}
  .proposal-controls .primary{background:var(--accent);color:#0b0d10;border-color:var(--accent);font-weight:700}
  html[data-theme=light] .proposal-controls .primary{color:#fff}
  .proposal-controls .secondary{border-color:var(--accent2);color:var(--accent2)}
  .proposal-hint{font-size:11px;color:var(--muted);margin-top:8px;line-height:1.55}
  .proposal-status{font-size:12px;color:var(--muted);padding:8px 10px;background:var(--bg-elev);border-left:2px solid var(--accent2);border-radius:var(--radius-sm);margin-top:12px;white-space:pre-wrap}
  .proposal-status.bad{border-color:var(--bad);color:var(--bad)}
  .proposal-status.good{border-color:var(--good);color:var(--good)}
  .proposal-preview{margin-top:18px;border-top:1px solid var(--border);padding-top:16px}
  .proposal-document{background:var(--bg-elev);border:1px solid var(--border);border-radius:var(--radius-sm);padding:18px 20px}
  .proposal-document header{border:0;padding:0 0 10px;margin:0 0 12px;display:block}
  .proposal-document h2{font-size:19px;margin:0;color:var(--fg-strong)}
  .proposal-document h3{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin:18px 0 7px}
  .proposal-document p{margin:6px 0;color:var(--fg);line-height:1.6}
  .proposal-document .proposal-subtitle{font-family:"IBM Plex Mono",monospace;color:var(--muted);font-size:11px}
  .proposal-table{width:100%;border-collapse:collapse;margin:6px 0}
  .proposal-table th,.proposal-table td{padding:7px 8px;border-bottom:1px solid var(--border-soft);font-size:12px;text-align:left;vertical-align:top}
  .proposal-table th{color:var(--muted);font-weight:500;width:42%}
  .proposal-table td{color:var(--fg-strong);font-family:"IBM Plex Mono",monospace}
  .proposal-document ul{margin:6px 0;padding-left:20px;color:var(--fg)}
  .proposal-document li+li{margin-top:4px}
  .proposal-callout{padding:8px 10px;margin:6px 0;border-left:2px solid var(--accent2);background:var(--panel);color:var(--muted);font-size:12px}
  .proposal-callout.warning{border-color:var(--warn);color:var(--warn)}
  .proposal-summary{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
  .proposal-chip{font-size:10.5px;color:var(--muted);border:1px solid var(--border);border-radius:999px;padding:4px 8px}
  .taxonomy-warning{margin:14px 28px 0;padding:10px 12px;border:1px solid color-mix(in srgb,var(--warn) 55%,var(--border));
    border-left:3px solid var(--warn);border-radius:var(--radius-sm);background:color-mix(in srgb,var(--warn) 8%,var(--panel));
    color:var(--muted);font-size:12px;line-height:1.55}
  .taxonomy-warning b{color:var(--warn)}
  @media (max-width:760px){.proposal-grid{grid-template-columns:1fr}.proposal-field.wide{grid-column:auto}}

  /* ── Calculator note ─────────────────────────────────────────────────────── */
  .note{font-size:11.5px;color:var(--muted);margin-top:12px;line-height:1.6;
    padding:9px 11px;background:var(--bg-elev);border-radius:var(--radius-sm);
    border-left:2px solid var(--accent2)}

  /* footer */
  footer{padding:22px 28px;color:var(--muted);font-size:12px;
    border-top:1px solid var(--border-soft);line-height:1.65}
  footer .row{display:flex;flex-wrap:wrap;gap:6px 14px;align-items:baseline}
  .kbd{display:inline-block;padding:1px 6px;background:var(--chip);
    border:1px solid var(--border);border-radius:4px;font-size:10.5px;
    font-family:"IBM Plex Mono",monospace}

  /* mobile */
  @media (max-width:760px){
    header.app{padding:14px 18px;grid-template-columns:1fr;gap:8px}
    .hero,.toolbar,.wrap,footer{padding-left:18px;padding-right:18px}
    .taxonomy-warning{margin-left:18px;margin-right:18px}
    .head-meta{justify-content:flex-start}
    .toolbar{top:0;gap:10px 14px}
    table.grid thead th{padding:9px 10px;font-size:10px;top:0}
    table.grid tbody td{padding:10px 10px;font-size:13px}
    .sheet{padding:20px}
    .cfg-grid{grid-template-columns:1fr}
    .cfg-grid label{text-align:left}
    .managed-grid{grid-template-columns:1fr}
  }
</style>
</head>
<body>
<header class="app">
  <div class="brand">
    <div class="logo">Contabo<span class="dot">.</span></div>
    <div class="sub">Pricing Report · ${(genAt || '').slice(0,10)}</div>
  </div>
  <div></div>
  <div class="head-meta">
    <span class="badge" id="reconBadge"></span>
    <span class="badge" id="fxBadge" title="EUR→INR live rate"></span>
    <button class="iconbtn" id="themeBtn" aria-label="Toggle theme">◐</button>
  </div>
</header>

${legacyTaxonomyFamilies.length ? `<aside class="taxonomy-warning" role="status"><b>Legacy taxonomy dataset — not release-current.</b> This snapshot still flattens products as Cloud VPS / Cloud VDS. The accepted current contract is Core VPS (SSD only), Performance VPS (NVMe only), and Max Performance VPS (legacy VDS alias). Do not present these family labels as current until the canonical migration gate passes.</aside>` : ''}

<section class="hero" id="hero" aria-label="Best monthly prices per family">
  <h2>Best 12-mo</h2>
  <div class="pill-row" id="bestRow"></div>
</section>

<div class="toolbar" role="toolbar" aria-label="Filters and display options">
  <div class="tb-group">
    <span class="glabel">Family</span>
    <div class="pill-row" id="famPills"></div>
  </div>
  <div class="tb-group">
    <span class="glabel">Currency</span>
    <span class="seg" id="curTog" role="group" aria-label="Currency">
      <button data-cur="EUR" aria-pressed="false">EUR</button>
      <button data-cur="INR" aria-pressed="false">INR</button>
      <button data-cur="BOTH" aria-pressed="true">Both</button>
    </span>
  </div>
  <div class="tb-group" id="gstGroup">
    <span class="glabel">India</span>
    <label class="check" title="Provider/vendor tax charged on the Contabo invoice; independent of Securiace registration">
      <input type="checkbox" id="gstToggle">
      <span>Provider tax <span class="pct">+18%</span></span>
    </label>
    <label class="check" title="Exclude provider tax from economic landed cost only with verified input-tax-credit evidence">
      <input type="checkbox" id="providerTaxRecoverable">
      <span>ITC recoverable</span>
    </label>
    <label class="check" title="Securiace customer output GST; requires verified registration and matching WHMCS tax settings">
      <input type="checkbox" id="outputTaxToggle">
      <span>Output GST <span class="pct">+18%</span></span>
    </label>
    <label class="field" title="Card forex markup applied on top of mid-market rate">
      FX markup
      <input type="number" id="fxMarkup" min="0" max="100" step="0.1" value="3.5"> %
    </label>
    <label class="field" title="Your cost-plus margin: provider tax treatment and acquisition buffers are resolved first; verified Securiace output GST is applied afterward">
      Owner markup
      <input type="number" id="ownerMarkup" min="0" max="100" step="0.1" value="0"> %
    </label>
    <label class="field" title="Choose which service charges receive your owner markup">
      Scope
      <select id="ownerMarkupScope" aria-label="Owner markup scope">
        <option value="provider_only">Provider charges</option>
        <option value="provider_and_managed">Provider + managed</option>
      </select>
    </label>
  </div>
  <div class="tb-group">
    <span class="glabel">Period</span>
    <span class="seg" id="periodTog" role="group" aria-label="Period">
      <button data-period="1" aria-pressed="false">1 mo</button>
      <button data-period="6" aria-pressed="true">6 mo</button>
      <button data-period="12" aria-pressed="false">12 mo</button>
      <button data-period="all" aria-pressed="false">All</button>
    </span>
    <label class="check" title="Reveal 3-month tier that is hidden from Contabo's UI">
      <input type="checkbox" id="showHidden"> 3 mo
    </label>
  </div>
  <div class="tb-group">
    <span class="glabel">Filter</span>
    <label class="field">vCPU ≥ <input type="number" id="minCpu" min="0" step="1" placeholder="0"></label>
    <label class="field">RAM ≥ <input type="number" id="minRam" min="0" step="1" placeholder="0"></label>
    <label class="field"><input type="text" id="search" placeholder="search plan…"></label>
  </div>
  <span class="spacer"></span>
  <button class="iconbtn proposal-launch" id="proposalBtn" type="button">Create proposal ↗</button>
  <span class="count" id="count"></span>
</div>

<div class="wrap">
  <table class="grid" id="grid">
    <thead><tr id="head"></tr></thead>
    <tbody id="body"></tbody>
  </table>
  <div class="empty" id="empty" style="display:none">No plans match the current filters.</div>
</div>

<div id="drawer" aria-label="Plan comparison">
  <div class="head">
    <h3 id="cmpTitle">Compare</h3>
    <span class="spacer" style="flex:1"></span>
    <button class="iconbtn" onclick="clearCompare()">Clear ✕</button>
  </div>
  <div style="overflow-x:auto"><table class="cmp" id="cmpTable"></table></div>
</div>

<div id="modal" onclick="if(event.target===this)closeModal()" role="dialog" aria-modal="true">
  <div class="sheet" id="sheet"></div>
</div>

<footer>
  <div class="row">
    <span>Prices in EUR, listed by Contabo <b>excl. VAT/GST</b>. Base config: EU region, Ubuntu OS, 1 IPv4.</span>
  </div>
  <div class="row" style="margin-top:6px">
    <span>Source: <a href="https://contabo.com" rel="noopener">contabo.com</a></span>
    <span>·</span>
    <span>Also see <a href="PRICES.md">PRICES.md</a></span>
    <span>·</span>
    <span id="fxLine"></span>
    <span>·</span>
    <span>Keys: <span class="kbd">/</span> search, <span class="kbd">G</span> GST, <span class="kbd">T</span> theme, <span class="kbd">Esc</span> close</span>
  </div>
  <div class="row" style="margin-top:6px">
    <span class="muted" id="gstNote" hidden></span>
    <span class="muted" id="inrNote" hidden>INR figures are indicative estimates (live ECB mid-market + FX markup + owner markup).</span>
  </div>
</footer>

<script type="application/json" id="contabo-data">
${dataJson}
</script>
<script>
${PROPOSAL_MODEL_SOURCE}
</script>
<script>
'use strict';
const DATA = JSON.parse(document.getElementById('contabo-data').textContent);
const ROWS = DATA.rows;
const ADDONS = DATA.planAddons || {};
const CFG = DATA.planConfig || {};
const META = DATA.planMeta || {};
const EXTRAS = DATA.planExtras || {};
const MANAGED = DATA.managedServices || {};
const PROPOSAL = DATA.proposal || {};
const PROPOSAL_MODEL = globalThis.ContaboProposalModel;
const MANAGED_PLANS = Array.isArray(MANAGED.plans) ? MANAGED.plans : [];
const MANAGED_FAMILIES = new Set(Array.isArray(MANAGED.eligible_families) ? MANAGED.eligible_families : []);
const MANAGED_STORAGE_KEY = 'contabo_managed_track_v1';
const FAMILIES = [...new Set(ROWS.map(r => r.canonical_family || r.family))];
// Family → billing shape. Object Storage is capacity-priced; Dedicated adds a
// one-off setup; the rest are plain recurring.
const FAM_STORAGE = 'Object Storage', FAM_DEDICATED = 'Dedicated Server';
const isStorageFam  = f => f === FAM_STORAGE;
const isComputeFam  = f => f !== FAM_STORAGE;   // VPS/VDS/Storage-VPS/Dedicated all have cpu/ram
const MODEL_LABEL = { fixed: '', fixed_plus_setup: '+setup', metered_capacity: 'metered' };
const ADDON_CATS = ['Control Panel', 'RAM', 'Storage', 'GPU', 'Networking', 'Security', 'Management', 'Software', 'Misc'];
const FX_META = DATA.fx || {};
const PROVIDER_PRICES_INCLUDE_TAX = FX_META.providerPricesIncludeTax === true;
const PROVIDER_TAX_RATE = Number(FX_META.providerTaxRate || 0.18);
const OUTPUT_TAX_RATE = Number(FX_META.outputTaxRate || 0.18);
const GST_REGISTRATION_VERIFIED = FX_META.outputTaxRegistrationVerified === true;
const TAX_MODE_SOURCE = FX_META.outputTaxSource || 'default disabled: registration evidence not verified';

// Group rows → plans { slug -> {meta, periods:{m->row}} }
const PLANS = {};
for (const r of ROWS) {
  const m = META[r.plan_slug] || {};
  const p = PLANS[r.plan_slug] ??= {
    slug:r.plan_slug, family:m.canonical_family || r.canonical_family || r.family,
    legacyFamily:m.legacy_family || r.legacy_family || r.family,
    storagePolicy:m.storage_policy || r.storage_policy || 'not_applicable',
    name:r.product_name, url:r.product_url,
    rank:r.plan_rank, frank:r.plan_family_rank,
    // Prefer the real per-family specs from the dataset; the view_model only
    // carries VPS-shaped keys (all null for Object Storage).
    specs:(m.specs && Object.keys(m.specs).length) ? m.specs : r.specs,
    model:m.pricing_model || 'fixed', avail:m.availability || null,
    extras:EXTRAS[r.plan_slug] || null,
    options:r.options_summary || {}, periods:{}
  };
  p.periods[r.period_months] = r;
}
// Object Storage headline unit: €/TB·mo (base price is the 0.25 TB tier).
function perTb(p){
  const s=p.specs||{};
  if(s.price_per_tb_eur!=null) return Number(s.price_per_tb_eur);
  const m=p.periods[1]?.effective_monthly, cap=s.capacity_tb;
  return (m!=null&&cap)? m/cap : null;
}
function modelChip(p){
  const t=MODEL_LABEL[p.model]; if(!t) return '';
  const cls = p.model==='metered_capacity' ? 'mchip metered' : 'mchip setup';
  return '<span class="'+cls+'" title="'+(p.model==='metered_capacity'?'Capacity-priced (per TB / month)':'One-time setup fee applies')+'">'+t+'</span>';
}
const PLAN_LIST = Object.values(PLANS);

const lsGet=(k)=>{ try{ return localStorage.getItem(k); }catch{ return null; } };
const lsSet=(k,v)=>{ try{ localStorage.setItem(k,v); }catch{} };

const defaultFxMarkup = Number(FX_META.fxMarkupDefault ?? 0.035);
const clampFxMarkup = raw => {
  const value = Number(raw);
  const fallback = Number.isFinite(defaultFxMarkup) ? defaultFxMarkup : 0.035;
  return Math.max(0, Math.min(1, Number.isFinite(value) ? value : fallback));
};
const storedFxMarkup = lsGet('contabo_fx_markup');
const storedOwnerMarkup = lsGet('contabo_owner_markup_pct');
const storedOwnerScope = lsGet('contabo_owner_markup_scope');
const storedProviderTaxCharged = lsGet('contabo_provider_tax_charged')==='1' || lsGet('contabo_gst')==='1';

const clampOwnerMarkup = raw => {
  const value = Number(raw);
  return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
};
const ownerScope = raw => raw === 'provider_and_managed' ? raw : 'provider_only';

let FX = {
  rate: (FX_META && FX_META.eurInr) || null,
  source: FX_META.source || '',
  rateDate: FX_META.rateDate || null,
  at: FX_META.at || null,
  // Storage is a fraction (0.035 == 3.5%), while the visible input is a
  // percentage. The explicit 0–100% UI boundary is identical to the pricing
  // model boundary; a visible 45% therefore calculates as 45%, never 15%.
  markup: clampFxMarkup(storedFxMarkup === null ? defaultFxMarkup : storedFxMarkup)
};

function loadManagedSelections(){
  try{
    const raw=JSON.parse(lsGet(MANAGED_STORAGE_KEY)||'{}');
    if(!raw || typeof raw!=='object' || Array.isArray(raw)) return {};
    const out={};
    for(const [slug, selection] of Object.entries(raw)){
      const plan=MANAGED_PLANS.find(p=>p.id===selection?.planId);
      if(!plan || !PLANS[slug] ||
        (!MANAGED_FAMILIES.has(PLANS[slug].family) && !MANAGED_FAMILIES.has(PLANS[slug].legacyFamily))) continue;
      const quantity=Math.max(1,Math.min(99,Math.round(Number(selection.quantity)||1)));
      out[slug]={planId:plan.id,quantity};
    }
    return out;
  }catch{ return {}; }
}

let state = {
  fam:'All', period:(lsGet('contabo_period')||'6'), showHidden:false,
  minCpu:0, minRam:0, q:'', sortKey:'frank', sortDir:1,
  compare:new Set(),
  cur:(lsGet('contabo_cur')||'BOTH'),
  // Historical key retained for migration; state.gst now means provider tax
  // charged, never Securiace output GST.
  gst:storedProviderTaxCharged,
  providerTaxRecoverable:storedProviderTaxCharged && lsGet('contabo_provider_tax_recoverable')==='1',
  outputTax:GST_REGISTRATION_VERIFIED && lsGet('contabo_output_tax')==='1',
  ownerMarkup: clampOwnerMarkup(storedOwnerMarkup === null ? 0 : Number(storedOwnerMarkup) / 100),
  ownerMarkupScope: ownerScope(storedOwnerScope),
  managed:loadManagedSelections(),
};

// ── Money helpers ───────────────────────────────────────────────────────────
const inrNF = new Intl.NumberFormat('en-IN',{style:'currency',currency:'INR',maximumFractionDigits:0});
const eurNF = new Intl.NumberFormat('en-US',{style:'currency',currency:'EUR',minimumFractionDigits:2,maximumFractionDigits:2});

function ownerMul(){ return 1+(state.ownerMarkup||0); }
function applyOwner(v, includeManaged=false){
  if(v==null) return v;
  if(includeManaged && state.ownerMarkupScope!=='provider_and_managed') return Number(v);
  return Number(v)*ownerMul();
}
function reportPricing(currency){
  return {currency,provider_prices_include_tax:PROVIDER_PRICES_INCLUDE_TAX,
    provider_tax_charged:state.gst,provider_tax_recoverable:state.providerTaxRecoverable,
    provider_tax_rate:PROVIDER_TAX_RATE,provider_tax_source:'report operator selection',
    output_tax_enabled:state.outputTax,
    output_tax_registration_verified:GST_REGISTRATION_VERIFIED,
    output_tax_rate:OUTPUT_TAX_RATE,output_tax_source:TAX_MODE_SOURCE,
    fx_rate:FX.rate,fx_markup:FX.markup,
    owner_markup:state.ownerMarkup,owner_markup_scope:state.ownerMarkupScope,
    fx_source:FX.source,fx_rate_date:FX.rateDate};
}
function reportPrice(v,currency){
  if(v==null||v==='') return v;
  return PROPOSAL_MODEL.calculateQuote({provider_monthly_eur:Number(v),period_months:1,
    pricing:reportPricing(currency)}).display.monthly;
}
// Compatibility name for existing render paths: this now applies the entire
// canonical seller-price pipeline, not tax alone.
function applyGst(v){ return reportPrice(v,'EUR'); }
function fmtEur(v){
  if(v==null||v==='') return '—';
  return eurNF.format(reportPrice(v,'EUR'));
}
function fmtInr(v){
  if(!FX.rate || v==null || v==='') return '';
  return inrNF.format(Math.round(reportPrice(v,'INR')));
}
function money(v){
  if(v==null||v==='') return '—';
  const e=fmtEur(v);
  if(state.cur==='EUR') return e;
  const r=fmtInr(v);
  if(state.cur==='INR') return r||e;
  return r ? '<span class="price'+(state.gst?'':'')+'">'+e+'</span><span class="inr">≈ '+r+'</span>'
           : '<span class="price">'+e+'</span>';
}
const eur = money;
const num = v => (v==null)?'—':v;
const r2  = x => Math.round((Number(x)+Number.EPSILON)*100)/100;
const esc = s => String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const sgn = v => {
  const f = eurNF.format(Math.abs(reportPrice(v,'EUR')));
  return (v<0?'−':'+') + f;
};
const managedEligible = p => !!p && (MANAGED_FAMILIES.has(p.family) || MANAGED_FAMILIES.has(p.legacyFamily));
const managedPlan = id => MANAGED_PLANS.find(p=>p.id===id) || null;
// Managed services are already canonical INR amounts. managedQuote applies
// the optional owner markup once and returns seller values in minor units, so
// this formatter must never add markup again while rendering those values.
const managedMinor = minor => inrNF.format(Math.round(Number(minor||0)/100));
const managedTime = minutes => {
  const hours=Number(minutes||0)/60;
  if(Number.isInteger(hours)) return hours+' hour'+(hours===1?'':'s')+'/month';
  return Math.round(minutes||0)+' min/month';
};
function managedQuote(plan, quantity){
  const qty=Math.max(1,Math.min(99,Math.round(Number(quantity)||1)));
  const ex=Math.round(Number(plan.annual_price_minor||0)*qty);
  const sellerPreTax=Math.round(applyOwner(ex/100, true)*100);
  const outputTax=state.outputTax ? Math.round(sellerPreTax*OUTPUT_TAX_RATE) : 0;
  const sellerTotal=sellerPreTax+outputTax;
  return {quantity:qty,ex,sellerPreTax,outputTax,total:ex,sellerTotal,
    monthly:Math.round(sellerTotal/Number(MANAGED.billing_term_months||12)),
    founderMinutes:Number(plan.founder_time?.minutes_per_month||0)*qty};
}
function persistManagedSelections(){
  lsSet(MANAGED_STORAGE_KEY,JSON.stringify(state.managed));
}
const savePct = p => {
  const a=p.periods[1]?.effective_monthly, b=p.periods[12]?.effective_monthly;
  if(!a||!b) return null;
  return Math.round((1-b/a)*100);
};

// ── Family pills ────────────────────────────────────────────────────────────
const famPills = document.getElementById('famPills');
['All',...FAMILIES].forEach(f=>{
  const b=document.createElement('button');
  b.className='pill'; b.textContent=f;
  b.setAttribute('aria-pressed', f===state.fam);
  b.onclick=()=>{ state.fam=f;
    [...famPills.children].forEach(c=>c.setAttribute('aria-pressed',c.textContent===f));
    render(); };
  famPills.appendChild(b);
});

// ── Hero: best 12-mo per family ────────────────────────────────────────────
function bestPerFamily(){
  const by={};
  for(const p of PLAN_LIST){
    const m = p.periods[12]?.effective_monthly;
    if(m==null) continue;
    if(!by[p.family] || m < by[p.family].periods[12].effective_monthly) by[p.family]=p;
  }
  return by;
}
function renderHero(){
  const bp = bestPerFamily();
  document.getElementById('bestRow').innerHTML = FAMILIES.map(f=>{
    const p = bp[f]; if(!p) return '';
    const stor = isStorageFam(f);
    const m = stor ? perTb(p) : p.periods[12].effective_monthly;
    const unit = stor ? '/TB·mo' : '/mo';
    const nm = stor ? esc(String(p.specs.region||p.name).replace('Object Storage: ','')) : esc(p.name);
    return '<span class="hero-card" data-slug="'+p.slug+'">'+
      '<span class="lbl">'+esc(f)+'</span>'+
      '<span class="nm">'+nm+'</span>'+
      '<span class="v">'+(m==null?'—':fmtEur(m))+unit+'</span></span>';
  }).join('');
  document.querySelectorAll('#bestRow .hero-card').forEach(c=>{
    c.style.cursor='pointer';
    c.onclick=()=>openModal(c.dataset.slug);
  });
}

// ── Columns (family-aware) ──────────────────────────────────────────────────
const dash = '<span class="muted">—</span>';
const capGb = p => (p.specs.capacity_tb!=null ? Math.round(p.specs.capacity_tb*1000)+' GB' : '—');
const colPlan = {k:'name', t:'Plan', l:true, sort:p=>p.frank,
  v:p=>'<span class="pn">'+esc(p.name)+modelChip(p)+'</span><span class="fm">'+esc(p.family)+'</span>'};
const priceCols = [
  {k:'p1',  t:'1 mo',  v:p=>eur(p.periods[1]?.effective_monthly), sort:p=>p.periods[1]?.effective_monthly??1e9},
  {k:'p3',  t:'3 mo',  hidden:true, v:p=>eur(p.periods[3]?.effective_monthly), sort:p=>p.periods[3]?.effective_monthly??1e9},
  {k:'p6',  t:'6 mo',  v:p=>eur(p.periods[6]?.effective_monthly), sort:p=>p.periods[6]?.effective_monthly??1e9},
  {k:'p12', t:'12 mo', v:p=>{
      const m=p.periods[12]?.effective_monthly;
      return m==null?'—':'<span class="price best">'+(state.cur==='INR'?fmtInr(m):fmtEur(m))+'</span>'+(state.cur==='BOTH'?'<span class="inr">≈ '+fmtInr(m)+'</span>':'');
    }, sort:p=>p.periods[12]?.effective_monthly??1e9},
  {k:'save', t:'Save', v:p=>{const s=savePct(p);return s?'<span class="save"><span class="bar" style="width:'+Math.min(s*0.6,30)+'px"></span>'+s+'%</span>':'—';}, sort:p=>savePct(p)??-1},
];
// Compute families (VPS/VDS/Storage-VPS/Dedicated) share cpu/ram/storage/port.
// Object Storage rows render adaptively in this set (so the "All" view stays clean).
const COLS_COMPUTE = [ colPlan,
  {k:'cpu',  t:'vCPU',    v:p=>isStorageFam(p.family)?dash:num(p.specs.cpu_count), sort:p=>p.specs.cpu_count||0},
  {k:'ram',  t:'RAM',     v:p=>isStorageFam(p.family)?dash:num(p.specs.ram_gb)+' GB', sort:p=>p.specs.ram_gb||0},
  {k:'stor', t:'Storage', sort:p=>isStorageFam(p.family)?(p.specs.capacity_tb||0)*1000:(p.specs.storage_primary_gb||0),
    v:p=>isStorageFam(p.family)
      ? '<span class="mono">'+capGb(p)+'</span> <span class="muted">base</span>'
      : num(p.specs.storage_primary_gb)+' GB <span class="muted">'+(p.specs.storage_primary_type||'')+'</span>'},
  {k:'port', t:'Port',    v:p=>isStorageFam(p.family)?'<span class="good">free egress</span>':num(p.specs.port_speed_mbps)+' Mbps', sort:p=>p.specs.port_speed_mbps||0},
  ...priceCols,
];
// Object-Storage-native columns (shown when that family is selected).
const COLS_STORAGE = [ colPlan,
  {k:'cap', t:'Base',      v:p=>'<span class="mono">'+capGb(p)+'</span>', sort:p=>p.specs.capacity_tb||0},
  {k:'ptb', t:'€ / TB·mo', v:p=>{const t=perTb(p);return t==null?'—':'<span class="price">'+fmtEur(t)+'</span>';}, sort:p=>perTb(p)??1e9},
  {k:'egr', t:'Egress',    v:p=>'<span class="good">free</span>', sort:p=>0},
  {k:'reg', t:'Region',    v:p=>'<span class="muted">'+esc(String(p.specs.region||'').replace('Object Storage: ',''))+'</span>', sort:p=>String(p.specs.region||'')},
  ...priceCols,
];
const SORTBY = {frank:p=>p.frank};
const colSet = () => state.fam===FAM_STORAGE ? COLS_STORAGE : COLS_COMPUTE;
const activeCols = () => colSet().filter(c => !c.hidden || state.showHidden);

const head = document.getElementById('head');
function renderHead(){
  head.innerHTML='<th></th>'+activeCols().map(c=>{
    const active = state.sortKey===c.k;
    const arr = active ? (state.sortDir>0?' ▲':' ▼') : '';
    return '<th class="'+(c.l?'l':'')+'" data-k="'+c.k+'">'+c.t+
      '<span class="arr">'+arr+'</span></th>';
  }).join('');
  head.querySelectorAll('th[data-k]').forEach(th=>{
    th.onclick=()=>{
      const k=th.dataset.k;
      if(state.sortKey===k) state.sortDir*=-1;
      else { state.sortKey=k; state.sortDir=1; }
      render();
    };
  });
}

function filtered(){
  return PLAN_LIST.filter(p=>{
    if(state.fam!=='All' && p.family!==state.fam) return false;
    if(state.minCpu && (p.specs.cpu_count||0) < state.minCpu) return false;
    if(state.minRam && (p.specs.ram_gb||0) < state.minRam) return false;
    if(state.q && !p.name.toLowerCase().includes(state.q.toLowerCase())) return false;
    return true;
  }).sort((a,b)=>{
    const col=colSet().find(c=>c.k===state.sortKey);
    const f = col?col.sort:(SORTBY[state.sortKey]||(x=>0));
    const av=f(a), bv=f(b);
    if(av<bv) return -1*state.sortDir;
    if(av>bv) return  1*state.sortDir;
    return 0;
  });
}

const body=document.getElementById('body');
const emptyEl=document.getElementById('empty');
function render(){
  renderHead();
  const list=filtered();
  const cols=activeCols();
  body.innerHTML = list.map(p=>{
    const checked = state.compare.has(p.slug)?'checked':'';
    return '<tr data-slug="'+p.slug+'">'+
      '<td><input type="checkbox" class="cmpck" data-slug="'+p.slug+'" '+checked+' aria-label="Add to compare"></td>'+
      cols.map((c,i)=>'<td class="'+(c.l?'l':'')+(i===0?' plan-cell':'')+'">'+c.v(p)+'</td>').join('')+
      '</tr>';
  }).join('');
  emptyEl.style.display = list.length?'none':'block';
  document.getElementById('grid').style.display = list.length?'':'none';
  document.getElementById('count').textContent =
    list.length+' / '+PLAN_LIST.length+' plans'+(state.gst?' · provider tax 18% charged':'')+
    (state.outputTax?' · output GST 18%':'')+
    (state.ownerMarkup?' · owner +'+(state.ownerMarkup*100).toFixed(1)+'%':'');
  body.querySelectorAll('tr').forEach(tr=>{
    tr.onclick=e=>{ if(e.target.classList.contains('cmpck')) return;
      openModal(tr.dataset.slug); };
  });
  body.querySelectorAll('.cmpck').forEach(ck=>{
    ck.onclick=e=>{ e.stopPropagation();
      const s=ck.dataset.slug;
      if(ck.checked){ if(state.compare.size>=4){ ck.checked=false;
          alert('Compare up to 4 plans.'); return; } state.compare.add(s); }
      else state.compare.delete(s);
      renderCompare();
    };
  });
  renderHero();
}

// ── Compare drawer ─────────────────────────────────────────────────────────
const drawer=document.getElementById('drawer');
function clearCompare(){ state.compare.clear(); render(); renderCompare(); }
function renderCompare(){
  const slugs=[...state.compare];
  if(slugs.length<2){ drawer.classList.remove('open'); return; }
  drawer.classList.add('open');
  const ps=slugs.map(s=>PLANS[s]);
  document.getElementById('cmpTitle').textContent='Comparing '+ps.length+' plans'+
    (state.gst?' · provider tax charged':'')+(state.outputTax?' · output GST':'');
  const base=ps[0];
  const row=(label,fn,fmt,dir)=>{
    const vals=ps.map(fn);
    return '<tr><td class="l">'+label+'</td>'+vals.map((v,i)=>{
      let d='';
      if(i>0 && typeof v==='number' && typeof vals[0]==='number'){
        const diff=v-vals[0];
        if(Math.abs(diff)>1e-9){
          const cls = (dir==='lowerBetter') ? (diff>0?'up':'down') : (diff>0?'down':'up');
          d=' <span class="delta '+cls+'">'+(diff>0?'+':'')+
            (fmt===eur?sgn(diff).replace(/^[+−]/,(diff>0?'+':'−')):(Math.round(diff*100)/100))+'</span>';
        }
      }
      return '<td>'+(fmt?fmt(v):v)+d+'</td>';
    }).join('')+'</tr>';
  };
  const MODEL_FULL={fixed:'Fixed',fixed_plus_setup:'Fixed + setup',metered_capacity:'Metered capacity'};
  const t=['<tr><th class="l">Spec</th>'+ps.map(p=>'<th>'+esc(p.name)+'</th>').join('')+'</tr>',
    row('Family',p=>p.family),
    row('Billing',p=>MODEL_FULL[p.model]||'Fixed'),
    row('vCPU / cores',p=>p.specs.cpu_count??'—',null,'higherBetter'),
    row('RAM (GB)',p=>p.specs.ram_gb??'—',null,'higherBetter'),
    row('Storage',p=>p.specs.storage_primary_gb!=null
      ? (p.specs.storage_primary_gb+' '+(p.specs.storage_primary_type||''))
      : (p.specs.capacity_tb!=null?(Math.round(p.specs.capacity_tb*1000)+' GB base'):'—')),
    row('€ / TB·mo',p=>{const v=perTb(p);return v!=null?fmtEur(v):'—';}),
    row('Port / egress',p=>isStorageFam(p.family)?'free egress':(p.specs.port_speed_mbps!=null?p.specs.port_speed_mbps+' Mbps':'—')),
    row('1 mo',p=>p.periods[1]?.effective_monthly,eur,'lowerBetter'),
    row('6 mo',p=>p.periods[6]?.effective_monthly,eur,'lowerBetter'),
    row('12 mo',p=>p.periods[12]?.effective_monthly,eur,'lowerBetter'),
    row('Setup (1 mo)',p=>p.periods[1]?.setup_fee,eur,'lowerBetter'),
  ].join('');
  document.getElementById('cmpTable').innerHTML=t;
}

// ── Detail modal ───────────────────────────────────────────────────────────
const modal=document.getElementById('modal');
let modalMode='none';
function closeModal(){ modal.classList.remove('open'); modalMode='none'; }
function tagList(items){
  return items.map(o=>{
    const cls=o.isDefault?'tag def':(o.delta>0?'tag paid':'tag');
    const price=o.delta>0?(' <code>+'+fmtEur(o.delta)+'</code>'):(o.isDefault?' (default)':'');
    return '<span class="'+cls+'">'+esc(o.label)+price+'</span>';
  }).join('');
}

// Calculator state
let MCFG=null, MPER=null, MSLUG=null;
function cfgPeriods(per){ return Object.keys(per).map(Number).sort((a,b)=>a-b); }
function cfgInitPeriod(per){
  const ps=cfgPeriods(per);
  const want=Number(state.period);
  if(state.period!=='all' && ps.includes(want)) return want;
  if(ps.includes(6)) return 6;
  return ps[0];
}
function optText(o){
  let t=esc(o.label);
  t += o.monthly>0 ? ' (+'+fmtEur(o.monthly)+'/mo)' : ' — free';
  if(o.setup>0) t += ' +'+fmtEur(o.setup)+' setup';
  return t;
}
function renderConfigurator(cfg, per){
  const ip=cfgInitPeriod(per);
  let h='<h4>Configure &amp; price</h4>';
  h+='<div class="cfg-bar"><label>Period <select id="cfgPeriod">'+
     cfgPeriods(per).map(m=>'<option value="'+m+'"'+(m===ip?' selected':'')+'>'+m+
       ' mo'+(per[m]&&per[m].is_hidden_from_ui?' (hidden)':'')+'</option>').join('')+
     '</select></label>'+
     '<button class="iconbtn" id="cfgReset">Reset defaults</button></div>';
  h+='<div class="cfg-grid">';
  cfg.controls.forEach((c,ci)=>{
    h+='<label for="csel_'+ci+'">'+esc(c.label)+'</label>'+
       '<select class="cfgsel" id="csel_'+ci+'" data-ci="'+ci+'">'+
       c.options.map((o,oi)=>'<option value="'+oi+'"'+(oi===c.defaultIdx?' selected':'')+
          '>'+optText(o)+'</option>').join('')+'</select>';
  });
  h+='</div><div class="osum" id="osum"></div>';
  return h;
}
function recalcCfg(){
  if(!MCFG) return;
  const cfg=MCFG, per=MPER;
  const p=Number(document.getElementById('cfgPeriod').value);
  const row=per[p]||{};
  const base=Number(row.effective_monthly)||0;
  const anchorM=cfg.defaultMonthlyByPeriod[String(p)]!=null
    ? Number(cfg.defaultMonthlyByPeriod[String(p)]) : base;
  const anchorS=cfg.defaultSetupByPeriod[String(p)]!=null
    ? Number(cfg.defaultSetupByPeriod[String(p)]) : (Number(row.setup_fee)||0);
  let mD=0,sD=0;
  const changes=[], selected=[];
  cfg.controls.forEach((c,ci)=>{
    const selI=Number(document.getElementById('csel_'+ci).value);
    const sel=c.options[selI], def=c.options[c.defaultIdx];
    mD+=sel.monthly-def.monthly; sD+=sel.setup-def.setup;
    selected.push(esc(c.label)+'='+esc(sel.label));
    if(selI!==c.defaultIdx)
      changes.push({label:c.label,from:def.label,to:sel.label,
        dm:r2(sel.monthly-def.monthly),ds:r2(sel.setup-def.setup)});
  });
  const cfgM=r2(anchorM+mD), cfgS=r2(anchorS+sD), tot=r2(cfgM*p+cfgS);
  let h='<div class="ln muted"><span>Default ('+p+' mo)</span><span>'+eur(anchorM)+'/mo</span></div>';
  for(const ch of changes){
    const up=ch.dm>0;
    h+='<div class="ln chg'+(up?' up':'')+'"><span>'+esc(ch.label)+': '+esc(ch.to)+
       '</span><span class="v">'+sgn(ch.dm)+'/mo'+(ch.ds?' · '+sgn(ch.ds)+' setup':'')+'</span></div>';
  }
  h+='<div class="ln tot"><span>Configured monthly</span><span class="v">'+eur(cfgM)+'/mo</span></div>';
  h+='<div class="ln"><span>Setup (one-time)</span><span>'+(cfgS>0?eur(cfgS):'—')+'</span></div>';
  h+='<div class="ln"><span>Billed total ('+p+' mo)</span><span>'+eur(tot)+'</span></div>';
  if(state.gst || state.outputTax || state.cur!=='EUR' || state.ownerMarkup){
    const bits=[];
    if(state.gst) bits.push('+18% provider tax'+(state.providerTaxRecoverable?' (cash only; ITC recoverable)':''));
    if(state.outputTax) bits.push('+18% Securiace output GST after margin');
    if(state.cur!=='EUR' && FX.rate){
      bits.push('× '+FX.rate.toFixed(3)+' EUR→INR');
      if(FX.markup) bits.push('× '+(1+FX.markup).toFixed(3)+' card markup');
    }
    if(state.ownerMarkup) bits.push('× '+ownerMul().toFixed(3)+' owner markup');
    h+='<div class="breakdown">'+bits.join('  ')+'</div>';
  }
  h+='<div class="sel"><b>Selected:</b> '+selected.join(' · ')+'</div>';
  document.getElementById('osum').innerHTML=h;
}
function wireCfg(slug){
  const cfg=CFG[slug];
  MCFG=cfg; MPER=PLANS[slug].periods;
  document.getElementById('cfgPeriod').onchange=recalcCfg;
  document.querySelectorAll('.cfgsel').forEach(s=>s.onchange=recalcCfg);
  document.getElementById('cfgReset').onclick=()=>{
    cfg.controls.forEach((c,ci)=>{ document.getElementById('csel_'+ci).value=c.defaultIdx; });
    recalcCfg();
  };
  recalcCfg();
}

// Family-aware spec line in the modal header.
function specsLine(p){
  const s=p.specs||{};
  if(isStorageFam(p.family)){
    const t=perTb(p);
    return '<b>'+capGb(p)+'</b> base · <b>'+(t==null?'—':fmtEur(t))+'</b>/TB·mo · '+
           '<span class="good">free egress</span> · '+esc(String(s.region||'').replace('Object Storage: ',''));
  }
  const cores = p.family===FAM_DEDICATED ? 'cores' : 'vCPU';
  let out='<b>'+num(s.cpu_count)+'</b> '+cores+' · <b>'+num(s.ram_gb)+'</b> GB RAM · <b>'+
          num(s.storage_primary_gb)+'</b> GB '+(s.storage_primary_type||'')+' · <b>'+num(s.port_speed_mbps)+'</b> Mbps';
  if(p.avail && (p.avail.out_of_stock||p.avail.unavailable)) out+=' · <span class="bad">out of stock</span>';
  return out;
}

// ── Object Storage: capacity → price calculator ──────────────────────────────
function renderStorageCalc(p){
  const per=p.periods, ip=cfgInitPeriod(per);
  let h='<h4>Capacity &amp; price</h4>';
  h+='<div class="cfg-bar"><label>Term <select id="stTerm">'+
     cfgPeriods(per).map(m=>'<option value="'+m+'"'+(m===ip?' selected':'')+'>'+m+' mo</option>').join('')+
     '</select></label><label>Storage <input type="number" id="stTb" min="'+(p.specs.capacity_tb||0.25)+
     '" step="0.25" value="1"> TB</label></div>';
  h+='<div class="osum" id="stOut"></div>';
  h+='<div class="note">Capacity scales in 250 GB steps via the Contabo API. Ingress and egress are free — no traffic metering.</div>';
  return h;
}
function recalcStorage(p){
  const cap=p.specs.capacity_tb||0.25;
  const term=Number(document.getElementById('stTerm').value);
  let tb=Number(document.getElementById('stTb').value)||cap;
  tb=Math.max(cap, Math.round(tb/0.25)*0.25);
  const baseMo=Number(p.periods[term]?.effective_monthly)||0;   // price of the base-tier capacity
  const rate=baseMo/cap, mo=r2(rate*tb), tot=r2(mo*term);
  let h='<div class="ln muted"><span>Rate ('+term+' mo)</span><span>'+eur(rate)+'/TB·mo</span></div>';
  h+='<div class="ln"><span>Storage</span><span>'+tb+' TB</span></div>';
  h+='<div class="ln tot"><span>Monthly</span><span class="v">'+eur(mo)+'/mo</span></div>';
  h+='<div class="ln"><span>Billed total ('+term+' mo)</span><span>'+eur(tot)+'</span></div>';
  document.getElementById('stOut').innerHTML=h;
}
function wireStorage(slug){
  const p=PLANS[slug];
  document.getElementById('stTerm').onchange=()=>recalcStorage(p);
  document.getElementById('stTb').oninput=()=>recalcStorage(p);
  recalcStorage(p);
}

// ── Dedicated: independent add-on checklist ─────────────────────────────────
function renderExtras(slug){
  const ex=EXTRAS[slug]; if(!ex||!ex.length) return '';
  const cats={}; ex.forEach(o=>(cats[o.category]??=[]).push(o));
  const ip=cfgInitPeriod(PLANS[slug].periods);
  let h='<h4>Add-ons <span class="muted" style="font-weight:400;text-transform:none;letter-spacing:0">· optional, independent</span></h4>';
  h+='<div class="cfg-bar"><label>Term <select id="exTerm">'+
     cfgPeriods(PLANS[slug].periods).map(m=>'<option value="'+m+'"'+(m===ip?' selected':'')+'>'+m+' mo</option>').join('')+
     '</select></label><button class="iconbtn" id="exReset">Clear all</button></div>';
  h+='<div class="chk-grid">';
  let idx=0;
  for(const cat of ADDON_CATS){
    const items=cats[cat]; if(!items) continue;
    h+='<div class="chk-cat">'+esc(cat)+'</div>';
    for(const o of items){
      const price=o.monthly>0?('+'+fmtEur(o.monthly)+'/mo'):(o.setup>0?'setup only':'free');
      const setup=o.setup>0?(' <span class="chk-setup">+'+fmtEur(o.setup)+' setup</span>'):'';
      h+='<label class="chk"><input type="checkbox" class="exck" data-m="'+o.monthly+'" data-s="'+o.setup+'">'+
         '<span class="chk-l">'+esc(o.label)+'</span><span class="chk-p">'+price+setup+'</span></label>';
      idx++;
    }
  }
  h+='</div><div class="osum" id="exOut"></div>';
  return h;
}
function recalcExtras(slug){
  const per=PLANS[slug].periods;
  const term=Number(document.getElementById('exTerm').value);
  const base=Number(per[term]?.effective_monthly)||0, baseSetup=Number(per[term]?.setup_fee)||0;
  let addM=0,addS=0,n=0;
  document.querySelectorAll('.exck').forEach(ck=>{ if(ck.checked){ addM+=Number(ck.dataset.m)||0; addS+=Number(ck.dataset.s)||0; n++; } });
  const mo=r2(base+addM), setup=r2(baseSetup+addS), tot=r2(mo*term+setup);
  let h='<div class="ln muted"><span>Base ('+term+' mo)</span><span>'+eur(base)+'/mo</span></div>';
  if(n) h+='<div class="ln chg up"><span>'+n+' add-on'+(n>1?'s':'')+' selected</span><span class="v">'+sgn(addM)+'/mo'+(addS?' · '+sgn(addS)+' setup':'')+'</span></div>';
  h+='<div class="ln tot"><span>Configured monthly</span><span class="v">'+eur(mo)+'/mo</span></div>';
  h+='<div class="ln"><span>Setup (one-time)</span><span>'+(setup>0?eur(setup):'—')+'</span></div>';
  h+='<div class="ln"><span>Billed total ('+term+' mo)</span><span>'+eur(tot)+'</span></div>';
  document.getElementById('exOut').innerHTML=h;
}
function wireExtras(slug){
  document.getElementById('exTerm').onchange=()=>recalcExtras(slug);
  document.querySelectorAll('.exck').forEach(c=>c.onchange=()=>recalcExtras(slug));
  document.getElementById('exReset').onclick=()=>{ document.querySelectorAll('.exck').forEach(c=>c.checked=false); recalcExtras(slug); };
  recalcExtras(slug);
}

// ── Founder Managed Track: INR service tiers alongside provider pricing ─────
function renderManagedCard(plan, selectedId){
  const quote=managedQuote(plan,1);
  const selected=plan.id===selectedId;
  const status=plan.sla?.status==='approval_required'
    ? '<span class="managed-card-status">SLA target · approval required</span>' : '';
  return '<label class="managed-card'+(selected?' selected':'')+'">'+
    '<input type="radio" class="managed-plan-radio" name="managedPlan" value="'+esc(plan.id)+'"'+(selected?' checked':'')+'>'+
    '<span class="managed-card-body">'+
      '<span class="managed-card-top"><span class="managed-card-name">'+esc(plan.name)+'</span>'+
        '<span class="managed-card-price">'+managedMinor(quote.sellerTotal)+'<small>/year · '+(state.outputTax?'incl. output GST':'no output GST')+'</small></span></span>'+
      '<span class="managed-card-time">'+esc(plan.founder_time?.label||managedTime(plan.founder_time?.minutes_per_month))+'</span>'+
      '<ul>'+plan.includes.map(item=>'<li>'+esc(item)+'</li>').join('')+'</ul>'+status+
    '</span></label>';
}
function renderManagedTrack(p){
  if(!managedEligible(p) || MANAGED_PLANS.length===0) return '';
  const selected=state.managed[p.slug];
  const selectedId=selected?.planId||'';
  const quantity=Math.max(1,Math.min(99,Math.round(Number(selected?.quantity)||1)));
  let h='<section class="managed-track" aria-label="Founder Managed Track">';
  h+='<h4>Managed Track <span class="muted" style="font-weight:400;text-transform:none;letter-spacing:0">· optional, INR annual, per managed server</span></h4>';
  h+='<div class="managed-note">Founder security and server-management time slices are quoted separately from Contabo\\'s EUR infrastructure charges. This catalog is <b>'+esc(MANAGED.status||'draft')+'</b> and uses a review path; selecting a tier prepares a quote request rather than provisioning service automatically.</div>';
  h+='<div class="managed-grid" role="radiogroup" aria-label="Managed service tier">';
  h+='<label class="managed-card managed-none'+(!selectedId?' selected':'')+'"><input type="radio" class="managed-plan-radio" name="managedPlan" value=""'+(!selectedId?' checked':'')+'><span class="managed-card-body"><span class="managed-card-name">No managed tier</span><span class="managed-card-time">Provider pricing only</span><ul><li>Keep this server on self-serve pricing</li><li>No Founder hours or managed SLA selected</li></ul></span></label>';
  h+=MANAGED_PLANS.map(plan=>renderManagedCard(plan,selectedId)).join('');
  h+='</div>';
  h+='<div class="managed-controls"><label>Servers covered <input type="number" id="managedQuantity" min="1" max="99" step="1" value="'+quantity+'"></label><button class="iconbtn" id="managedReset" type="button">Clear managed tier</button></div>';
  h+='<div class="managed-summary" id="managedOut" role="status" aria-live="polite"></div>';
  if(Array.isArray(MANAGED.review_flags) && MANAGED.review_flags.length){
    h+='<details class="managed-review"><summary>Draft and scope notes</summary><ul>'+MANAGED.review_flags.map(item=>'<li>'+esc(item)+'</li>').join('')+'</ul></details>';
  }
  h+='</section>';
  return h;
}
function syncManagedCardStates(){
  document.querySelectorAll('.managed-card').forEach(card=>{
    const input=card.querySelector('.managed-plan-radio');
    card.classList.toggle('selected',!!input&&input.checked);
  });
}
function recalcManaged(slug){
  const out=document.getElementById('managedOut');
  if(!out) return;
  const id=document.querySelector('.managed-plan-radio:checked')?.value||'';
  const plan=managedPlan(id);
  if(!plan){
    delete state.managed[slug];
    persistManagedSelections();
    out.innerHTML='<div class="ln muted"><span>Managed track</span><span>Not selected</span></div><div class="breakdown">Provider EUR pricing remains unchanged.</div>';
    syncManagedCardStates();
    return;
  }
  const quantity=Math.max(1,Math.min(99,Math.round(Number(document.getElementById('managedQuantity')?.value)||1)));
  document.getElementById('managedQuantity').value=quantity;
  state.managed[slug]={planId:plan.id,quantity};
  persistManagedSelections();
  const quote=managedQuote(plan,quantity);
  let h='<div class="ln muted"><span>'+esc(plan.name)+' × '+quantity+' '+esc(MANAGED.scope_unit||'server')+'</span><span>'+(state.outputTax?'incl. output GST':'no output GST')+'</span></div>';
  h+='<div class="ln tot"><span>Annual managed service</span><span class="v">'+managedMinor(quote.sellerTotal)+'</span></div>';
  h+='<div class="ln"><span>Monthly equivalent</span><span>'+managedMinor(quote.monthly)+'</span></div>';
  h+='<div class="ln"><span>Founder time</span><span>'+esc(managedTime(quote.founderMinutes))+'</span></div>';
  if(quote.sellerPreTax!==quote.ex) h+='<div class="ln muted"><span>Owner markup</span><span>'+managedMinor(quote.sellerPreTax-quote.ex)+'</span></div>';
  if(quote.outputTax) h+='<div class="ln muted"><span>Securiace output GST 18%</span><span>'+managedMinor(quote.outputTax)+'</span></div>';
  h+='<div class="breakdown">Canonical INR quote · EUR→INR FX and card markup do not apply to managed services; owner markup '+(state.ownerMarkupScope==='provider_and_managed'?'is included.':'is not applied.')+'</div>';
  if(plan.sla?.status==='approval_required')
    h+='<div class="pending">'+esc(plan.sla.label)+' is a target in the pricing brief, not a published guarantee. Evidence and scope approval are required before sale.</div>';
  h+='<div class="managed-review"><b>Includes:</b> '+plan.includes.map(esc).join(' · ')+'<br><b>Excludes:</b> '+[...(MANAGED.common_exclusions||[]),...(plan.excludes||[])].map(esc).join(' · ')+'</div>';
  out.innerHTML=h;
  syncManagedCardStates();
}
function wireManagedTrack(slug){
  document.querySelectorAll('.managed-plan-radio').forEach(radio=>radio.onchange=()=>recalcManaged(slug));
  const quantity=document.getElementById('managedQuantity');
  if(quantity) quantity.oninput=()=>recalcManaged(slug);
  const reset=document.getElementById('managedReset');
  if(reset) reset.onclick=()=>{
    const none=document.querySelector('.managed-plan-radio[value=""]');
    if(none){ none.checked=true; recalcManaged(slug); }
  };
  recalcManaged(slug);
}

function openModal(slug){
  const p=PLANS[slug]; if(!p) return;
  modalMode='plan';
  MSLUG=slug;
  const per=p.periods;
  const order=[1,3,6,12].filter(m=>per[m]);
  let h='<div class="top">';
  h+='<div><h2><a href="'+esc(p.url)+'" target="_blank" rel="noopener">'+esc(p.name)+'</a>'+modelChip(p)+'</h2>';
  h+='<div class="specs">'+specsLine(p)+'</div></div>';
  h+='<button class="iconbtn" onclick="closeModal()" aria-label="Close">Close ✕</button></div>';

  h+='<h4>Billing</h4><table><tr><th class="l"></th>'+
     order.map(m=>'<th>'+m+' mo</th>').join('')+'</tr>';
  h+='<tr><td class="l">Monthly</td>'+order.map(m=>'<td>'+eur(per[m].effective_monthly)+'</td>').join('')+'</tr>';
  if(order.some(m=>per[m].setup_fee>0))
    h+='<tr><td class="l">Setup fee</td>'+order.map(m=>'<td>'+(per[m].setup_fee>0?eur(per[m].setup_fee):'—')+'</td>').join('')+'</tr>';
  h+='<tr><td class="l">Billed total</td>'+order.map(m=>'<td>'+eur(per[m].total_period_cost)+'</td>').join('')+'</tr>';
  h+='</table>';
  if(state.gst || state.outputTax || state.cur!=='EUR' || state.ownerMarkup){
    const bits=[];
    if(state.gst) bits.push('+18% provider tax'+(state.providerTaxRecoverable?' (cash only; ITC recoverable)':''));
    if(state.outputTax) bits.push('+18% Securiace output GST after margin');
    if(state.cur!=='EUR' && FX.rate){
      bits.push('EUR→INR @ '+FX.rate.toFixed(3));
      if(FX.markup) bits.push((FX.markup*100).toFixed(1)+'% card markup');
    }
    if(state.ownerMarkup) bits.push((state.ownerMarkup*100).toFixed(1)+'% owner markup');
    h+='<div class="breakdown">Applied: '+bits.join(' · ')+'</div>';
  }

  const cfg=CFG[slug];
  const stor=isStorageFam(p.family);
  if(stor){
    h+=renderStorageCalc(p);
  } else if(cfg && cfg.controls && cfg.controls.length){
    h+=renderConfigurator(cfg, per);
    if(EXTRAS[slug]) h+=renderExtras(slug);
  } else if(EXTRAS[slug]){
    h+=renderExtras(slug);
  } else if(ADDONS[slug]){
    const ad=ADDONS[slug];
    const img=ad['Image']||[];
    const byCat=c=>img.filter(o=>o.category===c);
    const sec=(t,items)=>items.length?('<div class="pp"><b>'+t+'</b>'+tagList(items)+'</div>'):'';
    if(img.length){
      h+='<h4>Image</h4>';
      h+=sec('OS',byCat('OS'))+sec('Apps',byCat('Apps'))+
         sec('Control Panels',byCat('Panels'))+sec('Blockchain',byCat('Blockchain'));
    }
    const reg=ad['Region']||[];
    if(reg.length){
      const G=['Europe','America','Asia','Australia','Other'],g={};
      reg.forEach(o=>(g[o.regionGroup||'Other']??=[]).push(o));
      const ord=G.flatMap(x=>g[x]||[]);
      h+='<h4>Regions</h4><table><tr>'+
        ord.map(o=>'<th>'+o.label.replace('European Union','EU')+(o.isDefault?' *':'')+'</th>').join('')+'</tr><tr>'+
        ord.map(o=>'<td>'+(o.delta===0?'free':'+'+fmtEur(o.delta))+'</td>').join('')+'</tr></table>';
    }
    const net=ad['Networking']||[];
    if(net.length){
      const cat=c=>net.filter(o=>o.category===c);
      h+='<h4>Networking</h4>';
      const bw=cat('Bandwidth'), ip=cat('IPv4').filter(o=>o.delta>0), pv=cat('Private Networking').filter(o=>o.delta>0);
      if(bw.length) h+='<div class="pp"><b>Bandwidth</b>'+tagList(bw)+'</div>';
      if(ip.length) h+='<div class="pp"><b>Extra IPv4</b>'+tagList(ip)+'</div>';
      if(pv.length) h+='<div class="pp"><b>Private Network</b>'+tagList(pv)+'</div>';
    }
    const dp=(ad['Data Protection']||[]).filter(o=>o.delta>0);
    if(dp.length) h+='<h4>Backup</h4><div class="pp">'+tagList(dp)+'</div>';
    const st=ad['Storage']||[];
    if(st.length){ h+='<h4>Storage</h4><div class="pp">'+tagList(st)+'</div>'; }
  } else {
    h+='<h4>Add-ons</h4><div class="muted pp">Run the enrich step for add-on detail.</div>';
  }
  if(managedEligible(p)) h+=renderManagedTrack(p);
  document.getElementById('sheet').innerHTML=h;
  modal.classList.add('open');
  if(stor){ wireStorage(slug); }
  else {
    if(cfg && cfg.controls && cfg.controls.length) wireCfg(slug);
    if(EXTRAS[slug]) wireExtras(slug);
  }
  if(managedEligible(p)) wireManagedTrack(slug);
}

// ── Proposal workspace ─────────────────────────────────────────────────────
// Proposal inputs are captured as structured data first. The preview/export
// renderer only accepts the allow-listed ProposalDocument blocks from the
// shared model, so Codex can never inject arbitrary HTML into the report.
const PROPOSAL_POLICY_KEYS=['configuration','provider_pricing','provider_line_items',
  'managed_services','alternatives','source_links','tax','fx_markup','owner_markup',
  'client_notes','internal_notes'];
const PROPOSAL_POLICY_LABELS={
  configuration:'Configuration', provider_pricing:'Provider pricing',
  provider_line_items:'Provider line items', managed_services:'Managed services',
  alternatives:'Plan comparison', source_links:'Source links', tax:'Tax treatment',
  fx_markup:'FX markup', owner_markup:'Owner markup', client_notes:'Client notes',
  internal_notes:'Internal notes'
};
let proposalSnapshot=null, proposalDocument=null, proposalPrimarySlug=null;
let proposalGenerationCapability={checked:false,available:false,reason:'Not checked'};

function proposalPolicySelect(key, selected){
  const rule=PROPOSAL_MODEL.VISIBILITY_RULES?.[key]||{allowed:['show','internal_only','exclude'],help:''};
  const labels=PROPOSAL_MODEL.VISIBILITY_LABELS||{};
  return '<label class="proposal-field"><span>'+esc(PROPOSAL_POLICY_LABELS[key]||key)+'</span>'+\
    '<select id="proposalPolicy_'+esc(key)+'">'+\
    rule.allowed.map(value=>'<option value="'+value+'"'+(selected===value?' selected':'')+'>'+esc(labels[value]||value)+'</option>').join('')+\
    '</select>'+(rule.help?'<small>'+esc(rule.help)+'</small>':'')+'</label>';
}
function proposalProfileOptions(selected){
  const profiles=PROPOSAL.profiles||PROPOSAL_MODEL.PROFILE_DEFAULTS||{};
  return Object.entries(profiles).map(([id,p])=>'<option value="'+esc(id)+'"'+(id===selected?' selected':'')+'>'+esc(p.label||id)+'</option>').join('');
}
function proposalProfileVisibility(profile){
  const base={...(PROPOSAL_MODEL.DEFAULT_VISIBILITY||{})};
  const profileCfg=(PROPOSAL_MODEL.PROFILE_DEFAULTS||{})[profile];
  return Object.assign(base,profileCfg?.visibility||{});
}
function proposalTermFor(p){
  const available=Object.keys(p.periods||{}).map(Number).sort((a,b)=>a-b);
  const wanted=Number(state.period);
  return available.includes(wanted)?wanted:(available.includes(6)?6:(available[0]||1));
}
function proposalPlanInput(slug, current){
  const p=PLANS[slug]; if(!p) return null;
  const active=!!current && modalMode==='plan' && MSLUG===slug;
  let period=proposalTermFor(p), providerMonthly=Number(p.periods[period]?.effective_monthly)||0;
  let providerSetup=Number(p.periods[period]?.setup_fee)||0, selections=[], addons=[];
  const specs={...(p.specs||{})};
  if(isStorageFam(p.family) && active && document.getElementById('stTerm')){
    period=Number(document.getElementById('stTerm').value)||period;
    const baseCap=Number(p.specs.capacity_tb)||0.25;
    const base=Number(p.periods[period]?.effective_monthly)||0;
    let tb=Number(document.getElementById('stTb').value)||baseCap;
    tb=Math.max(baseCap,Math.round(tb/0.25)*0.25);
    providerMonthly=r2(base*(tb/baseCap));
    specs.capacity_tb=tb;
    selections.push({label:'Storage capacity: '+tb+' TB',monthly:0,setup:0});
  } else if(!isStorageFam(p.family) && active && MCFG && document.getElementById('cfgPeriod')){
    period=Number(document.getElementById('cfgPeriod').value)||period;
    const row=p.periods[period]||{};
    providerMonthly=Number(MCFG.defaultMonthlyByPeriod?.[String(period)] ?? row.effective_monthly)||0;
    providerSetup=Number(MCFG.defaultSetupByPeriod?.[String(period)] ?? row.setup_fee)||0;
    MCFG.controls.forEach((c,ci)=>{
      const select=document.getElementById('csel_'+ci); if(!select) return;
      const selected=c.options[Number(select.value)]||c.options[c.defaultIdx];
      const def=c.options[c.defaultIdx]||{monthly:0,setup:0};
      selections.push({label:c.label+': '+selected.label,
        monthly:r2(Number(selected.monthly)-Number(def.monthly)),
        setup:r2(Number(selected.setup)-Number(def.setup))});
    });
  } else if(active && document.getElementById('exTerm')){
    period=Number(document.getElementById('exTerm').value)||period;
    providerMonthly=Number(p.periods[period]?.effective_monthly)||providerMonthly;
    providerSetup=Number(p.periods[period]?.setup_fee)||providerSetup;
    document.querySelectorAll('.exck').forEach(ck=>{
      if(ck.checked) addons.push({label:ck.parentElement?.querySelector('.chk-l')?.textContent||'Add-on',
        monthly:Number(ck.dataset.m)||0,setup:Number(ck.dataset.s)||0});
    });
  } else if(p.options && typeof p.options==='object'){
    for(const [dimension,summary] of Object.entries(p.options)){
      if(summary?.default_label) selections.push({label:dimension+': '+summary.default_label,monthly:0,setup:0});
    }
  }
  return {plan_slug:p.slug,plan_name:p.name,family:p.family,plan_url:p.url,period_months:period,
    provider_monthly_eur:providerMonthly,provider_setup_eur:providerSetup,selections,addons,specs};
}
function proposalManagedPlanInput(plan,quantity){
  if(!plan) return null;
  return {plan_id:plan.id,name:plan.name,quantity:Math.max(1,Math.min(99,Math.round(Number(quantity)||1))),
    annual_price_minor:plan.annual_price_minor,
    founder_minutes_per_month:plan.founder_time?.minutes_per_month||0,
    billing_term_months:MANAGED.billing_term_months||12,
    output_tax_eligible:MANAGED.output_tax_eligible,
    nominal_output_tax_rate:MANAGED.nominal_output_tax_rate,
    catalog_version:MANAGED.version||null,
    commercial_terms_version:MANAGED.commercial_terms?.version||null,
    commercial_terms_snapshot:MANAGED.commercial_terms||null,
    includes:plan.includes||[],excludes:[...(MANAGED.common_exclusions||[]),...(plan.excludes||[])]};
}
function proposalManagedInput(slug){
  const saved=state.managed[slug]; if(!saved) return null;
  return proposalManagedPlanInput(managedPlan(saved.planId),saved.quantity);
}
function proposalManagedInputFromForm(slug){
  const p=PLANS[slug];
  const selected=document.getElementById('proposalManagedPlan')?.value||'';
  if(!selected || !managedEligible(p)) return null;
  return proposalManagedPlanInput(managedPlan(selected),document.getElementById('proposalManagedQuantity')?.value);
}
function proposalPricing(){
  return reportPricing(state.cur==='EUR'||!FX.rate?'EUR':'INR');
}
function proposalDefaultSlugs(){
  const selected=[...state.compare];
  const current=modalMode==='plan'&&MSLUG?MSLUG:null;
  const primary=current||selected[0]||filtered()[0]?.slug||PLAN_LIST[0]?.slug;
  return {primary,alternatives:selected.filter(slug=>slug!==primary).slice(0,4)};
}
function proposalSnapshotFromForm(){
  const primarySlug=document.getElementById('proposalPlan')?.value||proposalPrimarySlug;
  const selectedAlt=[...state.compare].filter(slug=>slug!==primarySlug).slice(0,4);
  const profile=document.getElementById('proposalProfile')?.value||'quick-quote';
  const visibility={};
  for(const key of PROPOSAL_POLICY_KEYS){
    const el=document.getElementById('proposalPolicy_'+key);
    visibility[key]=el?.value||'show';
  }
  const client={project_name:document.getElementById('proposalProject')?.value?.trim()||'',
    recipient:document.getElementById('proposalRecipient')?.value?.trim()||'',
    notes:document.getElementById('proposalNotes')?.value?.trim()||''};
  const internal={notes:document.getElementById('proposalInternalNotes')?.value?.trim().slice(0,4000)||''};
  return PROPOSAL_MODEL.makeSnapshot({profile,visibility,client,internal,pricing:proposalPricing(),
    primary:proposalPlanInput(primarySlug,primarySlug===MSLUG),
    alternatives:selectedAlt.map(slug=>proposalPlanInput(slug,false)),
    managed:proposalManagedInputFromForm(primarySlug),
    source:{snapshot_generated_at:DATA.meta?.generated_at||'',report_generated_at:DATA.meta?.generated_at||'',
      source:'contabo_view_model.json + contabo_pricing_dataset.json',consistency:DATA.consistency||{},
      fx_source:FX.source,fx_rate_date:FX.rateDate}});
}
function proposalStatus(message, kind){
  const el=document.getElementById('proposalStatus'); if(!el) return;
  el.className='proposal-status'+(kind?' '+kind:''); el.textContent=message||'';
}
function proposalSummary(snapshot){
  const q=snapshot.primary.quote;
  const moneyLabel=snapshot.pricing.currency==='INR'?'₹'+Math.round(q.display.period_total).toLocaleString('en-IN'):'€'+q.display.period_total.toFixed(2);
  return '<div class="proposal-summary"><span class="proposal-chip">'+esc(snapshot.primary.plan_name)+'</span>'+\
    '<span class="proposal-chip">'+esc(snapshot.primary.period_months+' mo')+'</span>'+\
    '<span class="proposal-chip">Estimated billed total '+esc(moneyLabel)+'</span>'+\
    (snapshot.managed?'<span class="proposal-chip">Managed add-on included</span>':'')+\
    (snapshot.warnings.length?'<span class="proposal-chip">'+snapshot.warnings.length+' review note'+(snapshot.warnings.length===1?'':'s')+'</span>':'')+'</div>';
}
function showProposalPreview(snapshot, proposalDoc, status){
  proposalSnapshot=snapshot; proposalDocument=proposalDoc;
  const preview=document.getElementById('proposalPreview'); if(!preview) return;
  preview.innerHTML=proposalSummary(snapshot)+PROPOSAL_MODEL.renderDocument(proposalDoc);
  const buttons=document.querySelectorAll('[data-proposal-export]'); buttons.forEach(b=>b.disabled=false);
  if(status) proposalStatus(status,'good');
}
function proposalExportHtml(document){
  const body=PROPOSAL_MODEL.renderDocument(document);
  return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'+esc(document.title||'Proposal')+'</title><style>'+\
    'body{margin:40px auto;max-width:820px;padding:0 20px;font:15px/1.6 system-ui,sans-serif;color:#20252d}article{border:1px solid #d8dee8;border-radius:12px;padding:28px}h2{margin:0}h3{margin-top:24px;font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:#687386}.proposal-subtitle{color:#687386}.proposal-table{width:100%;border-collapse:collapse}.proposal-table th,.proposal-table td{padding:8px;border-bottom:1px solid #e7ebf0;text-align:left}.proposal-table th{color:#687386;font-weight:500;width:42%}.proposal-callout{padding:10px;border-left:3px solid #e4a11b;background:#fff7df}'+'</style></head><body>'+body+'</body></html>';
}
function downloadProposal(name, content, type){
  const blob=new Blob([content],{type}); const url=URL.createObjectURL(blob); const a=document.createElement('a');
  a.href=url; a.download=name; document.body.appendChild(a); a.click(); a.remove(); setTimeout(()=>URL.revokeObjectURL(url),1000);
}
function copyProposalBrief(){
  if(!proposalSnapshot) return;
  const text=PROPOSAL_MODEL.toClientBrief(proposalSnapshot);
  if(navigator.clipboard?.writeText) navigator.clipboard.writeText(text).then(()=>proposalStatus('Brief copied to clipboard.','good')).catch(()=>proposalStatus('Clipboard permission was not available.','bad'));
  else { const ta=document.createElement('textarea'); ta.value=text; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove(); proposalStatus('Brief copied to clipboard.','good'); }
}
async function generateProposalWithCodex(){
  if(!proposalSnapshot) previewProposal();
  if(!proposalSnapshot) return;
  const deterministic=PROPOSAL_MODEL.deterministicDocument(proposalSnapshot);
  if(!proposalGenerationCapability.available){
    showProposalPreview(proposalSnapshot,deterministic,'Deterministic proposal is ready. Codex generation is unavailable in this server build.');
    return;
  }
  proposalStatus('Submitting the structured snapshot to the local proposal service…');
  try{
    const response=await fetch('/api/v1/proposals/generate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({
      profile:proposalSnapshot.profile,visibility:proposalSnapshot.visibility,client:proposalSnapshot.client,context:proposalSnapshot})});
    if(!response.ok) throw new Error('proposal service returned HTTP '+response.status);
    const queued=await response.json(); const id=queued.job_id;
    if(!id) throw new Error('proposal service did not return a job id');
    for(let attempt=0;attempt<60;attempt++){
      await new Promise(resolve=>setTimeout(resolve,1000));
      const poll=await fetch('/api/v1/proposals/'+encodeURIComponent(id));
      if(!poll.ok) throw new Error('proposal job polling failed with HTTP '+poll.status);
      const job=await poll.json();
      if(job.status==='succeeded'&&job.document){
        const safeDocument=PROPOSAL_MODEL.mergeSafeNarrative(deterministic,job.document);
        const usedCodex=job.provider==='codex-cli-safe'||job.provider==='codex-cli';
        showProposalPreview(proposalSnapshot,safeDocument,usedCodex
          ?'Proposal wording generated locally via Codex CLI; server-validated pricing and visibility policy retained. Review it before exporting.'
          :'Codex CLI was unavailable; deterministic server fallback is ready. Review it before exporting.');
        return;
      }
      if(job.status==='failed') throw new Error(job.error||'proposal generation failed');
      proposalStatus('Generating locally… '+(job.status||'working'));
    }
    throw new Error('proposal generation timed out in the report UI');
  }catch(error){
    showProposalPreview(proposalSnapshot,deterministic,'Codex generation was unavailable; deterministic proposal fallback is ready. '+String(error.message||error));
  }
}
function previewProposal(){
  try{
    const snapshot=proposalSnapshotFromForm();
    const document=PROPOSAL_MODEL.deterministicDocument(snapshot);
    showProposalPreview(snapshot,document,'Deterministic preview ready. Adjust visibility or client fields, then generate or export.');
  }catch(error){ proposalStatus('Could not build proposal preview: '+String(error.message||error),'bad'); }
}
async function detectProposalGenerationCapability(){
  const button=document.getElementById('proposalGenerateBtn');
  if(!button) return;
  button.disabled=true;
  button.textContent='Checking Codex capability…';
  if(location.protocol==='file:'){
    proposalGenerationCapability={checked:true,available:false,reason:'Static report artifact'};
  }else{
    try{
      const openapiResponse=await fetch('/api/v1/openapi.json',{headers:{accept:'application/json'}});
      if(!openapiResponse.ok) throw new Error('OpenAPI HTTP '+openapiResponse.status);
      const openapi=await openapiResponse.json();
      if(!openapi?.paths?.['/api/v1/proposals/capabilities']){
        proposalGenerationCapability={checked:true,available:false,
          reason:'This server build does not advertise proposal generation'};
        throw new Error(proposalGenerationCapability.reason);
      }
      const response=await fetch('/api/v1/proposals/capabilities',{headers:{accept:'application/json'}});
      if(!response.ok) throw new Error('HTTP '+response.status);
      const capability=await response.json();
      const available=capability?.generation?.available===true || capability?.codex_cli_available===true;
      proposalGenerationCapability={checked:true,available,
        reason:available?'Server capability confirmed':'Server reports generation unavailable'};
    }catch(error){
      if(!proposalGenerationCapability.checked) proposalGenerationCapability={checked:true,available:false,
        reason:'Capability discovery unavailable ('+String(error.message||error)+')'};
    }
  }
  button.disabled=!proposalGenerationCapability.available;
  button.textContent=proposalGenerationCapability.available
    ?'Generate with local Codex'
    :'Codex generation unavailable';
  button.title=proposalGenerationCapability.reason;
  if(!proposalGenerationCapability.available){
    proposalStatus('Deterministic preview and client exports are available. '+proposalGenerationCapability.reason+'.','');
  }
}
function wireProposalForm(){
  const profile=document.getElementById('proposalProfile');
  const syncProfile=()=>{
    const defaults=proposalProfileVisibility(profile.value);
    for(const key of PROPOSAL_POLICY_KEYS){
      const el=document.getElementById('proposalPolicy_'+key);
      if(el && !el.dataset.touched) el.value=defaults[key]||'show';
    }
  };
  profile.onchange=()=>{syncProfile();previewProposal();};
  PROPOSAL_POLICY_KEYS.forEach(key=>document.getElementById('proposalPolicy_'+key)?.addEventListener('change',e=>{
    e.target.dataset.touched='1'; previewProposal();
  }));
  const syncManagedControl=()=>{
    const slug=document.getElementById('proposalPlan')?.value||proposalPrimarySlug;
    const eligible=managedEligible(PLANS[slug]);
    const select=document.getElementById('proposalManagedPlan');
    const quantity=document.getElementById('proposalManagedQuantity');
    if(!select||!quantity) return;
    select.disabled=!eligible;
    if(!eligible) select.value='';
    quantity.disabled=!eligible||!select.value;
    select.title=eligible?'Optional Founder Managed add-on':'Managed service is unavailable for this product family';
  };
  document.getElementById('proposalPlan')?.addEventListener('change',()=>{syncManagedControl();previewProposal();});
  document.getElementById('proposalManagedPlan')?.addEventListener('change',()=>{syncManagedControl();previewProposal();});
  document.getElementById('proposalManagedQuantity')?.addEventListener('input',previewProposal);
  document.getElementById('proposalManagedQuantity')?.addEventListener('change',e=>{
    e.target.value=String(Math.max(1,Math.min(99,Math.round(Number(e.target.value)||1)))); previewProposal();
  });
  for(const id of ['proposalProject','proposalRecipient','proposalNotes','proposalInternalNotes']){
    document.getElementById(id)?.addEventListener('input',previewProposal);
  }
  document.getElementById('proposalPreviewBtn').onclick=previewProposal;
  document.getElementById('proposalGenerateBtn').onclick=generateProposalWithCodex;
  document.getElementById('proposalCopyBtn').onclick=copyProposalBrief;
  document.querySelectorAll('[data-proposal-export]').forEach(button=>button.onclick=()=>{
    if(!proposalSnapshot||!proposalDocument) return;
    const kind=button.dataset.proposalExport;
    if(kind==='client-html') downloadProposal('contabo-proposal-client.html',proposalExportHtml(PROPOSAL_MODEL.clientDocument(proposalSnapshot,proposalDocument)),'text/html');
    if(kind==='client-json') downloadProposal('contabo-proposal-client.json',JSON.stringify(PROPOSAL_MODEL.clientProjection(proposalSnapshot,proposalDocument),null,2)+'\\n','application/json');
    if(kind==='client-csv') downloadProposal('contabo-proposal-client.csv',PROPOSAL_MODEL.toCsv(proposalSnapshot),'text/csv');
    if(kind==='internal-json') downloadProposal('contabo-proposal-internal-evidence.json',JSON.stringify(PROPOSAL_MODEL.internalEvidence(proposalSnapshot),null,2)+'\\n','application/json');
  });
  syncProfile();
  syncManagedControl();
  previewProposal();
  detectProposalGenerationCapability();
}
function openProposalWizard(){
  const defaults=proposalDefaultSlugs(); if(!defaults.primary) return;
  proposalPrimarySlug=defaults.primary; modalMode='proposal';
  const profile='quick-quote', visibility=proposalProfileVisibility(profile);
  const savedManaged=state.managed[defaults.primary]||{};
  let h='<div class="top"><div><h2>Create proposal</h2><div class="specs">Structured quote workspace · local-only generation · review before export</div></div><button class="iconbtn" onclick="closeModal()" aria-label="Close">Close ✕</button></div>';
  h+='<p class="proposal-intro">Choose what the client sees, what is included only in totals, and what remains internal. Provider facts and prices come from this report snapshot; Codex may improve wording but cannot add arbitrary HTML or invent commercial terms.</p>';
  h+='<div class="proposal-controls"><button class="iconbtn primary" id="proposalPreviewBtn" type="button">Preview deterministic</button><button class="iconbtn secondary" id="proposalGenerateBtn" type="button" disabled>Checking Codex capability…</button><button class="iconbtn" id="proposalCopyBtn" type="button">Copy client brief</button><button class="iconbtn" data-proposal-export="client-html" type="button" disabled>Client HTML</button><button class="iconbtn" data-proposal-export="client-json" type="button" disabled>Client JSON</button><button class="iconbtn" data-proposal-export="client-csv" type="button" disabled>Client CSV</button><button class="iconbtn" data-proposal-export="internal-json" type="button" disabled title="Contains provider cost, owner margin, recipient, internal notes, and provenance. Never send to a client.">Internal evidence JSON</button></div>';
  h+='<div class="proposal-status" id="proposalStatus" role="status" aria-live="polite"></div>';
  h+='<div class="proposal-grid">';
  h+='<label class="proposal-field"><span>Primary plan</span><select id="proposalPlan">'+PLAN_LIST.map(p=>'<option value="'+esc(p.slug)+'"'+(p.slug===defaults.primary?' selected':'')+'>'+esc(p.name+' · '+p.family)+'</option>').join('')+'</select></label>';
  h+='<label class="proposal-field"><span>Proposal profile</span><select id="proposalProfile">'+proposalProfileOptions(profile)+'</select></label>';
  h+='<label class="proposal-field"><span>Founder Managed add-on</span><select id="proposalManagedPlan"><option value="">None</option>'+MANAGED_PLANS.map(plan=>'<option value="'+esc(plan.id)+'"'+(plan.id===savedManaged.planId?' selected':'')+'>'+esc(plan.name+' · '+managedMinor(plan.annual_price_minor)+'/year')+'</option>').join('')+'</select></label>';
  h+='<label class="proposal-field"><span>Managed server quantity</span><input id="proposalManagedQuantity" type="number" min="1" max="99" step="1" value="'+esc(savedManaged.quantity||1)+'"></label>';
  h+='<label class="proposal-field"><span>Client / project name</span><input id="proposalProject" type="text" maxlength="120" placeholder="Optional"></label>';
  h+='<label class="proposal-field"><span>Recipient</span><input id="proposalRecipient" type="text" maxlength="160" placeholder="Optional"></label>';
  h+='<label class="proposal-field wide"><span>Client-facing notes</span><textarea id="proposalNotes" maxlength="2000" placeholder="Scope, goals, or assumptions to include…"></textarea></label>';
  h+='<label class="proposal-field wide"><span>Internal notes (operator-only evidence)</span><textarea id="proposalInternalNotes" maxlength="4000" placeholder="Approval context, exceptions, or follow-up; never included in client artifacts…"></textarea></label>';
  h+='</div><h4>Content policy</h4><div class="proposal-grid">';
  for(const key of PROPOSAL_POLICY_KEYS) h+=proposalPolicySelect(key,visibility[key]||'show');
  h+='</div><div class="proposal-hint">Compared plans are taken from the compare drawer (up to four). “Silent include” keeps a value in pricing/context without mentioning it in the client document. Mandatory diagnostics remain on the administrator Review-before-sending rail and enter client artifacts only when explicitly marked client-facing.</div>';
  h+='<div class="proposal-hint"><b>Client artifacts</b> apply the visibility policy and omit internal/silent facts. <b>Internal evidence JSON</b> intentionally contains provider cost, owner margin, recipient, and provenance; keep it private.</div>';
  h+='<div class="proposal-preview" id="proposalPreview"></div>';
  document.getElementById('sheet').innerHTML=h; modal.classList.add('open'); wireProposalForm();
}

// ── Wire toolbar ───────────────────────────────────────────────────────────
document.getElementById('proposalBtn').onclick=openProposalWizard;
const periodTog=document.getElementById('periodTog');
function setPeriod(v){
  state.period=v; lsSet('contabo_period',v);
  periodTog.querySelectorAll('button').forEach(b=>
    b.setAttribute('aria-pressed', b.dataset.period===v));
  if(v!=='all'){ state.sortKey={'1':'p1','6':'p6','12':'p12'}[v]||state.sortKey;
    state.sortDir=1; }
  render();
}
periodTog.querySelectorAll('button').forEach(b=>b.onclick=()=>setPeriod(b.dataset.period));
setPeriod(state.period);

document.getElementById('showHidden').onchange=e=>{ state.showHidden=e.target.checked; render(); };
document.getElementById('minCpu').oninput=e=>{ state.minCpu=+e.target.value||0; render(); };
document.getElementById('minRam').oninput=e=>{ state.minRam=+e.target.value||0; render(); };
document.getElementById('search').oninput=e=>{ state.q=e.target.value; render(); };
document.getElementById('themeBtn').onclick=()=>{
  const r=document.documentElement;
  const next = r.dataset.theme==='dark'?'light':'dark';
  r.dataset.theme=next; lsSet('contabo_theme',next);
};
const savedTheme = lsGet('contabo_theme');
if(savedTheme==='light'||savedTheme==='dark') document.documentElement.dataset.theme=savedTheme;

// Provider/vendor tax, input-credit treatment, and Securiace output tax are
// intentionally independent controls. Only output tax uses the registration gate.
const gstToggle=document.getElementById('gstToggle');
gstToggle.checked=state.gst;
const providerTaxRecoverableEl=document.getElementById('providerTaxRecoverable');
providerTaxRecoverableEl.checked=state.providerTaxRecoverable;
const outputTaxToggle=document.getElementById('outputTaxToggle');
outputTaxToggle.checked=state.outputTax;
outputTaxToggle.disabled=!GST_REGISTRATION_VERIFIED;
if(!GST_REGISTRATION_VERIFIED) outputTaxToggle.parentElement.title=
  'Output GST disabled: verified registration evidence and matching WHMCS tax settings are required.';
const gstNote=document.getElementById('gstNote');
function applyGstUi(){
  const notes=[];
  if(state.gst) notes.push('Provider tax 18% charged'+(state.providerTaxRecoverable?' as cash only; excluded from economic landed cost.':'; included in economic landed cost.'));
  if(state.outputTax) notes.push('Securiace output GST 18% applied after owner margin.');
  if(!GST_REGISTRATION_VERIFIED) notes.push('Output GST disabled: registration not verified.');
  gstNote.textContent=notes.join(' '); gstNote.hidden=notes.length===0;
  render(); renderCompare();
  if(modal.classList.contains('open') && modalMode==='plan' && MSLUG) openModal(MSLUG);
}
gstToggle.onchange=e=>{
  state.gst=e.target.checked;
  if(!state.gst){
    state.providerTaxRecoverable=false;
    providerTaxRecoverableEl.checked=false;
    lsSet('contabo_provider_tax_recoverable','0');
  }
  lsSet('contabo_provider_tax_charged', state.gst?'1':'0'); applyGstUi();
};
providerTaxRecoverableEl.onchange=e=>{
  state.providerTaxRecoverable=state.gst && e.target.checked;
  e.target.checked=state.providerTaxRecoverable;
  lsSet('contabo_provider_tax_recoverable',state.providerTaxRecoverable?'1':'0'); applyGstUi();
};
outputTaxToggle.onchange=e=>{
  state.outputTax=GST_REGISTRATION_VERIFIED && e.target.checked;
  e.target.checked=state.outputTax;
  lsSet('contabo_output_tax',state.outputTax?'1':'0'); applyGstUi();
};
applyGstUi();

// FX markup
const fxMarkupEl=document.getElementById('fxMarkup');
fxMarkupEl.value=((FX.markup||0)*100).toFixed(1);
if(storedFxMarkup!==null && Number(storedFxMarkup)!==FX.markup)
  lsSet('contabo_fx_markup',String(FX.markup));
fxMarkupEl.oninput=e=>{
  const raw=Number(e.target.value);
  const v=clampFxMarkup((Number.isFinite(raw)?raw:0)/100);
  FX.markup=v;
  if(!Number.isFinite(raw)||raw<0||raw>100) e.target.value=(v*100).toFixed(1);
  lsSet('contabo_fx_markup', String(v));
  renderFxBadge();
  render(); renderCompare();
  if(modal.classList.contains('open') && modalMode==='plan' && MSLUG) openModal(MSLUG);
};
fxMarkupEl.onchange=e=>{ e.target.value=(FX.markup*100).toFixed(1); };

// Owner markup is a separate cost-plus adjustment. It is intentionally stored
// as a fraction, like FX, while the input is displayed as a percentage. The
// normalized value is written back immediately so stale values cannot remain
// visible while calculations use a capped value.
const ownerMarkupEl=document.getElementById('ownerMarkup');
const ownerMarkupScopeEl=document.getElementById('ownerMarkupScope');
ownerMarkupEl.value=((state.ownerMarkup||0)*100).toFixed(1);
ownerMarkupScopeEl.value=state.ownerMarkupScope;
if(storedOwnerMarkup!==null && Number(storedOwnerMarkup)/100!==state.ownerMarkup)
  lsSet('contabo_owner_markup_pct',String(state.ownerMarkup*100));
ownerMarkupEl.oninput=e=>{
  const raw=Number(e.target.value);
  const v=clampOwnerMarkup((Number.isFinite(raw)?raw:0)/100);
  state.ownerMarkup=v;
  if(!Number.isFinite(raw)||raw<0||raw>100) e.target.value=(v*100).toFixed(1);
  lsSet('contabo_owner_markup_pct',String(v*100));
  renderFxBadge();
  render(); renderCompare();
  if(modal.classList.contains('open') && modalMode==='plan' && MSLUG) openModal(MSLUG);
};
ownerMarkupEl.onchange=e=>{ e.target.value=(state.ownerMarkup*100).toFixed(1); };
ownerMarkupScopeEl.onchange=e=>{
  state.ownerMarkupScope=ownerScope(e.target.value);
  e.target.value=state.ownerMarkupScope;
  lsSet('contabo_owner_markup_scope',state.ownerMarkupScope);
  renderFxBadge();
  render(); renderCompare();
  if(modal.classList.contains('open') && modalMode==='plan' && MSLUG) openModal(MSLUG);
};

// Currency toggle
const curTog=document.getElementById('curTog');
const inrNote=document.getElementById('inrNote');
function updateCurUI(){
  curTog.querySelectorAll('button').forEach(b=>
    b.setAttribute('aria-pressed', b.dataset.cur===state.cur));
  inrNote.hidden = !(state.cur!=='EUR' && FX.rate);
  document.getElementById('gstGroup').style.opacity = state.cur==='EUR' && !state.gst && !state.outputTax ? '.85':'1';
}
function applyFx(){
  render(); renderCompare();
  if(modal.classList.contains('open') && modalMode==='plan' && MSLUG) openModal(MSLUG);
  updateCurUI();
}
curTog.querySelectorAll('button').forEach(b=>b.onclick=()=>{
  state.cur=b.dataset.cur; lsSet('contabo_cur',state.cur); applyFx();
});

// Reconciliation badge
const rb=document.getElementById('reconBadge');
const cc=DATA.consistency||{};
if(cc.mismatch_count===0){
  rb.className='badge ok';
  rb.innerHTML='<span class="dot-i"></span>reconciled · <strong>'+cc.checked+'</strong> checks';
} else {
  rb.className='badge bad';
  rb.innerHTML='<span class="dot-i"></span><strong>'+cc.mismatch_count+'</strong> mismatch';
}

// FX badge + footer line
function fxAgeText(){
  if(!FX.rateDate && !FX.at) return '';
  const d=new Date(FX.rateDate || FX.at);
  const days=Math.floor((Date.now()-d.getTime())/86400000);
  if(days<=0) return 'today';
  if(days===1) return 'yesterday';
  if(days<7) return days+' days ago';
  return d.toISOString().slice(0,10);
}
function renderFxBadge(){
  const fxb=document.getElementById('fxBadge');
  if(!FX.rate){ fxb.className='badge warn';
    fxb.innerHTML='<span class="dot-i"></span>FX unavailable'; return; }
  const age=fxAgeText();
  fxb.className='badge ok';
  fxb.innerHTML='<span class="dot-i"></span>EUR→INR <strong>'+FX.rate.toFixed(2)+'</strong> · '+age;
  document.getElementById('fxLine').innerHTML =
    'FX: EUR→INR <b>'+FX.rate.toFixed(4)+'</b> ('+esc(FX.source||'')+', '+age+
    '), FX markup '+((FX.markup||0)*100).toFixed(1)+'%, owner markup '+((state.ownerMarkup||0)*100).toFixed(1)+'%';
}
renderFxBadge();

// Refresh FX from the same-origin Rust API (cache 12h). The embedded build-time
// rate remains the silent fallback for static reports and unavailable APIs; the
// browser never calls Frankfurter directly, avoiding CORS noise.
(function refreshFx(){
  try{
    const c=JSON.parse(lsGet('contabo_fx_v2')||'null');
    if(c && c.rate>0 && (Date.now()-c.ts) < 12*3600*1000){
      FX.rate=c.rate; FX.rateDate=c.rateDate; renderFxBadge(); render(); return;
    }
    if(typeof fetch!=='function'||location.protocol==='file:') return;
    const ac=new AbortController(); const to=setTimeout(()=>ac.abort(),4000);
    fetch('/api/v1/fx',{signal:ac.signal,headers:{accept:'application/json'}})
      .then(r=>r.ok?r.json():null).then(j=>{
        clearTimeout(to);
        const rt=j&&j.rates&&j.rates.INR;
        if(typeof rt==='number'&&rt>0){
          FX.rate=rt; FX.rateDate=j.date||null; FX.source='same-origin /api/v1/fx (Frankfurter/ECB)';
          lsSet('contabo_fx_v2',JSON.stringify({rate:rt,ts:Date.now(),rateDate:j.date||null}));
          renderFxBadge(); applyFx();
        }
      }).catch(()=>{});
  }catch{}
})();

// Keyboard shortcuts
addEventListener('keydown',e=>{
  if(e.target.matches('input,select,textarea')) {
    if(e.key==='Escape') e.target.blur();
    return;
  }
  if(e.key==='Escape'){ closeModal(); return; }
  if(e.key==='/'){ e.preventDefault(); document.getElementById('search').focus(); return; }
  if(e.key==='g'||e.key==='G'){ gstToggle.checked=!gstToggle.checked; gstToggle.dispatchEvent(new Event('change')); return; }
  if(e.key==='t'||e.key==='T'){ document.getElementById('themeBtn').click(); return; }
});

updateCurUI();
applyGstUi();

// Sticky-table-header offset: align thead under the (also-sticky) toolbar so
// neither overlaps the other. Recompute on resize and after font load.
function setStickyOffsets(){
  const tb=document.querySelector('.toolbar');
  if(tb) document.documentElement.style.setProperty('--thead-top', tb.offsetHeight+'px');
}
setStickyOffsets();
addEventListener('resize', setStickyOffsets);
if(document.fonts && document.fonts.ready) document.fonts.ready.then(setStickyOffsets);
</script>
</body>
</html>
`;

fs.writeFileSync(HTML_PATH, html);
const planCount = new Set(rows.map(r => r.plan_slug)).size;
const fxLine = payload.fx && payload.fx.eurInr
  ? `EUR→INR ${payload.fx.eurInr} (${payload.fx.rateDate || 'live'})`
  : 'unavailable (browser will retry)';
console.log(
  `Generated report.html · ${planCount} plans · ${rows.length} rows · ${genAt}` +
  ` · reconciliation: ${mismatches.length === 0 ? 'OK' : mismatches.length + ' mismatch(es)'}` +
  ` · FX: ${fxLine}`
);
})();
