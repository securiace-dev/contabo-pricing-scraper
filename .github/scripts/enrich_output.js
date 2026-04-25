#!/usr/bin/env node
'use strict';

const fs   = require('fs');
const path = require('path');

const OUTPUT_DIR = path.resolve(__dirname, '../../data/output');

const p = (f) => path.join(OUTPUT_DIR, f);

// ── Load anchor metadata from pricing_dataset (always has schema/version info) ─
const datasetRaw = JSON.parse(fs.readFileSync(p('contabo_pricing_dataset.json'), 'utf8'));
const { schema_version, scraper_version, generated_at } = datasetRaw;

// ── Load quick_reference for slug→family/name map ─────────────────────────────
const qrRaw = JSON.parse(fs.readFileSync(p('contabo_quick_reference.json'), 'utf8'));
const qrPlans = qrRaw.plans || [];

const slugToFamily = {};
const slugToName   = {};
for (const plan of qrPlans) {
  slugToFamily[plan.plan_slug] = plan.family;
  slugToName[plan.plan_slug]   = plan.product_name;
}

// ── CSV helpers ───────────────────────────────────────────────────────────────
function csvCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return (s.includes(',') || s.includes('"') || s.includes('\n'))
    ? '"' + s.replace(/"/g, '""') + '"'
    : s;
}
const csvRow = (fields) => fields.map(csvCell).join(',');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1 — Wrap contabo_base_plans.json
// ─────────────────────────────────────────────────────────────────────────────
const bpRaw     = JSON.parse(fs.readFileSync(p('contabo_base_plans.json'), 'utf8'));
const bpArray   = Array.isArray(bpRaw) ? bpRaw : (bpRaw.plans || []);
const bpWrapped = {
  schema_version,
  scraper_version,
  generated_at,
  plan_count: bpArray.length,
  plans: bpArray,
};
fs.writeFileSync(p('contabo_base_plans.json'), JSON.stringify(bpWrapped, null, 2) + '\n');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2 — Wrap contabo_configs.json
// ─────────────────────────────────────────────────────────────────────────────
const cfgRaw    = JSON.parse(fs.readFileSync(p('contabo_configs.json'), 'utf8'));
const cfgPlans  = (cfgRaw.plans && typeof cfgRaw.plans === 'object' && !Array.isArray(cfgRaw.plans))
  ? cfgRaw.plans   // already wrapped
  : cfgRaw;        // bare URL-keyed map
const cfgWrapped = {
  schema_version,
  scraper_version,
  generated_at,
  plan_count: Object.keys(cfgPlans).length,
  plans: cfgPlans,
};
fs.writeFileSync(p('contabo_configs.json'), JSON.stringify(cfgWrapped, null, 2) + '\n');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3 — Enrich contabo_quick_reference.json
// ─────────────────────────────────────────────────────────────────────────────
const enrichedPlans = qrPlans.map((plan) => {
  const p1m  = plan.pricing?.['1m']?.effective_monthly  ?? null;
  const p6m  = plan.pricing?.['6m']?.effective_monthly  ?? null;
  const p12m = plan.pricing?.['12m']?.effective_monthly ?? null;

  const savings6m  = (p1m && p6m)  ? Math.round((1 - p6m  / p1m) * 100) : null;
  const savings12m = (p1m && p12m) ? Math.round((1 - p12m / p1m) * 100) : null;

  // Best price = lowest effective_monthly among visible periods
  const candidates = [
    { period: '1m',  price: p1m  },
    { period: '6m',  price: p6m  },
    { period: '12m', price: p12m },
  ].filter((c) => c.price != null).sort((a, b) => a.price - b.price);
  const best = candidates[0] ?? null;

  // Add total_period_cost alias alongside existing total_period
  const pricingEnriched = {};
  for (const [key, val] of Object.entries(plan.pricing || {})) {
    pricingEnriched[key] = {
      ...val,
      total_period_cost: val.total_period ?? null,
    };
  }

  return {
    ...plan,
    pricing: pricingEnriched,
    savings_6m_pct:    savings6m,
    savings_12m_pct:   savings12m,
    best_price_eur:    best?.price    ?? null,
    best_price_period: best?.period   ?? null,
  };
});

const qrEnriched = {
  schema_version,
  scraper_version,
  generated_at: qrRaw.generated_at,
  plan_count:   enrichedPlans.length,
  plans:        enrichedPlans,
};
fs.writeFileSync(p('contabo_quick_reference.json'), JSON.stringify(qrEnriched, null, 2) + '\n');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 4 — Enrich option_catalog in contabo_pricing_dataset.json
// ─────────────────────────────────────────────────────────────────────────────
const enrichedCatalog = (datasetRaw.option_catalog || []).map((opt) => ({
  ...opt,
  plan_slug:   opt.plan_sku,                           // canonical alias
  plan_family: slugToFamily[opt.plan_sku] ?? null,
  plan_name:   slugToName[opt.plan_sku]   ?? null,
}));

const datasetEnriched = {
  ...datasetRaw,
  option_catalog: enrichedCatalog,
};
fs.writeFileSync(p('contabo_pricing_dataset.json'), JSON.stringify(datasetEnriched, null, 2) + '\n');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 5 — Regenerate contabo_base_plans.csv (with parsed spec columns + 1m_total)
// ─────────────────────────────────────────────────────────────────────────────
const BP_HEADERS = [
  'family', 'product_name', 'product_slug', 'product_url', 'fetched_at',
  'cpu', 'ram', 'base_storage', 'snapshots', 'port', 'base_monthly_price',
  'cpu_count', 'ram_gb', 'storage_primary_gb', 'storage_primary_type', 'port_speed_mbps',
  '1m_effective_monthly', '1m_setup_fee', '1m_total',
  '3m_effective_monthly', '3m_setup_fee', '3m_total',
  '6m_effective_monthly', '6m_setup_fee', '6m_total', '6m_discount',
  '12m_effective_monthly', '12m_setup_fee', '12m_total', '12m_discount',
];

const bpRows = [BP_HEADERS.join(',')];
for (const plan of bpArray) {
  const periods = {};
  for (const per of (plan.periods || [])) periods[per.months] = per;
  const sp = plan.specs_parsed || {};

  bpRows.push(csvRow([
    plan.family, plan.product_name, plan.product_slug, plan.product_url, plan.fetched_at,
    plan.cpu, plan.ram, plan.base_storage, plan.snapshots, plan.port, plan.base_monthly_price,
    sp.cpu_count           ?? '',
    sp.ram_gb              ?? '',
    sp.storage_primary_gb  ?? '',
    sp.storage_primary_type ?? '',
    sp.port_speed_mbps     ?? '',
    periods[1]?.effective_monthly  ?? '', periods[1]?.setup_fee ?? '', periods[1]?.total_period_cost ?? '',
    periods[3]?.effective_monthly  ?? '', periods[3]?.setup_fee ?? '', periods[3]?.total_period_cost ?? '',
    periods[6]?.effective_monthly  ?? '', periods[6]?.setup_fee ?? '', periods[6]?.total_period_cost ?? '', periods[6]?.discount_total  ?? '',
    periods[12]?.effective_monthly ?? '', periods[12]?.setup_fee ?? '', periods[12]?.total_period_cost ?? '', periods[12]?.discount_total ?? '',
  ]));
}
fs.writeFileSync(p('contabo_base_plans.csv'), bpRows.join('\n') + '\n');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 6 — Regenerate contabo_option_catalog.csv (with plan_slug/family/name)
// ─────────────────────────────────────────────────────────────────────────────
const OC_HEADERS = [
  'plan_sku', 'plan_slug', 'plan_family', 'plan_name',
  'dimension', 'category', 'option_label',
  'monthly_price_delta', 'setup_fee_delta',
  'region_group', 'country', 'country_code', 'subregion',
  'is_default', 'currency',
];

const ocRows = [OC_HEADERS.join(',')];
for (const opt of enrichedCatalog) {
  ocRows.push(csvRow([
    opt.plan_sku,
    opt.plan_slug    ?? opt.plan_sku,
    opt.plan_family  ?? '',
    opt.plan_name    ?? '',
    opt.dimension, opt.category, opt.option_label,
    opt.monthly_price_delta, opt.setup_fee_delta,
    opt.region_group  ?? '',
    opt.country       ?? '',
    opt.country_code  ?? '',
    opt.subregion     ?? '',
    opt.is_default, opt.currency,
  ]));
}
fs.writeFileSync(p('contabo_option_catalog.csv'), ocRows.join('\n') + '\n');

// ─────────────────────────────────────────────────────────────────────────────
// STEP 7 — Delete stale *_enhanced files (from previous scraper version)
// ─────────────────────────────────────────────────────────────────────────────
const STALE = [
  'contabo_base_plans_enhanced.json',
  'contabo_base_plans_enhanced.csv',
  'contabo_configs_enhanced.json',
  'contabo_pricing_dataset_enhanced.json',
  'contabo_option_catalog_enhanced.csv',
];
let deleted = 0;
for (const f of STALE) {
  const fp = path.join(OUTPUT_DIR, f);
  if (fs.existsSync(fp)) { fs.unlinkSync(fp); deleted++; }
}

// ─────────────────────────────────────────────────────────────────────────────
console.log([
  `Enriched output files:`,
  `  base_plans.json   → wrapped (${bpArray.length} plans)`,
  `  configs.json      → wrapped (${Object.keys(cfgPlans).length} plans)`,
  `  quick_reference   → +schema_version, +savings, +best_price (${enrichedPlans.length} plans)`,
  `  pricing_dataset   → +plan_slug/family/name on ${enrichedCatalog.length} catalog entries`,
  `  base_plans.csv    → +cpu_count/ram_gb/storage/port parsed cols, +1m_total`,
  `  option_catalog.csv → +plan_slug/family/name cols`,
  `  deleted ${deleted} stale *_enhanced files`,
].join('\n'));
