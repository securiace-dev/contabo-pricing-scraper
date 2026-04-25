#!/usr/bin/env node
'use strict';

const fs   = require('fs');
const path = require('path');

const OUTPUT_DIR   = path.resolve(__dirname, '../../data/output');
const QR_PATH      = path.join(OUTPUT_DIR, 'contabo_quick_reference.json');
const DATASET_PATH = path.join(OUTPUT_DIR, 'contabo_pricing_dataset.json');
const OUT_PATH     = path.resolve(__dirname, '../../PRICES.md');

if (!fs.existsSync(QR_PATH)) {
  console.log(`No quick reference data at ${QR_PATH} — skipping PRICES.md generation.`);
  process.exit(0);
}

const data  = JSON.parse(fs.readFileSync(QR_PATH, 'utf8'));
const plans = data.plans;
const genAt = data.generated_at;

let optionCatalog = [];
if (fs.existsSync(DATASET_PATH)) {
  try {
    const dataset = JSON.parse(fs.readFileSync(DATASET_PATH, 'utf8'));
    optionCatalog = dataset.option_catalog || [];
  } catch { /* graceful: no addons if file is malformed */ }
}

// ── Build per-plan addon map: slug → dimension → sorted options[] ─────────────
// "No X" default options (pure negation rows) are stripped — they add noise.
const NEGATION_DEFAULTS = new Set([
  'No Data Protection', 'No Backup Space', 'No Private Networking',
]);

const planAddons = {};
for (const opt of optionCatalog) {
  if (opt.dimension === 'Storage Type') continue;          // redundant with base table
  if (NEGATION_DEFAULTS.has(opt.option_label)) continue;  // strip "No X" noise
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

// Sort each dimension: defaults first, then delta asc, then alpha
for (const slug of Object.keys(planAddons)) {
  for (const dim of Object.keys(planAddons[slug])) {
    planAddons[slug][dim].sort((a, b) => {
      if (a.isDefault !== b.isDefault) return a.isDefault ? -1 : 1;
      if (a.delta !== b.delta)         return a.delta - b.delta;
      return a.label.localeCompare(b.label);
    });
  }
}

// ── Formatters ────────────────────────────────────────────────────────────────
const eur   = (v) => (v != null && v !== '') ? `€${Number(v).toFixed(2)}` : '—';
const epd   = (pricing, period) => eur(pricing?.[period]?.effective_monthly);
const delta = (d) => d === 0 ? 'free' : `+€${d.toFixed(2)}`;
const lbl   = (o) => o.isDefault ? `${o.label} *(default)*` : o.label;

// ── Render per-plan addon table ───────────────────────────────────────────────
function renderPlanAddons(slug) {
  const addons = planAddons[slug];
  if (!addons) return [];

  const rows = [];
  rows.push('| Add-on | +Monthly |');
  rows.push('|--------|----------|');

  // Image: OS → Apps → Panels → Blockchain
  const imgOpts = addons['Image'] || [];
  if (imgOpts.length > 0) {
    const CAT_ORDER = [
      ['OS',         'Operating System'],
      ['Apps',       'Apps'],
      ['Panels',     'Control Panels'],
      ['Blockchain', 'Blockchain'],
    ];
    const seen = new Set();
    for (const [cat, label] of CAT_ORDER) {
      const items = imgOpts.filter((o) => o.category === cat);
      if (items.length === 0) continue;
      rows.push(`| **${label}** | |`);
      for (const o of items) { rows.push(`| ${lbl(o)} | ${delta(o.delta)} |`); seen.add(o.label); }
    }
    // catch-all for any unexpected categories
    const leftover = imgOpts.filter((o) => !seen.has(o.label));
    if (leftover.length > 0) {
      rows.push('| **Other Images** | |');
      for (const o of leftover) rows.push(`| ${lbl(o)} | ${delta(o.delta)} |`);
    }
  }

  // Region: grouped Europe → America → Asia → Australia
  const regionOpts = addons['Region'] || [];
  if (regionOpts.length > 0) {
    rows.push('| **Region** | |');
    const GRP_ORDER = ['Europe', 'America', 'Asia', 'Australia', 'Other'];
    const grouped   = {};
    for (const o of regionOpts) {
      const g = o.regionGroup || 'Other';
      (grouped[g] ??= []).push(o);
    }
    for (const grp of GRP_ORDER) {
      for (const o of (grouped[grp] || [])) rows.push(`| ${lbl(o)} | ${delta(o.delta)} |`);
    }
  }

  // Networking: Bandwidth → IPv4 → Private Networking
  const netOpts = addons['Networking'] || [];
  if (netOpts.length > 0) {
    rows.push('| **Networking** | |');
    const NET_ORDER = ['Bandwidth', 'IPv4', 'Private Networking'];
    const grouped   = {};
    for (const o of netOpts) (grouped[o.category || 'Other'] ??= []).push(o);
    for (const cat of NET_ORDER) {
      for (const o of (grouped[cat] || [])) rows.push(`| ${lbl(o)} | ${delta(o.delta)} |`);
    }
  }

  // Data Protection (only paid rows have real value; defaults already stripped above)
  const dpOpts = addons['Data Protection'] || [];
  if (dpOpts.length > 0) {
    rows.push('| **Backup & Protection** | |');
    for (const o of dpOpts) rows.push(`| ${lbl(o)} | ${delta(o.delta)} |`);
  }

  // Storage upgrades (VDS only)
  const storageOpts = addons['Storage'] || [];
  if (storageOpts.length > 0) {
    rows.push('| **Storage Upgrade** | |');
    for (const o of storageOpts) rows.push(`| ${lbl(o)} | ${delta(o.delta)} |`);
  }

  // Return nothing if we only have the header rows
  return rows.length > 2 ? rows : [];
}

// ── Build families map ────────────────────────────────────────────────────────
const families = {};
for (const p of plans) (families[p.family] ??= []).push(p);

// ── Render PRICES.md ──────────────────────────────────────────────────────────
const FAMILY_EMOJI = { 'Cloud VPS': '☁️', 'Storage VPS': '💾', 'Cloud VDS': '🖥️' };

const lines = [
  '# Contabo Pricing',
  '',
  `> **Last updated:** ${genAt}  `,
  '> Auto-refreshed twice daily · data from [contabo.com](https://contabo.com)',
  '',
];

for (const [family, fplans] of Object.entries(families)) {
  const emoji  = FAMILY_EMOJI[family] || '';
  const sorted = [...fplans].sort((a, b) => a.plan_family_rank - b.plan_family_rank);

  lines.push(`## ${emoji} ${family}`, '');

  // ── Overview comparison table ─────────────────────────────────────────────
  lines.push('| Plan | vCPU | RAM | Storage | Port | 1 mo | 6 mo | 12 mo |');
  lines.push('|------|------|-----|---------|------|------|------|-------|');
  for (const p of sorted) {
    lines.push(
      `| [${p.product_name}](${p.url})` +
      ` | ${p.cpu_count ?? '—'}` +
      ` | ${p.ram_gb ?? '—'} GB` +
      ` | ${p.storage_primary_gb ?? '—'} GB ${p.storage_primary_type ?? ''}` +
      ` | ${p.port_speed_mbps ?? '—'} Mbps` +
      ` | ${epd(p.pricing, '1m')} | ${epd(p.pricing, '6m')} | ${epd(p.pricing, '12m')} |`,
    );
  }
  lines.push('');

  // ── Per-plan detail cards ─────────────────────────────────────────────────
  for (const p of sorted) {
    const p1m  = p.pricing?.['1m'];
    const p6m  = p.pricing?.['6m'];
    const p12m = p.pricing?.['12m'];

    lines.push(`### [${p.product_name}](${p.url})`);
    lines.push('');
    lines.push(
      `**${p.cpu_count ?? '—'} vCPU · ${p.ram_gb ?? '—'} GB RAM · ` +
      `${p.storage_primary_gb ?? '—'} GB ${p.storage_primary_type ?? ''} · ` +
      `${p.port_speed_mbps ?? '—'} Mbps**`,
    );
    lines.push('');

    // Pricing table — include setup fee row only when at least one period charges it
    const hasSetup = [p1m, p6m, p12m].some((x) => x?.setup_fee > 0);
    lines.push('| | 1 Month | 6 Months | 12 Months |');
    lines.push('|---|---------|----------|-----------|');
    lines.push(
      `| **Monthly** | ${eur(p1m?.effective_monthly)} | ${eur(p6m?.effective_monthly)} | **${eur(p12m?.effective_monthly)}** |`,
    );
    if (hasSetup) {
      const sf = (x) => (x?.setup_fee > 0 ? eur(x.setup_fee) : '—');
      lines.push(`| Setup fee | ${sf(p1m)} | ${sf(p6m)} | — |`);
    }
    lines.push(
      `| Billed total | ${eur(p1m?.total_period)} | ${eur(p6m?.total_period)} | ${eur(p12m?.total_period)} |`,
    );
    lines.push('');

    // Add-ons
    if (optionCatalog.length > 0) {
      const addonRows = renderPlanAddons(p.plan_slug);
      if (addonRows.length > 0) {
        lines.push(...addonRows);
        lines.push('');
      }
    }

    lines.push('---', '');
  }
}

lines.push(
  '*Prices in EUR, excl. VAT. Base prices at EU region, Ubuntu OS, 1 IP. Generated by [contabo-pricing-scraper](../../)*',
);

fs.writeFileSync(OUT_PATH, lines.join('\n') + '\n');
console.log(`Generated PRICES.md · ${plans.length} plans · ${genAt}${optionCatalog.length ? ` · ${optionCatalog.length} add-on options` : ''}`);
