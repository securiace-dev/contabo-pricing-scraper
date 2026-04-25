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

// "No X" negation defaults are noise — absence of a feature doesn't need a row
const NEGATION_DEFAULTS = new Set([
  'No Data Protection', 'No Backup Space', 'No Private Networking',
]);

// ── Build per-plan addon map: slug → dimension → sorted options[] ─────────────
const planAddons = {};
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

// ── Formatters ────────────────────────────────────────────────────────────────
const eur = (v) => (v != null && v !== '') ? `€${Number(v).toFixed(2)}` : '—';
const epd = (pricing, period) => eur(pricing?.[period]?.effective_monthly);
const p$  = (d) => d === 0 ? 'free' : `\`+€${d.toFixed(2)}\``;

// ── Build compact add-on block for one plan ───────────────────────────────────
function renderPlanAddons(slug) {
  const addons = planAddons[slug];
  if (!addons) return [];

  const lines = [];

  // ── Image / OS / Apps / Panels / Blockchain ───────────────────────────────
  const imgOpts = addons['Image'] || [];
  if (imgOpts.length > 0) {
    const byCategory = (cat) => imgOpts.filter((o) => o.category === cat);
    const inlineList = (items) => {
      const free = items.filter((o) => o.delta === 0);
      const paid = items.filter((o) => o.delta > 0);
      const parts = [];
      if (free.length > 0)
        parts.push(free.map((o) => o.isDefault ? `${o.label} *(default)*` : o.label).join(', '));
      if (paid.length > 0)
        parts.push('Paid: ' + paid.map((o) => `${o.label} \`+€${o.delta.toFixed(2)}\``).join(' · '));
      return parts.join('  ·  ');
    };

    const osItems  = byCategory('OS');
    const appItems = byCategory('Apps');
    const panItems = byCategory('Panels');
    const bcItems  = byCategory('Blockchain');

    if (osItems.length > 0)  lines.push(`**OS** — ${inlineList(osItems)}`);
    if (appItems.length > 0) lines.push(`**Apps** — ${inlineList(appItems)}`);
    if (panItems.length > 0) lines.push(`**Control Panels** — ${inlineList(panItems)}`);
    if (bcItems.length > 0)  lines.push(`**Blockchain** — ${inlineList(bcItems)}`);

    // catch-all for unexpected categories
    const known = new Set(['OS', 'Apps', 'Panels', 'Blockchain']);
    const other = imgOpts.filter((o) => !known.has(o.category));
    if (other.length > 0) {
      lines.push(`**Other** — ${other.map((o) => `${o.isDefault ? o.label + ' *(default)*' : o.label} ${p$(o.delta)}`).join(', ')}`);
    }
  }

  // ── Region — horizontal table for instant visual comparison ──────────────
  const regionOpts = addons['Region'] || [];
  if (regionOpts.length > 0) {
    lines.push('');

    const GRP_ORDER = ['Europe', 'America', 'Asia', 'Australia', 'Other'];
    const grouped   = {};
    for (const o of regionOpts) (grouped[o.regionGroup || 'Other'] ??= []).push(o);
    const ordered = GRP_ORDER.flatMap((g) => grouped[g] || []);

    // Compact region labels — keep readable but short enough for table cells
    const shorten = (label) => label
      .replace('European Union', 'EU')
      .replace('United Kingdom', 'UK')
      .replace('United States (Central)', 'US-Central')
      .replace('United States (West)',    'US-West')
      .replace('United States (East)',    'US-East');

    const hdrs   = ordered.map((o) => shorten(o.label) + (o.isDefault ? ' *(def.)*' : ''));
    const prices = ordered.map((o) => o.delta === 0 ? 'free' : `+€${o.delta.toFixed(2)}`);

    lines.push(`| ${hdrs.join(' | ')} |`);
    lines.push(`|${hdrs.map(() => ':---:').join('|')}|`);
    lines.push(`| ${prices.join(' | ')} |`);
    lines.push('');
  }

  // ── Networking ────────────────────────────────────────────────────────────
  const netOpts = addons['Networking'] || [];
  if (netOpts.length > 0) {
    const bw   = netOpts.filter((o) => o.category === 'Bandwidth');
    const ipv4 = netOpts.filter((o) => o.category === 'IPv4');
    const pvt  = netOpts.filter((o) => o.category === 'Private Networking');

    const parts = [];

    const freeBW = bw.filter((o) => o.delta === 0).map((o) => o.isDefault ? `${o.label} *(default)*` : o.label).join(' / ');
    const paidBW = bw.filter((o) => o.delta > 0).map((o) => `${o.label} \`+€${o.delta.toFixed(2)}\``).join(' · ');
    if (freeBW || paidBW) parts.push('Bandwidth: ' + [freeBW, paidBW].filter(Boolean).join(' · '));

    const extraIP = ipv4.filter((o) => o.delta > 0).map((o) => `${o.label} \`+€${o.delta.toFixed(2)}\``).join(' · ');
    if (extraIP) parts.push(`Extra IPv4: ${extraIP}`);

    const pvtPaid = pvt.filter((o) => o.delta > 0).map((o) => `${o.label} \`+€${o.delta.toFixed(2)}\``).join(' · ');
    if (pvtPaid) parts.push(`Private Network: ${pvtPaid}`);

    if (parts.length > 0) lines.push(`**Networking** — ${parts.join(' · ')}`);
  }

  // ── Backup / Data Protection ──────────────────────────────────────────────
  const dpPaid = (addons['Data Protection'] || []).filter((o) => o.delta > 0);
  if (dpPaid.length > 0) {
    lines.push(`**Backup** — ${dpPaid.map((o) => `${o.label} \`+€${o.delta.toFixed(2)}\``).join(' · ')}`);
  }

  // ── Storage upgrades (VDS only) ───────────────────────────────────────────
  const storOpts = addons['Storage'] || [];
  if (storOpts.length > 0) {
    const def  = storOpts.find((o) => o.isDefault);
    const upgr = storOpts.filter((o) => !o.isDefault);
    let line   = `**Storage** — ${def?.label ?? '—'} *(included)*`;
    if (upgr.length > 0)
      line += ' · Upgrades: ' + upgr.map((o) => `${o.label} \`+€${o.delta.toFixed(2)}\``).join(' · ');
    lines.push(line);
  }

  return lines;
}

// ── Build families map ────────────────────────────────────────────────────────
const families = {};
for (const p of plans) (families[p.family] ??= []).push(p);

// ── Render PRICES.md ──────────────────────────────────────────────────────────
const FAMILY_EMOJI = { 'Cloud VPS': '☁️', 'Storage VPS': '💾', 'Cloud VDS': '🖥️' };

const out = [
  '# Contabo Pricing',
  '',
  `> **Last updated:** ${genAt}  `,
  '> Auto-refreshed twice daily · data from [contabo.com](https://contabo.com)',
  '',
];

for (const [family, fplans] of Object.entries(families)) {
  const emoji  = FAMILY_EMOJI[family] || '';
  const sorted = [...fplans].sort((a, b) => a.plan_family_rank - b.plan_family_rank);

  out.push(`## ${emoji} ${family}`, '');

  // Overview comparison table
  out.push('| Plan | vCPU | RAM | Storage | Port | 1 mo | 6 mo | 12 mo |');
  out.push('|------|:----:|:---:|---------|:----:|-----:|-----:|------:|');
  for (const p of sorted) {
    out.push(
      `| [${p.product_name}](${p.url})` +
      ` | ${p.cpu_count ?? '—'}` +
      ` | ${p.ram_gb ?? '—'} GB` +
      ` | ${p.storage_primary_gb ?? '—'} GB ${p.storage_primary_type ?? ''}` +
      ` | ${p.port_speed_mbps ?? '—'} Mbps` +
      ` | ${epd(p.pricing, '1m')} | ${epd(p.pricing, '6m')} | ${epd(p.pricing, '12m')} |`,
    );
  }
  out.push('');

  // Per-plan detail cards
  for (const p of sorted) {
    const p1m  = p.pricing?.['1m'];
    const p6m  = p.pricing?.['6m'];
    const p12m = p.pricing?.['12m'];

    out.push(`### [${p.product_name}](${p.url})`);
    out.push(`> **${p.cpu_count ?? '—'} vCPU · ${p.ram_gb ?? '—'} GB RAM · ${p.storage_primary_gb ?? '—'} GB ${p.storage_primary_type ?? ''} · ${p.port_speed_mbps ?? '—'} Mbps**`);
    out.push('');

    // Pricing table — setup fee row only appears when a period actually charges it
    const hasSetup = [p1m, p6m, p12m].some((x) => x?.setup_fee > 0);
    out.push('| | 1 Month | 6 Months | 12 Months |');
    out.push('|---|---------|----------|-----------|');
    out.push(`| **Monthly** | ${eur(p1m?.effective_monthly)} | ${eur(p6m?.effective_monthly)} | **${eur(p12m?.effective_monthly)}** |`);
    if (hasSetup) {
      const sf = (x) => x?.setup_fee > 0 ? eur(x.setup_fee) : '—';
      out.push(`| Setup fee | ${sf(p1m)} | ${sf(p6m)} | — |`);
    }
    out.push(`| Billed total | ${eur(p1m?.total_period)} | ${eur(p6m?.total_period)} | ${eur(p12m?.total_period)} |`);
    out.push('');

    // Compact add-ons
    if (optionCatalog.length > 0) {
      const addonLines = renderPlanAddons(p.plan_slug);
      if (addonLines.length > 0) {
        out.push(...addonLines);
        out.push('');
      }
    }

    out.push('---', '');
  }
}

out.push('*Prices in EUR, excl. VAT. Base prices at EU region, Ubuntu OS, 1 IP. Generated by [contabo-pricing-scraper](../../)*');

fs.writeFileSync(OUT_PATH, out.join('\n') + '\n');
console.log(`Generated PRICES.md · ${plans.length} plans · ${genAt}${optionCatalog.length ? ` · ${optionCatalog.length} add-on options` : ''}`);
