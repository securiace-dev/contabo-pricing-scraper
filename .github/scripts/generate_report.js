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

const data    = JSON.parse(fs.readFileSync(QR_PATH, 'utf8'));
const plans   = data.plans;
const genAt   = data.generated_at;

// ── Load option catalog ──────────────────────────────────────────────────────
let optionCatalog = [];
if (fs.existsSync(DATASET_PATH)) {
  try {
    const dataset = JSON.parse(fs.readFileSync(DATASET_PATH, 'utf8'));
    optionCatalog = dataset.option_catalog || [];
  } catch { /* graceful: no addons if file is malformed */ }
}

// ── Build family → dimension → deduped options (label-key, price range) ──────
// Dedup by label only so options with different per-plan pricing collapse to
// a single row showing the price range (e.g. Windows: +€7.50–€70.00).
const slugToFamily = {};
for (const p of plans) slugToFamily[p.plan_slug] = p.family;

// Storage Type omitted: base plan table already shows each plan's storage spec.
// Storage (VDS only) is kept as it shows real upgrade tiers with pricing.
const DIM_ORDER = ['Image', 'Region', 'Networking', 'Data Protection', 'Storage'];

const familyAddons = {};
for (const opt of optionCatalog) {
  const family = slugToFamily[opt.plan_sku];
  if (!family) continue;
  familyAddons[family] ??= {};
  familyAddons[family][opt.dimension] ??= new Map();
  const delta = opt.monthly_price_delta ?? 0;
  const key   = opt.option_label;                       // label-only key
  if (!familyAddons[family][opt.dimension].has(key)) {
    familyAddons[family][opt.dimension].set(key, {
      label:       opt.option_label,
      category:    opt.category,
      deltaMin:    delta,
      deltaMax:    delta,
      isDefault:   !!opt.is_default,
      regionGroup: opt.region_group || null,
    });
  } else {
    const e = familyAddons[family][opt.dimension].get(key);
    e.deltaMin = Math.min(e.deltaMin, delta);
    e.deltaMax = Math.max(e.deltaMax, delta);
    if (opt.is_default) e.isDefault = true;
  }
}

// Sort each dimension's options: defaults first, then deltaMin asc, then alpha
for (const family of Object.keys(familyAddons)) {
  for (const dim of Object.keys(familyAddons[family])) {
    const arr = [...familyAddons[family][dim].values()];
    arr.sort((a, b) => {
      if (a.isDefault !== b.isDefault) return a.isDefault ? -1 : 1;
      if (a.deltaMin !== b.deltaMin) return a.deltaMin - b.deltaMin;
      return a.label.localeCompare(b.label);
    });
    familyAddons[family][dim] = arr;
  }
}

// ── Price formatting ──────────────────────────────────────────────────────────
function fmtDelta(min, max) {
  if (min === 0 && max === 0) return 'included';
  if (min === max)            return `+€${min.toFixed(2)}`;
  if (min === 0)              return `included – +€${max.toFixed(2)}`;
  return `+€${min.toFixed(2)}–€${max.toFixed(2)}`;
}

function lbl(o) {
  return o.isDefault ? `${o.label} *(default)*` : o.label;
}

// ── Render add-on <details> block for one family ──────────────────────────────
function renderAddons(family) {
  const addons = familyAddons[family];
  if (!addons) return [];
  if (!DIM_ORDER.some((d) => addons[d]?.length > 0)) return [];

  const out = [''];
  out.push('<details>');
  out.push(`<summary>📦 Add-ons — ${family}</summary>`);
  out.push('');

  for (const dim of DIM_ORDER) {
    const opts = addons[dim];
    if (!opts || opts.length === 0) continue;

    out.push(`### ${dim === 'Image' ? 'Image / Operating System' : dim}`);
    out.push('');

    if (dim === 'Image') {
      const CAT_ORDER = ['OS', 'Apps', 'Panels', 'Blockchain'];
      out.push('| Option | Type | +Monthly |');
      out.push('|--------|------|----------|');
      const rendered = new Set();
      for (const cat of CAT_ORDER) {
        for (const o of opts.filter((x) => x.category === cat)) {
          out.push(`| ${lbl(o)} | ${cat} | ${fmtDelta(o.deltaMin, o.deltaMax)} |`);
          rendered.add(o.label);
        }
      }
      for (const o of opts) {
        if (!rendered.has(o.label)) {
          out.push(`| ${lbl(o)} | ${o.category} | ${fmtDelta(o.deltaMin, o.deltaMax)} |`);
        }
      }

    } else if (dim === 'Region') {
      const GRP_ORDER = ['Europe', 'America', 'Asia', 'Australia', 'Other'];
      const groups = {};
      for (const o of opts) {
        const g = o.regionGroup || 'Other';
        (groups[g] = groups[g] || []).push(o);
      }
      out.push('| Region | Group | +Monthly |');
      out.push('|--------|-------|----------|');
      for (const grp of GRP_ORDER) {
        for (const o of (groups[grp] || [])) {
          out.push(`| ${lbl(o)} | ${grp} | ${fmtDelta(o.deltaMin, o.deltaMax)} |`);
        }
      }

    } else if (dim === 'Networking') {
      const NET_ORDER = ['Bandwidth', 'IPv4', 'Private Networking'];
      out.push('| Option | Sub-type | +Monthly |');
      out.push('|--------|----------|----------|');
      const rendered = new Set();
      for (const cat of NET_ORDER) {
        for (const o of opts.filter((x) => x.category === cat)) {
          out.push(`| ${lbl(o)} | ${cat} | ${fmtDelta(o.deltaMin, o.deltaMax)} |`);
          rendered.add(o.label);
        }
      }
      for (const o of opts) {
        if (!rendered.has(o.label)) {
          out.push(`| ${lbl(o)} | ${o.category} | ${fmtDelta(o.deltaMin, o.deltaMax)} |`);
        }
      }

    } else {
      // Generic: Data Protection, Storage Type, Storage
      out.push('| Option | +Monthly |');
      out.push('|--------|----------|');
      for (const o of opts) {
        out.push(`| ${lbl(o)} | ${fmtDelta(o.deltaMin, o.deltaMax)} |`);
      }
    }

    out.push('');
  }

  out.push('</details>');
  return out;
}

// ── Build families map ────────────────────────────────────────────────────────
const families = {};
for (const p of plans) {
  (families[p.family] = families[p.family] || []).push(p);
}

// ── Render PRICES.md ──────────────────────────────────────────────────────────
const eur = (pricing, period) => {
  const v = pricing?.[period]?.effective_monthly;
  return v != null ? `€${Number(v).toFixed(2)}` : '—';
};

const lines = [
  '# Contabo Pricing',
  '',
  `> **Last updated:** ${genAt}  `,
  '> Auto-refreshed twice daily · data from [contabo.com](https://contabo.com)',
  '',
];

for (const [family, fplans] of Object.entries(families)) {
  lines.push(`## ${family}`, '');
  lines.push(
    '| Plan | CPU | RAM | Storage | Port | 1 mo | 6 mo | 12 mo |',
    '|------|-----|-----|---------|------|------|------|-------|',
  );
  for (const p of [...fplans].sort((a, b) => a.plan_family_rank - b.plan_family_rank)) {
    lines.push(
      `| [${p.product_name}](${p.url}) ` +
      `| ${p.cpu_count ?? '—'} vCPU ` +
      `| ${p.ram_gb ?? '—'} GB ` +
      `| ${p.storage_primary_gb ?? '—'} GB ${p.storage_primary_type ?? ''} ` +
      `| ${p.port_speed_mbps ?? '—'} Mbps ` +
      `| ${eur(p.pricing, '1m')} | ${eur(p.pricing, '6m')} | ${eur(p.pricing, '12m')} |`,
    );
  }
  lines.push(...renderAddons(family));
  lines.push('');
}

lines.push(
  '---',
  '*Prices in EUR, excl. VAT. Base prices include default add-ons (EU region, Ubuntu, 1 IP). Generated by [contabo-pricing-scraper](../../)*',
);

fs.writeFileSync(OUT_PATH, lines.join('\n') + '\n');
console.log(`Generated PRICES.md · ${plans.length} plans · ${genAt}${optionCatalog.length ? ` · ${optionCatalog.length} add-on options` : ''}`);
