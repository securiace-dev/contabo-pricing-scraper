#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const OUTPUT_DIR = '/Users/kritananda';

const PLAN_URLS = [
  'https://contabo.com/en/vps/cloud-vps-10/',
  'https://contabo.com/en/vps/cloud-vps-20/',
  'https://contabo.com/en/vps/cloud-vps-30/',
  'https://contabo.com/en/vps/cloud-vps-40/',
  'https://contabo.com/en/vps/cloud-vps-50/',
  'https://contabo.com/en/vps/cloud-vps-60/',
  'https://contabo.com/en/storage-vps/storage-vps-10/',
  'https://contabo.com/en/storage-vps/storage-vps-20/',
  'https://contabo.com/en/storage-vps/storage-vps-30/',
  'https://contabo.com/en/storage-vps/storage-vps-40/',
  'https://contabo.com/en/storage-vps/storage-vps-50/',
  'https://contabo.com/en/vds/vds-s/',
  'https://contabo.com/en/vds/vds-m/',
  'https://contabo.com/en/vds/vds-l/',
  'https://contabo.com/en/vds/vds-xl/',
  'https://contabo.com/en/vds/vds-xxl/',
];

const REGION_RULES = [
  [/^European Union$/i, { region_group: 'Europe', country: 'European Union', country_code: 'EU' }],
  [/^United Kingdom$/i, { region_group: 'Europe', country: 'United Kingdom', country_code: 'UK' }],
  [/^Germany$/i, { region_group: 'Europe', country: 'Germany', country_code: 'DE' }],
  [/^Canada/i, { region_group: 'America', country: 'Canada', country_code: 'CA' }],
  [/^United States \(([^)]+)\)$/i, (m) => ({ region_group: 'America', country: `United States (${m[1]})`, country_code: 'US', subregion: m[1] })],
  [/^United States$/i, { region_group: 'America', country: 'United States', country_code: 'US' }],
  [/^Asia \(([^)]+)\)$/i, (m) => ({ region_group: 'Asia', country: m[1], country_code: m[1].slice(0, 2).toUpperCase() })],
  [/^Australia \(([^)]+)\)$/i, { region_group: 'Australia', country: 'Australia', country_code: 'AU' }],
  [/^Australia$/i, { region_group: 'Australia', country: 'Australia', country_code: 'AU' }],
];

const IGNORE_TITLE_PATTERNS = [
  /^cPanel\/WHM \((?!5 accounts\))/i,
  /Object Storage/i,
  /FTP Storage/i,
  /Monitoring/i,
  /^Managed$/i,
  /^Unmanaged$/i,
  /SSL certificate/i,
  /Firewall/i,
  /DevOps Features/i,
  /Custom Images Storage/i,
  /^Password$/i,
  /SSH Keys/i,
  /^No Firewall$/i,
  /^No license required$/i,
  /^None$/i,
  /^Use your existing Custom Image Storage$/i,
  /^Backup Space$/i,
  /In order to use SSH Keys/i,
];

function escapeCsv(value) {
  const text = value == null ? '' : String(value);
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function slugFromUrl(url) {
  return url.replace(/\/$/, '').split('/').pop();
}

function titleFromSlug(slug) {
  if (slug.startsWith('cloud-vps-')) {
    return `Cloud VPS ${slug.split('-').pop()}`;
  }
  if (slug.startsWith('storage-vps-')) {
    return `Storage VPS ${slug.split('-').pop()}`;
  }
  if (slug.startsWith('vds-')) {
    return `Cloud VDS ${slug.split('-').pop().toUpperCase()}`;
  }
  return slug;
}

function familyFromProduct(product) {
  if (product.type === 'vps') return 'Cloud VPS';
  if (product.type === 'storage-vps') return 'Storage VPS';
  if (product.type === 'vds') return 'Cloud VDS';
  return product.type;
}

function monthlyPriceForPeriod(baseMonthly, period) {
  if (!period) return null;
  const total = baseMonthly * period.length - (period.discount?.EUR || 0);
  return Number((total / period.length).toFixed(2));
}

function extractSapper(html) {
  const start = html.indexOf('__SAPPER__=');
  const endMarker = '};(function(){try{eval("async function x(){}")';
  const end = html.indexOf(endMarker);
  if (start === -1 || end === -1) {
    throw new Error('Unable to locate __SAPPER__ payload');
  }
  const script = 'var window={}; var document={}; ' + html.slice(start, end + 1);
  const ctx = {};
  vm.createContext(ctx);
  vm.runInContext(script, ctx, { timeout: 20000 });
  return ctx.__SAPPER__;
}

function extractPasswordRules(html) {
  const match = html.match(/(\d+)-(\d+) alphanumeric characters \(no special characters\)/i);
  if (!match) return null;
  return {
    min_length: Number(match[1]),
    max_length: Number(match[2]),
    alphanumeric_only: true,
    no_special_chars: true,
  };
}

function classifyRegion(title) {
  for (const [pattern, value] of REGION_RULES) {
    const match = title.match(pattern);
    if (match) {
      return typeof value === 'function' ? value(match) : value;
    }
  }
  return null;
}

function isIgnoredTitle(title) {
  return IGNORE_TITLE_PATTERNS.some((pattern) => pattern.test(title));
}

function classifyAddon(addon, product, html) {
  const title = (addon.title || '').trim();
  if (!title) return { action: 'skip', reason: 'empty_title' };
  if (isIgnoredTitle(title)) return { action: 'skip', reason: 'ignored_addon' };

  const region = classifyRegion(title);
  if (region) {
    return {
      action: 'include',
      dimension: 'Region',
      category: region.region_group,
      option_label: region.country,
      region_group: region.region_group,
      country: region.country,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/^(\d+(?:\.\d+)?)\s*(GB|TB)\s*(NVMe|SSD)(?: SSD)?$/i.test(title)) {
    const m = title.match(/^(\d+(?:\.\d+)?)\s*(GB|TB)\s*(NVMe|SSD)/i);
    return {
      action: 'include',
      dimension: product.type === 'vds' ? 'Storage' : 'Storage Type',
      category: m[3].toUpperCase(),
      option_label: `${m[1]} ${m[2]} ${m[3].toUpperCase()}`,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/^Auto Backup$/i.test(title)) {
    return {
      action: 'include',
      dimension: 'Data Protection',
      category: 'Auto Backup',
      option_label: 'Auto Backup',
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/^No Private Networking$/i.test(title) || /^Private Networking Enabled$/i.test(title)) {
    return {
      action: 'include',
      dimension: 'Networking',
      category: 'Private Networking',
      option_label: title,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/Traffic/i.test(title) || /Out \+ Unlimited In/i.test(title)) {
    return {
      action: 'include',
      dimension: 'Networking',
      category: 'Bandwidth',
      option_label: title,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/IP Address|IP adress/i.test(title)) {
    return {
      action: 'include',
      dimension: 'Networking',
      category: 'IPv4',
      option_label: title,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/^Windows Server/i.test(title) || /^(Ubuntu|Debian|AlmaLinux|Rocky Linux|Arch Linux|FreeBSD)/i.test(title) || /Custom Image/i.test(title)) {
    return {
      action: 'include',
      dimension: 'Image',
      category: 'OS',
      option_label: title,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/^cPanel\/WHM \(5 accounts\)$/i.test(title) || /^Plesk Obsidian Web Admin Edition$/i.test(title) || /^Plesk Obsidian Web Pro Edition$/i.test(title) || /^Plesk Obsidian Web Host Edition$/i.test(title) || /^Webmin( \+ LAMP)?$/i.test(title)) {
    return {
      action: 'include',
      dimension: 'Image',
      category: 'Panels',
      option_label: /^Plesk Obsidian Web Admin Edition$/i.test(title)
        ? 'Plesk + Linux'
        : /^Webmin$/i.test(title)
          ? 'Webmin'
          : title,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  if (/Server$/i.test(title) || /^(Docker|LAMP|Coolify|DeepSeek|IPFS Node|Flux Node|Horizen Node|Bitcoin Node|Ethereum Staking Node)$/i.test(title)) {
    const category = /Node$/i.test(title) || /Staking Node$/i.test(title) ? 'Blockchain' : 'Apps';
    return {
      action: 'include',
      dimension: 'Image',
      category,
      option_label: /^Gitlab Server$/i.test(title) ? 'GitLab Server' : title,
      monthly_price_delta: addon.price?.EUR || 0,
      setup_fee_delta: addon.setupPrice?.EUR || 0,
    };
  }

  return { action: 'gap', reason: 'unclassified', title };
}

function injectDefaults(product, html, classified) {
  const titles = new Set(classified.map((item) => `${item.dimension}|${item.option_label}`));
  const add = (item) => {
    const key = `${item.dimension}|${item.option_label}`;
    if (!titles.has(key)) {
      titles.add(key);
      classified.push(item);
    }
  };

  add({
    plan_sku: product.slug,
    currency: 'EUR',
    dimension: 'Data Protection',
    category: 'None',
    option_label: product.type === 'vds' ? 'No Backup Space' : 'No Data Protection',
    monthly_price_delta: 0,
    setup_fee_delta: 0,
    is_default: true,
  });

  add({
    plan_sku: product.slug,
    currency: 'EUR',
    dimension: 'Networking',
    category: 'Private Networking',
    option_label: 'No Private Networking',
    monthly_price_delta: 0,
    setup_fee_delta: 0,
    is_default: true,
  });

  add({
    plan_sku: product.slug,
    currency: 'EUR',
    dimension: 'Networking',
    category: 'Bandwidth',
    option_label: 'Unlimited Traffic',
    monthly_price_delta: 0,
    setup_fee_delta: 0,
    is_default: true,
  });

  add({
    plan_sku: product.slug,
    currency: 'EUR',
    dimension: 'Networking',
    category: 'IPv4',
    option_label: '1 IP Address',
    monthly_price_delta: 0,
    setup_fee_delta: 0,
    is_default: true,
  });

  const storageSpec = (product.specs || []).find((spec) => spec.type === 'storage');
  if (storageSpec) {
    const primary = storageSpec.title?.replace(/ SSD$/i, ' SSD').trim();
    if (primary) {
      add({
        plan_sku: product.slug,
        currency: 'EUR',
        dimension: product.type === 'vds' ? 'Storage' : 'Storage Type',
        category: /NVMe/i.test(primary) ? 'NVME' : 'SSD',
        option_label: primary.replace(/NVMe SSD/i, 'NVMe'),
        monthly_price_delta: 0,
        setup_fee_delta: 0,
        is_default: true,
      });
    }
    if (storageSpec.subtitle) {
      add({
        plan_sku: product.slug,
        currency: 'EUR',
        dimension: product.type === 'vds' ? 'Storage' : 'Storage Type',
        category: /NVMe/i.test(storageSpec.subtitle) ? 'NVME' : 'SSD',
        option_label: storageSpec.subtitle.replace(/^or\s+/i, '').replace(/NVMe SSD/i, 'NVMe'),
        monthly_price_delta: 0,
        setup_fee_delta: 0,
        is_default: titles.has(`${product.type === 'vds' ? 'Storage' : 'Storage Type'}|${primary}`) ? false : true,
      });
    }
  }

  const ubuntuVisible = /Ubuntu/i.test(html);
  if (ubuntuVisible) {
    add({
      plan_sku: product.slug,
      currency: 'EUR',
      dimension: 'Image',
      category: 'OS',
      option_label: 'Ubuntu 24.04',
      monthly_price_delta: 0,
      setup_fee_delta: 0,
      is_default: true,
    });
  }

  if ((product.type === 'vds' || product.type === 'vps' || product.type === 'storage-vps') && /Windows Server/i.test(html)) {
    // leave actual paid Windows variants from addons, but ensure at least one visible label exists
    if (![...titles].some((key) => key.startsWith('Image|Windows Server'))) {
      add({
        plan_sku: product.slug,
        currency: 'EUR',
        dimension: 'Image',
        category: 'OS',
        option_label: 'Windows Server',
        monthly_price_delta: 0,
        setup_fee_delta: 0,
      });
    }
  }
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'accept-language': 'en-US,en;q=0.9',
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.text();
}

async function main() {
  const basePlans = [];
  const optionCatalog = [];
  const planConfigs = {};
  const gapReport = [];
  const seenPlans = new Set();

  for (const url of PLAN_URLS) {
    const slug = slugFromUrl(url);
    const html = await fetchHtml(url);
    const sapper = extractSapper(html);
    const products = sapper.preloaded?.[0]?.products || {};
    const product = Object.values(products).find((item) => item.slug === slug || item.title === titleFromSlug(slug));
    if (!product) {
      gapReport.push({ slug, gap: 'product_not_found' });
      continue;
    }
    if (seenPlans.has(product.slug)) {
      continue;
    }
    seenPlans.add(product.slug);

    const family = familyFromProduct(product);
    const storageSpec = (product.specs || []).find((spec) => spec.type === 'storage');
    const basePlan = {
      family,
      product_name: product.title,
      product_slug: product.slug,
      product_url: url,
      cpu: (product.specs || []).find((spec) => spec.type === 'cpu')?.title || '',
      ram: (product.specs || []).find((spec) => spec.type === 'ram')?.title || '',
      base_storage: (
        storageSpec
          ? [
              storageSpec.title,
              storageSpec.subtitle && !/More storage available/i.test(storageSpec.subtitle)
                ? storageSpec.subtitle.replace(/^or\s+/i, '')
                : '',
            ]
              .filter(Boolean)
              .join(' or ')
          : ''
      ).trim(),
      snapshots: (product.specs || []).find((spec) => spec.type === 'snapshot')?.title || '',
      port: (product.specs || []).find((spec) => spec.type === 'port')?.title || '',
      base_monthly_price: product.price?.EUR || 0,
      periods: (product.periods || []).map((period) => ({
        months: period.length,
        monthly: monthlyPriceForPeriod(product.price?.EUR || 0, period),
        setup: period.setup?.EUR || 0,
        discount_total: period.discount?.EUR || 0,
      })),
      password_rules: extractPasswordRules(html),
      source: 'sapper',
    };
    basePlans.push(basePlan);

    const classified = [];
    for (const addon of Object.values(product.addons || {})) {
      const result = classifyAddon(addon, product, html);
      if (result.action === 'include') {
        const { action, ...rest } = result;
        classified.push({ plan_sku: product.slug, currency: 'EUR', ...rest });
      } else if (result.action === 'gap') {
        gapReport.push({
          plan_sku: product.slug,
          gap: result.reason,
          title: result.title,
        });
      }
    }

    injectDefaults(product, html, classified);

    for (const item of classified) {
      if (item.dimension === 'Region' && item.option_label === 'European Union') {
        item.is_default = true;
      }
      if (item.dimension === 'Networking' && (item.option_label === 'Unlimited Traffic' || item.option_label === 'No Private Networking' || item.option_label === '1 IP Address')) {
        item.is_default = true;
      }
      if (item.dimension === 'Storage Type' || item.dimension === 'Storage') {
        const canonical = String(item.option_label).replace(/NVME/g, 'NVMe');
        item.option_label = canonical;
        if (storageSpec && canonical === storageSpec.title.replace(/NVMe SSD/i, 'NVMe')) {
          item.is_default = true;
        }
      }
      if (item.dimension === 'Image' && /^Ubuntu 24\.04$/i.test(item.option_label)) {
        item.is_default = true;
      }
    }

    const dedup = new Map();
    for (const item of classified) {
      const key = [item.plan_sku, item.dimension, item.category, item.option_label].join('|');
      if (!dedup.has(key)) dedup.set(key, item);
    }
    const finalOptions = Array.from(dedup.values()).sort((a, b) => {
      return [a.dimension, a.category, a.option_label].join('|').localeCompare([b.dimension, b.category, b.option_label].join('|'));
    });
    optionCatalog.push(...finalOptions);

    const byDimension = Object.create(null);
    for (const item of finalOptions) {
      byDimension[item.dimension] ||= [];
      byDimension[item.dimension].push(item);
    }

    planConfigs[url] = {
      slug: product.slug,
      family,
      title: product.title,
      base_monthly_price: product.price?.EUR || 0,
      contract_periods: basePlan.periods,
      options: byDimension,
      password_rules: basePlan.password_rules,
      order_summary_default: {
        monthly: product.price?.EUR || 0,
        one_time: product.periods?.[0]?.setup?.EUR || 0,
        due_today: (product.price?.EUR || 0) + (product.periods?.[0]?.setup?.EUR || 0),
      },
    };
  }

  const dataset = {
    generated_at: new Date().toISOString(),
    source: 'Contabo configurator __SAPPER__ payload + SSR HTML defaults',
    plans: basePlans,
    option_catalog: optionCatalog,
    gap_summary: Object.values(gapReport.reduce((acc, gap) => {
      const key = `${gap.gap}|${gap.title || ''}`;
      acc[key] ||= { gap: gap.gap, title: gap.title || '', count: 0, plans: [] };
      acc[key].count += 1;
      if (gap.plan_sku && !acc[key].plans.includes(gap.plan_sku)) acc[key].plans.push(gap.plan_sku);
      return acc;
    }, {})).sort((a, b) => b.count - a.count || a.title.localeCompare(b.title)),
    gaps: gapReport,
  };

  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_base_plans_enhanced.json'), JSON.stringify(basePlans, null, 2));
  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_configs_enhanced.json'), JSON.stringify(planConfigs, null, 2));
  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_pricing_dataset_enhanced.json'), JSON.stringify(dataset, null, 2));
  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_gap_report.json'), JSON.stringify(gapReport, null, 2));
  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_gap_summary.json'), JSON.stringify(dataset.gap_summary, null, 2));

  const baseCsvRows = [
    ['family', 'product_name', 'product_slug', 'product_url', 'cpu', 'ram', 'base_storage', 'snapshots', 'port', 'base_monthly_price', 'one_month_setup_fee'],
    ...basePlans.map((plan) => [
      plan.family,
      plan.product_name,
      plan.product_slug,
      plan.product_url,
      plan.cpu,
      plan.ram,
      plan.base_storage,
      plan.snapshots,
      plan.port,
      plan.base_monthly_price,
      plan.periods.find((p) => p.months === 1)?.setup ?? 0,
    ]),
  ];
  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_base_plans_enhanced.csv'), baseCsvRows.map((row) => row.map(escapeCsv).join(',')).join('\n'));

  const optionCsvRows = [
    ['plan_sku', 'dimension', 'category', 'option_label', 'monthly_price_delta', 'setup_fee_delta', 'region_group', 'country', 'is_default', 'currency'],
    ...optionCatalog.map((item) => [
      item.plan_sku,
      item.dimension,
      item.category,
      item.option_label,
      item.monthly_price_delta ?? 0,
      item.setup_fee_delta ?? 0,
      item.region_group || '',
      item.country || '',
      item.is_default ? 'true' : 'false',
      item.currency,
    ]),
  ];
  fs.writeFileSync(path.join(OUTPUT_DIR, 'contabo_option_catalog_enhanced.csv'), optionCsvRows.map((row) => row.map(escapeCsv).join(',')).join('\n'));

  console.log(`Wrote ${basePlans.length} plans`);
  console.log(`Wrote ${optionCatalog.length} option rows`);
  console.log(`Logged ${gapReport.length} gaps`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
