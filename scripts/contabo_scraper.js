#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const SCRAPER_VERSION = '2.0.0';
const SCHEMA_VERSION = '1.1';

// Exit codes
const EXIT_OK       = 0;  // all plans scraped successfully
const EXIT_ERROR    = 1;  // fatal / no plans scraped
const EXIT_PARTIAL  = 2;  // some plans failed, output still written

// ─── Plan URLs ────────────────────────────────────────────────────────────────

const ALL_PLAN_URLS = [
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

// ─── Classification tables ────────────────────────────────────────────────────

const REGION_RULES = [
  [/^European Union$/i,           { region_group: 'Europe',    country: 'European Union',   country_code: 'EU' }],
  [/^United Kingdom$/i,           { region_group: 'Europe',    country: 'United Kingdom',   country_code: 'UK' }],
  [/^Germany$/i,                  { region_group: 'Europe',    country: 'Germany',           country_code: 'DE' }],
  [/^Canada/i,                    { region_group: 'America',   country: 'Canada',            country_code: 'CA' }],
  [/^United States \(([^)]+)\)$/i, (m) => ({ region_group: 'America', country: `United States (${m[1]})`, country_code: 'US', subregion: m[1] })],
  [/^United States$/i,            { region_group: 'America',   country: 'United States',     country_code: 'US' }],
  [/^Asia \(([^)]+)\)$/i,        (m) => ({ region_group: 'Asia',    country: m[1], country_code: m[1].slice(0, 2).toUpperCase() })],
  [/^Australia \(([^)]+)\)$/i,    { region_group: 'Australia', country: 'Australia',         country_code: 'AU' }],
  [/^Australia$/i,                { region_group: 'Australia', country: 'Australia',         country_code: 'AU' }],
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

// ─── CLI ─────────────────────────────────────────────────────────────────────

function parseArgs(argv) {
  const args = argv.slice(2);
  const opts = {
    output: path.resolve(__dirname, '..', 'data', 'output'),
    concurrency: 4,
    retries: 3,
    plans: null,
    quiet: false,
    json: false,
    dryRun: false,
    help: false,
    version: false,
  };
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case '--help':        case '-h': opts.help    = true; break;
      case '--version':     case '-v': opts.version = true; break;
      case '--quiet':       case '-q': opts.quiet   = true; break;
      case '--json':        case '-j': opts.json    = true; break;
      case '--dry-run':               opts.dryRun   = true; break;
      case '--output':      case '-o': opts.output      = path.resolve(args[++i] ?? ''); break;
      case '--concurrency': case '-c': opts.concurrency = Math.max(1, parseInt(args[++i], 10) || 4); break;
      case '--retries':     case '-r': opts.retries     = Math.max(0, parseInt(args[++i], 10) || 3); break;
      case '--plans':       case '-p': opts.plans       = (args[++i] ?? '').split(',').map((s) => s.trim()).filter(Boolean); break;
    }
  }
  return opts;
}

function printHelp() {
  console.log(`
contabo-scraper v${SCRAPER_VERSION}

USAGE
  node scripts/contabo_scraper.js [options]

OPTIONS
  -o, --output <dir>       Output directory       (default: data/output/)
  -c, --concurrency <n>    Parallel fetches        (default: 4)
  -r, --retries <n>        Retries per URL         (default: 3)
  -p, --plans <slugs>      Comma-separated plan slugs to limit scraping
  -q, --quiet              Suppress progress output (stderr stays active)
  -j, --json               Print JSON summary to stdout on completion
      --dry-run            Fetch pages but do not write any output files
  -v, --version            Print version and exit
  -h, --help               Show this help

EXIT CODES
  0  All plans scraped and written successfully
  1  Fatal error — no output written
  2  Partial success — some plans failed, output written for the rest

EXAMPLES
  node scripts/contabo_scraper.js
  node scripts/contabo_scraper.js --output ./my-output --concurrency 2
  node scripts/contabo_scraper.js --plans cloud-vps-10,cloud-vps-20
  node scripts/contabo_scraper.js --json --quiet          # machine-readable
  node scripts/contabo_scraper.js --dry-run               # validate only

AI AGENT USAGE
  # Capture structured results
  node scripts/contabo_scraper.js --json --quiet > result.json
  # Check for failures
  node scripts/contabo_scraper.js --json --quiet | jq '.gaps'
`);
}

// ─── Concurrency ──────────────────────────────────────────────────────────────

function createSemaphore(limit) {
  let active = 0;
  const queue = [];
  return async function run(fn) {
    if (active >= limit) {
      await new Promise((resolve) => queue.push(resolve));
    }
    active++;
    try {
      return await fn();
    } finally {
      active--;
      if (queue.length > 0) queue.shift()();
    }
  };
}

// ─── HTTP ─────────────────────────────────────────────────────────────────────

const FETCH_HEADERS = {
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  'accept-language': 'en-US,en;q=0.9',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
};

async function fetchHtml(url) {
  const res = await fetch(url, { headers: FETCH_HEADERS });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return res.text();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, retries, log) {
  let lastError;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fetchHtml(url);
    } catch (err) {
      lastError = err;
      if (attempt < retries) {
        const delay = Math.min(1000 * 2 ** attempt, 8000);
        log(`  [retry ${attempt + 1}/${retries}] ${url} — ${err.message} (waiting ${delay}ms)`);
        await sleep(delay);
      }
    }
  }
  throw lastError;
}

// ─── Parsing ──────────────────────────────────────────────────────────────────

// Tries several known end-markers so a minor Contabo HTML change doesn't
// break extraction completely.
const SAPPER_END_MARKERS = [
  '};(function(){try{eval("async function x(){}")',
  '};(function()',
];

function extractSapper(html) {
  const start = html.indexOf('__SAPPER__=');
  if (start === -1) throw new Error('__SAPPER__ payload not found in HTML');

  let end = -1;
  for (const marker of SAPPER_END_MARKERS) {
    const idx = html.indexOf(marker, start);
    if (idx !== -1) {
      end = idx;
      break;
    }
  }
  if (end === -1) {
    // Fallback: find closing </script> after __SAPPER__
    const scriptEnd = html.indexOf('</script>', start);
    if (scriptEnd === -1) throw new Error('Could not find end of __SAPPER__ payload');
    // Trim back to last `}` before </script>
    const snippet = html.slice(start, scriptEnd).trimEnd();
    const lastBrace = snippet.lastIndexOf('}');
    if (lastBrace === -1) throw new Error('Malformed __SAPPER__ block');
    end = start + lastBrace;
  }

  const script = 'var window={}; var document={}; ' + html.slice(start, end + 1);
  const ctx = {};
  vm.createContext(ctx);
  vm.runInContext(script, ctx, { timeout: 20000 });
  if (!ctx.__SAPPER__) throw new Error('__SAPPER__ evaluated to empty/null');
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

// ─── Helpers ──────────────────────────────────────────────────────────────────

function escapeCsv(value) {
  const text = value == null ? '' : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function slugFromUrl(url) {
  return url.replace(/\/$/, '').split('/').pop();
}

function titleFromSlug(slug) {
  if (slug.startsWith('cloud-vps-'))   return `Cloud VPS ${slug.split('-').pop()}`;
  if (slug.startsWith('storage-vps-')) return `Storage VPS ${slug.split('-').pop()}`;
  if (slug.startsWith('vds-'))         return `Cloud VDS ${slug.split('-').pop().toUpperCase()}`;
  return slug;
}

function familyFromProduct(product) {
  if (product.type === 'vps')         return 'Cloud VPS';
  if (product.type === 'storage-vps') return 'Storage VPS';
  if (product.type === 'vds')         return 'Cloud VDS';
  return product.type ?? 'Unknown';
}

function normalizeStorageLabel(label) {
  return label
    .replace(/\bNVME\b/g, 'NVMe')
    .replace(/\bNVMe SSD\b/gi, 'NVMe')
    .trim();
}

function monthlyPriceForPeriod(baseMonthly, period) {
  if (!period) return null;
  const discountEUR = period.discount?.EUR ?? 0;
  const setupEUR   = period.setup?.EUR ?? 0;
  // Total cost over the period divided by months gives effective monthly rate.
  // We include the setup amortized, matching Contabo's own "effective monthly" display.
  const total = baseMonthly * period.length - discountEUR + setupEUR;
  const effective = (baseMonthly * period.length - discountEUR) / period.length;
  return {
    effective_monthly: Number(effective.toFixed(2)),
    setup_fee: Number(setupEUR.toFixed(2)),
    total_period_cost: Number(total.toFixed(2)),
    discount_total: Number(discountEUR.toFixed(2)),
  };
}

// ─── Region / addon classification ───────────────────────────────────────────

function classifyRegion(title) {
  for (const [pattern, value] of REGION_RULES) {
    const match = title.match(pattern);
    if (match) return typeof value === 'function' ? value(match) : value;
  }
  return null;
}

function isIgnoredTitle(title) {
  return IGNORE_TITLE_PATTERNS.some((p) => p.test(title));
}

function classifyAddon(addon, product, html) {
  const title = (addon.title ?? '').trim();
  if (!title) return { action: 'skip', reason: 'empty_title' };
  if (isIgnoredTitle(title)) return { action: 'skip', reason: 'ignored_addon' };

  const delta = {
    monthly_price_delta: addon.price?.EUR ?? 0,
    setup_fee_delta: addon.setupPrice?.EUR ?? 0,
  };

  const region = classifyRegion(title);
  if (region) {
    return { action: 'include', dimension: 'Region', category: region.region_group, option_label: region.country, ...region, ...delta };
  }

  // Storage
  const storageMd = title.match(/^(\d+(?:\.\d+)?)\s*(GB|TB)\s*(NVMe|SSD)(?: SSD)?$/i);
  if (storageMd) {
    const storageType = storageMd[3].toUpperCase() === 'NVME' ? 'NVMe' : storageMd[3].toUpperCase();
    return {
      action: 'include',
      dimension: product.type === 'vds' ? 'Storage' : 'Storage Type',
      category: storageType,
      option_label: normalizeStorageLabel(`${storageMd[1]} ${storageMd[2]} ${storageMd[3]}`),
      ...delta,
    };
  }

  // Data protection
  if (/^Auto Backup$/i.test(title)) {
    return { action: 'include', dimension: 'Data Protection', category: 'Auto Backup', option_label: 'Auto Backup', ...delta };
  }

  // Private networking
  if (/^No Private Networking$/i.test(title) || /^Private Networking Enabled$/i.test(title)) {
    return { action: 'include', dimension: 'Networking', category: 'Private Networking', option_label: title, ...delta };
  }

  // Bandwidth / traffic
  if (/Traffic/i.test(title) || /Out \+ Unlimited In/i.test(title)) {
    return { action: 'include', dimension: 'Networking', category: 'Bandwidth', option_label: title, ...delta };
  }

  // IPv4
  if (/IP Address|IP adress/i.test(title)) {
    return { action: 'include', dimension: 'Networking', category: 'IPv4', option_label: title, ...delta };
  }

  // OS images
  const osPattern = /^(Ubuntu|Debian|AlmaLinux|Rocky Linux|Arch Linux|FreeBSD|Fedora|CentOS|openSUSE|Gentoo)/i;
  if (/^Windows Server/i.test(title) || osPattern.test(title) || /Custom Image/i.test(title)) {
    return { action: 'include', dimension: 'Image', category: 'OS', option_label: title, ...delta };
  }

  // Control panels
  const panelPatterns = [
    /^cPanel\/WHM \(5 accounts\)$/i,
    /^Plesk Obsidian Web (Admin|Pro|Host) Edition$/i,
    /^Webmin( \+ LAMP)?$/i,
    /^DirectAdmin$/i,
    /^ISPConfig$/i,
  ];
  if (panelPatterns.some((p) => p.test(title))) {
    let option_label = title;
    if (/^Plesk Obsidian Web Admin Edition$/i.test(title)) option_label = 'Plesk + Linux';
    if (/^Webmin$/i.test(title))                           option_label = 'Webmin';
    return { action: 'include', dimension: 'Image', category: 'Panels', option_label, ...delta };
  }

  // Apps / blockchain images
  const appPattern = /Server$|^(Docker|LAMP|Coolify|DeepSeek|IPFS Node|Flux Node|Horizen Node|Bitcoin Node|Ethereum Staking Node)$/i;
  if (appPattern.test(title)) {
    const category = /Node$|Staking Node$/i.test(title) ? 'Blockchain' : 'Apps';
    const option_label = /^Gitlab Server$/i.test(title) ? 'GitLab Server' : title;
    return { action: 'include', dimension: 'Image', category, option_label, ...delta };
  }

  return { action: 'gap', reason: 'unclassified', title };
}

// ─── Default option injection ─────────────────────────────────────────────────

function injectDefaults(product, html, classified) {
  const titles = new Set(classified.map((item) => `${item.dimension}|${item.option_label}`));
  const add = (item) => {
    const key = `${item.dimension}|${item.option_label}`;
    if (!titles.has(key)) {
      titles.add(key);
      classified.push(item);
    }
  };
  const base = { plan_sku: product.slug, currency: 'EUR' };

  add({ ...base, dimension: 'Data Protection', category: 'None',
    option_label: product.type === 'vds' ? 'No Backup Space' : 'No Data Protection',
    monthly_price_delta: 0, setup_fee_delta: 0, is_default: true });

  add({ ...base, dimension: 'Networking', category: 'Private Networking',
    option_label: 'No Private Networking', monthly_price_delta: 0, setup_fee_delta: 0, is_default: true });

  add({ ...base, dimension: 'Networking', category: 'Bandwidth',
    option_label: 'Unlimited Traffic', monthly_price_delta: 0, setup_fee_delta: 0, is_default: true });

  add({ ...base, dimension: 'Networking', category: 'IPv4',
    option_label: '1 IP Address', monthly_price_delta: 0, setup_fee_delta: 0, is_default: true });

  const storageSpec = (product.specs ?? []).find((s) => s.type === 'storage');
  if (storageSpec) {
    const primary = normalizeStorageLabel((storageSpec.title ?? '').replace(/ SSD$/, ' SSD'));
    if (primary) {
      add({ ...base,
        dimension: product.type === 'vds' ? 'Storage' : 'Storage Type',
        category: /NVMe/i.test(primary) ? 'NVMe' : 'SSD',
        option_label: primary,
        monthly_price_delta: 0, setup_fee_delta: 0, is_default: true });
    }
    if (storageSpec.subtitle && !/More storage available/i.test(storageSpec.subtitle)) {
      const alt = normalizeStorageLabel(storageSpec.subtitle.replace(/^or\s+/i, ''));
      add({ ...base,
        dimension: product.type === 'vds' ? 'Storage' : 'Storage Type',
        category: /NVMe/i.test(alt) ? 'NVMe' : 'SSD',
        option_label: alt,
        monthly_price_delta: 0, setup_fee_delta: 0, is_default: false });
    }
  }

  if (/Ubuntu/i.test(html)) {
    add({ ...base, dimension: 'Image', category: 'OS',
      option_label: 'Ubuntu 24.04', monthly_price_delta: 0, setup_fee_delta: 0, is_default: true });
  }

  if (/Windows Server/i.test(html) && ![...titles].some((k) => k.startsWith('Image|Windows Server'))) {
    add({ ...base, dimension: 'Image', category: 'OS',
      option_label: 'Windows Server', monthly_price_delta: 0, setup_fee_delta: 0 });
  }
}

// ─── Per-plan processing ──────────────────────────────────────────────────────

async function processPlan(url, html, gapReport) {
  const slug = slugFromUrl(url);
  const sapper = extractSapper(html);
  const products = sapper.preloaded?.[0]?.products ?? {};

  const product = Object.values(products).find(
    (item) => item.slug === slug || item.title === titleFromSlug(slug),
  );
  if (!product) {
    gapReport.push({ slug, gap: 'product_not_found' });
    return null;
  }

  const family = familyFromProduct(product);
  const storageSpec = (product.specs ?? []).find((s) => s.type === 'storage');

  const periods = (product.periods ?? []).map((period) => {
    const priced = monthlyPriceForPeriod(product.price?.EUR ?? 0, period);
    return {
      months: period.length,
      ...priced,
    };
  });

  const basePlan = {
    family,
    product_name: product.title,
    product_slug: product.slug,
    product_url: url,
    fetched_at: new Date().toISOString(),
    cpu: (product.specs ?? []).find((s) => s.type === 'cpu')?.title ?? '',
    ram: (product.specs ?? []).find((s) => s.type === 'ram')?.title ?? '',
    base_storage: (
      storageSpec
        ? [
            storageSpec.title,
            storageSpec.subtitle && !/More storage available/i.test(storageSpec.subtitle)
              ? storageSpec.subtitle.replace(/^or\s+/i, '')
              : '',
          ]
            .filter(Boolean)
            .map(normalizeStorageLabel)
            .join(' or ')
        : ''
    ).trim(),
    snapshots: (product.specs ?? []).find((s) => s.type === 'snapshot')?.title ?? '',
    port: (product.specs ?? []).find((s) => s.type === 'port')?.title ?? '',
    base_monthly_price: product.price?.EUR ?? 0,
    periods,
    password_rules: extractPasswordRules(html),
    source: 'sapper',
  };

  const classified = [];
  for (const addon of Object.values(product.addons ?? {})) {
    const result = classifyAddon(addon, product, html);
    if (result.action === 'include') {
      const { action, ...rest } = result;
      classified.push({ plan_sku: product.slug, currency: 'EUR', ...rest });
    } else if (result.action === 'gap') {
      gapReport.push({ plan_sku: product.slug, gap: result.reason, title: result.title });
    }
  }

  injectDefaults(product, html, classified);

  // Mark defaults
  for (const item of classified) {
    if (item.dimension === 'Region' && item.option_label === 'European Union') item.is_default = true;
    if (item.dimension === 'Networking' &&
      (item.option_label === 'Unlimited Traffic' ||
       item.option_label === 'No Private Networking' ||
       item.option_label === '1 IP Address')) {
      item.is_default = true;
    }
    if ((item.dimension === 'Storage Type' || item.dimension === 'Storage') && storageSpec) {
      const canonical = normalizeStorageLabel(storageSpec.title ?? '');
      if (item.option_label === canonical) item.is_default = true;
    }
    if (item.dimension === 'Image' && /^Ubuntu 24\.04$/i.test(item.option_label)) item.is_default = true;
  }

  // Deduplicate and sort
  const dedup = new Map();
  for (const item of classified) {
    const key = [item.plan_sku, item.dimension, item.category, item.option_label].join('|');
    if (!dedup.has(key)) dedup.set(key, item);
  }
  const finalOptions = Array.from(dedup.values()).sort((a, b) =>
    [a.dimension, a.category, a.option_label].join('|').localeCompare([b.dimension, b.category, b.option_label].join('|')),
  );

  const byDimension = Object.create(null);
  for (const item of finalOptions) {
    byDimension[item.dimension] ??= [];
    byDimension[item.dimension].push(item);
  }

  const planConfig = {
    slug: product.slug,
    family,
    title: product.title,
    fetched_at: basePlan.fetched_at,
    base_monthly_price: product.price?.EUR ?? 0,
    contract_periods: periods,
    options: byDimension,
    password_rules: basePlan.password_rules,
    order_summary_default: {
      monthly: product.price?.EUR ?? 0,
      one_time: product.periods?.[0]?.setup?.EUR ?? 0,
      due_today: (product.price?.EUR ?? 0) + (product.periods?.[0]?.setup?.EUR ?? 0),
    },
  };

  return { basePlan, finalOptions, planConfig, url };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs(process.argv);
  if (opts.help)    { printHelp(); process.exit(EXIT_OK); }
  if (opts.version) { console.log(`contabo-scraper v${SCRAPER_VERSION}`); process.exit(EXIT_OK); }

  // Progress goes to stderr so stdout stays clean for --json consumers
  const log = opts.quiet ? () => {} : (...args) => process.stderr.write(args.join(' ') + '\n');

  let urls = ALL_PLAN_URLS;
  if (opts.plans) {
    urls = urls.filter((u) => opts.plans.includes(slugFromUrl(u)));
    if (urls.length === 0) {
      console.error(`No matching URLs for plans: ${opts.plans.join(', ')}`);
      process.exit(1);
    }
  }

  if (!opts.dryRun) fs.mkdirSync(opts.output, { recursive: true });

  log(`contabo-scraper v${SCRAPER_VERSION}`);
  log(`Scraping ${urls.length} plan(s) — concurrency=${opts.concurrency} retries=${opts.retries}${opts.dryRun ? ' [dry-run]' : ''}`);
  log(`Output → ${opts.output}\n`);

  const semaphore = createSemaphore(opts.concurrency);
  const gapReport = [];
  const seenSlugs = new Set();

  const started = Date.now();

  const results = await Promise.all(
    urls.map((url) =>
      semaphore(async () => {
        const slug = slugFromUrl(url);
        const t0 = Date.now();
        log(`  fetch  ${slug}`);
        let html;
        try {
          html = await fetchWithRetry(url, opts.retries, log);
        } catch (err) {
          process.stderr.write(`  ERROR  ${slug}: ${err.message}\n`);
          gapReport.push({ slug, gap: 'fetch_failed', error: err.message });
          return null;
        }
        let result;
        try {
          result = await processPlan(url, html, gapReport);
        } catch (err) {
          process.stderr.write(`  ERROR  ${slug} (parse): ${err.message}\n`);
          gapReport.push({ slug, gap: 'parse_failed', error: err.message });
          return null;
        }
        log(`  done   ${slug} (${Date.now() - t0}ms)`);
        return result;
      }),
    ),
  );

  const basePlans = [];
  const optionCatalog = [];
  const planConfigs = {};

  for (const result of results) {
    if (!result) continue;
    const { basePlan, finalOptions, planConfig, url } = result;
    if (seenSlugs.has(basePlan.product_slug)) continue;
    seenSlugs.add(basePlan.product_slug);
    basePlans.push(basePlan);
    optionCatalog.push(...finalOptions);
    planConfigs[url] = planConfig;
  }

  // ─── Build gap summary ────────────────────────────────────────────────────
  const gapSummary = Object.values(
    gapReport.reduce((acc, gap) => {
      const key = `${gap.gap}|${gap.title ?? gap.error ?? ''}`;
      acc[key] ??= { gap: gap.gap, title: gap.title ?? '', count: 0, plans: [] };
      acc[key].count += 1;
      if (gap.plan_sku && !acc[key].plans.includes(gap.plan_sku)) acc[key].plans.push(gap.plan_sku);
      return acc;
    }, {}),
  ).sort((a, b) => b.count - a.count || a.title.localeCompare(b.title));

  const dataset = {
    scraper_version: SCRAPER_VERSION,
    schema_version: SCHEMA_VERSION,
    generated_at: new Date().toISOString(),
    source: 'Contabo configurator __SAPPER__ payload + SSR HTML defaults',
    plan_count: basePlans.length,
    option_count: optionCatalog.length,
    gap_count: gapReport.length,
    plans: basePlans,
    option_catalog: optionCatalog,
    gap_summary: gapSummary,
    gaps: gapReport,
  };

  // ─── Write JSON ───────────────────────────────────────────────────────────
  const write = (filename, data) =>
    fs.writeFileSync(path.join(opts.output, filename), JSON.stringify(data, null, 2), 'utf8');

  if (!opts.dryRun) {
    write('contabo_base_plans.json', basePlans);
    write('contabo_configs.json', planConfigs);
    write('contabo_pricing_dataset.json', dataset);
    write('contabo_gap_report.json', gapReport);
    write('contabo_gap_summary.json', gapSummary);
  }

  // ─── Write CSV: base plans ────────────────────────────────────────────────
  const baseCsvHeader = [
    'family', 'product_name', 'product_slug', 'product_url', 'fetched_at',
    'cpu', 'ram', 'base_storage', 'snapshots', 'port', 'base_monthly_price',
    '1m_setup_fee', '1m_effective_monthly',
    '3m_effective_monthly', '3m_setup_fee', '3m_total',
    '6m_effective_monthly', '6m_setup_fee', '6m_total', '6m_discount',
    '12m_effective_monthly', '12m_setup_fee', '12m_total', '12m_discount',
  ];
  const getPeriod = (plan, months) => plan.periods.find((p) => p.months === months) ?? {};
  const baseCsvRows = [
    baseCsvHeader,
    ...basePlans.map((plan) => {
      const p1  = getPeriod(plan, 1);
      const p3  = getPeriod(plan, 3);
      const p6  = getPeriod(plan, 6);
      const p12 = getPeriod(plan, 12);
      return [
        plan.family, plan.product_name, plan.product_slug, plan.product_url, plan.fetched_at,
        plan.cpu, plan.ram, plan.base_storage, plan.snapshots, plan.port,
        plan.base_monthly_price,
        p1.setup_fee ?? 0, p1.effective_monthly ?? plan.base_monthly_price,
        p3.effective_monthly ?? '', p3.setup_fee ?? 0, p3.total_period_cost ?? '',
        p6.effective_monthly ?? '', p6.setup_fee ?? 0, p6.total_period_cost ?? '', p6.discount_total ?? 0,
        p12.effective_monthly ?? '', p12.setup_fee ?? 0, p12.total_period_cost ?? '', p12.discount_total ?? 0,
      ];
    }),
  ];
  if (!opts.dryRun) {
    fs.writeFileSync(
      path.join(opts.output, 'contabo_base_plans.csv'),
      baseCsvRows.map((row) => row.map(escapeCsv).join(',')).join('\n'),
      'utf8',
    );
  }

  // ─── Write CSV: option catalog ────────────────────────────────────────────
  const optCsvHeader = [
    'plan_sku', 'dimension', 'category', 'option_label',
    'monthly_price_delta', 'setup_fee_delta',
    'region_group', 'country', 'is_default', 'currency',
  ];
  const optCsvRows = [
    optCsvHeader,
    ...optionCatalog.map((item) => [
      item.plan_sku, item.dimension, item.category, item.option_label,
      item.monthly_price_delta ?? 0, item.setup_fee_delta ?? 0,
      item.region_group ?? '', item.country ?? '',
      item.is_default ? 'true' : 'false', item.currency,
    ]),
  ];
  if (!opts.dryRun) {
    fs.writeFileSync(
      path.join(opts.output, 'contabo_option_catalog.csv'),
      optCsvRows.map((row) => row.map(escapeCsv).join(',')).join('\n'),
      'utf8',
    );
  }

  // ─── Summary ──────────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  const failedCount = urls.length - basePlans.length;

  const summary = {
    scraper_version: SCRAPER_VERSION,
    schema_version: SCHEMA_VERSION,
    generated_at: dataset.generated_at,
    elapsed_seconds: Number(elapsed),
    plans_requested: urls.length,
    plans_scraped: basePlans.length,
    plans_failed: failedCount,
    options_total: optionCatalog.length,
    gaps_total: gapReport.length,
    output_dir: opts.dryRun ? null : opts.output,
    dry_run: opts.dryRun,
    gaps: gapSummary,
  };

  log(`\nDone in ${elapsed}s`);
  log(`  Plans:   ${basePlans.length}/${urls.length}${failedCount > 0 ? ` (${failedCount} failed)` : ''}`);
  log(`  Options: ${optionCatalog.length}`);
  log(`  Gaps:    ${gapReport.length}`);
  if (!opts.dryRun) log(`  Output:  ${opts.output}`);

  if (opts.json) {
    process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
  }

  if (basePlans.length === 0) process.exit(EXIT_ERROR);
  if (failedCount > 0)        process.exit(EXIT_PARTIAL);
  process.exit(EXIT_OK);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
