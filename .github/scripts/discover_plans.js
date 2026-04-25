#!/usr/bin/env node
'use strict';

// Discovers plan URLs from Contabo listing pages, diffs against data/plan_urls.json,
// and updates the catalog in-place. Writes GitHub Actions step outputs.

const fs   = require('fs');
const path = require('path');
const https = require('https');
const { URL } = require('url');

const CATALOG_PATH = path.resolve(__dirname, '../../data/plan_urls.json');
const GITHUB_OUTPUT = process.env.GITHUB_OUTPUT;

const LISTING_PAGES = [
  { url: 'https://contabo.com/en/vps/',         family: 'Cloud VPS' },
  { url: 'https://contabo.com/en/storage-vps/', family: 'Storage VPS' },
  { url: 'https://contabo.com/en/vds/',         family: 'Cloud VDS' },
];

const BROWSER_HEADERS = {
  'user-agent':                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'accept-language':           'en-US,en;q=0.9',
  'accept-encoding':           'gzip, deflate, br',
  'upgrade-insecure-requests': '1',
  'sec-fetch-dest':            'document',
  'sec-fetch-mode':            'navigate',
  'sec-fetch-site':            'none',
  'sec-fetch-user':            '?1',
  'cache-control':             'max-age=0',
};

function httpGet(rawUrl, followRedirects = true) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(rawUrl);
    const mod = parsed.protocol === 'https:' ? https : require('http');
    const req = mod.get({ hostname: parsed.hostname, path: parsed.pathname + parsed.search, headers: BROWSER_HEADERS }, (res) => {
      if (followRedirects && (res.statusCode === 301 || res.statusCode === 302)) {
        resolve({ status: res.statusCode, location: res.headers.location, body: '' });
        res.resume();
        return;
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, location: null, body: Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(new Error('timeout')); });
  });
}

function extractPlanUrls(html, baseFamily) {
  // Match href="/en/(vps|storage-vps|vds)/<slug>/"
  const re = /href="(\/en\/(vps|storage-vps|vds)\/([a-z0-9-]+)\/?)"/g;
  const found = new Map();
  let m;
  while ((m = re.exec(html)) !== null) {
    const slug = m[3];
    const section = m[2];
    if (slug === section) continue; // skip the listing page itself
    const url = `https://contabo.com${m[1].endsWith('/') ? m[1] : m[1] + '/'}`;
    if (!found.has(slug)) found.set(slug, { slug, url, family: baseFamily });
  }
  return [...found.values()];
}

function setOutput(key, value) {
  if (GITHUB_OUTPUT) {
    fs.appendFileSync(GITHUB_OUTPUT, `${key}=${value}\n`);
  } else {
    console.log(`  output ${key}=${value}`);
  }
}

async function checkRedirect(url) {
  try {
    const res = await httpGet(url, false);
    if ((res.status === 301 || res.status === 302) && res.location) {
      const loc = res.location.startsWith('http') ? res.location : `https://contabo.com${res.location}`;
      if (!loc.endsWith('/')) return loc + '/';
      return loc;
    }
    return null;
  } catch {
    return null;
  }
}

async function main() {
  const catalog = JSON.parse(fs.readFileSync(CATALOG_PATH, 'utf8'));
  const active = catalog.plans.filter((p) => p.status === 'active');
  const activeBySlug = new Map(active.map((p) => [p.slug, p]));

  // Fetch all listing pages in parallel
  const listingResults = await Promise.allSettled(
    LISTING_PAGES.map(async ({ url, family }) => {
      const res = await httpGet(url);
      if (res.status === 403 || res.status === 429) throw new Error(`HTTP ${res.status}`);
      return { family, plans: extractPlanUrls(res.body, family) };
    })
  );

  const allFailed = listingResults.every((r) => r.status === 'rejected');
  if (allFailed) {
    console.error('Discovery blocked — all listing pages failed (network/WAF). Keeping existing catalog.');
    setOutput('changed', 'false');
    setOutput('added', '0');
    setOutput('removed', '0');
    setOutput('renamed', '0');
    setOutput('plan_count', String(active.length));
    setOutput('discovery_blocked', 'true');
    return;
  }

  // Collect all discovered plans
  const discovered = new Map();
  for (const r of listingResults) {
    if (r.status === 'fulfilled') {
      for (const p of r.value.plans) discovered.set(p.slug, p);
    }
  }

  // Safety: if we fetched pages but found 0 plan URLs, WAF likely returned a
  // challenge page (200 with no matching hrefs). Treat as blocked — do not
  // incorrectly mark all active plans as discontinued.
  if (discovered.size === 0) {
    console.error('Discovery yielded 0 plan URLs — listing pages may have returned a WAF challenge. Keeping existing catalog.');
    setOutput('changed', 'false');
    setOutput('added', '0');
    setOutput('removed', '0');
    setOutput('renamed', '0');
    setOutput('plan_count', String(active.length));
    setOutput('discovery_blocked', 'true');
    return;
  }

  // Safety: never remove more than half the active catalog in one run.
  // If the discovered set is suspiciously small, it likely reflects a partial
  // WAF block rather than real discontinuations — bail out safely.
  const wouldRemove = active.filter((p) => !discovered.has(p.slug)).length;
  if (active.length > 0 && wouldRemove > active.length / 2) {
    console.error(`Discovery would remove ${wouldRemove}/${active.length} active plans — looks like a partial WAF block. Keeping existing catalog.`);
    setOutput('changed', 'false');
    setOutput('added', '0');
    setOutput('removed', '0');
    setOutput('renamed', '0');
    setOutput('plan_count', String(active.length));
    setOutput('discovery_blocked', 'true');
    return;
  }

  // Redirect check for active plans that aren't in discovered set
  const possiblyGone = active.filter((p) => !discovered.has(p.slug));
  const redirectChecks = await Promise.allSettled(
    possiblyGone.map(async (p) => ({ plan: p, newUrl: await checkRedirect(p.url) }))
  );

  const now = new Date().toISOString();
  let added = 0, removed = 0, renamed = 0;

  // Process redirects
  for (const r of redirectChecks) {
    if (r.status !== 'fulfilled') continue;
    const { plan, newUrl } = r.value;
    if (!newUrl || newUrl === plan.url) continue;
    const newSlug = newUrl.replace(/\/$/, '').split('/').pop();
    if (newSlug && newSlug !== plan.slug) {
      console.log(`  renamed: ${plan.slug} → ${newSlug} (${newUrl})`);
      plan.previous_url = plan.url;
      plan.url = newUrl;
      plan.slug = newSlug;
      activeBySlug.delete(plan.slug);
      activeBySlug.set(newSlug, plan);
      renamed++;
    }
  }

  // Plans in discovered but not in active catalog → add
  for (const [slug, dp] of discovered) {
    if (!activeBySlug.has(slug)) {
      console.log(`  added: ${slug} (${dp.url})`);
      catalog.plans.push({ slug, url: dp.url, family: dp.family, status: 'active', discovered_at: now, previous_url: null });
      added++;
    }
  }

  // Active plans still missing after redirect checks → discontinue
  for (const plan of active) {
    const currentSlug = plan.slug;
    if (!discovered.has(currentSlug) && !redirectChecks.some((r) => r.status === 'fulfilled' && r.value.plan.slug === currentSlug && r.value.newUrl)) {
      console.log(`  discontinued: ${currentSlug}`);
      plan.status = 'discontinued';
      catalog.discontinued.push(plan);
      catalog.plans = catalog.plans.filter((p) => p.slug !== currentSlug);
      removed++;
    }
  }

  const changed = added > 0 || removed > 0 || renamed > 0;
  const newActive = catalog.plans.filter((p) => p.status === 'active');

  if (changed) {
    catalog.updated_at = now;
    fs.writeFileSync(CATALOG_PATH, JSON.stringify(catalog, null, 2) + '\n');
    console.log(`Catalog updated: +${added} -${removed} ~${renamed}`);
  } else {
    console.log(`Catalog unchanged. ${newActive.length} active plans.`);
  }

  setOutput('changed', String(changed));
  setOutput('added', String(added));
  setOutput('removed', String(removed));
  setOutput('renamed', String(renamed));
  setOutput('plan_count', String(newActive.length));
  setOutput('discovery_blocked', 'false');
}

main().catch((err) => {
  console.error('discover_plans.js fatal:', err.message);
  setOutput('changed', 'false');
  setOutput('added', '0');
  setOutput('removed', '0');
  setOutput('renamed', '0');
  setOutput('plan_count', '0');
  setOutput('discovery_blocked', 'true');
});
