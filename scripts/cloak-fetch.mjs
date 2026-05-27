#!/usr/bin/env node
// CloakBrowser batch page fetcher — called by the Rust scraper's fallback path.
//
// Usage:
//   node scripts/cloak-fetch.mjs --urls <path-to-urls.json> --out-dir <dir>
//
// Reads a JSON array of URLs, fetches each via a single CloakBrowser instance
// (one Chromium cold-start amortised across all URLs), and writes each page's
// HTML to <out-dir>/<slug>.html.
//
// Exit codes:
//   0 — all URLs fetched successfully
//   1 — partial failure (some URLs fetched, some failed)
//   2 — all URLs failed (or fatal setup error)

import { launch } from 'cloakbrowser';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

const { values } = parseArgs({
  options: {
    urls:      { type: 'string' },
    'out-dir': { type: 'string' },
    proxy:     { type: 'string' },
  },
  strict: false,
});

const urlsFile = values['urls'];
const outDir   = values['out-dir'];

if (!urlsFile || !outDir) {
  process.stderr.write('Usage: cloak-fetch.mjs --urls <file.json> --out-dir <dir>\n');
  process.exit(2);
}

let urls;
try {
  urls = JSON.parse(readFileSync(urlsFile, 'utf8'));
} catch (err) {
  process.stderr.write(`[cloak] ERROR: cannot read urls file: ${err.message}\n`);
  process.exit(2);
}

if (!Array.isArray(urls) || urls.length === 0) {
  process.stderr.write('[cloak] ERROR: urls file must be a non-empty JSON array\n');
  process.exit(2);
}

mkdirSync(outDir, { recursive: true });

function slugFromUrl(url) {
  return url.replace(/\/$/, '').split('/').pop();
}

let succeeded = 0;
let failed    = 0;

let browser;
try {
  const launchOpts = {
    headless: true,
    humanize: false,  // not needed for SSR pages; saves time
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',  // required in CI environments with limited /dev/shm
    ],
  };
  if (values.proxy) {
    const u = new URL(values.proxy);
    launchOpts.proxy = {
      server:   `${u.protocol}//${u.host}`,
      username: u.username,
      password: u.password,
    };
  }
  browser = await launch(launchOpts);
} catch (err) {
  process.stderr.write(`[cloak] FATAL: cannot launch CloakBrowser: ${err.message}\n`);
  process.stderr.write('[cloak] Is `npm install cloakbrowser playwright-core` complete?\n');
  process.exit(2);
}

try {
  for (const url of urls) {
    const slug = slugFromUrl(url);
    const page = await browser.newPage();
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30_000 });
      const html = await page.content();
      // Warn if Sapper payload is absent — Rust will surface a proper gap error.
      if (!html.includes('__SAPPER__')) {
        process.stderr.write(`[cloak] WARN: __SAPPER__ not found in ${url}\n`);
      }
      writeFileSync(join(outDir, `${slug}.html`), html, 'utf8');
      process.stderr.write(`[cloak] OK: ${slug}\n`);
      succeeded++;
    } catch (err) {
      process.stderr.write(`[cloak] FAIL: ${slug} — ${err.message}\n`);
      failed++;
    } finally {
      await page.close().catch(() => {});
    }
  }
} finally {
  await browser.close().catch(() => {});
}

process.stderr.write(`[cloak] done: ${succeeded} OK, ${failed} failed of ${urls.length} total\n`);

if (failed === urls.length) {
  process.exit(2);
} else if (failed > 0) {
  process.exit(1);
} else {
  process.exit(0);
}
