#!/usr/bin/env node
'use strict';

// Reads gap_report.json + discovery outputs, creates/closes GitHub Issues via gh CLI.
// Usage: node post_scrape_audit.js --gap-report <path> [--discovery-changed true] ...

const fs    = require('fs');
const { execSync } = require('child_process');
const path  = require('path');

// ── Arg parsing ────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const args = {};
for (let i = 0; i < argv.length; i += 2) {
  args[argv[i].replace(/^--/, '')] = argv[i + 1];
}

const gapReportPath       = args['gap-report'] || 'data/output/contabo_gap_report.json';
const discoveryChanged    = args['discovery-changed'] === 'true';
const discoveryBlocked    = args['discovery-blocked'] === 'true';
const addedCount          = parseInt(args['added']   || '0', 10);
const removedCount        = parseInt(args['removed'] || '0', 10);
const renamedCount        = parseInt(args['renamed'] || '0', 10);

// ── Load gap report ────────────────────────────────────────────────────────────
let gaps = [];
if (fs.existsSync(gapReportPath)) {
  try {
    const raw = JSON.parse(fs.readFileSync(gapReportPath, 'utf8'));
    gaps = Array.isArray(raw) ? raw : (raw.gaps || []);
  } catch (e) {
    console.error('Could not parse gap report:', e.message);
  }
}

const byType = {};
for (const g of gaps) {
  (byType[g.gap] = byType[g.gap] || []).push(g);
}

const fetchFailed       = byType['fetch_failed']       || [];
const sapperFailed      = byType['sapper_extract_failed'] || [];
const unclassified      = byType['unclassified']       || [];

// ── gh CLI helper ─────────────────────────────────────────────────────────────
function gh(args, { noThrow = false } = {}) {
  try {
    return execSync(`gh ${args}`, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }).trim();
  } catch (e) {
    if (noThrow) return '';
    throw e;
  }
}

function openIssueWithLabel(label) {
  const out = gh(`issue list --label "${label}" --state open --json number,title --limit 1`);
  const list = JSON.parse(out || '[]');
  return list.length > 0 ? list[0] : null;
}

function createIssue(title, body, labels) {
  const labelFlag = labels.map((l) => `--label "${l}"`).join(' ');
  const bodyFlag  = `--body "${body.replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
  console.log(`Creating issue: ${title}`);
  gh(`issue create --title "${title}" ${bodyFlag} ${labelFlag}`);
}

function closeIssue(number, comment) {
  console.log(`Closing issue #${number}`);
  gh(`issue close ${number} --comment "${comment.replace(/"/g, '\\"')}"`);
}

function ensureLabelsExist(labels) {
  for (const label of labels) {
    gh(`label create "${label}" --color "B60205" --force`, { noThrow: true });
  }
}

// ── Run workflow URL ───────────────────────────────────────────────────────────
const repoUrl  = gh('repo view --json url -q .url', { noThrow: true });
const runId    = process.env.GITHUB_RUN_ID || '';
const runUrl   = runId ? `${repoUrl}/actions/runs/${runId}` : repoUrl;

// ── Audit logic ───────────────────────────────────────────────────────────────
ensureLabelsExist(['scraper-alert', 'plan-catalog-change', 'waf-blocked']);

// 1. SAPPER structure change — site redesign (highest severity)
if (sapperFailed.length > 0) {
  const existing = openIssueWithLabel('scraper-alert');
  if (!existing) {
    const slugs = sapperFailed.map((g) => g.plan_sku || g.slug || '?').join(', ');
    createIssue(
      `[scraper] SAPPER extraction failed on ${sapperFailed.length} plan(s)`,
      `Contabo may have changed their page structure.\n\nAffected plans: ${slugs}\n\nRun: ${runUrl}\n\nManual fix required — check __SAPPER__ marker in page source.`,
      ['scraper-alert'],
    );
  }
} else {
  const existing = openIssueWithLabel('scraper-alert');
  if (existing && !fetchFailed.length && !unclassified.length) {
    closeIssue(existing.number, 'All plans scraping successfully. Auto-closing.');
  }
}

// 2. Fetch failures after discovery ran
if (fetchFailed.length > 0) {
  const slugs = fetchFailed.map((g) => g.plan_sku || g.slug || '?').join(', ');
  const existing = openIssueWithLabel('scraper-alert');
  if (!existing) {
    createIssue(
      `[scraper] Fetch failed on ${fetchFailed.length} plan(s)`,
      `Discovery ran but these plans still failed to fetch.\n\nAffected: ${slugs}\n\nErrors:\n${fetchFailed.map((g) => `- ${g.plan_sku || g.slug}: ${g.error || ''}`).join('\n')}\n\nRun: ${runUrl}`,
      ['scraper-alert'],
    );
  }
}

// 3. WAF / discovery blocked
if (discoveryBlocked) {
  const existing = openIssueWithLabel('waf-blocked');
  if (!existing) {
    createIssue(
      '[scraper] Discovery blocked — listing pages returned 403',
      `All three Contabo listing pages were blocked (WAF / IP filter).\n\nThe scraper ran with the existing catalog.\n\nRun: ${runUrl}\n\nConsider setting RUNNER_TYPE to a self-hosted runner.`,
      ['waf-blocked'],
    );
  }
} else {
  const existing = openIssueWithLabel('waf-blocked');
  if (existing) closeIssue(existing.number, 'Discovery succeeded. Auto-closing WAF alert.');
}

// 4. Plan catalog changed
if (discoveryChanged) {
  createIssue(
    `[scraper] Plan catalog updated: +${addedCount} added, -${removedCount} removed, ~${renamedCount} renamed`,
    `Auto-healed plan catalog changes detected.\n\n- Added: ${addedCount}\n- Removed (discontinued): ${removedCount}\n- Renamed (URL redirects): ${renamedCount}\n\n\`data/plan_urls.json\` has been updated and committed.\n\nRun: ${runUrl}`,
    ['plan-catalog-change'],
  );
}

console.log(`Audit complete. fetch_failed=${fetchFailed.length} sapper_failed=${sapperFailed.length} unclassified=${unclassified.length}`);
