'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sourcePath = path.join(__dirname, '..', '.github', 'scripts', 'generate_html.js');
const source = fs.readFileSync(sourcePath, 'utf8');

test('report uses one reactive normalized pricing state across visible surfaces', () => {
  assert.match(source, /function reportPrice\(v,currency\)/);
  assert.match(source, /v:p=>eur\(p\.periods\[1\]\?\.effective_monthly\)/);
  assert.match(source, /row\('1 mo',p=>p\.periods\[1\]\?\.effective_monthly,eur/);
  assert.match(source, /order\.map\(m=>'<td>'\+eur\(per\[m\]\.effective_monthly\)/);
  assert.match(source, /FX\.markup=v;/);
  assert.match(source, /state\.ownerMarkup=v;/);
  assert.match(source, /lsSet\('contabo_fx_markup', String\(v\)\)/);
  assert.match(source, /lsSet\('contabo_owner_markup_pct',String\(v\*100\)\)/);
  assert.doesNotMatch(source, /Math\.min\(\s*0\.15|markup\s*[:=]\s*0\.15/i);
});

test('report exports client-safe artifacts separately from internal evidence', () => {
  assert.match(source, /data-proposal-export="client-html"/);
  assert.match(source, /data-proposal-export="client-json"/);
  assert.match(source, /data-proposal-export="client-csv"/);
  assert.match(source, /data-proposal-export="internal-json"/);
  assert.match(source, /PROPOSAL_MODEL\.clientProjection\(proposalSnapshot,proposalDocument\)/);
  assert.match(source, /proposalExportHtml\(PROPOSAL_MODEL\.clientDocument\(proposalSnapshot,proposalDocument\)\)/);
  assert.match(source, /PROPOSAL_MODEL\.internalEvidence\(proposalSnapshot\)/);
  assert.match(source, /PROPOSAL_MODEL\.toClientBrief\(proposalSnapshot\)/);
  assert.doesNotMatch(source, /contabo-proposal\.json',JSON\.stringify\(proposalSnapshot/);
});

test('Codex generation is capability-detected and deterministic output remains available', () => {
  assert.match(source, /fetch\('\/api\/v1\/openapi\.json'/);
  assert.match(source, /fetch\('\/api\/v1\/proposals\/capabilities'/);
  assert.match(source, /id="proposalGenerateBtn" type="button" disabled>Checking Codex capability/);
  assert.match(source, /Codex generation unavailable/);
  assert.match(source, /Deterministic preview and client exports are available/);
  assert.doesNotMatch(source, /Start the Rust server to use Codex CLI generation/);
});

test('proposal workspace includes managed selection, quantity, internal evidence notes, and sticky actions', () => {
  assert.match(source, /id="proposalManagedPlan"/);
  assert.match(source, /id="proposalManagedQuantity"/);
  assert.match(source, /proposalManagedInputFromForm\(primarySlug\)/);
  assert.match(source, /id="proposalInternalNotes"[^>]*maxlength="4000"/);
  assert.match(source, /internal=\{notes:document\.getElementById\('proposalInternalNotes'\)/);
  assert.match(source, /\.proposal-controls\{[^}]*position:sticky/s);
  assert.match(source, /max-height:calc\(100vh - 48px\);overflow:auto/);
});

test('browser FX uses same-origin API with embedded fallback and no direct Frankfurter request', () => {
  assert.match(source, /fetch\('\/api\/v1\/fx'/);
  const browserRefresh = source.slice(source.indexOf('// Refresh FX from the same-origin'));
  assert.doesNotMatch(browserRefresh, /fetch\('https:\/\/api\.frankfurter\.app/);
});

test('legacy flattened taxonomy is explicitly release-blocking in the report', () => {
  assert.match(source, /Legacy taxonomy dataset — not release-current/);
  assert.match(source, /Core VPS \(SSD only\), Performance VPS \(NVMe only\), and Max Performance VPS/);
  assert.match(source, /status: legacyTaxonomyFamilies\.length \? 'legacy_flattened' : 'canonical'/);
});

test('tax labels distinguish provider cash, recoverability, owner margin, and output GST order', () => {
  assert.match(source, /Provider tax <span class="pct">\+18%/);
  assert.match(source, /ITC recoverable/);
  assert.match(source, /Securiace output GST 18% applied after owner margin/);
  assert.match(source, /provider tax treatment and acquisition buffers are resolved first; verified Securiace output GST is applied afterward/);
});
