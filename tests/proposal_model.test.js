'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const model = require('../.github/scripts/proposal_model');

function input(overrides = {}) {
  return {
    profile: 'quick-quote',
    pricing: {
      currency: 'INR',
      gst_enabled: true,
      gst_rate: 0.18,
      fx_rate: 100,
      fx_markup: 0.1,
      owner_markup: 0.2,
      owner_markup_scope: 'provider_only',
    },
    primary: {
      plan_slug: 'core-vps-4',
      plan_name: 'Core VPS 4',
      family: 'Core VPS',
      period_months: 1,
      provider_monthly_eur: 11,
      provider_setup_eur: 2,
      selections: [{ label: 'Private Networking', monthly: 1, setup: 0 }],
      specs: { cpu_count: 4, ram_gb: 8, storage_primary_type: 'SSD' },
    },
    client: { project_name: 'Migration' },
    ...overrides,
  };
}

test('cost-plus pipeline applies GST, FX markup, owner markup, and setup consistently', () => {
  const snapshot = model.makeSnapshot(input());
  const quote = snapshot.primary.quote;
  assert.equal(quote.provider.monthly_eur, 12.0);
  assert.equal(quote.provider.setup_eur, 2);
  assert.equal(quote.provider.taxed_monthly_eur, 14.16);
  assert.equal(quote.provider.taxed_setup_eur, 2.36);
  // (14.16 × 100 × 1.10 × 1.20) + (2.36 × 100 × 1.10 × 1.20)
  assert.equal(quote.display.period_total, 2180.64);
  assert.equal(quote.display.owner_adjustment, 363.44);
});

test('FX and owner fields clamp to safe ranges and invalid values fall back', () => {
  const snapshot = model.makeSnapshot(input({ pricing: {
    currency: 'INR', fx_rate: 100, gst_rate: 0.18, gst_enabled: true,
    fx_markup: 45, owner_markup: 'not-a-number', owner_markup_scope: 'provider_only',
  }}));
  assert.equal(snapshot.pricing.fx_markup, 0.15);
  assert.equal(snapshot.pricing.owner_markup, 0);
  assert.ok(snapshot.warnings.some(warning => warning.code === 'fx_markup_capped'));
  assert.equal(model.clampPercent(45, 3.5, 100), 0.45);
  assert.equal(model.clampPercent(450, 3.5, 100), 1);
});

test('managed services remain canonical INR and only receive owner markup when opted in', () => {
  const managed = {
    plan_id: 'growth-managed', name: 'Growth Managed', annual_price_minor: 2430000,
    billing_term_months: 12, founder_minutes_per_month: 180, taxable: true,
    tax_basis: 'exclusive', includes: ['Monthly audit'], excludes: ['24/7 response'],
  };
  const withoutOwner = model.makeSnapshot(input({ managed }));
  assert.equal(withoutOwner.managed.provider.annual_inr_minor, 2867400);
  assert.equal(withoutOwner.managed.seller.annual_inr_minor, 2867400);

  const withOwner = model.makeSnapshot(input({ managed, pricing: {
    ...input().pricing, owner_markup_scope: 'provider_and_managed', owner_markup: 0.1,
  }}));
  assert.equal(withOwner.managed.seller.annual_inr_minor, 3154140);
  assert.equal(withOwner.managed.fx_applied, false);
});

test('visibility supports client output, silent inclusion, and mandatory warnings', () => {
  const snapshot = model.makeSnapshot(input({
    visibility: {
      owner_markup: 'silent_include',
      fx_markup: 'exclude',
      provider_line_items: 'total_only',
      internal_notes: 'show',
    },
    source: { snapshot_generated_at: '2020-01-01T00:00:00.000Z' },
  }));
  assert.equal(snapshot.visibility.owner_markup, 'silent_include');
  assert.ok(snapshot.warnings.some(warning => warning.code === 'stale_snapshot'));
  const document = model.deterministicDocument(snapshot);
  const html = model.renderDocument(document);
  assert.match(html, /Estimated billed total/);
  assert.doesNotMatch(html, /owner_markup/);
  assert.match(html, /more than 31 days old/);
});

test('comparison emits a family mismatch warning and document renderer escapes content', () => {
  const snapshot = model.makeSnapshot(input({
    primary: input().primary,
    alternatives: [{
      plan_slug: 'vds-s', plan_name: '<script>alert(1)</script>', family: 'Max Performance VPS',
      period_months: 1, provider_monthly_eur: 20, provider_setup_eur: 0,
    }],
  }));
  assert.ok(snapshot.warnings.some(warning => warning.code === 'comparison_family_mismatch'));
  const document = model.deterministicDocument(snapshot);
  const html = model.renderDocument({
    ...document,
    title: '<img src=x onerror=alert(1)>',
    sections: [{ id: 'x', title: '<b>unsafe</b>', blocks: [{ type: 'paragraph', text: '<script>bad()</script>' }] }],
  });
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;bad\(\)&lt;\/script&gt;/);
});

test('CSV export is deterministic and quotes user content', () => {
  const snapshot = model.makeSnapshot(input({ client: { project_name: 'Acme, Inc.' } }));
  const csv = model.toCsv(snapshot);
  assert.match(csv, /^kind,label,value,currency\n/);
  assert.match(csv, /provider,monthly,14\.16,EUR/);
  assert.equal(model.stableHash({ a: 1, b: 2 }), model.stableHash({ b: 2, a: 1 }));
});
