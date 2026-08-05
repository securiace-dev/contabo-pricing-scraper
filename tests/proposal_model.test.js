'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const model = require('../.github/scripts/proposal_model');

function input(overrides = {}) {
  return {
    profile: 'quick-quote',
    pricing: {
      currency: 'INR',
      provider_tax_charged: true,
      provider_tax_recoverable: false,
      provider_tax_rate: 0.18,
      fx_rate: 100,
      fx_markup: 0.10,
      payment_buffer: 0.02,
      owner_markup: 0.20,
      owner_markup_scope: 'provider_only',
      output_tax_enabled: true,
      output_tax_registration_verified: true,
      output_tax_rate: 0.18,
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

test('mirrors MarginCalculator landed-cost and cost-plus-percent semantics', () => {
  const quote = model.makeSnapshot(input()).primary.quote;
  assert.equal(quote.provider.monthly_eur, 12);
  assert.equal(quote.provider.tax_monthly_eur, 2.16);
  assert.deepEqual(quote.provider.landed_monthly, {
    local_base: 1200,
    provider_tax_cash: 216,
    fx_buffer: 120,
    payment_buffer: 24,
    landed: 1560,
  });
  assert.equal(quote.display.provider_monthly, 1560);
  assert.equal(quote.display.seller_pre_tax_monthly, 1872);
  assert.equal(quote.display.output_tax_monthly, 336.96);
  assert.equal(quote.display.monthly, 2208.96);
  assert.equal(quote.display.period_total, 2577.12);
  assert.equal(quote.display.owner_adjustment, 364);
});

test('recoverable provider tax remains cash provenance but is excluded from landed cost', () => {
  const quote = model.makeSnapshot(input({ pricing: {
    ...input().pricing, provider_tax_recoverable: true,
  }})).primary.quote;
  assert.equal(quote.provider.tax_monthly_eur, 2.16);
  assert.equal(quote.provider.landed_monthly.provider_tax_cash, 216);
  assert.equal(quote.provider.landed_monthly.landed, 1344);
});

test('visible 45% acquisition markup remains 45% and is never silently clamped to 15%', () => {
  const snapshot = model.makeSnapshot(input({ pricing: {
    currency: 'INR', fx_rate: 100, fx_markup: 0.45,
    owner_markup: 0, owner_markup_scope: 'provider_only',
  }}));
  assert.equal(snapshot.pricing.fx_markup, 0.45);
  assert.ok(!snapshot.warnings.some(warning => warning.code === 'fx_markup_capped'));
  assert.equal(model.clampPercent(45, 3.5, 100), 0.45);
  assert.equal(model.clampPercent(450, 3.5, 100), 1);
});

test('Core VPS 4 live combination captures ordered €13.34 recurring subtotal', () => {
  const snapshot = model.makeSnapshot(input({
    pricing: {
      currency: 'INR',
      provider_tax_charged: false,
      provider_tax_recoverable: false,
      provider_tax_rate: 0.18,
      fx_rate: 107.856,
      fx_markup: 0.45,
      payment_buffer: 0,
      owner_markup: 0.10,
      owner_markup_scope: 'provider_only',
      output_tax_enabled: false,
      output_tax_registration_verified: false,
      output_tax_rate: 0.18,
    },
    primary: {
      plan_slug: 'cloud-vps-core-4', plan_name: 'Core VPS 4', family: 'Core VPS',
      period_months: 1, provider_monthly_eur: 5.50, provider_setup_eur: 0,
      selections: [
        { label: 'Region: Asia (India)', monthly: 2.40 },
        { label: 'Data Protection: Auto Backup', monthly: 1.65 },
        { label: 'Private Networking: Enabled', monthly: 2.29 },
        { label: 'Storage: 200 GB SSD', monthly: 1.50 },
      ],
      specs: { storage_primary_type: 'SSD' },
    },
  }));
  const quote = snapshot.primary.quote;
  assert.equal(quote.provider.monthly_eur, 13.34);
  assert.equal(quote.provider.tax_monthly_eur, 0);
  assert.equal(quote.display.period_total, 2294.88);
  assert.deepEqual(quote.provenance.ordered_steps, [
    'provider_base_plus_options_eur',
    'provider_vendor_tax_cash_and_recoverability',
    'eur_to_inr_conversion_when_selected',
    'acquisition_card_fx_markup',
    'owner_margin',
    'managed_inr_lines_without_fx_or_vendor_tax',
    'securiace_output_tax_after_margin_when_verified',
    'display_rounding',
  ]);
  assert.equal(quote.provenance.output_tax_applied, false);
  assert.equal(quote.provenance.fx_card_markup, 0.45);
  assert.equal(quote.provenance.owner_margin, 0.10);
});

test('output GST fails closed without registration and applies after margin when verified', () => {
  const unverified = model.makeSnapshot(input({ pricing: {
    currency: 'EUR',
    provider_tax_charged: false,
    fx_markup: 0,
    owner_markup: 0.20,
    owner_markup_scope: 'provider_only',
    output_tax_enabled: true,
    output_tax_registration_verified: false,
    output_tax_rate: 0.18,
  }}));
  assert.equal(unverified.primary.quote.display.seller_pre_tax_monthly, 14.4);
  assert.equal(unverified.primary.quote.display.output_tax_monthly, 0);
  assert.equal(unverified.primary.quote.display.monthly, 14.4);
  assert.ok(unverified.warnings.some(warning => warning.code === 'output_tax_registration_unverified'));

  const enabled = model.makeSnapshot(input({ pricing: {
    ...unverified.pricing,
    output_tax_enabled: true,
    output_tax_registration_verified: true,
  }}));
  assert.equal(enabled.primary.quote.display.seller_pre_tax_monthly, 14.4);
  assert.equal(enabled.primary.quote.display.output_tax_monthly, 2.59);
  assert.equal(enabled.primary.quote.display.monthly, 16.99);
});

test('managed owner margin is pre-tax and output tax is calculated on seller amount', () => {
  const managed = {
    plan_id: 'growth-managed', name: 'Growth Managed', annual_price_minor: 2430000,
    billing_term_months: 12, founder_minutes_per_month: 180,
    output_tax_eligible: true, nominal_output_tax_rate: 0.18,
    includes: ['Monthly audit'], excludes: ['24/7 response'],
    catalog_version: '2026-08-05.1',
    commercial_terms_version: '2026-08-05.1',
    commercial_terms_snapshot: { founder_overage: { rate_minor_per_hour: 250000 } },
  };
  const snapshot = model.makeSnapshot(input({ managed, pricing: {
    ...input().pricing,
    provider_tax_charged: false,
    fx_markup: 0,
    payment_buffer: 0,
    owner_markup_scope: 'provider_and_managed',
    owner_markup: 0.10,
    output_tax_enabled: true,
    output_tax_registration_verified: true,
  }}));
  assert.equal(snapshot.managed.provider.annual_inr_minor, 2430000);
  assert.equal(snapshot.managed.seller.pre_tax_annual_inr_minor, 2673000);
  assert.equal(snapshot.managed.seller.owner_adjustment_inr_minor, 243000);
  assert.equal(snapshot.managed.seller.output_tax_inr_minor, 481140);
  assert.equal(snapshot.managed.seller.annual_inr_minor, 3154140);
  assert.equal(snapshot.managed.fx_applied, false);
  assert.equal(snapshot.managed.commercial_terms_snapshot.founder_overage.rate_minor_per_hour, 250000);
});

test('visibility supports all disclosure states and mandatory warnings', () => {
  const snapshot = model.makeSnapshot(input({
    visibility: {
      owner_markup: 'silent_include',
      fx_markup: 'exclude',
      provider_line_items: 'total_only',
      tax: 'calculated_only',
      internal_notes: 'show',
    },
    source: { snapshot_generated_at: '2020-01-01T00:00:00.000Z' },
  }));
  assert.equal(snapshot.visibility.owner_markup, 'silent_include');
  assert.equal(snapshot.visibility.tax, 'calculated_only');
  assert.ok(snapshot.warnings.some(warning => warning.code === 'stale_snapshot'));
  const html = model.renderDocument(model.deterministicDocument(snapshot));
  assert.match(html, /Estimated billed total/);
  assert.doesNotMatch(html, /owner_markup/);
  assert.match(html, /more than 31 days old/);
});

test('comparison warns on family mismatch and renderer escapes user content', () => {
  const snapshot = model.makeSnapshot(input({
    alternatives: [{
      plan_slug: 'vds-s', plan_name: '<script>alert(1)</script>', family: 'Max Performance VPS',
      period_months: 1, provider_monthly_eur: 20, provider_setup_eur: 0,
    }],
  }));
  assert.ok(snapshot.warnings.some(warning => warning.code === 'comparison_family_mismatch'));
  const html = model.renderDocument({
    title: '<img src=x onerror=alert(1)>',
    sections: [{ id: 'x', title: '<b>unsafe</b>', blocks: [{ type: 'paragraph', text: '<script>bad()</script>' }] }],
  });
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;bad\(\)&lt;\/script&gt;/);
});

test('HTML JSON CSV and safe narrative stay deterministic and policy-bound', () => {
  const snapshot = model.makeSnapshot(input({ profile: 'internal' }));
  const document = model.deterministicDocument(snapshot);
  const csv = model.toCsv(snapshot);
  const html = model.renderDocument(document);
  assert.match(csv, /^kind,label,value,currency\n/);
  assert.match(csv, /provider,monthly,12,EUR/);
  assert.match(csv, /provider_tax_cash,monthly,2\.16,EUR/);
  assert.match(csv, /adjustment,FX markup,10\.0%/);
  assert.match(csv, /adjustment,Owner markup,20\.0%/);
  assert.match(html, /Estimated billed total/);
  assert.equal(model.stableHash({ a: 1, b: 2 }), model.stableHash({ b: 2, a: 1 }));

  const safe = model.mergeSafeNarrative(document, {
    sections: [
      { id: 'summary', blocks: [{ type: 'paragraph', text: 'A concise migration summary.' }] },
      { id: 'pricing', blocks: [{ type: 'table', rows: [{ label: 'Total', value: '€999999' }] }] },
    ],
  });
  const safeHtml = model.renderDocument(safe);
  assert.match(safeHtml, /A concise migration summary/);
  assert.doesNotMatch(safeHtml, /999999/);
});
