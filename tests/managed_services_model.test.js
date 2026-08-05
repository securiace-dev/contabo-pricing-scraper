'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const path = require('node:path');

const {
  calculateManagedQuote,
  loadManagedCatalog,
  makeManagedPurchaseSnapshot,
  managedPlansForFamily,
  validateManagedCatalog,
} = require('../.github/scripts/managed_services_model');

const catalogPath = path.join(__dirname, '..', 'data', 'managed_services_catalog.json');
const catalog = loadManagedCatalog(catalogPath);

test('catalog preserves the three requested INR annual tiers', () => {
  assert.deepEqual(
    catalog.plans.map(plan => [plan.id, plan.annual_price_minor, plan.founder_time.minutes_per_month]),
    [
      ['solo-managed', 1440000, 60],
      ['growth-managed', 2430000, 180],
      ['business-managed', 4230000, 360],
    ],
  );
});

test('managed quote defaults to all-inclusive with separate GST disabled', () => {
  const quote = calculateManagedQuote(catalog, 'growth-managed');
  assert.equal(quote.annual_ex_gst_minor, 2430000);
  assert.equal(quote.gst_minor, 0);
  assert.equal(quote.annual_total_minor, 2430000);
  assert.equal(quote.tax_mode_snapshot.enabled, false);
  assert.equal(quote.tax_mode_snapshot.basis, 'all_inclusive');
});

test('managed quote applies GST once only with verified commercial tax mode', () => {
  const quote = calculateManagedQuote(catalog, 'growth-managed', { taxMode: {
    enabled: true,
    registration_verified: true,
    basis: 'exclusive',
    source: 'whmcs-tax-config:test',
  } });
  assert.equal(quote.gst_minor, 437400);
  assert.equal(quote.annual_total_minor, 2867400);
  assert.equal(quote.monthly_equivalent_minor, 238950);
  assert.equal(quote.tax_mode_snapshot.rate, 0.18);
  assert.equal(quote.tax_mode_snapshot.source, 'whmcs-tax-config:test');
});

test('unverified GST request fails closed without adding tax', () => {
  const quote = calculateManagedQuote(catalog, 'solo-managed', { taxMode: {
    enabled: true, registration_verified: false, basis: 'exclusive',
  } });
  assert.equal(quote.gst_minor, 0);
  assert.equal(quote.annual_total_minor, 1440000);
  assert.equal(quote.tax_mode_snapshot.requested, true);
  assert.equal(quote.tax_mode_snapshot.enabled, false);
});

test('quantity scales annual cost and founder minutes per managed server', () => {
  const quote = calculateManagedQuote(catalog, 'solo-managed', { quantity: 2 });
  assert.equal(quote.annual_total_minor, 2880000);
  assert.equal(quote.founder_minutes_per_month, 120);
});

test('purchase snapshot freezes rate, expiry, emergency, tax, and internal provenance versions', () => {
  const snapshot = makeManagedPurchaseSnapshot(catalog, 'business-managed', {
    purchasedAt: '2026-08-05T12:00:00.000Z',
  });
  assert.equal(snapshot.quote.commercial_terms_snapshot.founder_overage.rate_minor_per_hour, 250000);
  assert.equal(snapshot.quote.commercial_terms_snapshot.monthly_entitlement.automatic_rollover, false);
  assert.equal(snapshot.quote.commercial_terms_snapshot.emergency_stabilization.approval_guardrail_minutes, 60);
  assert.equal(snapshot.internal_provenance.source_sum, 9);
  assert.equal(snapshot.internal_provenance.affects_billing, false);
  assert.equal(snapshot.quote.catalog_version, catalog.version);
});

test('canonical and legacy server families are eligible while object storage is excluded', () => {
  for (const family of [
    'Core VPS',
    'Performance VPS',
    'Max Performance VPS',
    'Cloud VPS',
    'Cloud VDS',
    'Storage VPS',
    'Dedicated Server',
  ]) {
    assert.equal(managedPlansForFamily(catalog, family).length, 3, family);
  }
  assert.equal(managedPlansForFamily(catalog, 'Object Storage').length, 0);
});

test('validation rejects a changed tier count', () => {
  const invalid = { ...catalog, plans: catalog.plans.slice(0, 2) };
  assert.throws(() => validateManagedCatalog(invalid), /exactly three tiers/);
});
