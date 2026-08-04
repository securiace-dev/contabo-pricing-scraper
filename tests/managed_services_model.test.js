'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const path = require('node:path');

const {
  calculateManagedQuote,
  loadManagedCatalog,
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

test('managed quote applies GST independently of EUR FX markup', () => {
  const quote = calculateManagedQuote(catalog, 'growth-managed', { gstEnabled: true });
  assert.deepEqual(quote, {
    plan_id: 'growth-managed',
    quantity: 1,
    annual_ex_gst_minor: 2430000,
    gst_minor: 437400,
    annual_total_minor: 2867400,
    monthly_equivalent_minor: 238950,
    founder_minutes_per_month: 180,
  });
});

test('quantity scales annual cost and founder minutes per managed server', () => {
  const quote = calculateManagedQuote(catalog, 'solo-managed', { quantity: 2, gstEnabled: false });
  assert.equal(quote.annual_total_minor, 2880000);
  assert.equal(quote.founder_minutes_per_month, 120);
});

test('object storage is intentionally not eligible for managed server tiers', () => {
  assert.equal(managedPlansForFamily(catalog, 'Object Storage').length, 0);
  assert.equal(managedPlansForFamily(catalog, 'Storage VPS').length, 3);
});

test('validation rejects a changed tier count', () => {
  const invalid = { ...catalog, plans: catalog.plans.slice(0, 2) };
  assert.throws(() => validateManagedCatalog(invalid), /exactly three tiers/);
});
