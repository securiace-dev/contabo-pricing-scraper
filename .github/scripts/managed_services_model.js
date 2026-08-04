'use strict';

const fs = require('fs');

const MANAGED_SCHEMA_VERSION = 1;
const VALID_STATUSES = new Set(['draft', 'approved', 'retired']);
const VALID_TAX_BASES = new Set(['exclusive', 'inclusive']);
const VALID_PURCHASE_PATHS = new Set(['review', 'consultation']);

function fail(message) {
  throw new Error(`Managed services catalog: ${message}`);
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requireString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') fail(`${field} must be a non-empty string`);
}

function requireStringArray(value, field, { allowEmpty = false } = {}) {
  if (!Array.isArray(value) || (!allowEmpty && value.length === 0)) {
    fail(`${field} must be a non-empty string array`);
  }
  value.forEach((item, index) => requireString(item, `${field}[${index}]`));
}

function requireInteger(value, field, { min = 0 } = {}) {
  if (!Number.isSafeInteger(value) || value < min) fail(`${field} must be an integer >= ${min}`);
}

function validateFounderTime(time, planId) {
  if (!isPlainObject(time)) fail(`${planId}.founder_time must be an object`);
  requireInteger(time.minutes_per_month, `${planId}.founder_time.minutes_per_month`);
  requireString(time.scope, `${planId}.founder_time.scope`);
  for (const key of ['rollover_policy', 'overage_policy', 'proration_policy']) {
    requireString(time[key], `${planId}.founder_time.${key}`);
  }
}

function validatePlan(plan, index, catalog) {
  if (!isPlainObject(plan)) fail(`plans[${index}] must be an object`);
  requireString(plan.id, `plans[${index}].id`);
  requireString(plan.name, `${plan.id}.name`);
  requireInteger(plan.annual_price_minor, `${plan.id}.annual_price_minor`, { min: 1 });
  validateFounderTime(plan.founder_time, plan.id);
  requireStringArray(plan.includes, `${plan.id}.includes`);
  requireStringArray(plan.excludes, `${plan.id}.excludes`);
  requireStringArray(plan.support_channels, `${plan.id}.support_channels`, { allowEmpty: true });
  if (plan.sla !== null) {
    if (!isPlainObject(plan.sla)) fail(`${plan.id}.sla must be an object or null`);
    requireString(plan.sla.label, `${plan.id}.sla.label`);
    requireString(plan.sla.status, `${plan.id}.sla.status`);
    if (typeof plan.sla.requires_evidence !== 'boolean') {
      fail(`${plan.id}.sla.requires_evidence must be boolean`);
    }
  }
  if (catalog.eligible_families.length === 0) fail('eligible_families must not be empty');
}

function validateManagedCatalog(raw) {
  if (!isPlainObject(raw)) fail('root must be an object');
  if (raw.schema_version !== MANAGED_SCHEMA_VERSION) {
    fail(`schema_version must be ${MANAGED_SCHEMA_VERSION}`);
  }
  requireString(raw.catalog_id, 'catalog_id');
  requireString(raw.version, 'version');
  requireString(raw.effective_from, 'effective_from');
  if (!VALID_STATUSES.has(raw.status)) fail(`status must be one of ${[...VALID_STATUSES].join(', ')}`);
  if (raw.currency !== 'INR') fail('currency must be INR');
  if (raw.billing_term_months !== 12) fail('billing_term_months must be 12');
  if (typeof raw.taxable !== 'boolean') fail('taxable must be boolean');
  if (!VALID_TAX_BASES.has(raw.tax_basis)) fail('tax_basis must be exclusive or inclusive');
  if (typeof raw.gst_rate !== 'number' || !Number.isFinite(raw.gst_rate) || raw.gst_rate < 0 || raw.gst_rate > 1) {
    fail('gst_rate must be a finite ratio between 0 and 1');
  }
  requireStringArray(raw.eligible_families, 'eligible_families');
  requireString(raw.scope_unit, 'scope_unit');
  if (!VALID_PURCHASE_PATHS.has(raw.purchase_path)) fail('purchase_path must be review or consultation');
  requireString(raw.source_label, 'source_label');
  requireStringArray(raw.review_flags, 'review_flags', { allowEmpty: true });
  requireStringArray(raw.common_exclusions, 'common_exclusions');
  if (!Array.isArray(raw.plans) || raw.plans.length !== 3) fail('plans must contain exactly three tiers');

  const ids = new Set();
  for (const [index, plan] of raw.plans.entries()) {
    validatePlan(plan, index, raw);
    if (ids.has(plan.id)) fail(`duplicate plan id ${plan.id}`);
    ids.add(plan.id);
  }
  return raw;
}

function loadManagedCatalog(filePath) {
  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (error) {
    fail(`unable to read ${filePath}: ${error.message}`);
  }
  return validateManagedCatalog(raw);
}

function managedPlanFor(catalog, planId) {
  const plan = catalog.plans.find(item => item.id === planId);
  if (!plan) fail(`unknown plan id ${planId}`);
  return plan;
}

function managedPlansForFamily(catalog, family) {
  return catalog.eligible_families.includes(family) ? catalog.plans : [];
}

function roundMinor(value) {
  return Math.round(Number(value));
}

function calculateManagedQuote(catalog, planOrId, { quantity = 1, gstEnabled = false } = {}) {
  const plan = typeof planOrId === 'string' ? managedPlanFor(catalog, planOrId) : planOrId;
  if (!plan || !catalog.plans.includes(plan)) fail('quote plan must belong to the catalog');
  requireInteger(quantity, 'quantity', { min: 1 });
  const annualExGstMinor = plan.annual_price_minor * quantity;
  const appliesGst = catalog.taxable && catalog.tax_basis === 'exclusive' && gstEnabled;
  const gstMinor = appliesGst ? roundMinor(annualExGstMinor * catalog.gst_rate) : 0;
  const annualTotalMinor = catalog.tax_basis === 'inclusive'
    ? annualExGstMinor
    : annualExGstMinor + gstMinor;
  return {
    plan_id: plan.id,
    quantity,
    annual_ex_gst_minor: annualExGstMinor,
    gst_minor: gstMinor,
    annual_total_minor: annualTotalMinor,
    monthly_equivalent_minor: roundMinor(annualTotalMinor / catalog.billing_term_months),
    founder_minutes_per_month: plan.founder_time.minutes_per_month * quantity,
  };
}

module.exports = {
  MANAGED_SCHEMA_VERSION,
  calculateManagedQuote,
  loadManagedCatalog,
  managedPlanFor,
  managedPlansForFamily,
  validateManagedCatalog,
};
