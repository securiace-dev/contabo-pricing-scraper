'use strict';

const fs = require('fs');

const MANAGED_SCHEMA_VERSION = 2;
const VALID_STATUSES = new Set(['draft', 'approved', 'retired']);
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

function requireBoolean(value, field) {
  if (typeof value !== 'boolean') fail(`${field} must be boolean`);
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function validateCommercialTerms(terms) {
  if (!isPlainObject(terms)) fail('commercial_terms must be an object');
  requireString(terms.version, 'commercial_terms.version');

  const overage = terms.founder_overage;
  if (!isPlainObject(overage)) fail('commercial_terms.founder_overage must be an object');
  requireInteger(overage.rate_minor_per_hour, 'commercial_terms.founder_overage.rate_minor_per_hour', { min: 1 });
  if (overage.currency !== 'INR') fail('commercial_terms.founder_overage.currency must be INR');
  if (overage.tax_basis !== 'exclusive') fail('commercial_terms.founder_overage.tax_basis must be exclusive');
  requireBoolean(overage.approval_required_before_work, 'commercial_terms.founder_overage.approval_required_before_work');
  requireBoolean(overage.estimate_required_before_work, 'commercial_terms.founder_overage.estimate_required_before_work');

  const entitlement = terms.monthly_entitlement;
  if (!isPlainObject(entitlement)) fail('commercial_terms.monthly_entitlement must be an object');
  requireBoolean(entitlement.expires_at_month_end, 'commercial_terms.monthly_entitlement.expires_at_month_end');
  requireBoolean(entitlement.automatic_rollover, 'commercial_terms.monthly_entitlement.automatic_rollover');
  if (!entitlement.expires_at_month_end || entitlement.automatic_rollover) {
    fail('monthly Founder hours must expire without automatic rollover');
  }
  const credit = entitlement.discretionary_credit;
  if (!isPlainObject(credit)) fail('commercial_terms.monthly_entitlement.discretionary_credit must be an object');
  for (const key of ['admin_only', 'requires_reason', 'requires_issued_at', 'requires_expires_at', 'non_contractual']) {
    requireBoolean(credit[key], `commercial_terms.monthly_entitlement.discretionary_credit.${key}`);
    if (!credit[key]) fail(`commercial_terms.monthly_entitlement.discretionary_credit.${key} must be true`);
  }

  const emergency = terms.emergency_stabilization;
  if (!isPlainObject(emergency)) fail('commercial_terms.emergency_stabilization must be an object');
  requireStringArray(emergency.definition, 'commercial_terms.emergency_stabilization.definition');
  requireStringArray(emergency.excludes, 'commercial_terms.emergency_stabilization.excludes');
  requireBoolean(emergency.minimum_stabilization_only, 'commercial_terms.emergency_stabilization.minimum_stabilization_only');
  requireInteger(emergency.approval_guardrail_minutes, 'commercial_terms.emergency_stabilization.approval_guardrail_minutes', { min: 1 });
  requireString(emergency.guardrail_status, 'commercial_terms.emergency_stabilization.guardrail_status');
}

function validateInternalProvenance(provenance) {
  if (!isPlainObject(provenance)) fail('internal_provenance must be an object');
  requireInteger(provenance.source_sum, 'internal_provenance.source_sum');
  requireString(provenance.meaning, 'internal_provenance.meaning');
  if (provenance.visibility !== 'internal_only') fail('internal_provenance.visibility must be internal_only');
  for (const key of ['affects_billing', 'affects_quantity', 'affects_tax', 'affects_term']) {
    requireBoolean(provenance[key], `internal_provenance.${key}`);
    if (provenance[key]) fail(`internal_provenance.${key} must be false`);
  }
}

function validateTaxActivation(activation) {
  if (!isPlainObject(activation)) fail('tax_activation must be an object');
  requireBoolean(activation.default_enabled, 'tax_activation.default_enabled');
  requireBoolean(activation.requires_registration_verified, 'tax_activation.requires_registration_verified');
  if (activation.default_enabled) fail('tax_activation.default_enabled must be false until registration is verified');
  if (!activation.requires_registration_verified) fail('tax_activation.requires_registration_verified must be true');
  requireString(activation.authority, 'tax_activation.authority');
  requireString(activation.source_conflict_status, 'tax_activation.source_conflict_status');
}

function validateFounderTime(time, planId) {
  if (!isPlainObject(time)) fail(`${planId}.founder_time must be an object`);
  requireInteger(time.minutes_per_month, `${planId}.founder_time.minutes_per_month`);
  requireString(time.scope, `${planId}.founder_time.scope`);
  for (const key of ['expiry_policy', 'overage_policy', 'proration_policy']) {
    requireString(time[key], `${planId}.founder_time.${key}`);
  }
  requireBoolean(time.automatic_rollover, `${planId}.founder_time.automatic_rollover`);
  if (time.expiry_policy !== 'expires_monthly' || time.automatic_rollover) {
    fail(`${planId}.founder_time must expire monthly without automatic rollover`);
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
  if (typeof raw.output_tax_eligible !== 'boolean') fail('output_tax_eligible must be boolean');
  if (typeof raw.nominal_output_tax_rate !== 'number' || !Number.isFinite(raw.nominal_output_tax_rate) || raw.nominal_output_tax_rate < 0 || raw.nominal_output_tax_rate > 1) {
    fail('nominal_output_tax_rate must be a finite ratio between 0 and 1');
  }
  validateTaxActivation(raw.output_tax_activation);
  requireStringArray(raw.eligible_families, 'eligible_families');
  requireString(raw.scope_unit, 'scope_unit');
  if (!VALID_PURCHASE_PATHS.has(raw.purchase_path)) fail('purchase_path must be review or consultation');
  requireString(raw.source_label, 'source_label');
  validateCommercialTerms(raw.commercial_terms);
  validateInternalProvenance(raw.internal_provenance);
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

function normalizeTaxMode(catalog, taxMode) {
  const input = isPlainObject(taxMode) ? taxMode : {};
  const requested = input.enabled === true;
  const registrationVerified = input.registration_verified === true;
  const enabled = requested && registrationVerified;
  const basis = enabled ? (input.basis === 'inclusive' ? 'inclusive' : 'exclusive') : 'all_inclusive';
  return {
    enabled,
    requested,
    registration_verified: registrationVerified,
    basis,
    rate: enabled ? catalog.nominal_output_tax_rate : 0,
    source: typeof input.source === 'string' && input.source.trim()
      ? input.source : catalog.output_tax_activation.authority,
  };
}

function calculateManagedQuote(catalog, planOrId, { quantity = 1, taxMode = null } = {}) {
  const plan = typeof planOrId === 'string' ? managedPlanFor(catalog, planOrId) : planOrId;
  if (!plan || !catalog.plans.includes(plan)) fail('quote plan must belong to the catalog');
  requireInteger(quantity, 'quantity', { min: 1 });
  const annualExGstMinor = plan.annual_price_minor * quantity;
  const taxModeSnapshot = normalizeTaxMode(catalog, taxMode);
  const appliesGst = catalog.output_tax_eligible && taxModeSnapshot.enabled && taxModeSnapshot.basis === 'exclusive';
  const gstMinor = appliesGst ? roundMinor(annualExGstMinor * taxModeSnapshot.rate) : 0;
  const annualTotalMinor = taxModeSnapshot.basis === 'inclusive'
    ? annualExGstMinor
    : annualExGstMinor + gstMinor;
  return {
    catalog_id: catalog.catalog_id,
    catalog_version: catalog.version,
    commercial_terms_version: catalog.commercial_terms.version,
    plan_id: plan.id,
    quantity,
    annual_ex_gst_minor: annualExGstMinor,
    gst_minor: gstMinor,
    annual_total_minor: annualTotalMinor,
    monthly_equivalent_minor: roundMinor(annualTotalMinor / catalog.billing_term_months),
    founder_minutes_per_month: plan.founder_time.minutes_per_month * quantity,
    commercial_terms_snapshot: clone(catalog.commercial_terms),
    tax_mode_snapshot: taxModeSnapshot,
  };
}

function makeManagedPurchaseSnapshot(catalog, planOrId, options = {}) {
  const quote = calculateManagedQuote(catalog, planOrId, options);
  return {
    snapshot_schema: 'founder-managed.purchase.v1',
    purchased_at: options.purchasedAt || new Date().toISOString(),
    catalog_id: catalog.catalog_id,
    catalog_version: catalog.version,
    effective_from: catalog.effective_from,
    currency: catalog.currency,
    nominal_output_tax_rate: catalog.nominal_output_tax_rate,
    output_tax_activation: clone(catalog.output_tax_activation),
    quote,
    internal_provenance: clone(catalog.internal_provenance),
  };
}

module.exports = {
  MANAGED_SCHEMA_VERSION,
  calculateManagedQuote,
  loadManagedCatalog,
  makeManagedPurchaseSnapshot,
  managedPlanFor,
  managedPlansForFamily,
  validateManagedCatalog,
};
