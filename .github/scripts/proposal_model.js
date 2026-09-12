'use strict';

// Shared proposal math and document contract. The same module is required by
// Node tests/build tooling and injected into report.html for browser use. Keep
// this file dependency-free: generated reports are self-contained artifacts.
(function installProposalModel(root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.ContaboProposalModel = factory();
}(typeof globalThis === 'object' ? globalThis : this, function createProposalModel() {
  const SCHEMA_VERSION = 'proposal.snapshot.v1';
  const DOCUMENT_SCHEMA_VERSION = 'proposal.v1';
  // A visible percentage must never be silently recalculated at a different
  // percentage. Both FX/card markup and owner margin use the same explicit
  // 0–100% boundary at every input and calculation surface.
  const MAX_FX_MARKUP = 1;
  const MAX_OWNER_MARKUP = 1;
  const ROUND_SCALE = 100;

  const VISIBILITY_LABELS = Object.freeze({
    show: 'Show',
    total_only: 'Total only',
    silent_include: 'Silent include',
    internal_only: 'Internal only',
    exclude: 'Exclude',
    calculated_only: 'Calculated only',
  });

  const DEFAULT_VISIBILITY = Object.freeze({
    configuration: 'show',
    provider_pricing: 'show',
    provider_line_items: 'show',
    managed_services: 'show',
    alternatives: 'show',
    source_links: 'show',
    tax: 'show',
    fx_markup: 'internal_only',
    owner_markup: 'internal_only',
    client_notes: 'show',
    internal_notes: 'internal_only',
    warnings: 'show',
  });

  // Visibility is field-specific. Invalid combinations fail closed to an
  // internal/hidden state instead of becoming client-visible by accident.
  const VISIBILITY_RULES = Object.freeze({
    configuration: {
      allowed: ['show', 'total_only', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Show details, or keep configuration facts out of the client artifact.',
    },
    provider_pricing: {
      allowed: ['show', 'total_only', 'calculated_only', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Total/calculated modes expose only the seller total, never provider cost facts.',
    },
    provider_line_items: {
      allowed: ['show', 'total_only', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Line items are named only when explicitly set to Show.',
    },
    managed_services: {
      allowed: ['show', 'total_only', 'calculated_only', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Total/calculated modes include the managed total without naming tier or Founder time.',
    },
    alternatives: {
      allowed: ['show', 'total_only', 'calculated_only', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Total/calculated modes show anonymous alternative totals only.',
    },
    source_links: {
      allowed: ['show', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Source links are either client-visible, internal, or omitted.',
    },
    tax: {
      allowed: ['show', 'calculated_only', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Calculated-only keeps tax in the verified total without naming the tax treatment.',
    },
    fx_markup: {
      allowed: ['show', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'FX markup is named only when explicitly set to Show.',
    },
    owner_markup: {
      allowed: ['show', 'silent_include', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Owner margin is named only when explicitly set to Show.',
    },
    client_notes: {
      allowed: ['show', 'internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Notes are client-visible only when explicitly set to Show.',
    },
    internal_notes: {
      allowed: ['internal_only', 'exclude'],
      invalid: 'internal_only',
      help: 'Internal notes never enter a client artifact.',
    },
    warnings: {
      allowed: ['show'],
      invalid: 'show',
      help: 'Mandatory review warnings cannot be hidden.',
    },
  });

  const PROFILE_DEFAULTS = Object.freeze({
    'quick-quote': { label: 'Quick quote', visibility: {} },
    technical: {
      label: 'Technical proposal',
      visibility: { configuration: 'show', provider_line_items: 'show', alternatives: 'show' },
    },
    managed: {
      label: 'Managed proposal',
      visibility: { managed_services: 'show', owner_markup: 'internal_only' },
    },
    comparison: {
      label: 'Plan comparison',
      visibility: { alternatives: 'show', provider_line_items: 'total_only' },
    },
    internal: {
      label: 'Internal review',
      visibility: {
        fx_markup: 'show', owner_markup: 'show', internal_notes: 'internal_only',
        provider_line_items: 'show', warnings: 'show',
      },
    },
  });

  function finite(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
  }

  function clampFraction(raw, fallback = 0, maximum = 1) {
    const value = Number(raw);
    const safe = Number.isFinite(value) ? value : fallback;
    return Math.max(0, Math.min(maximum, safe));
  }

  function clampPercent(raw, fallback = 0, maximum = 100) {
    return clampFraction(raw, fallback, maximum) / 100;
  }

  function round(value, digits = 2) {
    const factor = 10 ** digits;
    return Math.round((finite(value) + Number.EPSILON) * factor) / factor;
  }

  function minor(value) {
    return Math.round(finite(value));
  }

  function sortedObject(value) {
    if (Array.isArray(value)) return value.map(sortedObject);
    if (!value || typeof value !== 'object') return value;
    return Object.keys(value).sort().reduce((out, key) => {
      out[key] = sortedObject(value[key]);
      return out;
    }, {});
  }

  function stableStringify(value) {
    return JSON.stringify(sortedObject(value));
  }

  // Deterministic 32-bit FNV-1a is sufficient for a local UI identity only.
  // It is NOT collision-resistant and must never be used as an integrity,
  // authenticity, approval, delivery, or security hash. Server-side persisted
  // artifacts use SHA-256 under the contract documented in Plan 005.
  function stableLocalId(value) {
    const raw = stableStringify(value);
    let hash = 2166136261;
    for (let i = 0; i < raw.length; i += 1) {
      hash ^= raw.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return `local-proposal-${(hash >>> 0).toString(16).padStart(8, '0')}`;
  }

  // Backward-compatible name for older report artifacts. Security-sensitive
  // callers must not use this alias; see stableLocalId above.
  const stableHash = stableLocalId;

  function normalizeVisibility(profile, provided) {
    const profileConfig = PROFILE_DEFAULTS[profile] || PROFILE_DEFAULTS['quick-quote'];
    const raw = provided && typeof provided === 'object' ? provided : {};
    const profileVisibility = profileConfig.visibility || {};
    const merged = {};
    for (const key of Object.keys(DEFAULT_VISIBILITY)) {
      const rule = VISIBILITY_RULES[key];
      const hasProvidedValue = Object.prototype.hasOwnProperty.call(raw, key);
      const candidate = hasProvidedValue
        ? raw[key]
        : (profileVisibility[key] ?? DEFAULT_VISIBILITY[key]);
      merged[key] = rule.allowed.includes(candidate) ? candidate : rule.invalid;
    }
    // A warning about missing FX or stale data cannot be hidden by a client
    // profile. A user may hide the non-mandatory source note separately.
    merged.warnings = 'show';
    return merged;
  }

  function sumDeltas(items) {
    const out = { monthly: 0, setup: 0, labels: [] };
    for (const item of Array.isArray(items) ? items : []) {
      const monthly = finite(item.monthly ?? item.monthly_price_delta);
      const setup = finite(item.setup ?? item.setup_fee_delta);
      out.monthly += monthly;
      out.setup += setup;
      const label = item.label ?? item.option_label;
      if (label != null && String(label).trim()) {
        out.labels.push({ label: String(label), monthly: round(monthly), setup: round(setup) });
      }
    }
    return out;
  }

  function normalizePricing(input) {
    const pricing = input && input.pricing && typeof input.pricing === 'object'
      ? input.pricing
      : (input && typeof input === 'object' ? input : {});
    const providerPricesIncludeTax = pricing.provider_prices_include_tax === true;
    const providerTaxCharged = pricing.provider_tax_charged === true;
    if (providerPricesIncludeTax && !providerTaxCharged) {
      throw new Error('provider_prices_include_tax=true requires provider_tax_charged=true');
    }
    const providerTaxRecoverable = providerTaxCharged && pricing.provider_tax_recoverable === true;
    const providerTaxRate = clampFraction(pricing.provider_tax_rate, 0.18, 1);
    const outputTaxRequested = pricing.output_tax_enabled === true || pricing.output_tax_requested === true;
    const outputTaxRegistrationVerified = pricing.output_tax_registration_verified === true;
    const outputTaxEnabled = outputTaxRequested && outputTaxRegistrationVerified;
    const outputTaxRate = clampFraction(pricing.output_tax_rate, 0.18, 1);
    const fxRate = finite(pricing.fx_rate, 0);
    const fxMarkup = clampFraction(pricing.fx_markup, 0, MAX_FX_MARKUP);
    const paymentBuffer = clampFraction(pricing.payment_buffer, 0, 1);
    const ownerMarkup = clampFraction(pricing.owner_markup, 0, MAX_OWNER_MARKUP);
    const ownerScope = pricing.owner_markup_scope || 'provider_only';
    const currency = pricing.currency === 'INR' ? 'INR' : 'EUR';
    return {
      currency,
      provider_prices_include_tax: providerPricesIncludeTax,
      provider_tax_charged: providerTaxCharged,
      provider_tax_recoverable: providerTaxRecoverable,
      provider_tax_rate: providerTaxRate,
      provider_tax_source: pricing.provider_tax_source || 'operator_pricing_snapshot',
      output_tax_enabled: outputTaxEnabled,
      output_tax_requested: outputTaxRequested,
      output_tax_registration_verified: outputTaxRegistrationVerified,
      output_tax_rate: outputTaxRate,
      output_tax_source: pricing.output_tax_source || 'whmcs_commercial_settings_snapshot',
      fx_rate: fxRate > 0 ? fxRate : null,
      fx_markup: fxMarkup,
      payment_buffer: paymentBuffer,
      owner_markup: ownerMarkup,
      owner_markup_scope: ['provider_only', 'provider_and_managed'].includes(ownerScope)
        ? ownerScope : 'provider_only',
      fx_source: pricing.fx_source || null,
      fx_rate_date: pricing.fx_rate_date || null,
    };
  }

  function providerTaxBreakdown(listedEurValue, pricing) {
    const listedEur = finite(listedEurValue);
    if (pricing.provider_prices_include_tax) {
      const divisor = 1 + pricing.provider_tax_rate;
      const netEur = divisor > 0 ? listedEur / divisor : listedEur;
      return {
        listed_eur: listedEur,
        net_eur: netEur,
        tax_cash_eur: listedEur - netEur,
        cash_eur: listedEur,
      };
    }
    const taxCashEur = pricing.provider_tax_charged
      ? listedEur * pricing.provider_tax_rate : 0;
    return {
      listed_eur: listedEur,
      net_eur: listedEur,
      tax_cash_eur: taxCashEur,
      cash_eur: listedEur + taxCashEur,
    };
  }

  // Narrow JS mirror of MarginCalculator::landedCostMonthly. The report uses
  // cost_plus_pct only; amount/fixed strategies remain server-side WHMCS work.
  function landedCost(taxParts, pricing) {
    const localMultiplier = pricing.currency === 'INR' && pricing.fx_rate
      ? pricing.fx_rate : 1;
    const localBase = taxParts.net_eur * localMultiplier;
    const providerTaxCash = taxParts.tax_cash_eur * localMultiplier;
    const fxBuffer = pricing.currency === 'INR' ? localBase * pricing.fx_markup : 0;
    const paymentBuffer = localBase * pricing.payment_buffer;
    const nonRecoverableProviderTax = pricing.provider_tax_recoverable ? 0 : providerTaxCash;
    return {
      local_base: localBase,
      provider_tax_cash: providerTaxCash,
      fx_buffer: fxBuffer,
      payment_buffer: paymentBuffer,
      landed: localBase + nonRecoverableProviderTax + fxBuffer + paymentBuffer,
    };
  }

  function calculateQuote(input) {
    const source = input && typeof input === 'object' ? input : {};
    const pricing = normalizePricing(source);
    const periodMonths = Math.max(1, Math.min(120, Math.round(finite(source.period_months, 1))));
    const optionDeltas = sumDeltas(source.selections);
    const addonDeltas = sumDeltas(source.addons);
    const baseMonthly = finite(source.provider_monthly_eur ?? source.base_monthly_eur);
    const baseSetup = finite(source.provider_setup_eur ?? source.base_setup_eur);
    const providerMonthly = round(baseMonthly + optionDeltas.monthly + addonDeltas.monthly);
    const providerSetup = round(baseSetup + optionDeltas.setup + addonDeltas.setup);
    const monthlyTaxParts = providerTaxBreakdown(providerMonthly, pricing);
    const setupTaxParts = providerTaxBreakdown(providerSetup, pricing);
    const providerTaxMonthly = round(monthlyTaxParts.tax_cash_eur);
    const providerTaxSetup = round(setupTaxParts.tax_cash_eur);
    const providerCashMonthly = round(monthlyTaxParts.cash_eur);
    const providerCashSetup = round(setupTaxParts.cash_eur);
    const providerPeriodTotal = round(providerCashMonthly * periodMonths + providerCashSetup);
    const ownerEnabled = pricing.owner_markup_scope === 'provider_only'
      || pricing.owner_markup_scope === 'provider_and_managed';
    const monthlyLanded = landedCost(monthlyTaxParts, pricing);
    const setupLanded = landedCost(setupTaxParts, pricing);
    const acquisitionMonthly = monthlyLanded.landed;
    const acquisitionSetup = setupLanded.landed;
    const sellerPreTaxMonthly = round(acquisitionMonthly * (ownerEnabled ? 1 + pricing.owner_markup : 1));
    const sellerPreTaxSetup = round(acquisitionSetup * (ownerEnabled ? 1 + pricing.owner_markup : 1));
    const outputTaxMonthly = pricing.output_tax_enabled
      ? round(sellerPreTaxMonthly * pricing.output_tax_rate) : 0;
    const outputTaxSetup = pricing.output_tax_enabled
      ? round(sellerPreTaxSetup * pricing.output_tax_rate) : 0;
    const displayedMonthly = round(sellerPreTaxMonthly + outputTaxMonthly);
    const displayedSetup = round(sellerPreTaxSetup + outputTaxSetup);
    const displayedPeriodTotal = round(displayedMonthly * periodMonths + displayedSetup);
    const ownerAdjustment = round((sellerPreTaxMonthly - acquisitionMonthly) * periodMonths +
      (sellerPreTaxSetup - acquisitionSetup));
    const outputTaxTotal = round(outputTaxMonthly * periodMonths + outputTaxSetup);
    const warnings = [];
    if (pricing.currency === 'INR' && !pricing.fx_rate) {
      warnings.push({ code: 'fx_unavailable', message: 'INR estimate unavailable until an EUR→INR rate is supplied.', mandatory: true });
    }
    if (pricing.output_tax_requested && !pricing.output_tax_registration_verified) {
      warnings.push({
        code: 'output_tax_registration_unverified',
        message: 'Securiace output GST was requested but not applied because verified registration evidence is absent.',
        mandatory: true,
      });
    }
    if (finite(source.pricing?.fx_markup ?? source.fx_markup) > MAX_FX_MARKUP) {
      warnings.push({ code: 'fx_markup_capped', message: 'FX/card markup was capped at the explicit 100% input boundary.', mandatory: true });
    }
    if (finite(source.pricing?.owner_markup ?? source.owner_markup) > MAX_OWNER_MARKUP) {
      warnings.push({ code: 'owner_markup_capped', message: 'Owner markup was capped at 100% for this report.', mandatory: true });
    }
    return {
      period_months: periodMonths,
      selections: optionDeltas.labels.concat(addonDeltas.labels),
      provider: {
        monthly_eur: providerMonthly,
        setup_eur: providerSetup,
        net_monthly_eur: round(monthlyTaxParts.net_eur),
        net_setup_eur: round(setupTaxParts.net_eur),
        tax_monthly_eur: providerTaxMonthly,
        tax_setup_eur: providerTaxSetup,
        cash_monthly_eur: providerCashMonthly,
        cash_setup_eur: providerCashSetup,
        landed_monthly: monthlyLanded,
        landed_setup: setupLanded,
        period_total_eur: providerPeriodTotal,
        prices_include_tax: pricing.provider_prices_include_tax,
        option_delta_monthly_eur: round(optionDeltas.monthly),
        option_delta_setup_eur: round(optionDeltas.setup),
        addon_delta_monthly_eur: round(addonDeltas.monthly),
        addon_delta_setup_eur: round(addonDeltas.setup),
      },
      display: {
        currency: pricing.currency,
        monthly: displayedMonthly,
        setup: displayedSetup,
        period_total: displayedPeriodTotal,
        provider_monthly: acquisitionMonthly,
        provider_setup: acquisitionSetup,
        seller_pre_tax_monthly: sellerPreTaxMonthly,
        seller_pre_tax_setup: sellerPreTaxSetup,
        output_tax_monthly: outputTaxMonthly,
        output_tax_setup: outputTaxSetup,
        output_tax_total: outputTaxTotal,
        owner_adjustment: ownerAdjustment,
      },
      provenance: {
        formula_version: 'margin-calculator-compatible.v1-cost-plus-pct',
        ordered_steps: [
          'provider_base_plus_options_eur',
          'provider_vendor_tax_cash_and_recoverability',
          'eur_to_inr_conversion_when_selected',
          'acquisition_card_fx_markup',
          'owner_margin',
          'managed_inr_lines_without_fx_or_vendor_tax',
          'securiace_output_tax_after_margin_when_verified',
          'display_rounding',
        ],
        provider_tax_charged: pricing.provider_tax_charged,
        provider_tax_recoverable: pricing.provider_tax_recoverable,
        provider_tax_rate: pricing.provider_tax_rate,
        provider_tax_source: pricing.provider_tax_source,
        output_tax_applied: pricing.output_tax_enabled,
        output_tax_rate: pricing.output_tax_rate,
        output_tax_source: pricing.output_tax_source,
        output_tax_registration_verified: pricing.output_tax_registration_verified,
        fx_rate: pricing.currency === 'INR' ? pricing.fx_rate : null,
        fx_card_markup: pricing.currency === 'INR' ? pricing.fx_markup : 0,
        payment_buffer: pricing.payment_buffer,
        owner_margin: pricing.owner_markup,
        owner_margin_scope: pricing.owner_markup_scope,
      },
      pricing,
      warnings,
    };
  }

  function calculateManaged(input, pricing) {
    const source = input && typeof input === 'object' ? input : {};
    if (!source.plan_id) return null;
    const quantity = Math.max(1, Math.min(99, Math.round(finite(source.quantity, 1))));
    const annualPrice = minor(source.annual_price_minor) * quantity;
    const applyOwner = pricing.owner_markup_scope === 'provider_and_managed';
    const sellerPreTax = applyOwner ? minor(annualPrice * (1 + pricing.owner_markup)) : annualPrice;
    const outputTax = pricing.output_tax_enabled && source.output_tax_eligible !== false
      ? minor(sellerPreTax * pricing.output_tax_rate) : 0;
    const sellerAnnual = sellerPreTax + outputTax;
    return {
      plan_id: String(source.plan_id),
      name: String(source.name || source.plan_id),
      quantity,
      founder_minutes_per_month: minor(source.founder_minutes_per_month) * quantity,
      provider: { annual_inr_minor: annualPrice, vendor_tax_inr_minor: 0, fx_applied: false },
      seller: {
        annual_inr_minor: sellerAnnual,
        pre_tax_annual_inr_minor: sellerPreTax,
        output_tax_inr_minor: outputTax,
        monthly_equivalent_inr_minor: minor(sellerAnnual / Math.max(1, finite(source.billing_term_months, 12))),
        owner_adjustment_inr_minor: sellerPreTax - annualPrice,
      },
      includes: Array.isArray(source.includes) ? source.includes.map(String) : [],
      excludes: Array.isArray(source.excludes) ? source.excludes.map(String) : [],
      currency: 'INR',
      fx_applied: false,
      catalog_version: source.catalog_version || null,
      commercial_terms_version: source.commercial_terms_version || null,
      commercial_terms_snapshot: source.commercial_terms_snapshot && typeof source.commercial_terms_snapshot === 'object'
        ? JSON.parse(JSON.stringify(source.commercial_terms_snapshot)) : null,
    };
  }

  function normalizePlan(plan, pricing) {
    const source = plan && typeof plan === 'object' ? plan : {};
    const quote = source.quote || calculateQuote({
      provider_monthly_eur: source.provider_monthly_eur,
      provider_setup_eur: source.provider_setup_eur,
      period_months: source.period_months,
      selections: source.selections,
      addons: source.addons,
      pricing,
    });
    return {
      plan_slug: String(source.plan_slug || ''),
      plan_name: String(source.plan_name || source.name || source.plan_slug || 'Selected plan'),
      family: String(source.family || 'Provider service'),
      plan_url: source.plan_url || source.url || null,
      period_months: quote.period_months,
      specs: source.specs && typeof source.specs === 'object' ? source.specs : {},
      selections: Array.isArray(source.selections) ? source.selections : [],
      addons: Array.isArray(source.addons) ? source.addons : [],
      quote,
    };
  }

  function makeSnapshot(input) {
    const source = input && typeof input === 'object' ? input : {};
    const profile = String(source.profile || 'quick-quote');
    const pricing = normalizePricing(source.pricing);
    const visibility = normalizeVisibility(profile, source.visibility);
    const primary = normalizePlan(source.primary, pricing);
    const alternatives = (Array.isArray(source.alternatives) ? source.alternatives : [])
      .slice(0, 4).map(plan => normalizePlan(plan, pricing));
    const managed = calculateManaged(source.managed, pricing);
    const client = source.client && typeof source.client === 'object' ? { ...source.client } : {};
    const internal = source.internal && typeof source.internal === 'object'
      ? { notes: String(source.internal.notes || '').slice(0, 4000) }
      : { notes: '' };
    const warnings = [...primary.quote.warnings];
    if (finite(source.pricing?.fx_markup) > MAX_FX_MARKUP &&
        !warnings.some(warning => warning.code === 'fx_markup_capped')) {
      warnings.push({ code: 'fx_markup_capped', message: 'FX/card markup was capped at the explicit 100% input boundary.', mandatory: true });
    }
    if (finite(source.pricing?.owner_markup) > MAX_OWNER_MARKUP &&
        !warnings.some(warning => warning.code === 'owner_markup_capped')) {
      warnings.push({ code: 'owner_markup_capped', message: 'Owner markup was capped at 100% for this report.', mandatory: true });
    }
    if (alternatives.some(plan => plan.family !== primary.family)) {
      warnings.push({ code: 'comparison_family_mismatch', message: 'Compared plans are from different product families; verify feature parity before sending.', mandatory: true });
    }
    if (!primary.plan_slug) {
      warnings.push({ code: 'missing_plan', message: 'No primary plan was selected.', mandatory: true });
    }
    if (source.source && source.source.snapshot_generated_at) {
      const generatedAt = Date.parse(source.source.snapshot_generated_at);
      if (Number.isFinite(generatedAt) && Date.now() - generatedAt > 31 * 86400000) {
        warnings.push({ code: 'stale_snapshot', message: 'The source pricing snapshot is more than 31 days old.', mandatory: true });
      }
    }
    const snapshot = {
      schema_version: SCHEMA_VERSION,
      generated_at: source.generated_at || new Date().toISOString(),
      profile,
      profile_label: (PROFILE_DEFAULTS[profile] || PROFILE_DEFAULTS['quick-quote']).label,
      visibility,
      pricing,
      client,
      internal,
      primary,
      alternatives,
      managed,
      source: source.source && typeof source.source === 'object' ? { ...source.source } : {},
      warnings,
    };
    snapshot.snapshot_id = stableLocalId({
      schema_version: snapshot.schema_version,
      profile: snapshot.profile,
      visibility: snapshot.visibility,
      pricing: snapshot.pricing,
      client: snapshot.client,
      internal: snapshot.internal,
      primary: snapshot.primary,
      alternatives: snapshot.alternatives,
      managed: snapshot.managed,
      source: snapshot.source,
    });
    snapshot.snapshot_id_kind = 'local_non_security_fingerprint';
    return snapshot;
  }

  function clientVisibility(snapshot, key) {
    const setting = snapshot.visibility?.[key] || 'show';
    if (setting === 'show') return 'show';
    if (setting === 'total_only' || setting === 'calculated_only') return 'total_only';
    return 'hidden';
  }

  function deterministicDocument(snapshot, options = {}) {
    const primary = snapshot.primary;
    const sections = [];
    const audience = options.audience === 'client' ? 'client' : 'operator';
    const configurationVisibility = clientVisibility(snapshot, 'configuration');
    const mayNameConfiguration = configurationVisibility !== 'hidden';
    const project = snapshot.client?.project_name ? ` for ${snapshot.client.project_name}` : '';
    sections.push({
      id: 'summary', title: 'Summary', blocks: [{
        type: 'paragraph',
        text: mayNameConfiguration
          ? `This ${snapshot.profile_label.toLowerCase()}${project} is based on the selected ${primary.plan_name} configuration and the current report snapshot.`
          : `This ${snapshot.profile_label.toLowerCase()}${project} is based on the approved commercial calculation and the current report snapshot.`,
      }],
    });
    if (configurationVisibility !== 'hidden') {
      const rows = [
        { label: 'Plan', value: primary.plan_name },
        { label: 'Family', value: primary.family },
        { label: 'Term', value: `${primary.period_months} month${primary.period_months === 1 ? '' : 's'}` },
      ];
      if (configurationVisibility === 'show') {
        for (const [key, value] of Object.entries(primary.specs || {})) {
          if (value != null && value !== '') rows.push({ label: key.replaceAll('_', ' '), value: String(value) });
        }
        for (const item of primary.quote.selections) rows.push({ label: item.label, value: item.monthly || item.setup ? `${item.monthly ? `+€${item.monthly}/mo` : ''}${item.setup ? ` +€${item.setup} setup` : ''}` : 'Included' });
      }
      sections.push({ id: 'configuration', title: 'Selected configuration', blocks: [{ type: 'table', rows }] });
    }
    if (snapshot.visibility.source_links === 'show') {
      const sourceRows = [];
      if (primary.plan_url) sourceRows.push({ label: 'Provider plan', value: primary.plan_url });
      if (snapshot.source.source) sourceRows.push({ label: 'Pricing snapshot', value: snapshot.source.source });
      if (sourceRows.length) sections.push({ id: 'sources', title: 'Sources', blocks: [{ type: 'table', rows: sourceRows }] });
    }
    const pricingVisibility = clientVisibility(snapshot, 'provider_pricing');
    if (pricingVisibility !== 'hidden') {
      const rows = [];
      const showLineItems = pricingVisibility === 'show' &&
        clientVisibility(snapshot, 'provider_line_items') === 'show';
      if (showLineItems) {
        rows.push({ label: 'Provider listed/configured monthly', value: `€${primary.quote.provider.monthly_eur.toFixed(2)}` });
        if (primary.quote.provider.tax_monthly_eur) {
          rows.push({ label: 'Provider/vendor tax cash', value: `€${primary.quote.provider.tax_monthly_eur.toFixed(2)}` });
        }
        if (primary.quote.provider.setup_eur) rows.push({ label: 'Provider setup', value: `€${primary.quote.provider.setup_eur.toFixed(2)}` });
      }
      if (pricingVisibility === 'show' && clientVisibility(snapshot, 'tax') === 'show') {
        rows.push({ label: 'Provider tax treatment', value: snapshot.pricing.provider_tax_charged
          ? `${(snapshot.pricing.provider_tax_rate * 100).toFixed(1)}% charged · ${snapshot.pricing.provider_tax_recoverable ? 'recoverable' : 'landed cost'}`
          : 'Not charged' });
        rows.push({ label: 'Securiace output tax', value: snapshot.pricing.output_tax_enabled
          ? `${(snapshot.pricing.output_tax_rate * 100).toFixed(1)}% applied after margin`
          : 'Disabled' });
      }
      rows.push({ label: 'Estimated billed total', value: snapshot.pricing.currency === 'INR'
        ? `₹${Math.round(primary.quote.display.period_total).toLocaleString('en-IN')}`
        : `€${primary.quote.display.period_total.toFixed(2)}` });
      sections.push({ id: 'pricing', title: 'Pricing', blocks: [{ type: 'pricing', rows }] });
    }
    const managedVisibility = clientVisibility(snapshot, 'managed_services');
    if (snapshot.managed && managedVisibility !== 'hidden') {
      const managedRows = [{ label: 'Annual managed service', value: `₹${round(snapshot.managed.seller.annual_inr_minor / 100).toLocaleString('en-IN')}` }];
      if (managedVisibility === 'show') {
        managedRows.unshift({ label: 'Managed tier', value: snapshot.managed.name });
        managedRows.push({ label: 'Founder time', value: `${snapshot.managed.founder_minutes_per_month} minutes/month` });
      }
      sections.push({ id: 'managed', title: 'Managed service add-on', blocks: [{ type: 'table', rows: managedRows }] });
    }
    const alternativesVisibility = clientVisibility(snapshot, 'alternatives');
    if (snapshot.alternatives.length && alternativesVisibility !== 'hidden') {
      sections.push({
        id: 'comparison', title: 'Compared plans', blocks: [{ type: 'table', rows: snapshot.alternatives.map(plan => ({
          label: alternativesVisibility === 'show' ? plan.plan_name : 'Alternative total',
          value: snapshot.pricing.currency === 'INR'
            ? `₹${Math.round(plan.quote.display.period_total).toLocaleString('en-IN')}`
            : `€${plan.quote.display.period_total.toFixed(2)}`,
        })) }],
      });
    }
    if (clientVisibility(snapshot, 'client_notes') === 'show' && snapshot.client.notes) {
      sections.push({ id: 'notes', title: 'Notes', blocks: [{ type: 'paragraph', text: String(snapshot.client.notes) }] });
    }
    if (snapshot.visibility.fx_markup === 'show' || snapshot.visibility.owner_markup === 'show') {
      const adjustmentRows = [];
      if (snapshot.visibility.fx_markup === 'show') adjustmentRows.push({ label: 'FX markup', value: `${(snapshot.pricing.fx_markup * 100).toFixed(1)}%` });
      if (snapshot.visibility.owner_markup === 'show') adjustmentRows.push({ label: 'Owner markup', value: `${(snapshot.pricing.owner_markup * 100).toFixed(1)}% · ${snapshot.pricing.owner_markup_scope}` });
      sections.push({ id: 'adjustments', title: 'Commercial adjustments', blocks: [{ type: 'table', rows: adjustmentRows }] });
    }
    sections.push({ id: 'next_steps', title: 'Next steps', blocks: [{
      type: 'list', items: ['Confirm the configuration and commercial terms.', 'Confirm quote validity before sending.', 'Provision only after the client accepts the final scope.'],
    }] });
    const visibleWarnings = snapshot.warnings.filter(warning => audience === 'operator' ||
      warning.client_facing === true || warning.audience === 'client');
    const warnings = visibleWarnings.map(warning => ({ type: 'callout', level: 'warning', text: warning.message }));
    if (warnings.length) sections.unshift({ id: 'warnings', title: 'Review before sending', blocks: warnings });
    const document = {
      schema_version: DOCUMENT_SCHEMA_VERSION,
      provider: 'deterministic',
      audience,
      title: snapshot.client?.project_name ? `Proposal · ${snapshot.client.project_name}` : 'Contabo pricing proposal',
      subtitle: mayNameConfiguration
        ? `${primary.plan_name} · ${primary.period_months} month${primary.period_months === 1 ? '' : 's'}`
        : 'Commercial proposal',
      sections,
      warnings: visibleWarnings,
    };
    if (audience === 'operator') {
      document.local_snapshot_id = snapshot.snapshot_id;
      document.local_snapshot_id_kind = snapshot.snapshot_id_kind;
    }
    return document;
  }

  function hasCommercialClaim(text) {
    const lower = String(text || '').toLowerCase();
    return ['€', '₹', '$', '£', '%', 'price', 'pricing', 'cost', 'markup', 'gst', 'sla',
      'discount', '/mo', '/month', 'annual'].some(marker => lower.includes(marker));
  }

  // Keep the report's policy-filtered deterministic sections authoritative. A
  // Codex response may contribute safe narrative wording only; it cannot
  // replace pricing, selections, managed terms, warnings, or visibility rules.
  function mergeSafeNarrative(baseDocument, candidate) {
    const base = JSON.parse(JSON.stringify(baseDocument || {}));
    const candidateSections = Array.isArray(candidate?.sections) ? candidate.sections : [];
    const allowed = new Set(['summary', 'next_steps']);
    for (const section of base.sections || []) {
      if (!allowed.has(section?.id)) continue;
      const source = candidateSections.find(item => item?.id === section.id);
      const blocks = Array.isArray(source?.blocks) ? source.blocks : [];
      const safe = [];
      for (const block of blocks) {
        if (block?.type === 'paragraph' && typeof block.text === 'string' &&
            block.text.length <= 2000 && !hasCommercialClaim(block.text)) {
          safe.push({ type: 'paragraph', text: block.text });
        } else if (block?.type === 'list' && Array.isArray(block.items)) {
          const items = block.items.filter(item => typeof item === 'string' && item.length <= 400 && !hasCommercialClaim(item)).slice(0, 12);
          if (items.length) safe.push({ type: 'list', items });
        }
        if (safe.length >= 8) break;
      }
      if (safe.length) section.blocks = safe;
    }
    base.provider = 'codex-cli-safe';
    return base;
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[char]));
  }

  function renderDocument(document) {
    const source = document && typeof document === 'object' ? document : {};
    const sections = Array.isArray(source.sections) ? source.sections : [];
    const renderBlock = block => {
      if (!block || typeof block !== 'object') return '';
      switch (block.type) {
        case 'paragraph': return `<p>${escapeHtml(block.text)}</p>`;
        case 'callout': return `<aside class="proposal-callout ${escapeHtml(block.level || 'info')}">${escapeHtml(block.text)}</aside>`;
        case 'list': return `<ul>${(Array.isArray(block.items) ? block.items : []).map(item => `<li>${escapeHtml(item)}</li>`).join('')}</ul>`;
        case 'table':
        case 'pricing':
          return `<table class="proposal-table"><tbody>${(Array.isArray(block.rows) ? block.rows : []).map(row => `<tr><th>${escapeHtml(row?.label)}</th><td>${escapeHtml(row?.value)}</td></tr>`).join('')}</tbody></table>`;
        default: return '';
      }
    };
    return `<article class="proposal-document"><header><h2>${escapeHtml(source.title || 'Proposal')}</h2>${source.subtitle ? `<p class="proposal-subtitle">${escapeHtml(source.subtitle)}</p>` : ''}</header>${sections.map(section => `<section><h3>${escapeHtml(section?.title || '')}</h3>${(Array.isArray(section?.blocks) ? section.blocks : []).map(renderBlock).join('')}</section>`).join('')}</article>`;
  }

  function clientDocument(snapshot, reviewedDocument) {
    const deterministicClient = deterministicDocument(snapshot, { audience: 'client' });
    return reviewedDocument
      ? mergeSafeNarrative(deterministicClient, reviewedDocument)
      : deterministicClient;
  }

  function clientProjection(snapshot, document) {
    const projectedDocument = clientDocument(snapshot, document);
    return {
      schema_version: 'proposal.client-artifact.v1',
      artifact_type: 'client',
      generated_at: snapshot.generated_at,
      profile: snapshot.profile,
      document: JSON.parse(JSON.stringify(projectedDocument)),
    };
  }

  function internalEvidence(snapshot) {
    return {
      schema_version: 'proposal.internal-evidence.v1',
      artifact_type: 'internal_evidence',
      contains_internal_commercial_data: true,
      local_identity_is_security_hash: false,
      snapshot: JSON.parse(JSON.stringify(snapshot)),
    };
  }

  function toCsv(snapshot) {
    const document = clientDocument(snapshot);
    const rows = [['section', 'kind', 'label', 'value']];
    for (const section of document.sections) {
      for (const block of section.blocks || []) {
        if (block.type === 'table' || block.type === 'pricing') {
          for (const row of block.rows || []) rows.push([section.title, block.type, row.label, row.value]);
        } else if (block.type === 'list') {
          for (const item of block.items || []) rows.push([section.title, 'list', '', item]);
        } else if (block.type === 'paragraph' || block.type === 'callout') {
          rows.push([section.title, block.type, '', block.text]);
        }
      }
    }
    return rows.map(row => row.map(value => {
      const text = String(value == null ? '' : value);
      return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
    }).join(',')).join('\n') + '\n';
  }

  function toClientBrief(snapshot) {
    const document = clientDocument(snapshot);
    const lines = [document.title];
    if (document.subtitle) lines.push(document.subtitle);
    for (const section of document.sections) {
      lines.push('', section.title);
      for (const block of section.blocks || []) {
        if (block.type === 'table' || block.type === 'pricing') {
          for (const row of block.rows || []) lines.push(`${row.label}: ${row.value}`);
        } else if (block.type === 'list') {
          for (const item of block.items || []) lines.push(`- ${item}`);
        } else if (block.type === 'paragraph' || block.type === 'callout') {
          lines.push(block.text);
        }
      }
    }
    return lines.join('\n').trim() + '\n';
  }

  return {
    SCHEMA_VERSION,
    DOCUMENT_SCHEMA_VERSION,
    DEFAULT_VISIBILITY,
    PROFILE_DEFAULTS,
    VISIBILITY_LABELS,
    VISIBILITY_RULES,
    clampFraction,
    clampPercent,
    stableStringify,
    stableLocalId,
    stableHash,
    normalizeVisibility,
    calculateQuote,
    calculateManaged,
    makeSnapshot,
    deterministicDocument,
    clientDocument,
    clientProjection,
    internalEvidence,
    mergeSafeNarrative,
    renderDocument,
    toCsv,
    toClientBrief,
  };
}));
