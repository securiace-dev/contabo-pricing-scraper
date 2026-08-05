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
  const MAX_FX_MARKUP = 0.15;
  const MAX_OWNER_MARKUP = 1;
  const ROUND_SCALE = 100;

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
        fx_markup: 'show', owner_markup: 'show', internal_notes: 'show',
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

  function stableHash(value) {
    const raw = stableStringify(value);
    let hash = 2166136261;
    for (let i = 0; i < raw.length; i += 1) {
      hash ^= raw.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return `proposal-${(hash >>> 0).toString(16).padStart(8, '0')}`;
  }

  function normalizeVisibility(profile, provided) {
    const profileConfig = PROFILE_DEFAULTS[profile] || PROFILE_DEFAULTS['quick-quote'];
    const raw = provided && typeof provided === 'object' ? provided : {};
    const merged = { ...DEFAULT_VISIBILITY, ...(profileConfig.visibility || {}), ...raw };
    const allowed = new Set(['show', 'total_only', 'silent_include', 'internal_only', 'exclude']);
    for (const key of Object.keys(merged)) {
      if (!allowed.has(merged[key])) merged[key] = DEFAULT_VISIBILITY[key] || 'show';
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
    const taxIncluded = Boolean(pricing.prices_include_gst);
    const gstEnabled = pricing.gst_enabled !== false;
    const gstRate = clampFraction(pricing.gst_rate, 0.18, 1);
    const fxRate = finite(pricing.fx_rate, 0);
    const fxMarkup = clampFraction(pricing.fx_markup, 0, MAX_FX_MARKUP);
    const ownerMarkup = clampFraction(pricing.owner_markup, 0, MAX_OWNER_MARKUP);
    const ownerScope = pricing.owner_markup_scope || 'provider_only';
    const currency = pricing.currency === 'INR' ? 'INR' : 'EUR';
    return {
      currency,
      prices_include_gst: taxIncluded,
      gst_enabled: gstEnabled,
      gst_rate: gstRate,
      fx_rate: fxRate > 0 ? fxRate : null,
      fx_markup: fxMarkup,
      owner_markup: ownerMarkup,
      owner_markup_scope: ['provider_only', 'provider_and_managed'].includes(ownerScope)
        ? ownerScope : 'provider_only',
      fx_source: pricing.fx_source || null,
      fx_rate_date: pricing.fx_rate_date || null,
    };
  }

  function taxMultiplier(pricing) {
    return pricing.prices_include_gst || !pricing.gst_enabled ? 1 : 1 + pricing.gst_rate;
  }

  function toDisplay(eurValue, pricing, includeOwner = false) {
    const value = finite(eurValue) * (pricing.currency === 'INR' && pricing.fx_rate
      ? pricing.fx_rate * (1 + pricing.fx_markup)
      : 1);
    return value * (includeOwner ? 1 + pricing.owner_markup : 1);
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
    const tax = taxMultiplier(pricing);
    const taxedMonthly = round(providerMonthly * tax);
    const taxedSetup = round(providerSetup * tax);
    const providerPeriodTotal = round(taxedMonthly * periodMonths + taxedSetup);
    const ownerEnabled = pricing.owner_markup_scope === 'provider_only'
      || pricing.owner_markup_scope === 'provider_and_managed';
    const displayedMonthly = round(toDisplay(taxedMonthly, pricing, ownerEnabled));
    const displayedSetup = round(toDisplay(taxedSetup, pricing, ownerEnabled));
    const displayedPeriodTotal = round(displayedMonthly * periodMonths + displayedSetup);
    const providerDisplayMonthly = round(toDisplay(taxedMonthly, pricing, false));
    const providerDisplaySetup = round(toDisplay(taxedSetup, pricing, false));
    const ownerAdjustment = round(displayedPeriodTotal -
      (providerDisplayMonthly * periodMonths + providerDisplaySetup));
    const warnings = [];
    if (pricing.currency === 'INR' && !pricing.fx_rate) {
      warnings.push({ code: 'fx_unavailable', message: 'INR estimate unavailable until an EUR→INR rate is supplied.', mandatory: true });
    }
    if (pricing.fx_markup >= MAX_FX_MARKUP) {
      warnings.push({ code: 'fx_markup_capped', message: 'FX markup was capped at 15% for this report.', mandatory: true });
    }
    if (pricing.owner_markup >= MAX_OWNER_MARKUP) {
      warnings.push({ code: 'owner_markup_capped', message: 'Owner markup was capped at 100% for this report.', mandatory: true });
    }
    return {
      period_months: periodMonths,
      selections: optionDeltas.labels.concat(addonDeltas.labels),
      provider: {
        monthly_eur: providerMonthly,
        setup_eur: providerSetup,
        taxed_monthly_eur: taxedMonthly,
        taxed_setup_eur: taxedSetup,
        period_total_eur: providerPeriodTotal,
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
        provider_monthly: providerDisplayMonthly,
        provider_setup: providerDisplaySetup,
        owner_adjustment: ownerAdjustment,
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
    const taxable = source.taxable !== false && source.tax_basis !== 'inclusive';
    const gst = taxable && pricing.gst_enabled ? minor(annualPrice * pricing.gst_rate) : 0;
    const canonicalAnnual = source.tax_basis === 'inclusive' ? annualPrice : annualPrice + gst;
    const applyOwner = pricing.owner_markup_scope === 'provider_and_managed';
    const sellerAnnual = applyOwner ? minor(canonicalAnnual * (1 + pricing.owner_markup)) : canonicalAnnual;
    return {
      plan_id: String(source.plan_id),
      name: String(source.name || source.plan_id),
      quantity,
      founder_minutes_per_month: minor(source.founder_minutes_per_month) * quantity,
      provider: { annual_inr_minor: canonicalAnnual, ex_gst_inr_minor: annualPrice, gst_inr_minor: gst },
      seller: {
        annual_inr_minor: sellerAnnual,
        monthly_equivalent_inr_minor: minor(sellerAnnual / Math.max(1, finite(source.billing_term_months, 12))),
        owner_adjustment_inr_minor: sellerAnnual - canonicalAnnual,
      },
      includes: Array.isArray(source.includes) ? source.includes.map(String) : [],
      excludes: Array.isArray(source.excludes) ? source.excludes.map(String) : [],
      currency: 'INR',
      fx_applied: false,
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
    const warnings = [...primary.quote.warnings];
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
      primary,
      alternatives,
      managed,
      source: source.source && typeof source.source === 'object' ? { ...source.source } : {},
      warnings,
    };
    snapshot.snapshot_id = stableHash({
      schema_version: snapshot.schema_version,
      profile: snapshot.profile,
      visibility: snapshot.visibility,
      pricing: snapshot.pricing,
      client: snapshot.client,
      primary: snapshot.primary,
      alternatives: snapshot.alternatives,
      managed: snapshot.managed,
      source: snapshot.source,
    });
    return snapshot;
  }

  function isVisible(snapshot, key, mode = 'show') {
    const setting = snapshot.visibility?.[key] || 'show';
    return setting === mode || (mode === 'show' && setting === 'total_only');
  }

  function deterministicDocument(snapshot) {
    const primary = snapshot.primary;
    const sections = [];
    const project = snapshot.client?.project_name ? ` for ${snapshot.client.project_name}` : '';
    sections.push({
      id: 'summary', title: 'Summary', blocks: [{
        type: 'paragraph',
        text: `This ${snapshot.profile_label.toLowerCase()}${project} is based on the selected ${primary.plan_name} configuration and the current report snapshot.`,
      }],
    });
    if (isVisible(snapshot, 'configuration')) {
      const rows = [
        { label: 'Plan', value: primary.plan_name },
        { label: 'Family', value: primary.family },
        { label: 'Term', value: `${primary.period_months} month${primary.period_months === 1 ? '' : 's'}` },
      ];
      for (const [key, value] of Object.entries(primary.specs || {})) {
        if (value != null && value !== '') rows.push({ label: key.replaceAll('_', ' '), value: String(value) });
      }
      if (snapshot.visibility.configuration !== 'total_only') {
        for (const item of primary.quote.selections) rows.push({ label: item.label, value: item.monthly || item.setup ? `${item.monthly ? `+€${item.monthly}/mo` : ''}${item.setup ? ` +€${item.setup} setup` : ''}` : 'Included' });
      }
      sections.push({ id: 'configuration', title: 'Selected configuration', blocks: [{ type: 'table', rows }] });
    }
    if (snapshot.visibility.provider_pricing !== 'exclude') {
      const rows = [];
      const showLineItems = snapshot.visibility.provider_line_items === 'show';
      if (showLineItems) {
        rows.push({ label: 'Provider monthly', value: `€${primary.quote.provider.taxed_monthly_eur.toFixed(2)}` });
        if (primary.quote.provider.taxed_setup_eur) rows.push({ label: 'Provider setup', value: `€${primary.quote.provider.taxed_setup_eur.toFixed(2)}` });
      }
      rows.push({ label: 'Estimated billed total', value: snapshot.pricing.currency === 'INR'
        ? `₹${Math.round(primary.quote.display.period_total).toLocaleString('en-IN')}`
        : `€${primary.quote.display.period_total.toFixed(2)}` });
      sections.push({ id: 'pricing', title: 'Pricing', blocks: [{ type: 'pricing', rows }] });
    }
    if (snapshot.managed && snapshot.visibility.managed_services !== 'exclude') {
      const managedRows = [
        { label: 'Managed tier', value: snapshot.managed.name },
        { label: 'Annual managed service', value: `₹${snapshot.managed.seller.annual_inr_minor.toLocaleString('en-IN')}` },
        { label: 'Founder time', value: `${snapshot.managed.founder_minutes_per_month} minutes/month` },
      ];
      sections.push({ id: 'managed', title: 'Managed service add-on', blocks: [{ type: 'table', rows: managedRows }] });
    }
    if (snapshot.alternatives.length && snapshot.visibility.alternatives !== 'exclude') {
      sections.push({
        id: 'comparison', title: 'Compared plans', blocks: [{ type: 'table', rows: snapshot.alternatives.map(plan => ({
          label: plan.plan_name,
          value: snapshot.pricing.currency === 'INR'
            ? `₹${Math.round(plan.quote.display.period_total).toLocaleString('en-IN')}`
            : `€${plan.quote.display.period_total.toFixed(2)}`,
        })) }],
      });
    }
    if (snapshot.visibility.client_notes !== 'exclude' && snapshot.client.notes) {
      sections.push({ id: 'notes', title: 'Notes', blocks: [{ type: 'paragraph', text: String(snapshot.client.notes) }] });
    }
    sections.push({ id: 'next_steps', title: 'Next steps', blocks: [{
      type: 'list', items: ['Confirm the configuration and commercial terms.', 'Confirm quote validity before sending.', 'Provision only after the client accepts the final scope.'],
    }] });
    const warnings = snapshot.warnings.map(warning => ({ type: 'callout', level: 'warning', text: warning.message }));
    if (warnings.length) sections.unshift({ id: 'warnings', title: 'Review before sending', blocks: warnings });
    return {
      schema_version: DOCUMENT_SCHEMA_VERSION,
      provider: 'deterministic',
      snapshot_id: snapshot.snapshot_id,
      title: snapshot.client?.project_name ? `Proposal · ${snapshot.client.project_name}` : 'Contabo pricing proposal',
      subtitle: `${primary.plan_name} · ${primary.period_months} month${primary.period_months === 1 ? '' : 's'}`,
      sections,
      warnings: snapshot.warnings,
    };
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

  function toCsv(snapshot) {
    const rows = [['kind', 'label', 'value', 'currency']];
    const quote = snapshot.primary.quote;
    rows.push(['provider', 'monthly', quote.provider.taxed_monthly_eur, 'EUR']);
    rows.push(['provider', 'setup', quote.provider.taxed_setup_eur, 'EUR']);
    rows.push(['seller', 'period_total', quote.display.period_total, snapshot.pricing.currency]);
    if (snapshot.managed) rows.push(['managed', snapshot.managed.name, snapshot.managed.seller.annual_inr_minor, 'INR']);
    return rows.map(row => row.map(value => {
      const text = String(value == null ? '' : value);
      return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
    }).join(',')).join('\n') + '\n';
  }

  return {
    SCHEMA_VERSION,
    DOCUMENT_SCHEMA_VERSION,
    DEFAULT_VISIBILITY,
    PROFILE_DEFAULTS,
    clampFraction,
    clampPercent,
    stableStringify,
    stableHash,
    normalizeVisibility,
    calculateQuote,
    calculateManaged,
    makeSnapshot,
    deterministicDocument,
    renderDocument,
    toCsv,
  };
}));
