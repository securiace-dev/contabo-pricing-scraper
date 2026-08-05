<?php
/**
 * Admin-first Proposal Studio. This view renders only ephemeral previews;
 * delivery controls are intentionally disabled until durable gates exist.
 *
 * @var \Closure $esc
 * @var string $module_link
 * @var array<int,array<string,mixed>> $plans
 * @var array<string,array<string,mixed>> $managed_tiers
 * @var \ContaboPricing\Settings $settings
 * @var array<string,string> $form
 * @var array<string,mixed>|null $result
 * @var string $error
 * @var array<string,mixed> $tax_gate
 * @var array<string,mixed> $delivery_gate
 */
$cb_value = static function (string $key, string $default = '') use ($form): string {
    return array_key_exists($key, $form) ? (string) $form[$key] : $default;
};
$cb_selected = static function (string $key, string $value, string $default = '') use ($cb_value): string {
    return $cb_value($key, $default) === $value ? ' selected' : '';
};
$cb_strip_data = [
    ['lbl' => 'Facts', 'v' => 'API validated', 'sub' => 'No narrative pricing', 'tone' => 'good'],
    ['lbl' => 'Output GST', 'v' => !empty($tax_gate['effective']) ? 'effective' : 'not charged', 'sub' => (string) $tax_gate['reason'], 'tone' => !empty($tax_gate['effective']) ? 'warn' : 'good'],
    ['lbl' => 'Preview', 'v' => 'ephemeral', 'sub' => 'Not an approval record', 'tone' => 'warn'],
    ['lbl' => 'Delivery', 'v' => 'blocked', 'sub' => 'Persistence gate incomplete', 'tone' => 'bad'],
];
require __DIR__ . '/_layout_open.tpl';
?>

<div class="cb-proposal">
  <header>
    <div>
      <p class="cb-card-sub cb-proposal-kicker">Hallmark operations</p>
      <h2 class="display cb-proposal-title">Proposal Studio</h2>
      <p class="cb-card-sub">Build a client-safe deterministic preview from authoritative provider facts. AI may rewrite narrative only.</p>
    </div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>">&larr; Addon dashboard</a>
  </header>

  <div class="cb-workflow" aria-label="Proposal workflow">
    <span><b>1</b> Provider facts</span><i>&rarr;</i><span><b>2</b> Commercial calculation</span><i>&rarr;</i><span><b>3</b> Client projection</span><i>&rarr;</i><span>4 Review approval</span><i>&rarr;</i><span>5 Delivery <strong>blocked</strong></span>
  </div>

  <?php if ($error !== ''): ?><div class="cb-flash error" role="alert"><?= $esc($error) ?></div><?php endif; ?>
  <?php if (!empty($catalog_error)): ?><div class="cb-callout cb-danger">Plan catalogue unavailable: <?= $esc((string) $catalog_error) ?>. Existing values are not guessed.</div><?php endif; ?>

  <form method="post" action="<?= $esc($module_link) ?>">
    <?= generate_token() ?>

    <section class="cb-section">
      <div class="cb-section-head"><h3>Client and purpose</h3><p>These fields identify the preview. Recipient is validated later; this slice cannot send.</p></div>
      <div class="cb-fields">
        <div class="cb-field"><label for="ps-client-id">WHMCS client ID</label><input id="ps-client-id" name="client_id" type="number" min="0" value="<?= $esc($cb_value('client_id', '0')) ?>"></div>
        <div class="cb-field"><label for="ps-client-name">Client name</label><input id="ps-client-name" name="client_name" required value="<?= $esc($cb_value('client_name')) ?>"></div>
        <div class="cb-field"><label for="ps-recipient">Future recipient</label><input id="ps-recipient" name="recipient" type="email" value="<?= $esc($cb_value('recipient')) ?>"><small>Stored nowhere and never contacted by preview.</small></div>
        <div class="cb-field wide"><label for="ps-title">Proposal title</label><input id="ps-title" name="proposal_title" value="<?= $esc($cb_value('proposal_title', 'Managed infrastructure proposal')) ?>"></div>
        <input type="hidden" name="profile" value="managed">
      </div>
    </section>

    <section class="cb-section">
      <div class="cb-section-head"><h3>Authoritative infrastructure facts</h3><p>The Rust API validates plan and term. Non-empty selection JSON is accepted only when the API explicitly certifies those deltas.</p></div>
      <div class="cb-fields">
        <div class="cb-field wide"><label for="ps-plan">Provider plan</label><select id="ps-plan" name="plan_slug" required><option value="">Choose current plan</option><?php foreach ($plans as $plan): ?><option value="<?= $esc((string) $plan['slug']) ?>"<?= $cb_selected('plan_slug', (string) $plan['slug']) ?>><?= $esc((string) $plan['name']) ?><?= !empty($plan['family']) ? ' · ' . $esc((string) $plan['family']) : '' ?></option><?php endforeach; ?></select></div>
        <div class="cb-field"><label for="ps-term">Provider term (months)</label><input id="ps-term" name="period_months" type="number" min="1" max="120" required value="<?= $esc($cb_value('period_months', '1')) ?>"></div>
        <div class="cb-field"><label for="ps-region">Region</label><input id="ps-region" name="region" value="<?= $esc($cb_value('region', 'Asia (India)')) ?>"></div>
        <div class="cb-field"><label for="ps-os">Operating system</label><input id="ps-os" name="os" value="<?= $esc($cb_value('os')) ?>"></div>
        <div class="cb-field full"><label for="ps-selections">Selected options JSON</label><textarea id="ps-selections" name="selections_json" spellcheck="false"><?= $esc($cb_value('selections_json', '{}')) ?></textarea><small>Fail-closed boundary: no selection surcharge is inferred from prose or browser state.</small></div>
      </div>
    </section>

    <section class="cb-section">
      <div class="cb-section-head"><h3>Commercial controls</h3><p>FX/card markup and owner margin remain distinct. Provider tax cash/recoverability is separate from Securiace output GST.</p></div>
      <div class="cb-fields">
        <div class="cb-field"><label for="ps-currency">Output currency</label><select id="ps-currency" name="currency"><option value="INR"<?= $cb_selected('currency', 'INR', 'INR') ?>>INR</option><option value="EUR"<?= $cb_selected('currency', 'EUR', 'INR') ?>>EUR</option></select></div>
        <div class="cb-field"><label for="ps-fx">EUR→INR rate</label><input id="ps-fx" name="fx_rate" type="number" min="0.0001" step="0.0001" value="<?= $esc($cb_value('fx_rate')) ?>"><small>Blank uses current API FX fact.</small></div>
        <div class="cb-field"><label for="ps-fx-markup">FX/card markup %</label><input id="ps-fx-markup" name="fx_card_markup_pct" type="number" min="0" max="100" step="0.01" value="<?= $esc($cb_value('fx_card_markup_pct', (string) $settings->fxMarkupPct)) ?>"></div>
        <div class="cb-field"><label for="ps-margin">Owner margin adjustment %</label><input id="ps-margin" name="owner_margin_pct" type="number" min="0" max="100" step="0.01" value="<?= $esc($cb_value('owner_margin_pct', '0')) ?>"><small>Internal commercial input; never labeled in a client artifact.</small></div>
        <div class="cb-field"><label for="ps-margin-scope">Owner margin scope</label><select id="ps-margin-scope" name="owner_margin_scope"><option value="provider_only"<?= $cb_selected('owner_margin_scope', 'provider_only', 'provider_only') ?>>Infrastructure only</option><option value="provider_and_managed"<?= $cb_selected('owner_margin_scope', 'provider_and_managed', 'provider_only') ?>>Infrastructure + managed</option></select></div>
        <div class="cb-field"><label for="ps-managed">Founder Managed addon</label><select id="ps-managed" name="managed_tier"><option value="">No managed addon</option><?php foreach ($managed_tiers as $id => $tier): ?><option value="<?= $esc((string) $id) ?>"<?= $cb_selected('managed_tier', (string) $id) ?>><?= $esc((string) $tier['name']) ?> · ₹<?= $esc(number_format(((float) $tier['annual_price_minor']) / 100, 0)) ?>/year · <?= $esc((string) (((int) $tier['founder_minutes_per_month']) / 60)) ?>h/mo</option><?php endforeach; ?></select></div>
        <div class="cb-field"><label for="ps-managed-quantity">Managed server quantity</label><input id="ps-managed-quantity" name="managed_quantity" type="number" min="1" max="99" step="1" value="<?= $esc($cb_value('managed_quantity', '1')) ?>"><small>Scales annual managed fees and included Founder minutes.</small></div>
      </div>
      <div class="cb-callout"><strong>Tax gate:</strong> <?= $esc((string) $tax_gate['reason']) ?><br><small>Provider prices: <?= $settings->proposalProviderPricesIncludeTax ? 'tax-inclusive (decomposed)' : 'tax-exclusive' ?>; provider tax <?= $settings->proposalProviderTaxCharged ? $esc((string) $settings->proposalProviderTaxRatePct) . '%' : 'not configured' ?>; recoverable <?= $settings->proposalProviderTaxRecoverable ? 'yes' : 'no' ?>.</small></div>
    </section>

    <section class="cb-section">
      <div class="cb-section-head"><h3>Client-safe visibility</h3><p>Each field exposes only valid modes. Hidden and calculated lines can contribute without leaking labels, keys, or provenance.</p></div>
      <div class="cb-fields">
        <div class="cb-field"><label>Infrastructure line</label><select name="provider_visibility"><?php foreach (['show','total_only','silent_include','internal_only','exclude','calculated_only'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('provider_visibility', $mode, 'show') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select></div>
        <div class="cb-field"><label>Configuration details</label><select name="configuration_visibility"><?php foreach (['show','total_only','internal_only','exclude'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('configuration_visibility', $mode, 'show') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select></div>
        <div class="cb-field"><label>Managed service</label><select name="managed_visibility"><?php foreach (['show','total_only','silent_include','internal_only','exclude','calculated_only'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('managed_visibility', $mode, 'exclude') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select></div>
        <div class="cb-field"><label>Owner margin</label><select name="owner_visibility"><?php foreach (['silent_include','internal_only','exclude','calculated_only'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('owner_visibility', $mode, 'internal_only') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select><small>No client-facing show mode exists.</small></div>
        <div class="cb-field"><label>Output tax</label><select name="tax_visibility"><?php foreach (['show','total_only','silent_include','internal_only','exclude','calculated_only'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('tax_visibility', $mode, 'total_only') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select></div>
        <div class="cb-field"><label>Plan comparisons</label><select name="comparison_visibility"><?php foreach (['show','total_only','internal_only','exclude','calculated_only'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('comparison_visibility', $mode, 'exclude') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select><small>Comparisons are informational and never contribute to commitment totals.</small></div>
        <div class="cb-field"><label>Client notes</label><select name="client_notes_visibility"><?php foreach (['show','internal_only','exclude'] as $mode): ?><option value="<?= $mode ?>"<?= $cb_selected('client_notes_visibility', $mode, 'show') ?>><?= $esc(str_replace('_', ' ', $mode)) ?></option><?php endforeach; ?></select></div>
        <div class="cb-field full"><label for="ps-comparisons">Alternate current plan slugs</label><textarea id="ps-comparisons" name="comparison_plan_slugs" placeholder="cloud-vps-20&#10;cloud-vps-30"><?= $esc($cb_value('comparison_plan_slugs')) ?></textarea><small>Comma, space, or newline separated; maximum four. Each alternative is resolved through authoritative plan metadata and a same-term API quote.</small></div>
      </div>
    </section>

    <section class="cb-section">
      <div class="cb-section-head"><h3>Narrative and evidence</h3><p>Report documents and AI can supply narrative only. Facts, prices, visibility, and internal evidence remain immutable inputs.</p></div>
      <div class="cb-fields">
        <div class="cb-field wide"><label for="ps-client-notes">Client notes</label><textarea id="ps-client-notes" name="client_notes"><?= $esc($cb_value('client_notes')) ?></textarea></div>
        <div class="cb-field"><label for="ps-internal-notes">Internal review notes</label><textarea id="ps-internal-notes" name="internal_notes"><?= $esc($cb_value('internal_notes')) ?></textarea></div>
        <div class="cb-field full"><label for="ps-report">Optional report_document JSON</label><textarea id="ps-report" name="report_document_json" spellcheck="false"><?= $esc($cb_value('report_document_json')) ?></textarea><small>Only bounded summary and next-step blocks are imported; commercial claims and markup are rejected.</small></div>
        <div class="cb-field"><label for="ps-narrative-mode">Narrative mode</label><select id="ps-narrative-mode" name="narrative_mode"><option value="deterministic"<?= $cb_selected('narrative_mode', 'deterministic', 'deterministic') ?>>Deterministic only</option><option value="ai"<?= $cb_selected('narrative_mode', 'ai', 'deterministic') ?>>Configured AI with deterministic fallback</option></select><small>AI is narrative-only and never blocks the base preview.</small></div>
      </div>
    </section>

    <div class="cb-actionbar">
      <p><strong>Next safe action:</strong> create a deterministic preview, then compare client and internal artifacts.</p>
      <div><button class="cb-btn primary" type="submit" name="action" value="proposal-preview">Create deterministic preview</button> <button class="cb-btn ghost" type="submit" name="action" value="proposal-generate-codex">Try narrative assist</button></div>
    </div>
  </form>

  <?php if (is_array($result)): ?>
    <section class="cb-section">
      <div class="cb-section-head"><h3>Review boundary</h3><p>Proposal version <code><?= $esc((string) $result['version_id']) ?></code>. This is an ephemeral preview, not an immutable approved version.</p></div>
      <div class="cb-preview-grid">
        <div class="cb-artifact client"><h3>Client artifact</h3><p class="cb-card-sub">Safe projection used by HTML, JSON, future ticket, and future email.</p><iframe title="Client proposal preview" sandbox srcdoc="<?= $esc('<link rel="stylesheet" href="/modules/addons/contabo_pricing/assets/app.css?v=' . rawurlencode((string) $cb_addon_version) . '">' . (string) $result['client_html']) ?>"></iframe><details><summary>Client JSON</summary><pre><?= $esc((string) $result['client_json']) ?></pre></details></div>
        <div class="cb-artifact internal"><h3>Internal evidence</h3><p class="cb-card-sub">Never attach or paste this object into client channels.</p><pre><?= $esc((string) json_encode($result['internal'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)) ?></pre><details><summary>Narrative provider metadata</summary><pre><?= $esc((string) json_encode($result['internal_ai'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)) ?></pre></details></div>
      </div>
      <div class="cb-callout cb-danger"><strong>Delivery hard-blocked.</strong> <?= $esc((string) $delivery_gate['reason']) ?></div>
      <p><button class="cb-btn" type="button" disabled>Send as support ticket</button> <button class="cb-btn" type="button" disabled>Send email</button></p>
    </section>
  <?php endif; ?>
</div>
</div>
