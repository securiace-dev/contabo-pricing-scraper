<?php
/** @var \Closure $esc */
/** @var string $module_link */
/** @var array<int,array<string,mixed>> $plans */
/** @var array<string,array<string,mixed>> $managed_tiers */
/** @var array<string,mixed> $form */
/** @var array<string,mixed>|null $result */
/** @var string $error */
/** @var string $catalog_error */
/** @var string $delivery_result */
require __DIR__ . '/_layout_open.tpl';

$f = is_array($form ?? null) ? $form : [];
$v = static function (string $key, string $default = '') use ($f): string {
    return isset($f[$key]) ? (string) $f[$key] : $default;
};
$selected = static function (string $key, string $value) use ($v): string {
    return $v($key) === $value ? ' selected' : '';
};
$hiddenKeys = [
    'client_id', 'client_name', 'proposal_title', 'plan_slug', 'period_months',
    'region', 'os', 'canonical_family', 'selections_json', 'managed_tier',
    'managed_visibility', 'owner_markup_pct', 'owner_markup_scope',
    'owner_visibility', 'comparison_plan_slug', 'comparison_visibility',
    'source_visibility', 'client_notes', 'internal_notes', 'fx_rate',
    'department_id', 'ticket_id', 'notify_client', 'report_document_json',
];
?>

<div class="cb-card" style="padding:22px 24px;background:linear-gradient(135deg,var(--accent-soft),var(--panel) 55%);">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:18px;flex-wrap:wrap;">
    <div>
      <div class="cb-card-sub" style="margin:0 0 4px;">Proposal workspace</div>
      <h2 class="cb-card-title display" style="font-size:30px;margin:0 0 6px;">Build, review, and deliver</h2>
      <p class="cb-card-sub" style="max-width:760px;margin:0;">
        Provider pricing is recalculated by the Contabo API. Founder Managed tiers,
        owner markup, hidden/silent content, comparisons, and client/internal notes
        are applied locally and rendered into a safe proposal artifact.
      </p>
    </div>
    <div style="display:flex;gap:6px;flex-wrap:wrap;">
      <span class="cb-pill <?= !empty($settings->proposalAiEnabled) && !empty($settings->proposalAiApiKey) ? 'good' : 'grey' ?>">
        <span class="dot"></span><?= !empty($settings->proposalAiEnabled) && !empty($settings->proposalAiApiKey) ? 'AI optional' : 'Deterministic mode' ?>
      </span>
      <span class="cb-pill <?= !empty($settings->proposalDeliveryEnabled) ? 'warn' : 'grey' ?>">
        <span class="dot"></span><?= !empty($settings->proposalDeliveryEnabled) ? 'Delivery enabled' : 'Delivery locked' ?>
      </span>
    </div>
  </div>
</div>

<?php if (!empty($catalog_error)): ?>
  <div class="cb-error">
    <strong>Catalogue unavailable:</strong> <?= $esc($catalog_error) ?>
    <div style="margin-top:4px;">No price is guessed. Confirm the addon API base URL before retrying.</div>
  </div>
<?php endif; ?>
<?php if (!empty($error)): ?>
  <div class="cb-error"><strong>Proposal preview failed:</strong> <?= $esc($error) ?></div>
<?php endif; ?>
<?php if (!empty($delivery_result)): ?>
  <div class="cb-flash"><?= $esc($delivery_result) ?></div>
<?php endif; ?>

<form method="post" action="<?= $esc($module_link) ?>" class="cb-card">
  <input type="hidden" name="action" value="proposal-preview">
  <?= generate_token() ?>
  <h3>1. Client and configuration</h3>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:0 18px;">
    <div class="cb-field">
      <label for="client_id">WHMCS client ID</label>
      <input id="client_id" name="client_id" type="number" min="1" value="<?= $esc($v('client_id')) ?>" placeholder="Required only for delivery">
    </div>
    <div class="cb-field">
      <label for="client_name">Client display name</label>
      <input id="client_name" name="client_name" type="text" maxlength="160" value="<?= $esc($v('client_name', 'Client')) ?>">
    </div>
    <div class="cb-field" style="grid-column:1/-1;">
      <label for="proposal_title">Proposal title</label>
      <input id="proposal_title" name="proposal_title" type="text" maxlength="180" value="<?= $esc($v('proposal_title', 'Managed infrastructure proposal')) ?>">
    </div>
    <div class="cb-field">
      <label for="plan_slug">Primary Contabo plan</label>
      <select id="plan_slug" name="plan_slug" required>
        <option value="">Choose a current plan…</option>
        <?php foreach ((array) $plans as $plan): ?>
          <?php $slug = (string) ($plan['slug'] ?? ''); ?>
          <option value="<?= $esc($slug) ?>"<?= $selected('plan_slug', $slug) ?>>
            <?= $esc((string) ($plan['name'] ?? $slug)) ?><?= !empty($plan['family']) ? ' · ' . $esc((string) $plan['family']) : '' ?>
          </option>
        <?php endforeach; ?>
      </select>
    </div>
    <div class="cb-field">
      <label for="period_months">Term length</label>
      <select id="period_months" name="period_months">
        <?php foreach ([1, 3, 6, 12, 24, 36] as $months): ?>
          <option value="<?= $months ?>"<?= $selected('period_months', (string) $months) ?>><?= $months ?> month<?= $months === 1 ? '' : 's' ?></option>
        <?php endforeach; ?>
      </select>
    </div>
    <div class="cb-field">
      <label for="region">Region</label>
      <input id="region" name="region" type="text" maxlength="120" value="<?= $esc($v('region', 'Asia (India)')) ?>">
    </div>
    <div class="cb-field">
      <label for="os">Operating system</label>
      <input id="os" name="os" type="text" maxlength="120" value="<?= $esc($v('os')) ?>" placeholder="e.g. Ubuntu 24.04">
    </div>
    <div class="cb-field">
      <label for="canonical_family">Canonical family</label>
      <select id="canonical_family" name="canonical_family">
        <?php foreach (['Core VPS', 'Performance VPS', 'Max Performance VPS', 'Cloud VPS', 'Dedicated Server', 'Storage VPS'] as $family): ?>
          <option value="<?= $esc($family) ?>"<?= $selected('canonical_family', $family) ?>><?= $esc($family) ?></option>
        <?php endforeach; ?>
      </select>
    </div>
    <div class="cb-field">
      <label for="fx_rate">EUR → INR FX rate <span class="muted">(optional)</span></label>
      <input id="fx_rate" name="fx_rate" type="number" min="0.01" step="0.0001" value="<?= $esc($v('fx_rate')) ?>" placeholder="Uses API rate when blank">
    </div>
    <div class="cb-field" style="grid-column:1/-1;">
      <label for="selections_json">Validated option selections JSON</label>
      <textarea id="selections_json" name="selections_json" rows="3" spellcheck="false"><?= $esc($v('selections_json', '{}')) ?></textarea>
      <small class="cb-card-sub" style="margin:0;">Use the API configurator labels, for example {"Storage":"SSD","Data Protection":"Include"}.</small>
    </div>
  </div>

  <h3 style="margin-top:24px;">2. Managed services and margin</h3>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:0 18px;">
    <div class="cb-field">
      <label for="managed_tier">Founder Managed Track</label>
      <select id="managed_tier" name="managed_tier">
        <option value="">No managed-service add-on</option>
        <?php foreach ((array) $managed_tiers as $tier): ?>
          <option value="<?= $esc((string) $tier['id']) ?>"<?= $selected('managed_tier', (string) $tier['id']) ?>>
            <?= $esc((string) $tier['name']) ?> · <?= (int) $tier['founder_hours'] ?> hr/mo
          </option>
        <?php endforeach; ?>
      </select>
    </div>
    <div class="cb-field">
      <label for="managed_visibility">Managed-service content policy</label>
      <select id="managed_visibility" name="managed_visibility">
        <option value="show"<?= $selected('managed_visibility', 'show') ?>>Show line items</option>
        <option value="summary_only"<?= $selected('managed_visibility', 'summary_only') ?>>Summary only</option>
        <option value="silent_include"<?= $selected('managed_visibility', 'silent_include') ?>>Silent include in total</option>
        <option value="internal_only"<?= $selected('managed_visibility', 'internal_only') ?>>Internal only</option>
        <option value="exclude"<?= $selected('managed_visibility', 'exclude') ?>>Exclude from total</option>
      </select>
    </div>
    <div class="cb-field">
      <label for="owner_markup_pct">Owner markup %</label>
      <input id="owner_markup_pct" name="owner_markup_pct" type="number" min="0" max="100" step="0.1" value="<?= $esc($v('owner_markup_pct', '0')) ?>">
    </div>
    <div class="cb-field">
      <label for="owner_markup_scope">Owner markup scope</label>
      <select id="owner_markup_scope" name="owner_markup_scope">
        <option value="provider_only"<?= $selected('owner_markup_scope', 'provider_only') ?>>Provider only</option>
        <option value="provider_and_managed"<?= $selected('owner_markup_scope', 'provider_and_managed') ?>>Provider + managed services</option>
      </select>
    </div>
    <div class="cb-field">
      <label for="owner_visibility">Owner markup visibility</label>
      <select id="owner_visibility" name="owner_visibility">
        <option value="internal_only"<?= $selected('owner_visibility', 'internal_only') ?>>Internal only</option>
        <option value="show"<?= $selected('owner_visibility', 'show') ?>>Show to client</option>
      </select>
    </div>
    <div class="cb-field">
      <label for="comparison_plan_slug">Compare with alternative</label>
      <select id="comparison_plan_slug" name="comparison_plan_slug">
        <option value="">No comparison</option>
        <?php foreach ((array) $plans as $plan): ?>
          <?php $slug = (string) ($plan['slug'] ?? ''); ?>
          <option value="<?= $esc($slug) ?>"<?= $selected('comparison_plan_slug', $slug) ?>><?= $esc((string) ($plan['name'] ?? $slug)) ?></option>
        <?php endforeach; ?>
      </select>
    </div>
    <div class="cb-field">
      <label for="comparison_visibility">Comparison policy</label>
      <select id="comparison_visibility" name="comparison_visibility">
        <option value="show"<?= $selected('comparison_visibility', 'show') ?>>Show comparison</option>
        <option value="internal_only"<?= $selected('comparison_visibility', 'internal_only') ?>>Internal only</option>
        <option value="exclude"<?= $selected('comparison_visibility', 'exclude') ?>>Exclude</option>
      </select>
    </div>
    <div class="cb-field">
      <label for="source_visibility">Source links</label>
      <select id="source_visibility" name="source_visibility">
        <option value="internal_only"<?= $selected('source_visibility', 'internal_only') ?>>Internal only</option>
        <option value="show"<?= $selected('source_visibility', 'show') ?>>Show to client</option>
        <option value="exclude"<?= $selected('source_visibility', 'exclude') ?>>Exclude</option>
      </select>
    </div>
  </div>

  <h3 style="margin-top:24px;">3. Narrative and delivery controls</h3>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:0 18px;">
    <div class="cb-field" style="grid-column:1/-1;">
      <label for="client_notes">Client-facing notes</label>
      <textarea id="client_notes" name="client_notes" rows="3" maxlength="4000"><?= $esc($v('client_notes')) ?></textarea>
    </div>
    <div class="cb-field" style="grid-column:1/-1;">
      <label for="internal_notes">Internal notes — never sent to client</label>
      <textarea id="internal_notes" name="internal_notes" rows="3" maxlength="4000"><?= $esc($v('internal_notes')) ?></textarea>
    </div>
    <div class="cb-field">
      <label for="department_id">Support department ID</label>
      <input id="department_id" name="department_id" type="number" min="1" value="<?= $esc($v('department_id')) ?>" placeholder="For delivery only">
    </div>
    <div class="cb-field">
      <label for="ticket_id">Existing ticket ID</label>
      <input id="ticket_id" name="ticket_id" type="number" min="1" value="<?= $esc($v('ticket_id')) ?>" placeholder="Blank creates a ticket">
    </div>
    <div class="cb-field">
      <label for="notify_client">Ticket notification</label>
      <select id="notify_client" name="notify_client">
        <option value="no"<?= $selected('notify_client', 'no') ?>>Do not notify client</option>
        <option value="yes"<?= $selected('notify_client', 'yes') ?>>Notify through WHMCS</option>
      </select>
    </div>
  </div>

  <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:18px;">
    <button class="cb-btn" type="submit">Preview proposal</button>
    <span class="cb-card-sub" style="margin:0;">Preview never sends a ticket or email.</span>
  </div>
</form>

<?php if (is_array($result)): ?>
  <?php
    $snapshot = is_array($result['snapshot'] ?? null) ? $result['snapshot'] : [];
    $pricing = is_array($snapshot['pricing'] ?? null) ? $snapshot['pricing'] : [];
    $narrative = is_array($result['narrative'] ?? null) ? $result['narrative'] : [];
  ?>
  <div class="cb-card">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap;">
      <div>
        <h3>Preview result</h3>
        <div class="cb-card-sub" style="margin:0;">
          Narrative: <strong><?= $esc((string) ($narrative['mode'] ?? 'deterministic')) ?></strong>
          <?php if (!empty($narrative['model'])): ?> · model <span class="mono"><?= $esc((string) $narrative['model']) ?></span><?php endif; ?>
        </div>
      </div>
      <div style="text-align:right;">
        <div class="cb-card-sub" style="margin:0;">Client total</div>
        <div class="mono" style="font-size:25px;color:var(--accent);"><?= $esc((string) ($pricing['currency'] ?? 'INR')) ?> <?= $esc(number_format((float) ($pricing['total'] ?? 0), 2)) ?></div>
      </div>
    </div>
    <?php if (!empty($narrative['warning'])): ?><div class="cb-error" style="margin-top:12px;"><?= $esc((string) $narrative['warning']) ?></div><?php endif; ?>
    <?php if (!empty($result['report_generation_provider'])): ?>
      <div class="cb-flash" style="margin-top:12px;">
        Report generator: <strong><?= $esc((string) $result['report_generation_provider']) ?></strong>
        <?php if (!empty($result['report_generation_warning'])): ?> · <?= $esc((string) $result['report_generation_warning']) ?><?php endif; ?>
      </div>
    <?php endif; ?>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:16px 0;">
      <div class="cb-stat"><div class="lbl">Provider subtotal</div><div class="v"><?= $esc(number_format((float) ($pricing['provider_subtotal'] ?? 0), 2)) ?></div></div>
      <div class="cb-stat"><div class="lbl">Owner markup</div><div class="v"><?= $esc(number_format((float) (($pricing['owner_provider'] ?? 0) + ($pricing['owner_managed'] ?? 0)), 2)) ?></div></div>
      <div class="cb-stat"><div class="lbl">Managed subtotal</div><div class="v"><?= $esc(number_format((float) ($pricing['managed_subtotal'] ?? 0), 2)) ?></div></div>
      <div class="cb-stat"><div class="lbl">Snapshot hash</div><div class="v mono" style="font-size:12px;word-break:break-all;"><?= $esc((string) ($snapshot['hash'] ?? '')) ?></div></div>
    </div>
    <div class="cb-card" style="background:#fffefb;border-color:var(--border);">
      <h3>Client artifact</h3>
      <div style="border:1px solid var(--border);border-radius:8px;overflow:auto;background:#fff;padding:4px;"><?= (string) ($result['html'] ?? '') ?></div>
    </div>
    <?php if (!empty($snapshot['warnings'])): ?>
      <div class="cb-card" style="background:#fffaf0;border-color:#ecd9a3;">
        <h3>Internal warnings and controls</h3>
        <ul><?php foreach ((array) $snapshot['warnings'] as $warning): ?><li><?= $esc((string) $warning) ?></li><?php endforeach; ?></ul>
      </div>
    <?php endif; ?>
    <?php if (!empty($snapshot['notes']['internal'])): ?>
      <div class="cb-card" style="background:#f6f7fb;">
        <h3>Internal notes</h3>
        <div><?= nl2br($esc((string) $snapshot['notes']['internal'])) ?></div>
      </div>
    <?php endif; ?>

    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
      <?php foreach ($hiddenKeys as $key): ?>
        <?php if (isset($f[$key]) && is_scalar($f[$key])): ?>
          <input type="hidden" form="proposal-generate-codex" name="<?= $esc($key) ?>" value="<?= $esc((string) $f[$key]) ?>">
          <input type="hidden" form="proposal-send-ticket" name="<?= $esc($key) ?>" value="<?= $esc((string) $f[$key]) ?>">
          <input type="hidden" form="proposal-send-email" name="<?= $esc($key) ?>" value="<?= $esc((string) $f[$key]) ?>">
        <?php endif; ?>
      <?php endforeach; ?>
      <form id="proposal-generate-codex" method="post" action="<?= $esc($module_link) ?>">
        <input type="hidden" name="action" value="proposal-generate-codex">
        <?= generate_token() ?>
        <button class="cb-btn" type="submit">Generate with Codex CLI</button>
      </form>
      <form id="proposal-send-ticket" method="post" action="<?= $esc($module_link) ?>">
        <input type="hidden" name="action" value="proposal-send-ticket">
        <?= generate_token() ?>
        <button class="cb-btn subtle" type="submit" <?= empty($settings->proposalDeliveryEnabled) ? 'disabled' : '' ?>>Send as support ticket</button>
      </form>
      <form id="proposal-send-email" method="post" action="<?= $esc($module_link) ?>">
        <input type="hidden" name="action" value="proposal-send-email">
        <?= generate_token() ?>
        <button class="cb-btn subtle" type="submit" <?= empty($settings->proposalDeliveryEnabled) ? 'disabled' : '' ?>>Send direct email</button>
      </form>
      <?php if (empty($settings->proposalDeliveryEnabled)): ?>
        <span class="cb-card-sub" style="margin:0;">Enable delivery in Addon Configure only after verifying the target client, department, mail transport, and attachment behavior.</span>
      <?php endif; ?>
    </div>
  </div>
<?php endif; ?>

</div>
