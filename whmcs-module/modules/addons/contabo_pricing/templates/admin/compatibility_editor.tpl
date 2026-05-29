<?php
/**
 * Compatibility editor (design §5) — author which configurable-option values may
 * be combined for a plan. Rows are enumerated from the plan's live configurator
 * dimensions and overlaid with any saved mod_contabo_option_compatibility rules.
 * The rules feed SelectionValidator / ConfigOptionCompatibilityRepository::
 * validateCombination() at order + provisioning time.
 *
 * Value-keys are entered one per line (or comma-separated); they may contain
 * spaces/slashes (e.g. "Panels:cPanel/WHM (5 accounts)"), so only newline + comma
 * separate them.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<string,mixed>       $profile
 * @var string                    $plan_slug
 * @var list<array<string,mixed>> $rows        {dimension_key,value_key,label,row}
 * @var string                    $api_error
 * @var string                    $flash
 */

$cb_rows = isset($rows) && is_array($rows) ? $rows : [];
$cb_pid  = (int) ($profile['id'] ?? 0);

/** Decode a *_json column to a newline-joined textarea value. */
$cb_listText = static function ($raw): string {
    if ($raw === null || $raw === '') { return ''; }
    $decoded = json_decode((string) $raw, true);
    if (!is_array($decoded)) { return ''; }
    $out = [];
    foreach ($decoded as $item) { $out[] = (string) $item; }
    return implode("\n", $out);
};

$cb_rules_n = 0;
foreach ($cb_rows as $cb_r) {
    $cb_rr = isset($cb_r['row']) && is_array($cb_r['row']) ? $cb_r['row'] : [];
    if ($cb_rr !== []) { $cb_rules_n++; }
}

$cb_strip_data = [
    ['lbl' => 'Options', 'v' => (string) count($cb_rows), 'sub' => 'plan dimension/value rows', 'tone' => ''],
    ['lbl' => 'Rules saved', 'v' => (string) $cb_rules_n, 'sub' => 'values with a rule', 'tone' => ''],
    ['lbl' => 'Plan', 'v' => (string) ($plan_slug ?? ''), 'sub' => 'Contabo plan slug', 'tone' => ''],
    ['lbl' => 'Enforced', 'v' => 'at order', 'sub' => 'validateCombination()', 'tone' => ''],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;">Compatibility editor</h2>
    <p class="cb-card-sub" style="margin:0; max-width:70ch;">
      Define which option values can be combined for
      <strong><?= $esc($profile['name'] ?? $profile['plan_slug'] ?? ('#' . $cb_pid)) ?></strong>
      (<code class="mono"><?= $esc($plan_slug ?? '') ?></code>). Reference other values by their
      <code class="mono">value_key</code> (shown under each row), one per line.
    </p>
  </div>
  <div style="display:flex; gap:8px;">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=capability-editor&amp;id=<?= $cb_pid ?>">&larr; Capability</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">Back to preview</a>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<?php if (!empty($api_error)): ?>
  <div class="cb-card" style="padding:12px 16px; margin-bottom:14px; border-left:3px solid var(--bad);">
    <strong>Couldn't load this plan's options from the pricing API.</strong>
    <span class="muted"><?= $esc($api_error) ?></span>
    Showing only compatibility rules already saved for this plan.
  </div>
<?php endif; ?>

<?php if ($cb_rows === []): ?>
  <div class="cb-card"><div class="cb-empty">
    <div class="display">No option dimensions to constrain</div>
    <p>The pricing API returned no configurable dimensions for <code class="mono"><?= $esc($plan_slug ?? '') ?></code>.</p>
  </div></div>
<?php else: ?>
  <form class="cb-card" method="post" action="<?= $esc($module_link) ?>" style="padding:0; overflow:hidden;">
    <input type="hidden" name="action" value="compatibility-editor-save">
    <input type="hidden" name="id" value="<?= $cb_pid ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>

    <div style="overflow-x:auto;">
      <table class="cb-table" style="min-width:900px;">
        <thead>
          <tr>
            <th style="text-align:left;">Dimension / value</th>
            <th>Incompatible with <span class="muted">(value_keys)</span></th>
            <th>Requires <span class="muted">(value_keys)</span></th>
            <th style="text-align:center;">Min qty</th>
            <th style="text-align:center;">Max qty</th>
          </tr>
        </thead>
        <tbody>
          <?php foreach ($cb_rows as $cb_i => $cb_r):
              $cb_dim   = (string) ($cb_r['dimension_key'] ?? '');
              $cb_val   = (string) ($cb_r['value_key'] ?? '');
              $cb_lab   = (string) ($cb_r['label'] ?? $cb_val);
              $cb_saved = isset($cb_r['row']) && is_array($cb_r['row']) ? $cb_r['row'] : [];
              $cb_minV  = isset($cb_saved['min_value']) && $cb_saved['min_value'] !== null ? (string) (int) $cb_saved['min_value'] : '';
              $cb_maxV  = isset($cb_saved['max_value']) && $cb_saved['max_value'] !== null ? (string) (int) $cb_saved['max_value'] : '';
          ?>
          <tr>
            <td style="vertical-align:top;">
              <input type="hidden" name="row[<?= (int) $cb_i ?>][dimension_key]" value="<?= $esc($cb_dim) ?>">
              <input type="hidden" name="row[<?= (int) $cb_i ?>][value_key]" value="<?= $esc($cb_val) ?>">
              <div style="font-weight:600;"><?= $esc($cb_lab) ?></div>
              <div class="muted mono" style="font-size:11px;"><?= $esc($cb_dim) ?> · <?= $esc($cb_val) ?></div>
            </td>
            <td style="vertical-align:top;">
              <textarea name="row[<?= (int) $cb_i ?>][incompatible_with]" rows="2" style="width:220px; font-size:12px;"
                        placeholder="one value_key per line"><?= $esc($cb_listText($cb_saved['incompatible_with_json'] ?? null)) ?></textarea>
            </td>
            <td style="vertical-align:top;">
              <textarea name="row[<?= (int) $cb_i ?>][required_values]" rows="2" style="width:220px; font-size:12px;"
                        placeholder="one value_key per line"><?= $esc($cb_listText($cb_saved['required_values_json'] ?? null)) ?></textarea>
            </td>
            <td style="text-align:center; vertical-align:top;">
              <input type="number" name="row[<?= (int) $cb_i ?>][min_value]" value="<?= $esc($cb_minV) ?>" min="0" style="width:70px;">
            </td>
            <td style="text-align:center; vertical-align:top;">
              <input type="number" name="row[<?= (int) $cb_i ?>][max_value]" value="<?= $esc($cb_maxV) ?>" min="0" style="width:70px;">
            </td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>

    <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; padding:14px 16px; border-top:1px solid var(--border);">
      <button type="submit" class="cb-btn">Save compatibility</button>
      <span class="muted" style="font-size:12px;">
        Blank rows are ignored; clearing a saved rule removes its constraints. Min/Max apply to quantity options (e.g. IPv4).
      </span>
    </div>
  </form>
<?php endif; ?>
