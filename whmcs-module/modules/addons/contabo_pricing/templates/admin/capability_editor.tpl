<?php
/**
 * Capability editor (design §4 / amendment #6) — curate the per-(plan,dimension,
 * value) capability matrix that gates whether an option change may auto-apply.
 * Rows are enumerated from the plan's live configurator dimensions and overlaid
 * with any saved mod_contabo_option_capability rows. Saving records intent;
 * only capability_source = api_verified lets a destructive change auto-apply.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<string,mixed>            $profile
 * @var string                         $plan_slug
 * @var list<array<string,mixed>>      $rows           {dimension_key,value_key,label,row}
 * @var list<string>                   $boolean_flags
 * @var list<string>                   $valid_sources
 * @var string                         $api_error
 * @var string                         $flash
 */

$cb_rows = isset($rows) && is_array($rows) ? $rows : [];
$cb_pid  = (int) ($profile['id'] ?? 0);
$cb_flags   = isset($boolean_flags) && is_array($boolean_flags) ? $boolean_flags : [];
$cb_sources = isset($valid_sources) && is_array($valid_sources) ? $valid_sources : [];

// Short, human column headers for the boolean flags (falls back to the raw key).
$cb_flagLabel = [
    'allowed_on_create'         => 'Create',
    'allowed_on_reinstall'      => 'Reinstall',
    'allowed_on_post_provision' => 'Post-prov',
    'allowed_on_upgrade'        => 'Upgrade',
    'allowed_on_downgrade'      => 'Downgrade',
    'requires_reinstall'        => 'Req reinstall',
    'requires_recreate'         => 'Req recreate',
    'destructive_change'        => 'Destructive',
    'data_loss_expected'        => 'Data loss',
    'requires_backup_warning'   => 'Backup warn',
    'requires_admin_approval'   => 'Approval',
    'billing_change_possible'   => 'Billing Δ',
];

$cb_destructive_n = 0;
foreach ($cb_rows as $cb_r) {
    if (!empty($cb_r['row']['destructive_change'])) { $cb_destructive_n++; }
}

$cb_strip_data = [
    ['lbl' => 'Options', 'v' => (string) count($cb_rows), 'sub' => 'plan dimension/value rows', 'tone' => ''],
    ['lbl' => 'Destructive', 'v' => (string) $cb_destructive_n, 'sub' => 'changes flagged destructive', 'tone' => $cb_destructive_n > 0 ? 'warn' : ''],
    ['lbl' => 'Plan', 'v' => (string) ($plan_slug ?? ''), 'sub' => 'Contabo plan slug', 'tone' => ''],
    ['lbl' => 'Gate', 'v' => 'amendment 6', 'sub' => 'api_verified auto-applies', 'tone' => ''],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;">Capability editor</h2>
    <p class="cb-card-sub" style="margin:0; max-width:70ch;">
      Classify each option change for
      <strong><?= $esc($profile['name'] ?? $profile['plan_slug'] ?? ('#' . $cb_pid)) ?></strong>
      (<code class="mono"><?= $esc($plan_slug ?? '') ?></code>): what is allowed where, what is
      destructive, and how trustworthy that answer is. Only <code class="mono">api_verified</code>
      rows let a destructive change apply without admin approval.
    </p>
  </div>
  <div style="display:flex; gap:8px;">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=compatibility-editor&amp;id=<?= $cb_pid ?>">Compatibility &rarr;</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">&larr; Back to preview</a>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<?php if (!empty($api_error)): ?>
  <div class="cb-card" style="padding:12px 16px; margin-bottom:14px; border-left:3px solid var(--bad);">
    <strong>Couldn't load this plan's options from the pricing API.</strong>
    <span class="muted"><?= $esc($api_error) ?></span>
    Showing only capability rows already saved for this plan.
  </div>
<?php endif; ?>

<?php if ($cb_rows === []): ?>
  <div class="cb-card"><div class="cb-empty">
    <div class="display">No option dimensions to classify</div>
    <p>The pricing API returned no configurable dimensions for <code class="mono"><?= $esc($plan_slug ?? '') ?></code>.</p>
  </div></div>
<?php else: ?>
  <form class="cb-card" method="post" action="<?= $esc($module_link) ?>" style="padding:0; overflow:hidden;">
    <input type="hidden" name="action" value="capability-editor-save">
    <input type="hidden" name="id" value="<?= $cb_pid ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>

    <div style="overflow-x:auto;">
      <table class="cb-table" style="min-width:1100px;">
        <thead>
          <tr>
            <th style="text-align:left;">Dimension / value</th>
            <?php foreach ($cb_flags as $cb_f): ?>
              <th style="text-align:center; font-size:11px;" title="<?= $esc($cb_f) ?>"><?= $esc($cb_flagLabel[$cb_f] ?? $cb_f) ?></th>
            <?php endforeach; ?>
            <th>Provisioning action</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          <?php foreach ($cb_rows as $cb_i => $cb_r):
              $cb_dim   = (string) ($cb_r['dimension_key'] ?? '');
              $cb_val   = (string) ($cb_r['value_key'] ?? '');
              $cb_lab   = (string) ($cb_r['label'] ?? $cb_val);
              $cb_saved = isset($cb_r['row']) && is_array($cb_r['row']) ? $cb_r['row'] : [];
          ?>
          <tr>
            <td>
              <input type="hidden" name="row[<?= (int) $cb_i ?>][dimension_key]" value="<?= $esc($cb_dim) ?>">
              <input type="hidden" name="row[<?= (int) $cb_i ?>][value_key]" value="<?= $esc($cb_val) ?>">
              <div style="font-weight:600;"><?= $esc($cb_lab) ?></div>
              <div class="muted mono" style="font-size:11px;"><?= $esc($cb_dim) ?> · <?= $esc($cb_val) ?></div>
            </td>
            <?php foreach ($cb_flags as $cb_f): ?>
              <td style="text-align:center;">
                <input type="checkbox" name="row[<?= (int) $cb_i ?>][<?= $esc($cb_f) ?>]" value="1"<?= !empty($cb_saved[$cb_f]) ? ' checked' : '' ?>>
              </td>
            <?php endforeach; ?>
            <td>
              <input type="text" name="row[<?= (int) $cb_i ?>][provisioning_action]"
                     value="<?= $esc((string) ($cb_saved['provisioning_action'] ?? '')) ?>"
                     placeholder="e.g. reinstall" style="width:130px;">
            </td>
            <td>
              <?php $cb_src = (string) ($cb_saved['capability_source'] ?? 'manual_assumption'); ?>
              <select name="row[<?= (int) $cb_i ?>][capability_source]">
                <?php foreach ($cb_sources as $cb_s): ?>
                  <option value="<?= $esc($cb_s) ?>"<?= $cb_s === $cb_src ? ' selected' : '' ?>><?= $esc($cb_s) ?></option>
                <?php endforeach; ?>
              </select>
            </td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>

    <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; padding:14px 16px; border-top:1px solid var(--border);">
      <button type="submit" class="cb-btn">Save capabilities</button>
      <span class="muted" style="font-size:12px;">
        Saved rows feed the amendment-6 auto-apply gate — destructive changes stay admin-gated unless the source is <code class="mono">api_verified</code>.
      </span>
    </div>
  </form>
<?php endif; ?>
