<?php
/**
 * A.6.3 — configurable-options PREVIEW (read-only / dry-run).
 *
 * Shows the WHMCS configurable options the ConfigurableOptionsSyncer would
 * create for a profile, with per-cycle pricing. Nothing is written here; the
 * apply path is a separate confirmed action.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<string,mixed> $profile
 * @var array<string,mixed> $version
 * @var array<string,mixed> $report
 * @var list<array<string,mixed>> $omitted
 * @var string   $currency_iso
 * @var string   $markup_strategy
 * @var float    $markup_value
 * @var float    $landed_mult
 * @var string   $flash
 */

$cb_cycles = [
    'monthly'      => 'Monthly',
    'quarterly'    => 'Quarterly',
    'semiannually' => 'Semi-annually',
    'annually'     => 'Annually',
    'biennially'   => 'Biennially',
    'triennially'  => 'Triennially',
];
$cb_typeLabel = [1 => 'Dropdown', 2 => 'Radio', 3 => 'Yes/No', 4 => 'Quantity'];
$cb_totals = isset($report['totals']) && is_array($report['totals']) ? $report['totals'] : ['options' => 0, 'values' => 0, 'skipped' => 0];
$cb_cur = $currency_iso !== '' ? $currency_iso : 'INR';

$cb_strip_data = [
    ['lbl' => 'Options', 'v' => (string) ($cb_totals['options'] ?? 0), 'sub' => 'WHMCS configurable options', 'tone' => ''],
    ['lbl' => 'Sub-values', 'v' => (string) ($cb_totals['values'] ?? 0), 'sub' => 'priced selectable values', 'tone' => ''],
    ['lbl' => 'Skipped', 'v' => (string) ($cb_totals['skipped'] ?? 0), 'sub' => 'not priced (see below)', 'tone' => ((int) ($cb_totals['skipped'] ?? 0)) > 0 ? 'warn' : ''],
    ['lbl' => 'Mode', 'v' => 'preview', 'sub' => 'dry-run — no writes', 'tone' => ''],
];

require __DIR__ . '/_layout_open.tpl';

$cb_fmt = static function ($n) use ($cb_cur): string {
    return $cb_cur . ' ' . number_format((float) $n, 2);
};
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;">Configurable options — preview</h2>
    <p class="cb-card-sub" style="margin:0; max-width:64ch;">
      What the syncer would create for
      <strong><?= $esc($profile['name'] ?? $profile['plan_slug'] ?? ('#' . (int) ($report['profile_id'] ?? 0))) ?></strong>
      (<code class="mono"><?= $esc($profile['plan_slug'] ?? '') ?></code>). This is a
      <strong>dry run</strong> — nothing is written to WHMCS.
    </p>
  </div>
  <div style="display:flex; gap:8px;">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">&larr; Profiles</a>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<?php if (!empty($api_error)): ?>
  <div class="cb-error">Couldn't load the plan's options from the API: <?= $esc($api_error) ?>.
    The preview below may be empty — check that the Contabo API is reachable (Settings &rarr; Test connection).</div>
<?php endif; ?>

<div class="cb-card" style="padding:12px 16px; margin-bottom:14px; border-left:3px solid var(--accent);">
  <strong>Preview only.</strong> No WHMCS configurable options, sub-options or pricing
  rows have been created or changed. Review the structure and pricing below, then use
  <em>Apply</em> to write them (apply is gated behind the configurable-option preflight).
</div>

<?php /* Pricing assumptions ------------------------------------------------ */ ?>
<div class="cb-card" style="padding:14px 16px; margin-bottom:14px;">
  <div class="cb-card-sub" style="margin:0 0 8px;">Pricing assumptions</div>
  <div style="display:flex; flex-wrap:wrap; gap:10px 26px; font-size:13px;">
    <div><span class="glabel">Currency</span><br><?= $esc($cb_cur) ?> <span class="muted">(base, id 1)</span></div>
    <div><span class="glabel">Markup</span><br><?= $esc($markup_strategy) ?> = <?= $esc(rtrim(rtrim(number_format((float) $markup_value, 4, '.', ''), '0'), '.')) ?></div>
    <div><span class="glabel">Landed mult.</span><br><?= $esc(number_format((float) $landed_mult, 4)) ?> <span class="muted">(EUR&rarr;<?= $esc($cb_cur) ?>/mo)</span></div>
    <div><span class="glabel">Rounding</span><br>exact 2 decimals</div>
  </div>
  <p class="cb-card-sub" style="margin:10px 0 0; font-size:12px;">
    Each value's price = its EUR delta &times; landed multiplier, then the markup per cycle.
    Cheaper-than-default values are clamped to <code class="mono">0</code> (v1 policy), so they show as free.
  </p>
</div>

<?php if (empty($report['options'])): ?>
  <div class="cb-card"><div class="cb-empty">
    <div class="display">No options to preview</div>
    <p>This profile's snapshot has no configurable dimensions. Run a sync so the snapshot carries the parsed dimensions.</p>
  </div></div>
<?php else: ?>

<?php foreach ($report['options'] as $opt):
    $dim  = (string) ($opt['dimension_key'] ?? '');
    $type = (int) ($opt['optiontype'] ?? 0);
    $vals = isset($opt['values']) && is_array($opt['values']) ? $opt['values'] : [];
?>
  <div class="cb-card" style="padding:0; overflow:hidden; margin-bottom:14px;">
    <div style="display:flex; align-items:center; gap:10px; padding:12px 16px; border-bottom:1px solid var(--border);">
      <strong style="font-size:14px;"><?= $esc($dim) ?></strong>
      <span class="cb-pill grey"><?= $esc($cb_typeLabel[$type] ?? ('type ' . $type)) ?></span>
      <?php if (!empty($opt['is_quantity'])): ?><span class="cb-pill grey">qty</span><?php endif; ?>
      <span class="muted" style="margin-left:auto; font-size:12px;"><?= count($vals) ?> value<?= count($vals) === 1 ? '' : 's' ?></span>
    </div>
    <div style="overflow-x:auto;">
      <table class="cb-table" style="min-width:720px;">
        <thead>
          <tr>
            <th>Value</th>
            <?php foreach ($cb_cycles as $cy => $clabel): ?><th style="text-align:right;"><?= $esc($clabel) ?></th><?php endforeach; ?>
            <th style="text-align:right;">Setup</th>
          </tr>
        </thead>
        <tbody>
          <?php foreach ($vals as $v):
              $cp = isset($v['cycle_prices']) && is_array($v['cycle_prices']) ? $v['cycle_prices'] : [];
              $sf = isset($v['setup_fees']) && is_array($v['setup_fees']) ? $v['setup_fees'] : [];
              $setupVal = $sf['monthly'] ?? null;
          ?>
          <tr>
            <td>
              <?= $esc($v['label'] ?? '') ?>
              <?php if (!empty($v['is_default'])): ?> <span class="cb-pill" style="background:var(--ok-bg,#e6f4ea); color:#1f7a3d;">default</span><?php endif; ?>
            </td>
            <?php foreach (array_keys($cb_cycles) as $cy): ?>
              <td style="text-align:right; font-variant-numeric:tabular-nums;<?= ((float) ($cp[$cy] ?? 0)) <= 0 ? ' color:var(--muted);' : '' ?>"><?= $esc($cb_fmt($cp[$cy] ?? 0)) ?></td>
            <?php endforeach; ?>
            <td style="text-align:right; font-variant-numeric:tabular-nums;<?= $setupVal === null ? ' color:var(--muted);' : '' ?>"><?= $setupVal === null ? '—' : $esc($cb_fmt($setupVal)) ?></td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>
  </div>
<?php endforeach; ?>

<?php endif; ?>

<?php if (!empty($report['skipped'])): ?>
  <div class="cb-card" style="padding:14px 16px; margin-bottom:14px;">
    <div class="cb-card-sub" style="margin:0 0 8px; color:#9a6a00;">Skipped (<?= count($report['skipped']) ?>)</div>
    <ul style="margin:0; padding-left:18px; font-size:13px;">
      <?php foreach ($report['skipped'] as $s): ?>
        <li><code class="mono"><?= $esc($s['dimension_key'] ?? '') ?></code><?php if (!empty($s['value'])): ?> &rarr; <?= $esc($s['value']) ?><?php endif; ?> — <?= $esc($s['reason'] ?? '') ?></li>
      <?php endforeach; ?>
    </ul>
  </div>
<?php endif; ?>

<?php if (!empty($omitted)): ?>
  <div class="cb-card" style="padding:14px 16px; margin-bottom:14px;">
    <div class="cb-card-sub" style="margin:0 0 8px;">Omitted dimensions (<?= count($omitted) ?>)</div>
    <ul style="margin:0; padding-left:18px; font-size:13px;">
      <?php foreach ($omitted as $o): ?>
        <li><code class="mono"><?= $esc($o['dimension_key'] ?? '') ?></code> — <?= $esc($o['reason'] ?? '') ?> (<?= (int) ($o['value_count'] ?? 0) ?> values)</li>
      <?php endforeach; ?>
    </ul>
  </div>
<?php endif; ?>

<?php $cb_mapped = isset($mapped_products) && is_array($mapped_products) ? $mapped_products : []; ?>
<?php if (empty($report['options'])): ?>
  <?php /* nothing to apply */ ?>
<?php elseif (empty($cb_mapped)): ?>
  <div class="cb-card" style="padding:14px 16px;">
    <strong>Apply unavailable.</strong> Map this profile to a WHMCS product first on the
    <a href="<?= $esc($module_link) ?>&amp;action=mappings">Mappings</a> page, then return here to apply.
  </div>
<?php else: ?>
  <form class="cb-card" method="post" action="<?= $esc($module_link) ?>" style="padding:14px 16px; display:flex; flex-wrap:wrap; align-items:center; gap:12px;">
    <input type="hidden" name="action" value="config-apply">
    <input type="hidden" name="id" value="<?= (int) ($report['profile_id'] ?? 0) ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <strong>Apply to product</strong>
    <select name="product_id" required style="padding:7px 10px; border:1px solid var(--border); border-radius:8px; background:var(--panel); font:inherit; font-size:13px;">
      <?php foreach ($cb_mapped as $cb_mp): ?>
        <option value="<?= (int) $cb_mp['id'] ?>"><?= $esc($cb_mp['name']) ?> (#<?= (int) $cb_mp['id'] ?>)</option>
      <?php endforeach; ?>
    </select>
    <label style="display:flex; align-items:center; gap:6px; font-size:13px; cursor:pointer;">
      <input type="checkbox" name="confirm" value="1" required>
      I understand this writes these options &amp; pricing to the live product
    </label>
    <button type="submit" class="cb-btn">Apply</button>
    <span class="muted" style="font-size:12px;">Idempotent &amp; ownership-scoped — re-applying makes no duplicates. Base currency only.</span>
  </form>
<?php endif; ?>
