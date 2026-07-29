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

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55">Configurable options — preview</h2>
    <p class="cb-card-sub" data-cb-u="u-1beb43d512">
      What the syncer would create for
      <strong><?= $esc($profile['name'] ?? $profile['plan_slug'] ?? ('#' . (int) ($report['profile_id'] ?? 0))) ?></strong>
      (<code class="mono"><?= $esc($profile['plan_slug'] ?? '') ?></code>). This is a
      <strong>dry run</strong> — nothing is written to WHMCS.
    </p>
  </div>
  <div data-cb-u="u-b887bfd543">
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

<div class="cb-card" data-cb-u="u-e9341aa696">
  <strong>Preview only.</strong> No WHMCS configurable options, sub-options or pricing
  rows have been created or changed. Review the structure and pricing below, then use
  <em>Apply</em> to write them (apply is gated behind the configurable-option preflight).
</div>

<?php /* Pricing assumptions ------------------------------------------------ */ ?>
<div class="cb-card" data-cb-u="u-73d1a88749">
  <div class="cb-card-sub" data-cb-u="u-bb5bea3857">Pricing assumptions</div>
  <div data-cb-u="u-14c22a9a52">
    <div><span class="glabel">Currency</span><br><?= $esc($cb_cur) ?> <span class="muted">(base, id 1)</span></div>
    <div><span class="glabel">Markup</span><br><?= $esc($markup_strategy) ?> = <?= $esc(rtrim(rtrim(number_format((float) $markup_value, 4, '.', ''), '0'), '.')) ?></div>
    <div><span class="glabel">Landed mult.</span><br><?= $esc(number_format((float) $landed_mult, 4)) ?> <span class="muted">(EUR&rarr;<?= $esc($cb_cur) ?>/mo)</span></div>
    <div><span class="glabel">Rounding</span><br>exact 2 decimals</div>
  </div>
  <p class="cb-card-sub" data-cb-u="u-c7f8fc7126">
    Each value's price = its EUR delta &times; landed multiplier, then the markup per cycle.
    Cheaper-than-default values are clamped to <code class="mono">0</code> (v1 policy), so they show as free.
  </p>
</div>

<?php /* Default-selection validation (compatibility + capability) ----------- */ ?>
<?php
$cb_val = isset($validation) && is_array($validation) ? $validation : ['valid' => true, 'violations' => [], 'capability_warnings' => []];
$cb_violations = isset($cb_val['violations']) && is_array($cb_val['violations']) ? $cb_val['violations'] : [];
$cb_capwarn = isset($cb_val['capability_warnings']) && is_array($cb_val['capability_warnings']) ? $cb_val['capability_warnings'] : [];
?>
<?php if ($cb_violations !== [] || $cb_capwarn !== []): ?>
  <div class="cb-card" data-cb-u="u-73d1a88749">
    <div class="cb-card-sub" data-cb-u="u-bb5bea3857">Default-configuration checks</div>
    <?php if ($cb_violations !== []): ?>
      <div class="cb-error" data-cb-u="u-bb5bea3857">
        <strong><?= count($cb_violations) ?> compatibility violation<?= count($cb_violations) === 1 ? '' : 's' ?></strong> in the default selection:
        <ul data-cb-u="u-38dd9dbcc8">
          <?php foreach ($cb_violations as $v): ?>
            <li><code class="mono"><?= $esc($v['dimension_key'] ?? '') ?></code> &rarr; <?= $esc($v['value_key'] ?? '') ?> — <?= $esc($v['reason'] ?? '') ?>: <?= $esc($v['detail'] ?? '') ?></li>
          <?php endforeach; ?>
        </ul>
      </div>
    <?php endif; ?>
    <?php if ($cb_capwarn !== []): ?>
      <div data-cb-u="u-cdcef186c8">
        <strong data-cb-u="u-3a6f9d137d"><?= count($cb_capwarn) ?> option<?= count($cb_capwarn) === 1 ? '' : 's' ?> with destructive change semantics</strong>
        (changing them later requires a reinstall/recreate; only <code class="mono">api_verified</code> capabilities auto-apply):
        <ul data-cb-u="u-38dd9dbcc8">
          <?php foreach ($cb_capwarn as $w): ?>
            <li><code class="mono"><?= $esc($w['dimension_key'] ?? '') ?></code> &rarr; <?= $esc($w['value_key'] ?? '') ?>
              <?php if (!empty($w['requires_backup_warning'])): ?><span class="cb-pill grey">backup warning</span><?php endif; ?>
              <?php if (!empty($w['requires_admin_approval'])): ?><span class="cb-pill grey">admin approval</span><?php endif; ?>
              <span class="muted">(<?= $esc($w['capability_source'] ?? 'unknown') ?>)</span>
            </li>
          <?php endforeach; ?>
        </ul>
      </div>
    <?php endif; ?>
  </div>
<?php endif; ?>

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
  <div class="cb-card" data-cb-u="u-f2c57be93c">
    <div data-cb-u="u-faefb0a74b">
      <strong data-cb-u="u-905228b6d5"><?= $esc($dim) ?></strong>
      <span class="cb-pill grey"><?= $esc($cb_typeLabel[$type] ?? ('type ' . $type)) ?></span>
      <?php if (!empty($opt['is_quantity'])): ?><span class="cb-pill grey">qty</span><?php endif; ?>
      <span class="muted" data-cb-u="u-386c1519f8"><?= count($vals) ?> value<?= count($vals) === 1 ? '' : 's' ?></span>
    </div>
    <div data-cb-u="u-fbff25e9fc">
      <table class="cb-table" data-cb-u="u-a2b5b9b303">
        <thead>
          <tr>
            <th>Value</th>
            <?php foreach ($cb_cycles as $cy => $clabel): ?><th data-cb-u="u-9b75d38881"><?= $esc($clabel) ?></th><?php endforeach; ?>
            <th data-cb-u="u-9b75d38881">Setup</th>
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
              <?php if (!empty($v['is_default'])): ?> <span class="cb-pill" data-cb-u="u-1bfc716d99">default</span><?php endif; ?>
            </td>
            <?php foreach (array_keys($cb_cycles) as $cy): ?>
              <td class="cb-number<?= ((float) ($cp[$cy] ?? 0)) <= 0 ? ' cb-tone-muted' : '' ?>"><?= $esc($cb_fmt($cp[$cy] ?? 0)) ?></td>
            <?php endforeach; ?>
            <td class="cb-number<?= $setupVal === null ? ' cb-tone-muted' : '' ?>"><?= $setupVal === null ? '—' : $esc($cb_fmt($setupVal)) ?></td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>
  </div>
<?php endforeach; ?>

<?php endif; ?>

<?php if (!empty($report['skipped'])): ?>
  <div class="cb-card" data-cb-u="u-73d1a88749">
    <div class="cb-card-sub" data-cb-u="u-43a7fae855">Skipped (<?= count($report['skipped']) ?>)</div>
    <ul data-cb-u="u-44c90ade49">
      <?php foreach ($report['skipped'] as $s): ?>
        <li><code class="mono"><?= $esc($s['dimension_key'] ?? '') ?></code><?php if (!empty($s['value'])): ?> &rarr; <?= $esc($s['value']) ?><?php endif; ?> — <?= $esc($s['reason'] ?? '') ?></li>
      <?php endforeach; ?>
    </ul>
  </div>
<?php endif; ?>

<?php if (!empty($omitted)): ?>
  <div class="cb-card" data-cb-u="u-73d1a88749">
    <div class="cb-card-sub" data-cb-u="u-bb5bea3857">Omitted dimensions (<?= count($omitted) ?>)</div>
    <ul data-cb-u="u-44c90ade49">
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
  <div class="cb-card" data-cb-u="u-7760a857b8">
    <strong>Apply unavailable.</strong> Map this profile to a WHMCS product first on the
    <a href="<?= $esc($module_link) ?>&amp;action=mappings">Mappings</a> page, then return here to apply.
  </div>
<?php else: ?>
  <form class="cb-card" method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-b681533037">
    <input type="hidden" name="action" value="config-diff">
    <input type="hidden" name="id" value="<?= (int) ($report['profile_id'] ?? 0) ?>">
    <strong>Apply to product</strong>
    <select name="product_id" required data-cb-u="u-a1fddde4ad">
      <?php foreach ($cb_mapped as $cb_mp): ?>
        <option value="<?= (int) $cb_mp['id'] ?>"><?= $esc($cb_mp['name']) ?> (#<?= (int) $cb_mp['id'] ?>)</option>
      <?php endforeach; ?>
    </select>
    <button type="submit" class="cb-btn">Review changes (diff) &rarr;</button>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-exposure&amp;id=<?= (int) ($report['profile_id'] ?? 0) ?>">Edit exposure</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=capability-editor&amp;id=<?= (int) ($report['profile_id'] ?? 0) ?>">Edit capabilities</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=compatibility-editor&amp;id=<?= (int) ($report['profile_id'] ?? 0) ?>">Edit compatibility</a>
    <span class="muted" data-cb-u="u-c42f23b8ec">You’ll see a per-dimension diff of exactly what changes on the live product <em>before</em> anything is written. Idempotent &amp; ownership-scoped. Base currency only.</span>
  </form>
<?php endif; ?>
