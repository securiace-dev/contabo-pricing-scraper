<?php
/**
 * Skipped report — services where `applied = false`, grouped by skip_reason.
 *
 * "Cost exposure" is the sum of (proposed_new_price - old_price) across
 * non-applied decisions whose skip reason is actionable (awaiting approval,
 * notice scheduled, etc.). Phase/observe skips don't count toward exposure
 * because they're a deliberate engine-wide block, not a per-service issue.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var list<array{reason:string,count:int,rows:list<array<string,mixed>>}> $groups
 * @var float    $cost_exposure
 * @var string   $currency
 */

$cb_total = 0;
foreach ($groups as $cb_g) { $cb_total += (int) $cb_g['count']; }

$cb_exposure_fmt = number_format((float) $cost_exposure, 2);
$cb_currency = isset($currency) ? (string) $currency : 'INR';

$cb_strip_data = [
    [
        'lbl' => 'Skipped decisions',
        'v'   => (string) $cb_total,
        'sub' => 'across all skip reasons',
        'tone' => $cb_total > 0 ? 'warn' : '',
    ],
    [
        'lbl' => 'Skip reasons',
        'v'   => (string) count($groups),
        'sub' => 'distinct buckets',
        'tone' => '',
    ],
    [
        'lbl' => 'Cost exposure',
        'v'   => $cb_currency . ' ' . $cb_exposure_fmt,
        'sub' => 'would-be applied total',
        'tone' => ((float) $cost_exposure) > 0 ? 'bad' : '',
    ],
    [
        'lbl' => 'Engine phase',
        'v'   => 'observe',
        'sub' => 'no writes during Phase A',
        'tone' => '',
    ],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55">Skipped report</h2>
    <p class="cb-card-sub" data-cb-u="u-8c7c145b64">
      Decisions the engine chose not to apply, grouped by reason. Use this to spot
      bottlenecks (e.g. lots of <code class="mono">notice_scheduled</code>) before flipping the apply phase.
    </p>
  </div>
  <div data-cb-u="u-b887bfd543">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=repricing">&larr; Dashboard</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=price-decisions">Audit log</a>
  </div>
</header>

<?php if (empty($groups)): ?>

<div class="cb-card">
  <div class="cb-empty">
    <div class="display">Nothing to report</div>
    <p>Either every decision was applied, or the engine hasn't produced any rows yet.</p>
  </div>
</div>

<?php else: ?>

<?php foreach ($groups as $g):
  $reason = (string) $g['reason'];
  $count = (int) $g['count'];
  $sample = is_array($g['rows']) ? $g['rows'] : [];

  // Skip-reason tone heuristic.
  $tone = 'grey';
  if (in_array($reason, ['notice_failed', 'awaiting_force_approval_max_increase_exceeded', 'service_terminated'], true)) {
      $tone = 'bad';
  } elseif (in_array($reason, ['awaiting_admin_approval', 'within_notice_window', 'notice_scheduled', 'fx_unavailable'], true)) {
      $tone = 'warn';
  } elseif (in_array($reason, ['phase_observe_only', 'phase_opt_in_required', 'lifetime_grandfather'], true)) {
      $tone = '';
  }
?>
<div class="cb-card" data-cb-skip-group="<?= $esc($reason) ?>">
  <div data-cb-u="u-77d1039815">
    <h3 data-cb-u="u-38965f9b18">
      <span class="cb-pill <?= $esc($tone) ?>"><?= $esc($reason) ?></span>
      <span class="mono" data-cb-u="u-84f087897c">× <?= (int) $count ?></span>
    </h3>
    <button type="button" class="cb-btn ghost" data-cb-toggle-group="<?= $esc($reason) ?>" aria-expanded="true">Collapse</button>
  </div>

  <div data-cb-group-body="<?= $esc($reason) ?>" data-cb-u="u-7b590caf29">
    <table class="cb-table" data-cb-u="u-b7aa1ec004">
      <thead>
        <tr>
          <th>Service</th>
          <th>Decided</th>
          <th>Policy</th>
          <th>Cycle</th>
          <th class="right">Old</th>
          <th class="right">Proposed</th>
          <th class="right">Δ</th>
        </tr>
      </thead>
      <tbody>
        <?php foreach ($sample as $r):
          $svcId = (int) ($r['service_id'] ?? 0);
          $decidedAt = (string) ($r['decided_at'] ?? '');
          $policy = (string) ($r['policy_used'] ?? '');
          $cycle = (string) ($r['billing_cycle'] ?? '');
          $old = (float) ($r['old_price'] ?? 0);
          $new = (float) ($r['proposed_new_price'] ?? 0);
          $delta = $new - $old;
        ?>
        <tr>
          <td class="mono">#<?= (int) $svcId ?></td>
          <td class="mono" data-cb-u="u-a49cca52be"><?= $esc(substr($decidedAt, 0, 16)) ?></td>
          <td><span class="cb-pill grey"><?= $esc($policy) ?></span></td>
          <td class="mono" data-cb-u="u-a49cca52be"><?= $esc($cycle) ?></td>
          <td class="right mono price"><?= $esc(number_format($old, 2)) ?></td>
          <td class="right mono price"><?= $esc(number_format($new, 2)) ?></td>
          <td class="right mono">
            <?php $deltaTone = $delta > 0 ? 'cb-tone-accent' : ($delta < 0 ? 'cb-tone-good' : 'cb-tone-muted'); ?>
            <span class="<?= $esc($deltaTone) ?>">
              <?= ($delta >= 0 ? '+' : '') . number_format($delta, 2) ?>
            </span>
          </td>
        </tr>
        <?php endforeach; ?>
      </tbody>
    </table>

    <?php if ($count > count($sample)): ?>
      <p data-cb-u="u-503692af4b">
        Showing the most recent <?= (int) count($sample) ?> of <?= (int) $count ?>. Use the audit log for the full list.
      </p>
    <?php endif; ?>
  </div>
</div>
<?php endforeach; ?>

<?php endif; ?>

</div>
