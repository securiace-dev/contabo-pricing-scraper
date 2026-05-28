<?php
/**
 * Repricing dashboard — Phase A read-mostly view.
 *
 * Shows a 4-tile KPI strip + a recent-decisions table with filter pills.
 * Phase A means every decision row here will have `applied = false` /
 * `skip_reason = phase_observe_only` until the admin flips the phase in
 * tax-settings later. The page still surfaces every decision so the admin
 * can confirm the engine's logic before enabling writes.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var int      $services_tracked
 * @var int      $decisions_today
 * @var int      $applied_today
 * @var int      $awaiting
 * @var int      $notices_scheduled
 * @var list<array<string,mixed>> $rows
 * @var string   $phase
 * @var array<string,int> $cycle_breakdown   cycle => services count
 * @var int      $cycle_exposure            count of services whose billingcycle is not in renewal_cycles_mask
 * @var list<int> $cycle_exposure_svc       (optional) first N exposed service ids
 */

$cb_phase = isset($phase) ? (string) $phase : 'observe';
$cb_phase_tone = $cb_phase === 'enforce' ? 'good' : ($cb_phase === 'opt_in' ? 'warn' : 'grey');

$cb_cycle_breakdown = isset($cycle_breakdown) && is_array($cycle_breakdown) ? $cycle_breakdown : [];
$cb_cycle_exposure  = isset($cycle_exposure) ? (int) $cycle_exposure : 0;

// Compose a compact "Annually 132 / Quarterly 9 …" sub-string ordered by count desc.
$cb_breakdown_pairs = [];
foreach ($cb_cycle_breakdown as $cyc => $n) {
    if ((int) $n > 0) { $cb_breakdown_pairs[$cyc] = (int) $n; }
}
arsort($cb_breakdown_pairs);
$cb_breakdown_summary = '';
foreach ($cb_breakdown_pairs as $cyc => $n) {
    $cb_breakdown_summary .= ($cb_breakdown_summary !== '' ? ' / ' : '') . $cyc . ' ' . $n;
}
$cb_breakdown_total = array_sum($cb_breakdown_pairs);

// Status strip — 4 KPI tiles consumed by _layout_open.tpl.
$cb_strip_data = [
    [
        'lbl'  => 'Services tracked',
        'v'    => (string) (int) $services_tracked,
        'sub'  => 'mapped to a Contabo profile',
        'tone' => ((int) $services_tracked) > 0 ? '' : 'warn',
    ],
    [
        'lbl'  => 'Decisions today',
        'v'    => (string) (int) $decisions_today,
        'sub'  => 'observe-only sweep',
        'tone' => '',
    ],
    [
        'lbl'  => 'Applied today',
        'v'    => (string) (int) $applied_today,
        'sub'  => $cb_phase === 'observe' ? 'phase = observe; no apply' : 'writes via PreInvoicing hook',
        'tone' => ((int) $applied_today) > 0 ? 'good' : '',
    ],
    [
        'lbl'  => 'Awaiting approval',
        'v'    => (string) (int) $awaiting,
        'sub'  => 'queue + force-approve combined',
        'tone' => ((int) $awaiting) > 0 ? 'warn' : '',
    ],
    [
        'lbl'  => 'Services per cycle',
        'v'    => (string) (int) $cb_breakdown_total,
        'sub'  => $cb_breakdown_summary !== '' ? $cb_breakdown_summary : 'no mapped services yet',
        'tone' => '',
    ],
    [
        'lbl'  => 'Cycle exposure',
        'v'    => (string) $cb_cycle_exposure,
        'sub'  => $cb_cycle_exposure > 0
            ? 'services whose cycle is not in renewal_cycles_mask'
            : 'every mapped service has its cycle covered',
        'tone' => $cb_cycle_exposure > 0 ? 'warn' : 'good',
    ],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;">Repricing</h2>
    <p class="cb-card-sub" style="margin:0; max-width:62ch;">
      Renewal pricing policy engine — decisions are emitted by the daily observe sweep.
      Phase A is read-only; nothing here writes <code class="mono">tblhosting.recurringamount</code>.
    </p>
  </div>
  <div style="display:flex; gap:8px; align-items:center;">
    <span class="cb-pill <?= $esc($cb_phase_tone) ?>" title="Repricing phase">
      <span class="dot"></span>phase: <?= $esc($cb_phase) ?>
    </span>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=price-decisions">Audit log</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=skipped-report">Skipped report</a>
    <a class="cb-btn<?= ((int) $awaiting) > 0 ? '' : ' ghost' ?>" href="<?= $esc($module_link) ?>&amp;action=approval-queue">Approval queue<?php if (((int) $awaiting) > 0): ?> (<?= $esc((string) (int) $awaiting) ?>)<?php endif; ?></a>
    <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=tax-settings">Tax settings</a>
  </div>
</header>

<?php /* Toolbar — filter pills + search ----------------------------------- */ ?>
<div class="cb-toolbar" role="region" aria-label="Decision filters">
  <span class="glabel">Status</span>
  <div class="cb-filter-pills" data-cb-filter-group="repricing-status" role="group">
    <button type="button" data-cb-filter="all"        aria-pressed="true">All</button>
    <button type="button" data-cb-filter="applied"    aria-pressed="false">Applied</button>
    <button type="button" data-cb-filter="skipped"    aria-pressed="false">Skipped</button>
    <button type="button" data-cb-filter="awaiting"   aria-pressed="false">Awaiting approval</button>
    <button type="button" data-cb-filter="notice"     aria-pressed="false">Notice scheduled</button>
  </div>

  <div class="cb-search" role="search">
    <input type="search" placeholder="Search policy, reason, service&hellip;"
           data-cb-search data-cb-search-scope="repricing"
           aria-label="Search recent decisions">
  </div>

  <div class="spacer"></div>
  <div class="count">
    Notices scheduled: <span class="mono"><?= (int) $notices_scheduled ?></span>
  </div>
</div>

<?php /* Cycle filter pill row — drives `data-cb-cycle` row attr below. ------ */ ?>
<div class="cb-toolbar" role="region" aria-label="Cycle filter" style="margin-top:-6px;">
  <span class="glabel">Cycle</span>
  <div class="cb-filter-pills" data-cb-filter-group="repricing-cycle" role="group">
    <button type="button" data-cb-filter-cycle="all"           aria-pressed="true">All</button>
    <button type="button" data-cb-filter-cycle="Monthly"       aria-pressed="false">Monthly</button>
    <button type="button" data-cb-filter-cycle="Quarterly"     aria-pressed="false">Quarterly</button>
    <button type="button" data-cb-filter-cycle="Semi-Annually" aria-pressed="false">Semi-Annually</button>
    <button type="button" data-cb-filter-cycle="Annually"      aria-pressed="false">Annually</button>
    <button type="button" data-cb-filter-cycle="Biennially"    aria-pressed="false">Biennially</button>
    <button type="button" data-cb-filter-cycle="Triennially"   aria-pressed="false">Triennially</button>
  </div>
  <div class="spacer"></div>
  <div class="count">
    Exposure: <span class="mono"><?= (int) $cb_cycle_exposure ?></span>
  </div>
</div>

<?php if (empty($rows)): ?>

<div class="cb-card">
  <div class="cb-empty">
    <div class="display">No decisions yet</div>
    <p>The observe sweep will emit one decision per mapped service the next time the daily cron fires.</p>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>">&larr; Back to dashboard</a>
  </div>
</div>

<?php else: ?>

<div class="cb-card" style="padding:0; overflow:hidden;">
  <table class="cb-table" data-cb-table="repricing-decisions">
    <thead>
      <tr>
        <th>Service</th>
        <th>Cycle</th>
        <th>Policy</th>
        <th class="right">Old</th>
        <th class="right">Proposed</th>
        <th class="right">Δ%</th>
        <th>Status</th>
        <th>Skip reason</th>
        <th>Decided</th>
      </tr>
    </thead>
    <tbody>
      <?php foreach ($rows as $r):
        $svcId   = (int) ($r['service_id'] ?? 0);
        $policy  = (string) ($r['policy_used'] ?? '—');
        $cycle   = (string) ($r['billing_cycle'] ?? '');
        $old     = (float) ($r['old_price'] ?? 0);
        $new     = (float) ($r['proposed_new_price'] ?? 0);
        $applied = !empty($r['applied']);
        $awaitingApproval = !empty($r['requires_admin_approval']);
        $skipReason = (string) ($r['skip_reason'] ?? '');
        $decidedAt  = (string) ($r['decided_at'] ?? '');
        $deltaPct = ($old > 0) ? (($new - $old) / $old * 100.0) : 0.0;
        $statusBucket = $applied
            ? 'applied'
            : ($awaitingApproval ? 'awaiting'
              : (in_array($skipReason, ['notice_scheduled', 'within_notice_window'], true) ? 'notice' : 'skipped'));

        $policyTone = 'grey';
        switch ($policy) {
            case 'lifetime':         $policyTone = 'good'; break;
            case 'frozen_until':     $policyTone = ''; break;
            case 'current_term':     $policyTone = 'warn'; break;
            case 'margin_floor':     $policyTone = ''; break;
            case 'reprice_renewal':  $policyTone = 'warn'; break;
            case 'manual':           $policyTone = 'grey'; break;
        }
      ?>
      <tr data-cb-status="<?= $esc($statusBucket) ?>"
          data-cb-cycle="<?= $esc($cycle) ?>"
          data-cb-search-text="<?= $esc(strtolower($policy . ' ' . $skipReason . ' service-' . $svcId . ' ' . $cycle)) ?>"
          data-cb-open-drawer="decision"
          data-cb-decision-id="<?= (int) ($r['id'] ?? 0) ?>"
          style="cursor:pointer">
        <td class="mono">#<?= (int) $svcId ?></td>
        <td class="mono" style="font-size:12px"><?= $esc($cycle) ?></td>
        <td><span class="cb-pill <?= $esc($policyTone) ?>"><?= $esc($policy) ?></span></td>
        <td class="right mono price"><?= $esc(number_format($old, 2)) ?></td>
        <td class="right mono price"><?= $esc(number_format($new, 2)) ?></td>
        <td class="right mono">
          <?php $tone = $deltaPct > 0 ? 'var(--accent)' : ($deltaPct < 0 ? 'var(--good)' : 'var(--muted)'); ?>
          <span style="color: <?= $tone ?>">
            <?= ($deltaPct >= 0 ? '+' : '') . number_format($deltaPct, 1) ?>%
          </span>
        </td>
        <td>
          <?php if ($applied): ?>
            <span class="cb-pill good"><span class="dot"></span>applied</span>
          <?php elseif ($awaitingApproval): ?>
            <span class="cb-pill warn"><span class="dot"></span>awaiting</span>
          <?php elseif ($statusBucket === 'notice'): ?>
            <span class="cb-pill warn"><span class="dot"></span>notice</span>
          <?php else: ?>
            <span class="cb-pill grey">skipped</span>
          <?php endif; ?>
        </td>
        <td><code class="mono" style="font-size:11.5px"><?= $esc($skipReason) ?></code></td>
        <td class="mono" style="font-size:12px"><?= $esc(substr($decidedAt, 0, 16)) ?></td>
      </tr>
      <?php endforeach; ?>
    </tbody>
  </table>
</div>

<p style="margin-top:14px; color:var(--muted); font-size:12.5px">
  Showing the most recent <?= (int) count($rows) ?> decisions.
  Use the <a href="<?= $esc($module_link) ?>&amp;action=price-decisions">audit log</a> for full history + CSV export.
</p>

<?php endif; ?>

</div>
