<?php
/**
 * Per-service "Contabo Pricing" tab body embedded in the WHMCS admin service
 * profile via the `AdminClientServicesTabFields` hook.
 *
 * Phase A is read-only: no controls. Shows
 *   - active policy badge + locked values + manual override state,
 *   - last 5 decisions (link out to the audit log for full history),
 *   - current service amount (tblhosting.`amount`) vs the most recently APPLIED
 *     decision's `proposed_new_price` so admins can spot manual edits ("manual_edit_detected").
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array    $service          tblhosting row
 * @var array|null $policy_row     mod_contabo_service_policy row (or null)
 * @var list<array<string,mixed>> $decisions
 * @var array|null $latest_applied
 */

$cb_policy  = $policy_row['policy'] ?? '';
// tblhosting's real recurring-charge column is `amount` (NOT `recurringamount`,
// which is an API/model field, not a raw column). `$service` here is a raw
// tblhosting row; recurringamount kept only as a defensive back-compat fallback.
$cb_current = (float) ($service['amount'] ?? $service['recurringamount'] ?? 0);
$cb_appliedPrice = $latest_applied ? (float) ($latest_applied['proposed_new_price'] ?? 0) : null;
$cb_drift = ($cb_appliedPrice !== null && $cb_appliedPrice > 0)
    ? abs($cb_current - $cb_appliedPrice) >= 0.005 : false;

// Policy badge tone map.
$cb_policyTones = [
    'manual'          => 'grey',
    'lifetime'        => 'good',
    'frozen_until'    => '',
    'current_term'    => 'warn',
    'margin_floor'    => '',
    'reprice_renewal' => 'warn',
];
$cb_tone = $cb_policyTones[$cb_policy] ?? 'grey';

// Manual override surface.
$cb_overridePrice = isset($policy_row['manual_override_price']) ? (float) $policy_row['manual_override_price'] : null;
$cb_overrideExpires = isset($policy_row['manual_override_expires_at']) ? (string) $policy_row['manual_override_expires_at'] : '';
?>

<div class="cb-wrap" style="padding:8px 0;">
  <style>
    .cb-svc-tab .cb-row { display:flex; justify-content:space-between; align-items:baseline; padding:6px 0; border-bottom:1px solid var(--border-soft); }
    .cb-svc-tab .cb-row:last-child { border-bottom:0; }
    .cb-svc-tab .lbl { color: var(--muted); font-size: 12px; }
    .cb-svc-tab .val { font-family: "IBM Plex Mono", monospace; font-variant-numeric: tabular-nums; }
  </style>

  <div class="cb-svc-tab" style="display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px;">

    <div class="cb-card" style="margin:0;">
      <h3>Policy</h3>
      <?php if ($cb_policy === ''): ?>
        <div class="cb-empty" style="padding:14px 0;">
          <div>No service policy row yet.</div>
          <p style="margin:6px 0 0; font-size:12px; color:var(--muted)">
            The Phase A backfill will create a <code class="mono">lifetime</code> policy on first cron pass.
          </p>
        </div>
      <?php else: ?>
        <div class="cb-row">
          <span class="lbl">Active policy</span>
          <span class="cb-pill <?= $esc($cb_tone) ?>"><?= $esc((string) $cb_policy) ?></span>
        </div>
        <?php if (!empty($policy_row['locked_price'])): ?>
        <div class="cb-row">
          <span class="lbl">Locked price</span>
          <span class="val price"><?= $esc(number_format((float) $policy_row['locked_price'], 2)) ?></span>
        </div>
        <?php endif; ?>
        <?php if ($cb_overridePrice !== null): ?>
        <div class="cb-row">
          <span class="lbl">Manual override</span>
          <span class="val price"><?= $esc(number_format($cb_overridePrice, 2)) ?></span>
        </div>
        <div class="cb-row">
          <span class="lbl">Override expires</span>
          <span class="val"><?= $esc($cb_overrideExpires !== '' ? substr($cb_overrideExpires, 0, 16) : 'no expiry') ?></span>
        </div>
        <?php endif; ?>
        <?php if (!empty($policy_row['frozen_until'])): ?>
        <div class="cb-row">
          <span class="lbl">Frozen until</span>
          <span class="val"><?= $esc((string) $policy_row['frozen_until']) ?></span>
        </div>
        <?php endif; ?>
        <?php if (!empty($policy_row['margin_floor_pct'])): ?>
        <div class="cb-row">
          <span class="lbl">Margin floor</span>
          <span class="val"><?= $esc(number_format((float) $policy_row['margin_floor_pct'], 2) . '%') ?></span>
        </div>
        <?php endif; ?>
      <?php endif; ?>
    </div>

    <div class="cb-card" style="margin:0;">
      <h3>Pricing</h3>
      <div class="cb-row">
        <span class="lbl">Current recurring</span>
        <span class="val price"><?= $esc(number_format($cb_current, 2)) ?></span>
      </div>
      <?php if ($cb_appliedPrice !== null): ?>
        <div class="cb-row">
          <span class="lbl">Last applied (engine)</span>
          <span class="val price"><?= $esc(number_format($cb_appliedPrice, 2)) ?></span>
        </div>
        <?php if ($cb_drift): ?>
          <div class="cb-row">
            <span class="lbl">Drift detected</span>
            <span class="cb-pill warn"><span class="dot"></span>manual edit?</span>
          </div>
        <?php endif; ?>
      <?php endif; ?>
      <?php if (!empty($service['billingcycle'])): ?>
        <div class="cb-row">
          <span class="lbl">Cycle</span>
          <span class="val"><?= $esc((string) $service['billingcycle']) ?></span>
        </div>
      <?php endif; ?>
      <?php if (!empty($service['nextduedate'])): ?>
        <div class="cb-row">
          <span class="lbl">Next renewal</span>
          <span class="val"><?= $esc((string) $service['nextduedate']) ?></span>
        </div>
      <?php endif; ?>
    </div>

  </div>

  <div class="cb-card" style="margin-top:14px;">
    <h3>Recent decisions</h3>
    <?php if (empty($decisions)): ?>
      <div class="cb-empty" style="padding:14px 0;">
        <div>No decisions recorded for this service yet.</div>
      </div>
    <?php else: ?>
      <div style="overflow:auto">
        <table class="cb-table" style="width:100%">
          <thead>
            <tr>
              <th>Decided</th>
              <th>Policy</th>
              <th class="right">Old</th>
              <th class="right">Proposed</th>
              <th>Applied</th>
              <th>Skip reason</th>
            </tr>
          </thead>
          <tbody>
            <?php foreach ($decisions as $r):
              $decidedAt = (string) ($r['decided_at'] ?? '');
              $polUsed   = (string) ($r['policy_used'] ?? '');
              $old       = (float) ($r['old_price'] ?? 0);
              $new       = (float) ($r['proposed_new_price'] ?? 0);
              $applied   = !empty($r['applied']);
              $skipRsn   = (string) ($r['skip_reason'] ?? '');
            ?>
            <tr>
              <td class="mono" style="font-size:12px"><?= $esc(substr($decidedAt, 0, 16)) ?></td>
              <td><span class="cb-pill grey"><?= $esc($polUsed) ?></span></td>
              <td class="right mono price"><?= $esc(number_format($old, 2)) ?></td>
              <td class="right mono price"><?= $esc(number_format($new, 2)) ?></td>
              <td><?= $applied ? '<span class="cb-pill good">yes</span>' : '<span class="cb-pill grey">no</span>' ?></td>
              <td><code class="mono" style="font-size:11.5px"><?= $esc($skipRsn) ?></code></td>
            </tr>
            <?php endforeach; ?>
          </tbody>
        </table>
      </div>
    <?php endif; ?>
  </div>

  <p style="margin-top:12px; font-size:12px; color:var(--muted)">
    Read-only in Phase A. Per-service policy editing ships in Phase B.
    <a href="<?= $esc($module_link) ?>&amp;action=price-decisions">Open audit log &rarr;</a>
  </p>
</div>
