<?php
/**
 * Phase C — Approval Queue (admin sign-off for price changes that breached a
 * threshold during a renewal cron pass).
 *
 * RenewalEngine emits two skip reasons that need a human decision before the
 * price write proceeds:
 *   - awaiting_admin_approval                          (soft threshold breach)
 *   - awaiting_force_approval_max_increase_exceeded    (hard ceiling breach)
 *
 * This page lists the pending decisions and offers Approve / Reject, each a
 * CSRF-protected POST. The controller (approvalApprove / approvalReject) does
 * the actual write through ServicePriceWriter and records an append-only
 * decision row — this template only renders and submits.
 *
 * @var \Closure                          $esc
 * @var string                            $module_link
 * @var array<int,array<string,mixed>>    $pending       pending decision rows
 * @var int                               $soft_count
 * @var int                               $force_count
 * @var string                            $phase          observe|opt_in|enforce
 * @var int                               $page
 * @var int                               $total_pages
 * @var string|null                       $flash
 */

$cb_pending = isset($pending) && is_array($pending) ? $pending : [];
$cb_soft    = (int) ($soft_count ?? 0);
$cb_force   = (int) ($force_count ?? 0);
$cb_phase   = (string) ($phase ?? 'observe');
$cb_page    = max(1, (int) ($page ?? 1));
$cb_pages   = max(1, (int) ($total_pages ?? 1));

// Oldest pending age, derived from the smallest decided_at across the page.
$cb_oldest_label = '—';
$cb_oldest_ts = null;
foreach ($cb_pending as $cb_row) {
    $cb_dt = strtotime((string) ($cb_row['decided_at'] ?? ''));
    if ($cb_dt !== false && ($cb_oldest_ts === null || $cb_dt < $cb_oldest_ts)) {
        $cb_oldest_ts = $cb_dt;
    }
}
if ($cb_oldest_ts !== null) {
    $cb_days = (int) floor((time() - $cb_oldest_ts) / 86400);
    $cb_oldest_label = $cb_days <= 0 ? 'today' : ($cb_days . 'd');
}

$cb_strip_data = [
    ['lbl' => 'Soft-threshold pending', 'v' => (string) $cb_soft, 'tone' => ($cb_soft > 0 ? 'warn' : 'good'), 'sub' => 'Above large-increase threshold'],
    ['lbl' => 'Force-approval pending', 'v' => (string) $cb_force, 'tone' => ($cb_force > 0 ? 'bad' : 'good'), 'sub' => 'Above hard ceiling'],
    ['lbl' => 'Oldest pending', 'v' => $cb_oldest_label, 'sub' => 'Since decision recorded'],
    ['lbl' => 'Total on this page', 'v' => (string) count($cb_pending), 'sub' => 'Page ' . $cb_page . ' of ' . $cb_pages],
];

require __DIR__ . '/_layout_open.tpl';
?>

<div style="display:flex; align-items:baseline; justify-content:space-between; gap:16px; margin: 4px 0 12px;">
  <div>
    <h2 class="cb-card-title">Approval queue</h2>
    <p class="cb-card-sub">Price changes that breached a threshold during a renewal pass and are awaiting your sign-off. Approving applies the new price through the single write path and records an append-only audit row.</p>
  </div>
  <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=repricing">← Repricing</a>
</div>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc((string) $flash) ?></div>
<?php endif; ?>

<?php if ($cb_phase === 'observe'): ?>
  <div class="cb-card" style="border-left:3px solid var(--warn);">
    <p style="margin:0;color:var(--warn);">
      <strong>Phase is “observe”.</strong> Approving records the decision but the price write is suppressed
      (the writer is disabled outside <code class="mono">opt_in</code> / <code class="mono">enforce</code>).
      Switch the repricing phase before approving if you want the change to take effect.
    </p>
  </div>
<?php endif; ?>

<?php if (empty($cb_pending)): ?>
  <div class="cb-card">
    <div class="cb-empty">
      <div class="display">No pending approvals</div>
      <div>All price changes are within the auto-apply thresholds.</div>
    </div>
  </div>
<?php else: ?>
  <div class="cb-card">
    <table class="cb-table">
      <thead>
        <tr>
          <th>Decision</th>
          <th>Service</th>
          <th>Client</th>
          <th>Domain</th>
          <th class="right">Old</th>
          <th class="right">Proposed</th>
          <th class="right">Change</th>
          <th>Reason</th>
          <th>Pending since</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        <?php foreach ($cb_pending as $cb_row): ?>
          <?php
            $cb_did     = (int) ($cb_row['id'] ?? 0);
            $cb_sid     = (int) ($cb_row['service_id'] ?? 0);
            $cb_cid     = (int) ($cb_row['client_id'] ?? 0);
            $cb_domain  = (string) ($cb_row['domain'] ?? '');
            $cb_old     = (float) ($cb_row['current_price'] ?? 0);
            $cb_new     = (float) ($cb_row['proposed_new_price'] ?? 0);
            $cb_pct     = (float) ($cb_row['proposed_change_pct'] ?? 0);
            $cb_reason  = (string) ($cb_row['skip_reason'] ?? '');
            $cb_when    = (string) ($cb_row['decided_at'] ?? '');
            $cb_is_force = ($cb_reason === 'awaiting_force_approval_max_increase_exceeded');
          ?>
          <tr>
            <td class="mono">#<?= $esc((string) $cb_did) ?></td>
            <td class="mono">#<?= $esc((string) $cb_sid) ?></td>
            <td class="mono">#<?= $esc((string) $cb_cid) ?></td>
            <td><?= $esc($cb_domain !== '' ? $cb_domain : '—') ?></td>
            <td class="right"><span class="price"><?= $esc(number_format($cb_old, 2)) ?></span></td>
            <td class="right"><span class="price"><?= $esc(number_format($cb_new, 2)) ?></span></td>
            <td class="right mono"><?= $esc(($cb_pct >= 0 ? '+' : '') . number_format($cb_pct, 1)) ?>%</td>
            <td>
              <?php if ($cb_is_force): ?>
                <span class="cb-pill bad"><span class="dot"></span> Hard ceiling breach</span>
              <?php else: ?>
                <span class="cb-pill warn"><span class="dot"></span> Soft threshold</span>
              <?php endif; ?>
            </td>
            <td class="mono"><?= $esc($cb_when) ?></td>
            <td>
              <div style="display:flex; gap:6px; flex-direction:column;">
                <form method="post" action="<?= $esc($module_link) ?>&amp;action=approval-approve" style="margin:0;">
                  <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
                  <input type="hidden" name="decision_id" value="<?= $esc((string) $cb_did) ?>">
                  <input type="hidden" name="page" value="<?= $esc((string) $cb_page) ?>">
                  <?php if ($cb_is_force): ?>
                    <p class="cb-card-sub" style="margin:0 0 4px; max-width:240px;">This exceeds your hard ceiling. Confirm you have reviewed the margin impact before approving.</p>
                  <?php endif; ?>
                  <button type="submit" class="cb-btn" style="background:var(--good);border-color:var(--good);"
                    onclick="return confirm('Approve decision #<?= $esc((string) $cb_did) ?> and apply the new price?');">Approve</button>
                </form>
                <form method="post" action="<?= $esc($module_link) ?>&amp;action=approval-reject" style="margin:0;">
                  <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
                  <input type="hidden" name="decision_id" value="<?= $esc((string) $cb_did) ?>">
                  <input type="hidden" name="page" value="<?= $esc((string) $cb_page) ?>">
                  <button type="submit" class="cb-btn danger"
                    onclick="return confirm('Reject decision #<?= $esc((string) $cb_did) ?>? No price change will be applied.');">Reject</button>
                </form>
              </div>
            </td>
          </tr>
        <?php endforeach; ?>
      </tbody>
    </table>
  </div>

  <?php if ($cb_pages > 1): ?>
    <div style="display:flex; align-items:center; justify-content:space-between; gap:8px; margin-top:10px;">
      <?php if ($cb_page > 1): ?>
        <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=approval-queue&amp;page=<?= $esc((string) ($cb_page - 1)) ?>">← Previous</a>
      <?php else: ?>
        <span></span>
      <?php endif; ?>
      <span class="muted mono">Page <?= $esc((string) $cb_page) ?> / <?= $esc((string) $cb_pages) ?></span>
      <?php if ($cb_page < $cb_pages): ?>
        <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=approval-queue&amp;page=<?= $esc((string) ($cb_page + 1)) ?>">Next →</a>
      <?php else: ?>
        <span></span>
      <?php endif; ?>
    </div>
  <?php endif; ?>
<?php endif; ?>

</div><?php /* close .cb-wrap from _layout_open.tpl */ ?>
