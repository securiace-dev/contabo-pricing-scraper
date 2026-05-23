<?php
/**
 * Pre-apply live DIFF (0.5.1) — what config-apply would do to THIS product before
 * any write. Read-only render of ConfigurableOptionsSyncer::diff(). The Apply form
 * lives here, so the billing-impacting apply is always preceded by this diff.
 *
 * @var \Closure                 $esc
 * @var string                   $module_link
 * @var array<string,mixed>      $profile
 * @var int                      $profile_id
 * @var int                      $product_id
 * @var string                   $plan_slug
 * @var array<string,mixed>|null $diff
 * @var string|null              $error
 */

$cb_actionTone = ['create' => 'good', 'update' => 'warn', 'noop' => 'grey', 'drift_skip' => 'bad'];
$cb_d   = is_array($diff) ? $diff : null;
$cb_sum = $cb_d !== null && isset($cb_d['summary']) ? $cb_d['summary'] : ['create' => 0, 'update' => 0, 'noop' => 0, 'drift_skip' => 0];
$cb_rows = $cb_d !== null && isset($cb_d['rows']) && is_array($cb_d['rows']) ? $cb_d['rows'] : [];

$cb_strip_data = $cb_d !== null ? [
    ['lbl' => 'Create',     'v' => (string) (int) $cb_sum['create'],     'tone' => ((int) $cb_sum['create'] > 0 ? 'good' : ''), 'sub' => 'new options'],
    ['lbl' => 'Update',     'v' => (string) (int) $cb_sum['update'],     'tone' => ((int) $cb_sum['update'] > 0 ? 'warn' : ''), 'sub' => 'existing → changed'],
    ['lbl' => 'No change',  'v' => (string) (int) $cb_sum['noop'],       'sub' => 'already match'],
    ['lbl' => 'Drift-skip', 'v' => (string) (int) $cb_sum['drift_skip'], 'tone' => ((int) $cb_sum['drift_skip'] > 0 ? 'bad' : ''), 'sub' => 'admin-edited; preserved'],
] : [];

require __DIR__ . '/_layout_open.tpl';
?>

<header style="display:flex; align-items:flex-end; justify-content:space-between; gap:16px; margin:4px 0 14px;">
  <div>
    <h2 class="cb-card-title">Apply preview — diff against product #<?= $esc((string) (int) $product_id) ?></h2>
    <p class="cb-card-sub" style="margin:0;">Plan <code class="mono"><?= $esc($plan_slug) ?></code>. This is what <strong>Apply</strong> would change on the live product. Nothing has been written.</p>
  </div>
  <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $esc((string) (int) $profile_id) ?>">&larr; Preview</a>
</header>

<?php if ($error !== null): ?>
  <div class="cb-error"><?= $esc($error) ?></div>
<?php elseif ($cb_d === null): ?>
  <div class="cb-error">No diff data.</div>
<?php else: ?>

  <?php if ((int) $cb_sum['drift_skip'] > 0): ?>
    <div class="cb-error" style="margin-top:0;">
      <strong><?= $esc((string) (int) $cb_sum['drift_skip']) ?> option(s) were hand-edited on the live product since the last apply.</strong>
      Apply will <strong>skip</strong> them — your manual edits are preserved, not overwritten.
    </div>
  <?php endif; ?>

  <div class="cb-card">
    <h3>Per-dimension changes</h3>
    <table class="cb-table">
      <thead><tr><th>Action</th><th>Dimension</th><th class="right">Type</th><th class="right">Values</th><th>WHMCS option</th><th>Detail</th></tr></thead>
      <tbody>
        <?php foreach ($cb_rows as $r): ?>
          <?php $act = (string) ($r['action'] ?? ''); $tone = $cb_actionTone[$act] ?? 'grey'; ?>
          <tr>
            <td><span class="cb-pill <?= $esc($tone) ?>"><span class="dot"></span> <?= $esc($act) ?></span></td>
            <td><?= $esc((string) ($r['dimension_key'] ?? '')) ?><?php if (!empty($r['will_be_hidden'])): ?> <span class="cb-pill grey" style="font-size:10.5px;">hidden</span><?php endif; ?></td>
            <td class="right mono"><?= $esc((string) (int) ($r['optiontype'] ?? 0)) ?></td>
            <td class="right mono"><?= $esc((string) (int) ($r['values'] ?? 0)) ?></td>
            <td class="mono"><?= ((int) ($r['whmcs_option_id'] ?? 0) > 0) ? '#' . $esc((string) (int) $r['whmcs_option_id']) : '<span class="muted">—</span>' ?></td>
            <td style="font-size:12.5px; color:var(--muted);"><?= $esc((string) ($r['detail'] ?? '')) ?></td>
          </tr>
        <?php endforeach; ?>
        <?php if ($cb_rows === []): ?>
          <tr><td colspan="6"><div class="cb-empty"><div class="display">No dimensions to apply.</div></div></td></tr>
        <?php endif; ?>
      </tbody>
    </table>
  </div>

  <?php $cb_writes = (int) $cb_sum['create'] + (int) $cb_sum['update']; ?>
  <div class="cb-card" style="border-color: rgba(180,83,9,.35);">
    <h3>Apply</h3>
    <p class="cb-card-sub" style="margin-top:-4px;">
      <?php if ($cb_writes > 0): ?>
        Apply will write <strong><?= $esc((string) $cb_writes) ?></strong> option change(s) to product #<?= $esc((string) (int) $product_id) ?>
        (<?= $esc((string) (int) $cb_sum['create']) ?> create, <?= $esc((string) (int) $cb_sum['update']) ?> update),
        skip <?= $esc((string) (int) $cb_sum['drift_skip']) ?> drifted, leave <?= $esc((string) (int) $cb_sum['noop']) ?> unchanged.
      <?php else: ?>
        Apply will make <strong>no option changes</strong> (<?= $esc((string) (int) $cb_sum['noop']) ?> already match,
        <?= $esc((string) (int) $cb_sum['drift_skip']) ?> drift-skipped). Sub-option/pricing rows are still re-asserted.
      <?php endif; ?>
    </p>
    <form method="post" action="<?= $esc($module_link) ?>" style="margin:0;"
          onsubmit="return confirm('Apply these configurable options to product #<?= $esc((string) (int) $product_id) ?>?');">
      <input type="hidden" name="action" value="config-apply">
      <input type="hidden" name="id" value="<?= $esc((string) (int) $profile_id) ?>">
      <input type="hidden" name="product_id" value="<?= $esc((string) (int) $product_id) ?>">
      <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
      <label style="display:flex; align-items:center; gap:8px; margin:10px 0; font-size:13.5px; cursor:pointer;">
        <input type="checkbox" name="confirm" value="1" required>
        <span>I’ve reviewed the diff above and want to apply these changes.</span>
      </label>
      <button type="submit" class="cb-btn">Apply these changes</button>
    </form>
  </div>

<?php endif; ?>

</div><?php /* close .cb-wrap from _layout_open.tpl */ ?>
