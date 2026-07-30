<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Manual sync result view.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<string,mixed>|object $summary
 */

// Normalise summary to an array regardless of object/array input.
$cb_s = is_object($summary) ? get_object_vars($summary) : (array) $summary;

$cb_status         = (string) ($cb_s['status'] ?? 'unknown');
$cb_started        = (string) ($cb_s['started_at'] ?? '');
$cb_finished       = (string) ($cb_s['finished_at'] ?? '');
$cb_snap           = (string) ($cb_s['snapshot_generated_at'] ?? '');
$cb_checked        = (int) ($cb_s['profiles_checked'] ?? 0);
$cb_changed        = (int) ($cb_s['profiles_changed'] ?? 0);
$cb_products       = (int) ($cb_s['products_updated'] ?? 0);
$cb_errors         = isset($cb_s['errors']) && is_array($cb_s['errors']) ? $cb_s['errors'] : array();
$cb_changes        = isset($cb_s['change_list']) && is_array($cb_s['change_list']) ? $cb_s['change_list'] : array();
$cb_error_count    = count($cb_errors);
$cb_change_count   = count($cb_changes);

// Status pill tone.
$cb_status_tone = 'grey';
if ($cb_status === 'succeeded')      { $cb_status_tone = 'good'; }
elseif ($cb_status === 'failed')     { $cb_status_tone = 'bad'; }
elseif ($cb_status === 'no-change')  { $cb_status_tone = 'grey'; }
elseif ($cb_status === 'running')    { $cb_status_tone = 'warn'; }

// Strip tone — paint the "changed" stat orange when non-zero, etc.
$cb_changed_tone  = $cb_changed > 0  ? 'warn' : '';
$cb_products_tone = $cb_products > 0 ? 'good' : '';

// Duration label.
$cb_dur = '';
if ($cb_started !== '' && $cb_finished !== '') {
    $ta = strtotime($cb_started); $tb = strtotime($cb_finished);
    if ($ta !== false && $tb !== false && $tb >= $ta) {
        $d = $tb - $ta;
        if ($d < 60)         { $cb_dur = $d . 's'; }
        elseif ($d < 3600)   { $cb_dur = floor($d / 60) . 'm ' . ($d % 60) . 's'; }
        else                 { $cb_dur = floor($d / 3600) . 'h ' . floor(($d % 3600) / 60) . 'm'; }
    }
}
?>

<!-- ───────────────────── Header card ───────────────────── -->
<div class="cb-card">
  <div data-cb-u="u-fc3f2cef5b">
    <div data-cb-u="u-a2e0c59bb0">
      <h2 class="cb-card-title display" data-cb-u="u-ab79ea2b85">Sync <?= $esc($cb_status) ?></h2>
      <p class="cb-card-sub" data-cb-u="u-af3c2c2206">
        Manual sync triggered from the admin dashboard.
      </p>
      <div data-cb-u="u-4b8f2faec5">
        <div><span class="glabel">Started</span> <span class="mono" data-cb-u="u-4b17347c23"><?= $esc($cb_started !== '' ? $cb_started : '—') ?></span></div>
        <div><span class="glabel">Finished</span> <span class="mono" data-cb-u="u-4b17347c23"><?= $esc($cb_finished !== '' ? $cb_finished : '—') ?></span></div>
        <?php if ($cb_dur !== ''): ?>
          <div><span class="glabel">Duration</span> <span class="mono" data-cb-u="u-4b17347c23"><?= $esc($cb_dur) ?></span></div>
        <?php endif; ?>
        <?php if ($cb_snap !== ''): ?>
          <div><span class="glabel">Snapshot</span> <span class="mono" data-cb-u="u-4b17347c23"><?= $esc($cb_snap) ?></span></div>
        <?php endif; ?>
      </div>
    </div>
    <div>
      <span class="cb-pill <?= $esc($cb_status_tone) ?>" data-cb-u="u-ddca59c930">
        <span class="dot"></span><?= $esc($cb_status) ?>
      </span>
    </div>
  </div>
</div>

<!-- ───────────────────── KPI strip ───────────────────── -->
<div class="cb-strip">
  <div class="cb-stat">
    <div class="lbl">Profiles checked</div>
    <div class="v"><?= (int) $cb_checked ?></div>
    <div class="sub">considered this run</div>
  </div>
  <div class="cb-stat<?= $cb_changed_tone !== '' ? ' ' . $esc($cb_changed_tone) : '' ?>">
    <div class="lbl">Profiles changed</div>
    <div class="v"><?= (int) $cb_changed ?></div>
    <div class="sub"><?= $cb_changed === 0 ? 'no price movement' : 'new version(s) created' ?></div>
  </div>
  <div class="cb-stat<?= $cb_products_tone !== '' ? ' ' . $esc($cb_products_tone) : '' ?>">
    <div class="lbl">Products updated</div>
    <div class="v"><?= (int) $cb_products ?></div>
    <div class="sub">WHMCS pricing rows touched</div>
  </div>
</div>

<!-- ───────────────────── Errors (if any) ───────────────────── -->
<?php if ($cb_error_count > 0): ?>
<div class="cb-card">
  <h3 data-cb-u="u-5bd8ae8682">Errors (<?= (int) $cb_error_count ?>)</h3>
  <div class="cb-error">
    <ul data-cb-u="u-6cf362a80b">
      <?php foreach ($cb_errors as $cb_e): ?>
        <li><code class="mono" data-cb-u="u-a49cca52be"><?= $esc((string) $cb_e) ?></code></li>
      <?php endforeach; ?>
    </ul>
  </div>
</div>
<?php endif; ?>

<!-- ───────────────────── Changes table ───────────────────── -->
<?php if ($cb_change_count > 0): ?>
<div class="cb-card">
  <h3>Changes (<?= (int) $cb_change_count ?>)</h3>
  <table class="cb-table">
    <thead>
      <tr>
        <th>Profile</th>
        <th>Plan</th>
        <th class="right">Period</th>
        <th class="right">Previous</th>
        <th class="right">New</th>
        <th>Δ</th>
        <th>Currency</th>
      </tr>
    </thead>
    <tbody>
      <?php foreach ($cb_changes as $cb_c): ?>
        <?php
          $cb_slug  = (string) ($cb_c['profile_slug'] ?? '');
          $cb_plan  = (string) ($cb_c['plan_slug'] ?? '');
          $cb_per   = (int) ($cb_c['period_months'] ?? 0);
          $cb_prev  = isset($cb_c['previous_final']) && $cb_c['previous_final'] !== null
                        ? (float) $cb_c['previous_final'] : null;
          $cb_new   = (float) ($cb_c['new_final'] ?? 0.0);
          $cb_cur   = (string) ($cb_c['currency'] ?? '');
          $cb_d     = $cb_prev !== null ? ($cb_new - $cb_prev) : null;
        ?>
        <tr>
          <td><span class="mono"><?= $esc($cb_slug) ?></span></td>
          <td><?= $esc($cb_plan) ?></td>
          <td class="right"><?= (int) $cb_per ?> mo</td>
          <td class="right mono"><?= $cb_prev !== null ? $esc(number_format($cb_prev, 2)) : '—' ?></td>
          <td class="right"><span class="price mono"><?= $esc(number_format($cb_new, 2)) ?></span></td>
          <td>
            <?php if ($cb_d === null): ?>
              <span class="cb-pill grey">new</span>
            <?php elseif (abs($cb_d) < 0.005): ?>
              <span class="cb-pill grey">no change</span>
            <?php elseif ($cb_d > 0): ?>
              <span class="cb-pill bad" title="Price increased">
                <span aria-hidden="true">▲</span>
                <span class="mono">+<?= $esc(number_format($cb_d, 2)) ?></span>
              </span>
            <?php else: ?>
              <span class="cb-pill good" title="Price decreased">
                <span aria-hidden="true">▼</span>
                <span class="mono"><?= $esc(number_format($cb_d, 2)) ?></span>
              </span>
            <?php endif; ?>
          </td>
          <td><span class="cb-pill grey"><?= $esc($cb_cur) ?></span></td>
        </tr>
      <?php endforeach; ?>
    </tbody>
  </table>
</div>
<?php endif; ?>

<?php if ($cb_change_count === 0 && $cb_error_count === 0): ?>
<div class="cb-card">
  <div class="cb-empty">
    <?php if ($cb_checked === 0): ?>
      <div class="display">No profiles to sync yet</div>
      <p>You haven't created any profiles. A <em>profile</em> is a named template
        that pairs a Contabo plan with a billing period (e.g. "Cloud VPS 10 — EU — Ubuntu 24 — 12 mo").
        Sync runs walk your profiles, fetch the latest pricing from the API,
        record a new version when prices change, and (per profile setting)
        update the linked WHMCS product prices.</p>
      <p data-cb-u="u-45c58e1470">
        <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=profiles">Create your first profile</a>
        <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=mappings">Or map a WHMCS product</a>
      </p>
    <?php else: ?>
      <div class="display">Nothing to report</div>
      <p>No price changes detected and no errors raised. Catalog is in sync with Contabo.</p>
    <?php endif; ?>
  </div>
</div>
<?php endif; ?>

<!-- ───────────────────── Footer actions ───────────────────── -->
<p data-cb-u="u-f8849d34e1">
  <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=sync-history">View full history</a>
  <a class="cb-btn ghost" href="<?= $esc($module_link) ?>">← Back to dashboard</a>
</p>

</div>
