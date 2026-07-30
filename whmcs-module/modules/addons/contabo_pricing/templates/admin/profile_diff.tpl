<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Profile version history timeline.
 *
 * @var \Closure                   $esc
 * @var string                     $module_link
 * @var array<string,mixed>        $profile
 * @var array<int,array<string,mixed>> $versions   Newest-first.
 */

$cb_profile_id   = (int) ($profile['id'] ?? 0);
$cb_profile_name = (string) ($profile['name'] ?? '');
$cb_profile_slug = (string) ($profile['slug'] ?? '');
$cb_plan_slug    = (string) ($profile['plan_slug'] ?? '');
$cb_period       = (int) ($profile['period_months'] ?? 0);
$cb_region       = (string) ($profile['region'] ?? '');
$cb_os           = (string) ($profile['os'] ?? '');
$cb_sync_strat   = (string) ($profile['sync_strategy'] ?? '');
$cb_active       = !empty($profile['active']);
$cb_latest_vid   = (int) ($profile['latest_version_id'] ?? 0);

$cb_version_count = is_array($versions) ? count($versions) : 0;

// Inline first/last/min/max finals for the sparkline hero labels.
$cb_finals = array();
if ($cb_version_count > 0) {
    foreach ($versions as $cb_v) {
        $cb_finals[] = (float) ($cb_v['final_monthly'] ?? 0.0);
    }
}
$cb_first_final = $cb_version_count > 0 ? $cb_finals[$cb_version_count - 1] : null; // oldest
$cb_last_final  = $cb_version_count > 0 ? $cb_finals[0] : null;                     // newest
$cb_min_final   = $cb_version_count > 0 ? min($cb_finals) : null;
$cb_max_final   = $cb_version_count > 0 ? max($cb_finals) : null;
$cb_currency    = $cb_version_count > 0 ? (string) ($versions[0]['currency_iso'] ?? '') : '';

/**
 * Best-effort relative time. Falls back to raw timestamp on failure.
 */
$cb_rel_time = static function ($ts) {
    if ($ts === null || $ts === '') { return '—'; }
    $t = strtotime((string) $ts);
    if ($t === false) { return (string) $ts; }
    $diff = time() - $t;
    if ($diff < 0)        { return date('Y-m-d H:i', $t); }
    if ($diff < 60)       { return $diff . 's ago'; }
    if ($diff < 3600)     { return (int) ($diff / 60) . 'm ago'; }
    if ($diff < 86400)    { return (int) ($diff / 3600) . 'h ago'; }
    if ($diff < 86400*30) { return (int) ($diff / 86400) . 'd ago'; }
    return date('Y-m-d', $t);
};

// Pre-compute Δ vs prior so JS doesn't need to duplicate the arithmetic.
// $versions are newest-first; "prior" for row i is the row at i+1.
$cb_deltas = array();
for ($cb_i = 0; $cb_i < $cb_version_count; $cb_i++) {
    if ($cb_i + 1 >= $cb_version_count) {
        $cb_deltas[$cb_i] = null;
        continue;
    }
    $cb_now_v   = (float) ($versions[$cb_i]['final_monthly'] ?? 0.0);
    $cb_prior_v = (float) ($versions[$cb_i + 1]['final_monthly'] ?? 0.0);
    $cb_deltas[$cb_i] = $cb_now_v - $cb_prior_v;
}
?>

<!-- ───────────────────── Header card ───────────────────── -->
<div class="cb-card">
  <h2 class="cb-card-title display" data-cb-u="u-ab79ea2b85"><?= $esc($cb_profile_name) ?></h2>
  <p class="cb-card-sub" data-cb-u="u-f1319bea90">
    <span class="cb-pill grey"><span class="mono"><?= $esc($cb_profile_slug) ?></span></span>
    <span class="cb-pill"><?= $esc($cb_plan_slug) ?></span>
    <span class="cb-pill"><?= (int) $cb_period ?> mo</span>
    <?php if ($cb_region !== ''): ?><span class="cb-pill"><?= $esc($cb_region) ?></span><?php endif; ?>
    <?php if ($cb_os !== ''): ?><span class="cb-pill"><?= $esc($cb_os) ?></span><?php endif; ?>
    <?php if ($cb_sync_strat !== ''): ?><span class="cb-pill"><span class="glabel" data-cb-u="u-1ac2df864b">sync</span><?= $esc($cb_sync_strat) ?></span><?php endif; ?>
    <?php if ($cb_active): ?><span class="cb-pill good"><span class="dot"></span>active</span><?php else: ?><span class="cb-pill grey">inactive</span><?php endif; ?>
  </p>
  <div class="muted" data-cb-u="u-e1612f0331">
    <?= (int) $cb_version_count ?> version<?= $cb_version_count === 1 ? '' : 's' ?> tracked
    <?php if ($cb_latest_vid > 0): ?>· latest version row id <span class="mono">#<?= (int) $cb_latest_vid ?></span><?php endif; ?>
  </div>
</div>

<!-- ───────────────────── Sparkline hero ───────────────────── -->
<?php if ($cb_version_count > 1): ?>
<div class="cb-card">
  <h3>Price trajectory</h3>
  <div data-cb-u="u-5839a855bc">
    <svg class="sparkline-large"
         data-cb-sparkline-large
         data-cb-profile-id="<?= (int) $cb_profile_id ?>"
         width="520" height="120"
         viewBox="0 0 520 120"
         preserveAspectRatio="none"
         aria-hidden="true"
         data-cb-u="u-082a58d32f"></svg>

    <div data-cb-u="u-8da823607c">
      <div>
        <div class="glabel" data-cb-u="u-6c850f109c">First</div>
        <div class="mono" data-cb-u="u-a3970b9f8a"><?= $cb_first_final !== null ? $esc(number_format($cb_first_final, 2)) : '—' ?></div>
      </div>
      <div>
        <div class="glabel" data-cb-u="u-6c850f109c">Latest</div>
        <div class="mono" data-cb-u="u-d55ffe22fc"><?= $cb_last_final !== null ? $esc(number_format($cb_last_final, 2)) : '—' ?></div>
      </div>
      <div>
        <div class="glabel" data-cb-u="u-6c850f109c">Min</div>
        <div class="mono" data-cb-u="u-c7d8bb823a"><?= $cb_min_final !== null ? $esc(number_format($cb_min_final, 2)) : '—' ?></div>
      </div>
      <div>
        <div class="glabel" data-cb-u="u-6c850f109c">Max</div>
        <div class="mono" data-cb-u="u-735cc40047"><?= $cb_max_final !== null ? $esc(number_format($cb_max_final, 2)) : '—' ?></div>
      </div>
      <div data-cb-u="u-53239731c4">
        <?= $esc($cb_currency) ?> / month
      </div>
    </div>
  </div>
</div>
<?php endif; ?>

<!-- ───────────────────── Empty state ───────────────────── -->
<?php if ($cb_version_count === 0): ?>
<div class="cb-card">
  <div class="cb-empty">
    <div class="display">No versions yet</div>
    <p>The first sync that detects pricing for this profile will create Version 1.</p>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">← Back to profiles</a>
  </div>
</div>
<?php else: ?>

<!-- ───────────────────── Versions table ───────────────────── -->
<div class="cb-card">
  <h3>Version timeline</h3>
  <table class="cb-table">
    <thead>
      <tr>
        <th>Version</th>
        <th>Generated</th>
        <th class="right">Base €/mo</th>
        <th class="right">Configured €/mo</th>
        <th class="right">FX</th>
        <th class="right">GST</th>
        <th class="right">Final / mo (<?= $esc($cb_currency) ?>)</th>
        <th>Δ vs prior</th>
        <th class="right">Actions</th>
      </tr>
    </thead>
    <tbody>
      <?php foreach ($versions as $cb_idx => $cb_v): ?>
        <?php
        $cb_v_num   = (int) ($cb_v['version'] ?? 0);
        $cb_v_id    = (int) ($cb_v['id'] ?? 0);
        $cb_v_gen   = (string) ($cb_v['snapshot_generated_at'] ?? ($cb_v['created_at'] ?? ''));
        $cb_v_base  = (float) ($cb_v['base_monthly_eur'] ?? 0.0);
        $cb_v_conf  = (float) ($cb_v['configured_monthly_eur'] ?? 0.0);
        $cb_v_fx    = isset($cb_v['fx_rate']) && $cb_v['fx_rate'] !== null ? (float) $cb_v['fx_rate'] : null;
        $cb_v_gst   = (float) ($cb_v['gst_pct'] ?? 0.0);
        $cb_v_final = (float) ($cb_v['final_monthly'] ?? 0.0);
        $cb_v_cur   = (string) ($cb_v['currency_iso'] ?? $cb_currency);
        $cb_delta   = $cb_deltas[$cb_idx];
        $cb_is_latest = ($cb_idx === 0);
        ?>
        <tr data-cb-version-row data-cb-version-id="<?= (int) $cb_v_id ?>" data-cb-version="<?= (int) $cb_v_num ?>">
          <td>
            <span class="mono" data-cb-u="u-aea0af9e9a">v<?= (int) $cb_v_num ?></span>
            <?php if ($cb_is_latest): ?><span class="cb-pill good" data-cb-u="u-4b17347c23"><span class="dot"></span>latest</span><?php endif; ?>
          </td>
          <td>
            <div><?= $esc($cb_rel_time($cb_v_gen)) ?></div>
            <div class="mono" data-cb-u="u-9f803cd406"><?= $esc(substr($cb_v_gen, 0, 16)) ?></div>
          </td>
          <td class="right mono"><?= $esc(number_format($cb_v_base, 2)) ?></td>
          <td class="right mono"><?= $esc(number_format($cb_v_conf, 2)) ?></td>
          <td class="right mono"><?= $cb_v_fx !== null ? $esc(number_format($cb_v_fx, 4)) : '<span class="muted" data-cb-u="u-eac7694072">—</span>' ?></td>
          <td class="right"><?= $esc(number_format($cb_v_gst, 1)) ?>%</td>
          <td class="right"><span class="price mono"><?= $esc(number_format($cb_v_final, 2)) ?></span></td>
          <td>
            <?php if ($cb_delta === null): ?>
              <span class="cb-pill grey">—</span>
            <?php elseif (abs($cb_delta) < 0.005): ?>
              <span class="cb-pill grey">no change</span>
            <?php elseif ($cb_delta > 0): ?>
              <span class="cb-pill bad" title="Up vs prior version">
                <span aria-hidden="true">▲</span>
                <span class="mono">+<?= $esc(number_format($cb_delta, 2)) ?></span>
              </span>
            <?php else: ?>
              <span class="cb-pill good" title="Down vs prior version">
                <span aria-hidden="true">▼</span>
                <span class="mono"><?= $esc(number_format($cb_delta, 2)) ?></span>
              </span>
            <?php endif; ?>
          </td>
          <td class="right">
            <?php if ($cb_is_latest): ?>
              <span class="cb-pill grey">current</span>
            <?php else: ?>
              <!-- TODO: profile-restore-version handler not yet wired by Agent D -->
              <form method="post"
                    action="<?= $esc($module_link) ?>"
                    data-cb-u="u-6b8b63d565"
                    data-cb-form="profile-restore-version"
                    data-cb-disabled-reason="coming-soon">
                <input type="hidden" name="action" value="profile-restore-version">
                <input type="hidden" name="profile_id" value="<?= (int) $cb_profile_id ?>">
                <input type="hidden" name="version_id" value="<?= (int) $cb_v_id ?>">
                <?= generate_token() ?>
                <button type="submit"
                        class="cb-btn subtle disabled"
                        disabled
                        title="Restore this version — coming soon"
                        data-cb-action="profile-restore-version">
                  Restore
                </button>
              </form>
            <?php endif; ?>
          </td>
        </tr>
      <?php endforeach; ?>
    </tbody>
  </table>
</div>
<?php endif; ?>

<p data-cb-u="u-92c18f6bda">
  <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">← Back to profiles</a>
</p>

</div>
