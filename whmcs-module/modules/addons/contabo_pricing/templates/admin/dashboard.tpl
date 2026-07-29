<?php
/**
 * @var \Closure $esc
 * @var string   $module_link
 * @var array    $meta
 * @var array|null $last_sync
 * @var int      $profile_count
 * @var int      $version_count
 * @var int      $mapping_count
 * @var \ContaboPricing\Settings $settings
 * @var string|null $connect_error
 */

// Derived display values used by the hero + strip.
$cb_api_base   = isset($settings) && is_object($settings) && !empty($settings->apiBaseUrl)
    ? (string) $settings->apiBaseUrl
    : '';
$cb_scraper_v  = isset($meta['scraper_version']) ? (string) $meta['scraper_version'] : '';
$cb_plan_count = isset($meta['snapshot_meta']['plan_count']) ? (int) $meta['snapshot_meta']['plan_count'] : 0;
$cb_generated  = isset($meta['snapshot_meta']['generated_at']) ? (string) $meta['snapshot_meta']['generated_at'] : '';
$cb_has_error  = !empty($connect_error);

// Health tone for hero pill.
if ($cb_has_error) {
    $cb_health_tone  = 'bad';
    $cb_health_label = 'API unreachable';
} elseif ($cb_plan_count > 0) {
    $cb_health_tone  = 'good';
    $cb_health_label = 'API healthy';
} else {
    $cb_health_tone  = 'warn';
    $cb_health_label = 'API reachable · empty snapshot';
}

// Last sync derived display.
$cb_last_status   = isset($last_sync['status']) ? (string) $last_sync['status'] : '';
$cb_last_started  = isset($last_sync['started_at']) ? (string) $last_sync['started_at'] : '';
$cb_last_relative = $cb_last_started !== '' ? substr($cb_last_started, 0, 16) : '—';

// Status strip — 4 KPIs shown above every page (driven by _layout_open.tpl).
$cb_strip_data = array(
    array(
        'lbl'  => 'API health',
        'v'    => $cb_has_error ? 'down' : 'ok',
        'sub'  => $cb_api_base !== '' ? $cb_api_base : 'no URL configured',
        'tone' => $cb_has_error ? 'bad' : 'good',
    ),
    array(
        'lbl'  => 'Plans in snapshot',
        'v'    => (string) $cb_plan_count,
        'sub'  => $cb_scraper_v !== '' ? ('scraper ' . $cb_scraper_v) : 'scraper —',
        'tone' => $cb_plan_count > 0 ? 'good' : 'warn',
    ),
    array(
        'lbl'  => 'Last sync',
        'v'    => $cb_last_relative,
        'sub'  => $cb_last_status !== '' ? $cb_last_status : 'no runs yet',
        'tone' => $cb_last_status === 'succeeded' ? 'good' : ($cb_last_status === 'failed' ? 'bad' : ''),
    ),
    array(
        'lbl'  => 'Active profiles',
        'v'    => (string) ((int) $profile_count),
        'sub'  => 'mappings: ' . ((int) $mapping_count),
        'tone' => ((int) $profile_count) > 0 ? '' : 'warn',
    ),
);

require __DIR__ . '/_layout_open.tpl';
?>

<?php /* Flash (e.g. from Run sync / Trigger API refresh redirects) -------- */ ?>
<?php if (!empty($flash)): ?>
  <?php $cb_flash_bad = (stripos((string) $flash, 'fail') !== false || stripos((string) $flash, 'error') !== false); ?>
  <div class="<?= $cb_flash_bad ? 'cb-error' : 'cb-flash' ?>"><?= $esc($flash) ?></div>
<?php endif; ?>

<?php /* Hero ----------------------------------------------------------- */ ?>
<div class="cb-card" data-cb-u="u-48ca9c4f12">
  <div data-cb-u="u-0dfd52ecd9">
    <div data-cb-u="u-5f62ac8378">
      <div class="cb-card-sub" data-cb-u="u-0cbe035c55">Contabo Pricing</div>
      <h2 class="cb-card-title" data-cb-u="u-f83807b020">
        <?php if ($cb_api_base !== ''): ?>
          Connected to <span class="mono" data-cb-u="u-169bb84a59"><?= $esc($cb_api_base) ?></span>
        <?php else: ?>
          Contabo Pricing
        <?php endif; ?>
      </h2>
      <div class="cb-card-sub" data-cb-u="u-38965f9b18">
        <?php if ($cb_scraper_v !== ''): ?>
          Scraper <span class="mono"><?= $esc($cb_scraper_v) ?></span>
        <?php else: ?>
          Scraper version unknown
        <?php endif; ?>
        <?php if ($cb_generated !== ''): ?>
          · snapshot generated <span class="mono"><?= $esc(substr($cb_generated, 0, 16)) ?></span>
        <?php endif; ?>
      </div>
    </div>
    <div>
      <span class="cb-pill <?= $esc($cb_health_tone) ?>">
        <span class="dot"></span><?= $esc($cb_health_label) ?>
      </span>
    </div>
  </div>

  <?php if ($cb_has_error): ?>
    <div class="cb-error" data-cb-u="u-8d84299030">
      Can't reach the API: <code class="mono"><?= $esc((string) $connect_error) ?></code><br>
      Check the URL and bearer token under
      <a href="<?= $esc($module_link) ?>&amp;action=settings">Settings</a>.
    </div>
  <?php endif; ?>
</div>

<?php /* Get-started callout — shows only when no profiles exist yet -------- */ ?>
<?php if (((int) $profile_count) === 0 && !$cb_has_error): ?>
<div class="cb-card" data-cb-u="u-ad0f812542">
  <div data-cb-u="u-8bc29ba9f1">
    <div data-cb-u="u-5f62ac8378">
      <h3 data-cb-u="u-64970cd978">Get started</h3>
      <div class="cb-card-title" data-cb-u="u-7259bbafaa">You're connected, but nothing is being synced yet.</div>
      <p class="cb-card-sub" data-cb-u="u-2b0e8ce8c2">
        The sync engine walks <em>profiles</em> — named templates that pair a Contabo plan
        with a billing period, region, and OS. Until you create at least one profile,
        every sync run will report 0 profiles checked.
      </p>
      <ol data-cb-u="u-b44378b3c1">
        <li><strong>Create a profile</strong> (e.g. <em>Cloud VPS 10 — EU — Ubuntu — 12 mo</em>)</li>
        <li><strong>Map it</strong> to one or more WHMCS products + tick the billing cycles to update</li>
        <li><strong>Choose a sync strategy</strong> per profile: <code>manual</code> (record only), <code>notify</code> (email on drift), or <code>auto-apply</code> (push prices immediately)</li>
        <li><strong>Run sync</strong> — the daily cron will keep it fresh after the first manual run</li>
      </ol>
      <div data-cb-u="u-9e77998f13">
        <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=profiles">Create your first profile</a>
        <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=mappings">Map a WHMCS product</a>
      </div>
    </div>
  </div>
</div>
<?php endif; ?>

<?php /* Quick actions ---------------------------------------------------- */ ?>
<div class="cb-card">
  <h3>Quick actions</h3>
  <div data-cb-u="u-cc44f1c604">
    <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-3cb81f8dc9">
      <input type="hidden" name="action" value="sync-run">
      <?= generate_token() ?>
      <button class="cb-btn" type="submit" data-cb-action="sync-run">Run sync now</button>
    </form>
    <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-3cb81f8dc9">
      <input type="hidden" name="action" value="refresh-api">
      <?= generate_token() ?>
      <button class="cb-btn subtle" type="submit" data-cb-action="refresh-api">Trigger API refresh</button>
    </form>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">Manage profiles</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=mappings">Edit mappings</a>
    <span data-cb-u="u-6253876a64"></span>
    <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=settings">Settings</a>
  </div>
</div>

<?php /* Last sync ------------------------------------------------------- */ ?>
<div class="cb-card">
  <h3>Last sync</h3>
  <?php if (empty($last_sync)): ?>
    <div class="cb-empty">
      <div class="display">No sync runs recorded yet</div>
      <div>Run a sync from the actions above to populate this card.</div>
    </div>
  <?php else: ?>
    <?php
      $cb_pill_class = $cb_last_status === 'succeeded'
          ? 'good'
          : ($cb_last_status === 'failed' ? 'bad' : 'grey');
      $cb_finished = isset($last_sync['finished_at']) ? (string) $last_sync['finished_at'] : '';
      $cb_trigger  = isset($last_sync['trigger']) ? (string) $last_sync['trigger'] : '—';
      $cb_changed  = isset($last_sync['profiles_changed']) ? (int) $last_sync['profiles_changed'] : 0;
      $cb_updated  = isset($last_sync['products_updated']) ? (int) $last_sync['products_updated'] : 0;
      $cb_errmsg   = isset($last_sync['error_message']) ? (string) $last_sync['error_message'] : '';
    ?>
    <div data-cb-u="u-8dbbdf5477">
      <div>
        <div class="cb-card-sub" data-cb-u="u-ef38380253">Status</div>
        <span class="cb-pill <?= $esc($cb_pill_class) ?>">
          <span class="dot"></span><?= $esc($cb_last_status !== '' ? $cb_last_status : 'unknown') ?>
        </span>
      </div>
      <div>
        <div class="cb-card-sub" data-cb-u="u-ef38380253">Trigger</div>
        <div class="mono"><?= $esc($cb_trigger) ?></div>
      </div>
      <div>
        <div class="cb-card-sub" data-cb-u="u-ef38380253">Started</div>
        <div class="mono"><?= $esc($cb_last_started !== '' ? $cb_last_started : '—') ?></div>
      </div>
      <div>
        <div class="cb-card-sub" data-cb-u="u-ef38380253">Finished</div>
        <div class="mono"><?= $esc($cb_finished !== '' ? $cb_finished : '—') ?></div>
      </div>
      <div>
        <div class="cb-card-sub" data-cb-u="u-ef38380253">Profiles changed</div>
        <div data-cb-u="u-8ba0c206b7">
          <span class="mono" data-cb-u="u-60f42128d0"><?= (int) $cb_changed ?></span>
          <svg class="sparkline" data-cb-sparkline data-cb-source="profiles-changed" aria-hidden="true"></svg>
        </div>
      </div>
      <div>
        <div class="cb-card-sub" data-cb-u="u-ef38380253">Products updated</div>
        <span class="mono" data-cb-u="u-60f42128d0"><?= (int) $cb_updated ?></span>
      </div>
    </div>

    <?php if ($cb_errmsg !== ''): ?>
      <div class="cb-error" data-cb-u="u-8d84299030">
        <strong>Error:</strong> <code class="mono"><?= $esc($cb_errmsg) ?></code>
      </div>
    <?php endif; ?>
  <?php endif; ?>
</div>

<?php /* Three-card summary row ------------------------------------------ */ ?>
<div data-cb-u="u-7c4d3c876f">

  <div class="cb-card">
    <h3>Profiles</h3>
    <div class="cb-card-title"><span class="mono"><?= (int) $profile_count ?></span> active</div>
    <div class="cb-card-sub">
      <span class="mono"><?= (int) $version_count ?></span> stored version<?= ((int) $version_count) === 1 ? '' : 's' ?> across all profiles.
    </div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">Manage profiles &rarr;</a>
  </div>

  <div class="cb-card">
    <h3>Mappings</h3>
    <div class="cb-card-title"><span class="mono"><?= (int) $mapping_count ?></span> active</div>
    <div class="cb-card-sub">
      Profile-to-WHMCS product bindings driving renewal pricing.
    </div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=mappings">Edit mappings &rarr;</a>
  </div>

  <div class="cb-card">
    <h3>Recent activity</h3>
    <div class="cb-card-title">
      <?php if (!empty($last_sync)): ?>
        <span class="mono"><?= (int) ($cb_changed + $cb_updated) ?></span> change<?= (($cb_changed + $cb_updated) === 1) ? '' : 's' ?>
      <?php else: ?>
        <span class="mono">0</span> changes
      <?php endif; ?>
    </div>
    <div class="cb-card-sub">
      From the latest sync run<?= !empty($last_sync) && $cb_last_started !== '' ? ' on ' . $esc(substr($cb_last_started, 0, 10)) : '' ?>.
    </div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=sync-history">View sync history &rarr;</a>
  </div>

</div>

</div>
