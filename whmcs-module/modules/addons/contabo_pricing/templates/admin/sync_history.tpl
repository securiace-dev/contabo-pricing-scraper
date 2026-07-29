<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Sync log history — filterable timeline.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<int,array<string,mixed>> $logs  Newest-first.
 */

$cb_log_count = is_array($logs) ? count($logs) : 0;

// Counts per status for filter pill badges.
$cb_counts = array('all' => $cb_log_count, 'succeeded' => 0, 'no-change' => 0, 'failed' => 0);
if ($cb_log_count > 0) {
    foreach ($logs as $cb_l) {
        $cb_st = (string) ($cb_l['status'] ?? '');
        if (isset($cb_counts[$cb_st])) { $cb_counts[$cb_st]++; }
    }
}

/** Truncate helper. */
$cb_trunc = static function ($s, $n = 80) {
    $s = (string) $s;
    if ($s === '') { return ''; }
    if (strlen($s) <= $n) { return $s; }
    return substr($s, 0, $n - 1) . '…';
};

/** Best-effort duration label. */
$cb_duration = static function ($a, $b) {
    if (!$a || !$b) { return ''; }
    $ta = strtotime((string) $a); $tb = strtotime((string) $b);
    if ($ta === false || $tb === false || $tb < $ta) { return ''; }
    $d = $tb - $ta;
    if ($d < 60)   { return $d . 's'; }
    if ($d < 3600) { return floor($d / 60) . 'm ' . ($d % 60) . 's'; }
    return floor($d / 3600) . 'h ' . floor(($d % 3600) / 60) . 'm';
};
?>

<!-- ───────────────────── Header ───────────────────── -->
<div class="cb-card">
  <h2 class="cb-card-title display" data-cb-u="u-ab79ea2b85">Sync history</h2>
  <p class="cb-card-sub" data-cb-u="u-5074b45d61">
    Recent automated and manual sync runs against the Contabo pricing API. Click a row for the full summary JSON.
  </p>
</div>

<?php if ($cb_log_count === 0): ?>

<div class="cb-card">
  <div class="cb-empty">
    <div class="display">No sync runs yet</div>
    <p>Trigger a manual sync from the dashboard, or wait for the cron to fire.</p>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>">← Back to dashboard</a>
  </div>
</div>

<?php else: ?>

<!-- ───────────────────── Toolbar ───────────────────── -->
<div class="cb-toolbar" role="region" aria-label="Sync history filters">
  <span class="glabel">Status</span>
  <div class="cb-filter-pills" data-cb-filter-group="sync-history-status">
    <button type="button" data-cb-filter="all"       aria-pressed="true">All <span class="mono" data-cb-u="u-8dbc1c006c"><?= (int) $cb_counts['all'] ?></span></button>
    <button type="button" data-cb-filter="succeeded" aria-pressed="false">Succeeded <span class="mono" data-cb-u="u-8dbc1c006c"><?= (int) $cb_counts['succeeded'] ?></span></button>
    <button type="button" data-cb-filter="no-change" aria-pressed="false">No change <span class="mono" data-cb-u="u-8dbc1c006c"><?= (int) $cb_counts['no-change'] ?></span></button>
    <button type="button" data-cb-filter="failed"    aria-pressed="false">Failed <span class="mono" data-cb-u="u-8dbc1c006c"><?= (int) $cb_counts['failed'] ?></span></button>
  </div>

  <div class="cb-field" data-cb-u="u-ff46992490">
    <label class="glabel" for="cb-sh-from" data-cb-u="u-0aa89a4011">From</label>
    <input id="cb-sh-from" type="date" data-cb-date-from data-cb-u="u-485fc5be97">
    <label class="glabel" for="cb-sh-to" data-cb-u="u-19db841668">To</label>
    <input id="cb-sh-to" type="date" data-cb-date-to data-cb-u="u-485fc5be97">
  </div>

  <div class="cb-search">
    <input type="search"
           placeholder="Search trigger or error…"
           data-cb-search
           data-cb-search-scope="sync-history"
           aria-label="Search sync history">
  </div>

  <div class="spacer"></div>
  <div class="count" data-cb-result-count>
    <span data-cb-visible-count><?= (int) $cb_log_count ?></span> of <?= (int) $cb_log_count ?> runs
  </div>
</div>

<!-- ───────────────────── Table ───────────────────── -->
<div class="cb-card" data-cb-u="u-a5c8b31f20">
  <table class="cb-table" data-cb-table="sync-history">
    <thead>
      <tr>
        <th>#</th>
        <th>Started</th>
        <th>Finished</th>
        <th>Trigger</th>
        <th>Status</th>
        <th class="right">Profiles checked / changed</th>
        <th class="right">Products updated</th>
        <th>Snapshot</th>
        <th>Error</th>
      </tr>
    </thead>
    <tbody>
      <?php foreach ($logs as $cb_l):
        $cb_id        = (int) ($cb_l['id'] ?? 0);
        $cb_trigger   = (string) ($cb_l['trigger'] ?? '');
        $cb_status    = (string) ($cb_l['status'] ?? '');
        $cb_started   = (string) ($cb_l['started_at'] ?? '');
        $cb_finished  = (string) ($cb_l['finished_at'] ?? '');
        $cb_checked   = (int) ($cb_l['profiles_checked'] ?? 0);
        $cb_changed   = (int) ($cb_l['profiles_changed'] ?? 0);
        $cb_products  = (int) ($cb_l['products_updated'] ?? 0);
        $cb_error_msg = (string) ($cb_l['error_message'] ?? '');
        $cb_summary_raw = (string) ($cb_l['summary'] ?? '');
        $cb_summary_decoded = $cb_summary_raw !== '' ? json_decode($cb_summary_raw, true) : null;
        $cb_snap_at   = '';
        if (is_array($cb_summary_decoded) && isset($cb_summary_decoded['snapshot_generated_at'])) {
            $cb_snap_at = (string) $cb_summary_decoded['snapshot_generated_at'];
        }
        $cb_dur = $cb_duration($cb_started, $cb_finished);
      ?>
        <tr data-cb-log-row
            data-cb-log-id="<?= (int) $cb_id ?>"
            data-cb-status="<?= $esc($cb_status) ?>"
            data-cb-trigger="<?= $esc($cb_trigger) ?>"
            data-cb-started="<?= $esc($cb_started) ?>"
            data-cb-search-text="<?= $esc(strtolower($cb_trigger . ' ' . $cb_error_msg . ' ' . $cb_status)) ?>">
          <td class="mono">#<?= (int) $cb_id ?></td>
          <td>
            <div class="mono" data-cb-u="u-7e6609e5f6"><?= $esc(substr($cb_started, 0, 10)) ?></div>
            <div class="mono" data-cb-u="u-9f803cd406"><?= $esc(substr($cb_started, 11, 8)) ?></div>
          </td>
          <td>
            <?php if ($cb_finished !== ''): ?>
              <div class="mono" data-cb-u="u-7e6609e5f6"><?= $esc(substr($cb_finished, 11, 8)) ?></div>
              <?php if ($cb_dur !== ''): ?>
                <div data-cb-u="u-9f803cd406">Duration <?= $esc($cb_dur) ?></div>
              <?php endif; ?>
            <?php else: ?>
              <span class="cb-pill warn"><span class="dot"></span>running</span>
            <?php endif; ?>
          </td>
          <td>
            <?php
              $cb_tt = $cb_trigger === 'cron' ? 'grey' : ($cb_trigger === 'webhook' ? 'warn' : '');
            ?>
            <span class="cb-pill <?= $esc($cb_tt) ?>"><?= $esc($cb_trigger !== '' ? $cb_trigger : '—') ?></span>
          </td>
          <td>
            <?php if ($cb_status === 'succeeded'): ?>
              <span class="cb-pill good"><span class="dot"></span>succeeded</span>
            <?php elseif ($cb_status === 'failed'): ?>
              <span class="cb-pill bad"><span class="dot"></span>failed</span>
            <?php elseif ($cb_status === 'no-change'): ?>
              <span class="cb-pill grey">no-change</span>
            <?php elseif ($cb_status === 'running'): ?>
              <span class="cb-pill warn"><span class="dot"></span>running</span>
            <?php else: ?>
              <span class="cb-pill"><?= $esc($cb_status !== '' ? $cb_status : '—') ?></span>
            <?php endif; ?>
          </td>
          <td class="right mono">
            <?= (int) $cb_checked ?>
            <span data-cb-u="u-eac7694072"> / </span>
            <span class="<?= $cb_changed > 0 ? 'cb-tone-accent' : 'cb-tone-muted' ?>"><?= (int) $cb_changed ?></span>
          </td>
          <td class="right mono"><?= (int) $cb_products ?></td>
          <td class="mono" data-cb-u="u-685394115d"><?= $esc(substr($cb_snap_at, 0, 16)) ?></td>
          <td>
            <?php if ($cb_error_msg !== ''): ?>
              <code class="mono" data-cb-u="u-21e471ac42"
                    title="<?= $esc($cb_error_msg) ?>"><?= $esc($cb_trunc($cb_error_msg, 60)) ?></code>
            <?php else: ?>
              <span data-cb-u="u-eac7694072">—</span>
            <?php endif; ?>
          </td>
        </tr>
      <?php endforeach; ?>
    </tbody>
  </table>
</div>

<?php endif; ?>

<p data-cb-u="u-c53ea1da23">
  <a class="cb-btn ghost" href="<?= $esc($module_link) ?>">← Back to dashboard</a>
</p>

</div>
