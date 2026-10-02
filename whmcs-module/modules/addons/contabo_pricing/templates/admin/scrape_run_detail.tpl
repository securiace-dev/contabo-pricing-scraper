<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * One scrape run: outcome, gates, provider attempts, decision, and the diff
 * against the last good run. needs_review runs offer an Import button that
 * re-hashes the stored envelope before importing.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<string,mixed> $run
 * @var list<array<string,mixed>> $attempts
 * @var array<string,mixed>|null $decision
 * @var list<array<string,mixed>> $diffs
 * @var int|null $baseline_run_id
 * @var array<string,mixed>|null $summary
 * @var string $flash
 */
$cb_usd = static function ($micro, $dp = 4) {
    return '$' . number_format(((int) $micro) / 1000000, (int) $dp, '.', '');
};
$cb_tone = static function ($state) {
    switch ((string) $state) {
        case 'succeeded': case 'auto_import': case 'safe': return 'good';
        case 'needs_review': case 'dry_run': case 'running': case 'risky': return 'warn';
        case 'rejected': case 'failed': case 'anomaly': return 'bad';
        default: return 'grey';
    }
};
$cb_flash = isset($flash) ? (string) $flash : '';
$cb_gates_blob = is_array($run['gates_json'] ?? null) ? $run['gates_json'] : [];
$cb_gates = is_array($cb_gates_blob['gates'] ?? null) ? $cb_gates_blob['gates'] : [];
$cb_warnings = is_array($cb_gates_blob['warnings'] ?? null) ? $cb_gates_blob['warnings'] : [];
$cb_jev = is_array($cb_gates_blob['jev'] ?? null) ? $cb_gates_blob['jev'] : null;
$cb_outcome = (string) ($cb_gates_blob['outcome'] ?? ($decision['outcome'] ?? ''));
$cb_fam_blob = is_array($run['families_json'] ?? null) ? $run['families_json'] : [];
$cb_fam = is_array($cb_fam_blob['families'] ?? null) ? $cb_fam_blob['families'] : [];
$cb_pages = is_array($cb_fam_blob['pages'] ?? null) ? $cb_fam_blob['pages'] : [];
$cb_fdiff = is_array($cb_gates_blob['families'] ?? null) ? $cb_gates_blob['families'] : [];
$cb_state = (string) ($run['state'] ?? '');
$cb_id = (int) ($run['id'] ?? 0);
?>

<header>
  <div>
    <h2 class="display">Scrape run #<?= $cb_id ?></h2>
    <p class="cb-card-sub">
      <span class="cb-pill <?= $esc($cb_tone($cb_state)) ?>"><span class="dot"></span><?= $esc($cb_state) ?></span>
      <?php if ($cb_outcome !== ''): ?> outcome <span class="cb-pill <?= $esc($cb_tone($cb_outcome)) ?>"><?= $esc($cb_outcome) ?></span><?php endif; ?>
      &middot; trigger <?= $esc((string) ($run['trigger'] ?? '')) ?>
      <?= (int) ($run['dry_run'] ?? 0) === 1 ? '&middot; dry run (nothing imported)' : '' ?>
    </p>
  </div>
  <div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=scrape-runs">&larr; Scrape runs</a>
  </div>
</header>

<?php if ($cb_flash !== ''): ?>
  <div class="cb-flash"><?= $esc($cb_flash) ?></div>
<?php endif; ?>

<div class="cb-card">
  <h3>Summary</h3>
  <table class="cb-table">
    <tr><th>Started</th><td class="mono"><?= $esc((string) ($run['started_at'] ?? '')) ?></td></tr>
    <tr><th>Finished</th><td class="mono"><?= $esc((string) ($run['finished_at'] ?? '')) ?></td></tr>
    <tr><th>Plans</th><td class="mono"><?= (int) ($run['plan_count'] ?? 0) ?></td></tr>
    <tr><th>Cost</th><td class="mono"><?= $esc($cb_usd($run['total_cost_micro'] ?? 0)) ?></td></tr>
    <tr><th>Catalog version</th><td class="mono"><?= $esc((string) ($run['catalog_version'] ?? '')) ?></td></tr>
    <tr><th>Envelope hash</th><td class="mono"><?= $esc((string) ($run['envelope_hash'] ?? '')) ?></td></tr>
    <?php if (!empty($run['error'])): ?>
    <tr><th>Error</th><td class="mono"><?= $esc((string) $run['error']) ?></td></tr>
    <?php endif; ?>
  </table>

  <?php if ($cb_state === 'needs_review'): ?>
  <form method="post" action="<?= $esc($module_link) ?>">
    <input type="hidden" name="action" value="scrape-run-import">
    <input type="hidden" name="id" value="<?= $cb_id ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <button type="submit" class="cb-btn">Import this run</button>
    <span class="cb-card-sub">The stored envelope is re-hashed first; a mismatch refuses the import.</span>
  </form>
  <?php endif; ?>
</div>

<div class="cb-card">
  <h3>Gates</h3>
  <?php if (empty($cb_gates)): ?>
    <p class="cb-card-sub">No gates were evaluated (the run ended before validation).</p>
  <?php else: ?>
  <table class="cb-table">
    <thead><tr><th>Gate</th><th>Result</th><th>Detail</th></tr></thead>
    <tbody>
    <?php foreach ($cb_gates as $cb_name => $cb_g): ?>
      <tr>
        <td class="mono"><?= $esc((string) $cb_name) ?></td>
        <td><span class="cb-pill <?= !empty($cb_g['ok']) ? 'good' : 'bad' ?>"><span class="dot"></span><?= !empty($cb_g['ok']) ? 'pass' : 'FAIL' ?></span></td>
        <td class="mono"><?= $esc(substr((string) json_encode($cb_g['detail'] ?? null, JSON_UNESCAPED_SLASHES), 0, 400)) ?></td>
      </tr>
    <?php endforeach; ?>
    </tbody>
  </table>
  <?php endif; ?>
  <?php foreach ($cb_warnings as $cb_w): ?>
    <p class="cb-card-sub"><span class="cb-pill warn">warning</span> <?= $esc((string) $cb_w) ?></p>
  <?php endforeach; ?>
  <?php if ($cb_jev !== null): ?>
    <p class="cb-card-sub">
      Jev: <span class="cb-pill <?= ($cb_jev['status'] ?? '') === 'ok' ? (!empty($cb_jev['veto']) ? 'warn' : 'good') : 'grey' ?>"><?= $esc((string) ($cb_jev['status'] ?? '')) ?></span>
      <?= !empty($cb_jev['veto']) ? 'raised a doubt (moved to review)' : '' ?>
      <?= isset($cb_jev['reason']) ? $esc((string) $cb_jev['reason']) : '' ?>
    </p>
  <?php endif; ?>
</div>

<div class="cb-card">
  <h3>Provider attempts</h3>
  <?php if (empty($attempts)): ?>
    <p class="cb-card-sub">No provider was called<?= !empty($run['error']) ? ' (' . $esc((string) $run['error']) . ')' : '' ?>.</p>
  <?php else: ?>
  <table class="cb-table">
    <thead><tr><th>Family</th><th>Source</th><th>Served by</th><th>HTTP</th><th>OK</th><th>Strategy</th><th class="right">Plans</th><th class="right">Cost</th><th class="right">ms</th><th>Page SHA-256</th><th>Page / nav titles</th><th>Error</th></tr></thead>
    <tbody>
    <?php foreach ($attempts as $cb_a): ?>
      <tr>
        <td><?= $esc((string) $cb_a['family']) ?></td>
        <td class="mono"><?= $esc((string) $cb_a['source_id']) ?></td>
        <td class="mono"><?= $esc((string) ($cb_a['served_by'] ?? '')) ?></td>
        <td class="mono"><?= $esc((string) ($cb_a['http_status'] ?? '')) ?></td>
        <td><span class="cb-pill <?= (int) $cb_a['ok'] === 1 ? 'good' : 'bad' ?>"><?= (int) $cb_a['ok'] === 1 ? 'ok' : 'failed' ?></span></td>
        <td><?= $esc((string) ($cb_a['strategy'] ?? '')) ?><?= (int) ($cb_a['sapper_present'] ?? 0) === 1 ? ' <span class="cb-pill grey">sapper</span>' : '' ?></td>
        <td class="right mono"><?= (int) $cb_a['plan_count'] ?></td>
        <td class="right mono"><?= $esc($cb_usd($cb_a['cost_micro'])) ?></td>
        <td class="right mono"><?= (int) $cb_a['latency_ms'] ?></td>
        <td class="mono"><?= $esc(substr((string) ($cb_a['html_sha256'] ?? ''), 0, 12)) ?></td>
        <td class="mono"><?= $esc((string) ($cb_a['url'] ?? '')) ?>
          <?php $cb_nt = json_decode((string) ($cb_a['nav_titles_json'] ?? ''), true); ?>
          <?php if (is_array($cb_nt) && $cb_nt !== []): ?><div class="cb-card-sub">nav: <?= $esc(implode(' | ', array_map('strval', $cb_nt))) ?></div><?php endif; ?>
        </td>
        <td class="mono"><?= $esc(substr((string) ($cb_a['error'] ?? ''), 0, 160)) ?><?= !empty($cb_a['final_url']) ? ' &rarr; ' . $esc((string) $cb_a['final_url']) : '' ?></td>
      </tr>
    <?php endforeach; ?>
    </tbody>
  </table>
  <?php endif; ?>
  <?php if (!empty($cb_pages)): ?>
  <p class="cb-card-sub">Pages fetched:
    <?php foreach ($cb_pages as $cb_pg): ?>
      <span class="mono"><?= $esc((string) ($cb_pg['url'] ?? '')) ?></span> <?= !empty($cb_pg['ok']) ? '<span class="cb-pill good">blob</span>' : '<span class="cb-pill bad">no blob</span>' ?>&nbsp;
    <?php endforeach; ?>
  </p>
  <?php endif; ?>
</div>

<div class="cb-card">
  <h3>Families</h3>
  <?php if (empty($cb_fam) && empty($cb_fdiff)): ?>
    <p class="cb-card-sub">No family information (the run ended before the catalogue blob was read).</p>
  <?php else: ?>
  <table class="cb-table">
    <thead><tr><th>Family</th><th>Status</th><th class="right">Plans / typical</th><th>Approval</th></tr></thead>
    <tbody>
    <?php foreach ($cb_fam as $cb_fn => $cb_fi): ?>
      <tr>
        <td><?= $esc((string) $cb_fn) ?> <span class="mono cb-card-sub"><?= $esc((string) ($cb_fi['slug'] ?? '')) ?></span></td>
        <td><?= $esc((string) ($cb_fi['status'] ?? '')) ?></td>
        <td class="right mono"><?= (int) ($cb_fi['count'] ?? 0) ?> / <?= (int) ($cb_fi['typical'] ?? 0) ?></td>
        <td><?= !empty($cb_fi['approved']) ? '<span class="cb-pill good">approved</span>' : '<span class="cb-pill warn">not approved</span>' ?></td>
      </tr>
    <?php endforeach; ?>
    </tbody>
  </table>
  <?php endif; ?>
  <?php
  $cb_lines = [];
  foreach ((array) ($cb_fdiff['new'] ?? []) as $cb_x) { if (($cb_x['status'] ?? '') !== 'hidden') { $cb_lines[] = ['new', 'New family "' . ($cb_x['label'] ?? '') . '" (' . (int) ($cb_x['plans'] ?? 0) . ' plans, ' . ($cb_x['reason'] ?? 'first seen') . ')']; } }
  foreach ((array) ($cb_fdiff['renamed'] ?? []) as $cb_x) { $cb_lines[] = ['renamed', 'Renamed "' . ($cb_x['from'] ?? '') . '" to "' . ($cb_x['to'] ?? '') . '"' . (($cb_x['kind'] ?? '') === 'id_change' ? ' (new category id; history carried forward)' : '')]; }
  foreach ((array) ($cb_fdiff['retired'] ?? []) as $cb_x) { $cb_lines[] = ['retired', 'Vanished from the catalogue: "' . ($cb_x['label'] ?? '') . '"']; }
  foreach ((array) ($cb_fdiff['delisted'] ?? []) as $cb_x) { $cb_lines[] = ['delisted', 'No longer linked from the nav: "' . ($cb_x['label'] ?? '') . '"']; }
  foreach ((array) ($cb_fdiff['reappeared'] ?? []) as $cb_x) { $cb_lines[] = ['reappeared', 'Back after retirement: "' . ($cb_x['label'] ?? '') . '"']; }
  foreach ((array) ($cb_fdiff['count_changes'] ?? []) as $cb_x) { $cb_lines[] = ['count', '"' . ($cb_x['label'] ?? '') . '" has ' . (int) ($cb_x['current'] ?? 0) . ' plans vs typical ' . (int) ($cb_x['typical'] ?? 0) . ' (' . ($cb_x['pct'] ?? 0) . '%)']; }
  foreach ((array) ($cb_fdiff['plan_moves'] ?? []) as $cb_x) { $cb_lines[] = ['moved', 'Plan ' . ($cb_x['slug'] ?? '') . ' moved from "' . ($cb_x['from_label'] ?? '') . '" to "' . ($cb_x['to_label'] ?? '') . '"']; }
  ?>
  <?php foreach ($cb_lines as $cb_ln): ?>
    <p class="cb-card-sub"><span class="cb-pill warn"><?= $esc($cb_ln[0]) ?></span> <?= $esc($cb_ln[1]) ?></p>
  <?php endforeach; ?>
  <?php if (!empty($cb_fdiff) && empty($cb_fdiff['persisted'])): ?>
    <p class="cb-card-sub">The registry was not updated by this run (dry run, rejected or failed). The same findings are raised again until a run is accepted.</p>
  <?php endif; ?>
  <p class="cb-card-sub"><a href="<?= $esc($module_link) ?>&amp;action=data-sources#families">Manage families &rarr;</a></p>
</div>

<div class="cb-card">
  <h3>Decision</h3>
  <?php if ($decision === null): ?>
    <p class="cb-card-sub">No decision was recorded.</p>
  <?php else: ?>
    <p>
      <span class="cb-pill <?= $esc($cb_tone($decision['outcome'] ?? '')) ?>"><?= $esc((string) $decision['outcome']) ?></span>
      decided by <strong><?= $esc((string) $decision['decided_by']) ?></strong>
      <span class="mono"><?= $esc((string) ($decision['created_at'] ?? '')) ?></span>
    </p>
  <?php endif; ?>
</div>

<div class="cb-card">
  <h3>Changes vs last good run<?= $baseline_run_id !== null ? ' (#' . (int) $baseline_run_id . ')' : ' (none yet)' ?></h3>
  <?php if (empty($diffs)): ?>
    <p class="cb-card-sub">No differences.</p>
  <?php else: ?>
  <table class="cb-table">
    <thead><tr><th>Plan</th><th>Change</th><th>Class</th><th class="right">Before</th><th class="right">After</th><th>Note</th></tr></thead>
    <tbody>
    <?php foreach ($diffs as $cb_d): ?>
      <tr>
        <td class="mono"><?= $esc((string) ($cb_d['catalog_sku'] ?? '—')) ?></td>
        <td><?= $esc((string) $cb_d['kind']) ?></td>
        <td><span class="cb-pill <?= $esc($cb_tone($cb_d['bucket'] ?? '')) ?>"><?= $esc((string) ($cb_d['bucket'] ?? '')) ?></span></td>
        <td class="right mono"><?= $cb_d['cost_before'] === null ? '' : $esc(number_format((float) $cb_d['cost_before'], 2)) ?></td>
        <td class="right mono"><?= $cb_d['cost_after'] === null ? '' : $esc(number_format((float) $cb_d['cost_after'], 2)) ?></td>
        <td class="mono"><?= $esc(substr((string) ($cb_d['notes'] ?? ''), 0, 200)) ?></td>
      </tr>
    <?php endforeach; ?>
    </tbody>
  </table>
  <?php endif; ?>
</div>

</div>
