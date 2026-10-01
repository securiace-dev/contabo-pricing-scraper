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
$cb_fam = is_array($run['families_json'] ?? null) ? $run['families_json'] : [];
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
    <thead><tr><th>Family</th><th>Source</th><th>Served by</th><th>HTTP</th><th>OK</th><th>Strategy</th><th class="right">Plans</th><th class="right">Cost</th><th class="right">ms</th><th>Page SHA-256</th><th>Error</th></tr></thead>
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
        <td class="mono"><?= $esc(substr((string) ($cb_a['error'] ?? ''), 0, 160)) ?><?= !empty($cb_a['final_url']) ? ' &rarr; ' . $esc((string) $cb_a['final_url']) : '' ?></td>
      </tr>
    <?php endforeach; ?>
    </tbody>
  </table>
  <?php endif; ?>
  <?php if (!empty($cb_fam)): ?>
  <p class="cb-card-sub">
    <?php foreach ($cb_fam as $cb_fn => $cb_fi): ?>
      <?= $esc((string) $cb_fn) ?>: <span class="mono"><?= (int) ($cb_fi['count'] ?? 0) ?>/<?= (int) ($cb_fi['min'] ?? 0) ?></span>
      <?= empty($cb_fi['fetched']) ? '(satisfied by an earlier page)' : '' ?>&nbsp;&nbsp;
    <?php endforeach; ?>
  </p>
  <?php endif; ?>
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
