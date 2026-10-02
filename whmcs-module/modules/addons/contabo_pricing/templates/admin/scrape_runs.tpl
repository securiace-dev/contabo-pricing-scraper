<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Scrape runs: trigger a dry or live run, start the manual agent run, and list recent runs.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var list<array<string,mixed>> $runs
 * @var array<string,string> $scrape
 * @var int      $month_spend_micro
 * @var string   $flash
 * @var array<string,mixed>|null $agent_confirm
 */
$cb_usd = static function ($micro, $dp = 4) {
    return '$' . number_format(((int) $micro) / 1000000, (int) $dp, '.', '');
};
$cb_tone = static function ($state) {
    switch ((string) $state) {
        case 'succeeded': return 'good';
        case 'needs_review': case 'dry_run': case 'running': return 'warn';
        case 'rejected': case 'failed': return 'bad';
        default: return 'grey';
    }
};
$cb_flash = isset($flash) ? (string) $flash : '';
?>

<header>
  <div>
    <h2 class="display">Scrape runs</h2>
    <p class="cb-card-sub">
      Spent this month <span class="mono"><?= $esc($cb_usd($month_spend_micro)) ?></span>
      of <span class="mono"><?= $esc($cb_usd((int) ($scrape['scrape.monthly_budget_micro'] ?? 0))) ?></span>.
      Scheduled scraping is <strong><?= ($scrape['scrape.enabled'] ?? '0') === '1' ? 'on' : 'off' ?></strong>.
    </p>
  </div>
  <div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=data-sources">Data sources</a>
  </div>
</header>

<?php if ($cb_flash !== ''): ?>
  <div class="cb-flash"><?= $esc($cb_flash) ?></div>
<?php endif; ?>

<?php if (!empty($agent_confirm)): ?>
<div class="cb-card" role="alertdialog" aria-labelledby="cb-agent-h">
  <h3 id="cb-agent-h">Confirm agent run</h3>
  <p>
    The TinyFish agent is billed per step. This run can cost up to
    <strong class="mono"><?= $esc($cb_usd($agent_confirm['max_cost_micro'], 3)) ?></strong>
    (<?= (int) $agent_confirm['max_steps'] ?> steps &times; $0.016).
    Mode: <strong><?= $esc((string) $agent_confirm['mode']) ?></strong>.
  </p>
  <?php if (empty($agent_confirm['enabled'])): ?>
    <p class="cb-error">The agent source is not enabled (or has no key); enable it under Data sources first.</p>
  <?php endif; ?>
  <p class="cb-card-sub">The monthly and per-run budgets still apply; raise them under Data sources if this exceeds them.</p>
  <form method="post" action="<?= $esc($module_link) ?>">
    <input type="hidden" name="action" value="scrape-agent-run">
    <input type="hidden" name="confirm" value="1">
    <input type="hidden" name="mode" value="<?= $esc((string) $agent_confirm['mode']) ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <button type="submit" class="cb-btn">Run agent (up to <?= $esc($cb_usd($agent_confirm['max_cost_micro'], 3)) ?>)</button>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=scrape-runs">Cancel</a>
  </form>
</div>
<?php endif; ?>

<div class="cb-card">
  <h3>Run now</h3>
  <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
    <input type="hidden" name="action" value="scrape-run">
    <input type="hidden" name="mode" value="dry">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <button type="submit" class="cb-btn subtle">Dry run (never imports)</button>
  </form>
  <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
    <input type="hidden" name="action" value="scrape-run">
    <input type="hidden" name="mode" value="live">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <button type="submit" class="cb-btn">Live run</button>
  </form>
  <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
    <input type="hidden" name="action" value="scrape-agent-run">
    <input type="hidden" name="mode" value="dry">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <button type="submit" class="cb-btn ghost">Agent run (manual)&hellip;</button>
  </form>
  <p class="cb-card-sub">A live run still stops for review when a price rises, a plan disappears, or human review is required.</p>
</div>

<div class="cb-card">
<?php if (empty($runs)): ?>
  <div class="cb-empty">
    <div class="display">No scrape runs yet</div>
    <p>Run a dry run to see what the scraper would import.</p>
  </div>
<?php else: ?>
  <table class="cb-table">
    <thead>
      <tr><th>#</th><th>Started</th><th>Trigger</th><th>State</th><th class="right">Plans</th><th class="right">Cost</th><th>Catalog</th><th></th></tr>
    </thead>
    <tbody>
    <?php foreach ($runs as $cb_r): ?>
      <tr>
        <td class="mono">#<?= (int) $cb_r['id'] ?></td>
        <td class="mono"><?= $esc((string) ($cb_r['started_at'] ?? '')) ?></td>
        <td><span class="cb-pill grey"><?= $esc((string) ($cb_r['trigger'] ?? '')) ?></span><?= (int) ($cb_r['dry_run'] ?? 0) === 1 ? ' <span class="cb-pill">dry</span>' : '' ?></td>
        <td><span class="cb-pill <?= $esc($cb_tone($cb_r['state'] ?? '')) ?>"><span class="dot"></span><?= $esc((string) ($cb_r['state'] ?? '')) ?></span></td>
        <td class="right mono"><?= (int) ($cb_r['plan_count'] ?? 0) ?></td>
        <td class="right mono"><?= $esc($cb_usd($cb_r['total_cost_micro'] ?? 0)) ?></td>
        <td class="mono"><?= $esc((string) ($cb_r['catalog_version'] ?? '')) ?></td>
        <td><a href="<?= $esc($module_link) ?>&amp;action=scrape-run-detail&amp;id=<?= (int) $cb_r['id'] ?>">Details</a></td>
      </tr>
      <?php if (!empty($cb_r['error'])): ?>
      <tr><td></td><td colspan="7" class="muted mono"><?= $esc(substr((string) $cb_r['error'], 0, 240)) ?></td></tr>
      <?php endif; ?>
    <?php endforeach; ?>
    </tbody>
  </table>
<?php endif; ?>
</div>

</div>
