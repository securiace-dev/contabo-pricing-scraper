<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Data sources: provider registry for the native catalog scraper, plus the
 * decision/budget settings and the (optional) Jev second-opinion key.
 *
 * API keys are never rendered: a "set · ENC" pill plus SecretStore::mask()'s
 * masked tail only. Key inputs are write-only; leaving one blank keeps the
 * stored key.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var list<array<string,mixed>> $sources
 * @var array<string,string> $scrape
 * @var bool     $jev_key_set
 * @var string   $jev_key_mask
 * @var int      $month_spend_micro
 * @var string   $flash
 */

$cb_usd = static function ($micro, $dp = 4) {
    return '$' . number_format(((int) $micro) / 1000000, (int) $dp, '.', '');
};
$cb_budget_in = static function ($micro) {
    return rtrim(rtrim(number_format(((int) $micro) / 1000000, 6, '.', ''), '0'), '.') ?: '0';
};
$cb_flash = isset($flash) ? (string) $flash : '';
$cb_g = static function ($k, $d = '') use ($scrape) {
    return isset($scrape[$k]) ? (string) $scrape[$k] : $d;
};
?>

<header>
  <div>
    <h2 class="display">Data sources</h2>
    <p class="cb-card-sub">
      Hosted providers that fetch the upstream pricing page for the native scraper.
      One page carries every plan, so a normal run makes a single paid call.
      Spent this month: <span class="mono"><?= $esc($cb_usd($month_spend_micro)) ?></span>
      of <span class="mono"><?= $esc($cb_usd((int) $cb_g('scrape.monthly_budget_micro', '0'))) ?></span>.
    </p>
  </div>
  <div>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=scrape-runs">Scrape runs &rarr;</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=settings">&larr; Settings</a>
  </div>
</header>

<?php if ($cb_flash !== ''): ?>
  <div class="cb-flash"><?= $esc($cb_flash) ?></div>
<?php endif; ?>

<form method="post" action="<?= $esc($module_link) ?>">
  <input type="hidden" name="action" value="data-sources-save">
  <?php if (function_exists('generate_token')) { echo generate_token(); } ?>

  <!-- ───────────── Providers ───────────── -->
  <?php foreach ($sources as $cb_s):
    $cb_row = $cb_s['row'];
    $cb_id = (string) $cb_row['source_id'];
    $cb_fid = preg_replace('/[^A-Za-z0-9_-]/', '', $cb_id);
    $cb_manual = !empty($cb_s['manual_only']);
    $cb_nosapper = empty($cb_s['supports_sapper']);
  ?>
  <div class="cb-card" data-cb-source="<?= $esc($cb_id) ?>">
    <h3>
      <?= $esc((string) ($cb_row['display_name'] ?? $cb_id)) ?>
      <span class="mono"><?= $esc($cb_id) ?></span>
      <?php if ($cb_manual): ?><span class="cb-pill warn">manual only</span><?php endif; ?>
      <?php if ($cb_nosapper): ?><span class="cb-pill grey">cross-check only</span><?php endif; ?>
      <?php if ((int) ($cb_row['consecutive_failures'] ?? 0) > 0): ?>
        <span class="cb-pill bad"><span class="dot"></span><?= (int) $cb_row['consecutive_failures'] ?> failures in a row</span>
      <?php endif; ?>
    </h3>

    <table class="cb-table">
      <tr>
        <th>Enabled</th>
        <td>
          <label>
            <input type="checkbox" name="src[<?= $esc($cb_id) ?>][enabled]" value="1"<?= (int) ($cb_row['enabled'] ?? 0) === 1 ? ' checked' : '' ?>>
            use this source in scrape runs
          </label>
        </td>
      </tr>
      <tr>
        <th>API key</th>
        <td>
          <?php if (!empty($cb_s['key_set'])): ?>
            <span class="cb-pill good"><span class="dot"></span>set &middot; ENC</span>
            <span class="mono"><?= $esc((string) $cb_s['key_mask']) ?></span>
          <?php else: ?>
            <span class="cb-pill bad"><span class="dot"></span>not set</span>
          <?php endif; ?>
          <div class="cb-field">
            <label for="cb-key-<?= $esc($cb_fid) ?>">Replace key (leave blank to keep)</label>
            <input id="cb-key-<?= $esc($cb_fid) ?>" type="password" autocomplete="new-password"
                   name="src[<?= $esc($cb_id) ?>][api_key]" value="" placeholder="&bull;&bull;&bull;&bull; write-only">
          </div>
        </td>
      </tr>
      <tr>
        <th>Base URL</th>
        <td>
          <input type="url" name="src[<?= $esc($cb_id) ?>][base_url]" value="<?= $esc((string) ($cb_row['base_url'] ?? '')) ?>"
                 placeholder="provider default" aria-label="Base URL for <?= $esc($cb_id) ?>">
        </td>
      </tr>
      <tr>
        <th>Priority</th>
        <td>
          <input type="number" min="0" step="1" name="src[<?= $esc($cb_id) ?>][priority]"
                 value="<?= $esc((string) ($cb_row['priority'] ?? '')) ?>" aria-label="Priority for <?= $esc($cb_id) ?>">
          <span class="cb-card-sub">used in manual rank mode (low first; blank = last)</span>
        </td>
      </tr>
      <tr>
        <th>Budgets (USD)</th>
        <td>
          <label>Monthly
            <input type="number" min="0" step="0.0001" name="src[<?= $esc($cb_id) ?>][monthly_budget_usd]"
                   value="<?= $esc($cb_budget_in($cb_row['monthly_budget_micro'] ?? 0)) ?>">
          </label>
          <label>Per run
            <input type="number" min="0" step="0.0001" name="src[<?= $esc($cb_id) ?>][per_run_cap_usd]"
                   value="<?= $esc($cb_budget_in($cb_row['per_run_cap_micro'] ?? 0)) ?>">
          </label>
          <span class="cb-card-sub">spent this month: <span class="mono"><?= $esc($cb_usd($cb_s['month_spend_micro'])) ?></span></span>
        </td>
      </tr>
      <tr>
        <th>Options (JSON)</th>
        <td>
          <textarea rows="3" class="mono" name="src[<?= $esc($cb_id) ?>][options_json]"
                    aria-label="Options JSON for <?= $esc($cb_id) ?>"><?= $esc((string) ($cb_row['options_json'] ?? '')) ?></textarea>
        </td>
      </tr>
      <tr>
        <th>Prior</th>
        <td class="mono">
          <?= $esc($cb_usd($cb_s['prior']['price'])) ?>/page &middot;
          ok <?= $esc(number_format((float) $cb_s['prior']['ok'] * 100, 1)) ?>% &middot;
          p50 <?= $esc(number_format(((int) $cb_s['prior']['p50']) / 1000, 1)) ?>s
          <?php if ($cb_manual): ?>
            &middot; worst case <?= $esc($cb_usd($cb_s['max_cost_micro'])) ?> (<?= (int) $cb_s['max_steps'] ?> steps &times; $0.016)
          <?php endif; ?>
        </td>
      </tr>
      <tr>
        <th>Test</th>
        <td>
          <button type="button" class="cb-btn subtle<?= !empty($cb_s['key_set']) ? '' : ' disabled' ?>"
                  <?= !empty($cb_s['key_set']) ? '' : 'disabled' ?>
                  data-cb-action="test-source"
                  data-source-id="<?= $esc($cb_id) ?>">
            Test connection
          </button>
          <span data-cb-result="source-test" role="status" aria-live="polite"></span>
          <?php if (!$cb_manual): ?>
            <span class="cb-card-sub">makes one real page fetch (about <?= $esc($cb_usd($cb_s['prior']['price'])) ?>)</span>
          <?php endif; ?>
        </td>
      </tr>
      <?php if (!empty($cb_row['last_error'])): ?>
      <tr>
        <th>Last error</th>
        <td class="mono"><?= $esc((string) $cb_row['last_error']) ?> <span class="cb-card-sub"><?= $esc((string) ($cb_row['last_fail_at'] ?? '')) ?></span></td>
      </tr>
      <?php endif; ?>
    </table>
  </div>
  <?php endforeach; ?>

  <!-- ───────────── Run policy ───────────── -->
  <div class="cb-card">
    <h3>Run policy</h3>
    <table class="cb-table">
      <tr>
        <th>Scheduled scraping</th>
        <td>
          <label><input type="checkbox" name="scrape_enabled" value="1"<?= $cb_g('scrape.enabled') === '1' ? ' checked' : '' ?>>
            run from the daily cron (before the price sync)</label>
        </td>
      </tr>
      <tr>
        <th>Rank mode</th>
        <td>
          <select name="rank_mode" aria-label="Rank mode">
            <option value="auto"<?= $cb_g('scrape.rank_mode', 'auto') === 'auto' ? ' selected' : '' ?>>auto (cost / reliability / latency)</option>
            <option value="manual"<?= $cb_g('scrape.rank_mode') === 'manual' ? ' selected' : '' ?>>manual (priority order)</option>
          </select>
        </td>
      </tr>
      <tr>
        <th>Budgets (USD)</th>
        <td>
          <label>Monthly, all sources
            <input type="number" min="0" step="0.0001" name="monthly_budget_usd" value="<?= $esc($cb_budget_in($cb_g('scrape.monthly_budget_micro', '0'))) ?>">
          </label>
          <label>Per run
            <input type="number" min="0" step="0.0001" name="per_run_cap_usd" value="<?= $esc($cb_budget_in($cb_g('scrape.per_run_cap_micro', '0'))) ?>">
          </label>
        </td>
      </tr>
      <tr>
        <th>Minimum plans</th>
        <td>
          <label>Cloud VPS <input type="number" min="0" step="1" name="min_plans_cloud_vps" value="<?= $esc($cb_g('scrape.min_plans_cloud_vps')) ?>"></label>
          <label>Storage VPS <input type="number" min="0" step="1" name="min_plans_storage_vps" value="<?= $esc($cb_g('scrape.min_plans_storage_vps')) ?>"></label>
          <label>Cloud VDS <input type="number" min="0" step="1" name="min_plans_cloud_vds" value="<?= $esc($cb_g('scrape.min_plans_cloud_vds')) ?>"></label>
        </td>
      </tr>
      <tr>
        <th>Thresholds (%)</th>
        <td>
          <label>Max plan-count drop <input type="number" min="0" max="100" step="0.1" name="max_drop_pct" value="<?= $esc($cb_g('scrape.max_drop_pct')) ?>"></label>
          <label>Max price change <input type="number" min="0" max="1000" step="0.1" name="max_price_change_pct" value="<?= $esc($cb_g('scrape.max_price_change_pct')) ?>"></label>
        </td>
      </tr>
      <tr>
        <th>Human review</th>
        <td>
          <label><input type="checkbox" name="require_human_review" value="1"<?= $cb_g('scrape.require_human_review', '1') !== '0' ? ' checked' : '' ?>>
            hold every run for approval (no automatic import)</label>
        </td>
      </tr>
      <tr>
        <th>Plan source</th>
        <td>
          <select name="plan_source" aria-label="Plan source">
            <option value="local"<?= $cb_g('scrape.plan_source', 'local') === 'local' ? ' selected' : '' ?>>local (imported catalog)</option>
            <option value="native"<?= $cb_g('scrape.plan_source') === 'native' ? ' selected' : '' ?>>native (this scraper)</option>
          </select>
        </td>
      </tr>
    </table>
  </div>

  <!-- ───────────── Jev ───────────── -->
  <div class="cb-card">
    <h3>Second opinion (Jev)</h3>
    <p class="cb-card-sub">
      Advisory only: it can move an automatic import to review, never the other way, and never touches a number.
      Any outage is ignored.
    </p>
    <table class="cb-table">
      <tr>
        <th>Enabled</th>
        <td><label><input type="checkbox" name="jev_enabled" value="1"<?= $cb_g('scrape.jev_enabled') === '1' ? ' checked' : '' ?>> consult Jev before an automatic import</label></td>
      </tr>
      <tr>
        <th>API key</th>
        <td>
          <?php if ($jev_key_set): ?>
            <span class="cb-pill good"><span class="dot"></span>set &middot; ENC</span>
            <span class="mono"><?= $esc($jev_key_mask) ?></span>
          <?php else: ?>
            <span class="cb-pill grey">not set</span>
          <?php endif; ?>
          <div class="cb-field">
            <label for="cb-jev-key">Replace key (leave blank to keep)</label>
            <input id="cb-jev-key" type="password" autocomplete="new-password" name="jev_api_key" value="" placeholder="&bull;&bull;&bull;&bull; write-only">
          </div>
        </td>
      </tr>
      <tr>
        <th>Model / confidence</th>
        <td>
          <label>Model <input type="text" name="jev_model" value="<?= $esc($cb_g('scrape.jev_model')) ?>"></label>
          <label>Minimum confidence <input type="number" min="0" max="1" step="0.01" name="jev_confidence_min" value="<?= $esc($cb_g('scrape.jev_confidence_min')) ?>"></label>
        </td>
      </tr>
    </table>
  </div>

  <div class="cb-card">
    <button type="submit" class="cb-btn">Save data sources</button>
  </div>
</form>

</div>
