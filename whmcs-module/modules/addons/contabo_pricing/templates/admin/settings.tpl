<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Runtime/config reference view — sectioned cards.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var object   $settings  Settings object (defaultSyncStrategy,
 *                          currencyIso, applyGst18, fxMarkupPct, logRetentionDays, moduleLink).
 */

$cb_sync_strat  = isset($settings->defaultSyncStrategy) ? (string) $settings->defaultSyncStrategy : '';
$cb_currency    = isset($settings->currencyIso) ? (string) $settings->currencyIso : '';
$cb_apply_gst   = !empty($settings->applyGst18);
$cb_fx_markup   = isset($settings->fxMarkupPct) ? (float) $settings->fxMarkupPct : 0.0;
$cb_log_ret     = isset($settings->logRetentionDays) ? (int) $settings->logRetentionDays : 0;
$cb_flash       = isset($flash) ? (string) $flash : '';
$cb_term_policy = isset($term_policy) ? (string) $term_policy : 'nearest-term';
$cb_term_addon  = isset($term_addon_policy) ? (string) $term_addon_policy : 'proportional';
$cb_term_adjust = isset($term_adjust_pct_json) ? (string) $term_adjust_pct_json : '{}';
?>

<!-- ───────────────────── Header ───────────────────── -->
<div class="cb-card">
  <h2 class="cb-card-title display" data-cb-u="u-ab79ea2b85">Runtime &amp; configuration</h2>
  <p class="cb-card-sub" data-cb-u="u-5074b45d61">
    Edit these values from <strong>Setup → Addon Modules → Contabo Pricing Sync → Configure</strong>.
    Values shown here are read-only.
    Scraper providers, budgets and review rules live under
    <a href="<?= $esc($module_link) ?>&amp;action=data-sources"><strong>Data sources</strong></a>.
  </p>
</div>

<!-- ───────────────────── Sync behaviour ───────────────────── -->
<div class="cb-card">
  <h3>Sync behaviour</h3>
  <table class="cb-table" data-cb-u="u-eb2cdaa55e">
    <tr>
      <th data-cb-u="u-e104eb5fa2">Default sync strategy</th>
      <td>
        <?php if ($cb_sync_strat !== ''): ?>
          <span class="cb-pill"><?= $esc($cb_sync_strat) ?></span>
        <?php else: ?>
          <span class="cb-pill grey">—</span>
        <?php endif; ?>
      </td>
    </tr>
    <tr>
      <th>Log retention</th>
      <td>
        <span class="mono"><?= (int) $cb_log_ret ?></span> days
        <?php if ($cb_log_ret <= 0): ?>
          <span class="cb-pill warn" data-cb-u="u-9d5367afed">logs kept indefinitely</span>
        <?php endif; ?>
      </td>
    </tr>
  </table>
</div>

<!-- ───────────────────── Currency & tax ───────────────────── -->
<div class="cb-card">
  <h3>Currency &amp; tax</h3>
  <table class="cb-table" data-cb-u="u-eb2cdaa55e">
    <tr>
      <th data-cb-u="u-e104eb5fa2">Base currency</th>
      <td>
        <?php if ($cb_currency !== ''): ?>
          <span class="cb-pill"><span class="mono"><?= $esc($cb_currency) ?></span></span>
        <?php else: ?>
          <span class="cb-pill grey">—</span>
        <?php endif; ?>
      </td>
    </tr>
    <tr>
      <th>Apply 18% GST</th>
      <td>
        <?php if ($cb_apply_gst): ?>
          <span class="cb-pill warn"><span class="dot"></span>yes</span>
          <div class="muted">Securiace is not GST registered. Set this to No under Setup → Addon Modules → Contabo Pricing Sync → Configure. The term-translation engine never adds GST.</div>
        <?php else: ?>
          <span class="cb-pill grey">no</span>
        <?php endif; ?>
      </td>
    </tr>
    <tr>
      <th>FX markup</th>
      <td>
        <span class="mono"><?= $esc(number_format($cb_fx_markup, 2)) ?>%</span>
        <?php if ($cb_fx_markup > 0): ?>
          <span data-cb-u="u-68abf08f77">applied on top of mid-market rate</span>
        <?php endif; ?>
      </td>
    </tr>
  </table>
</div>

<!-- ───────────────────── FX live preview ───────────────────── -->
<div class="cb-card">
  <h3>FX live preview</h3>
  <div data-cb-fx-preview
       data-cb-fx-base="EUR"
       data-cb-fx-quote="<?= $esc($cb_currency !== '' ? $cb_currency : 'INR') ?>"
       data-cb-u="u-0dfbf8c0b4">
    <div>
      <span class="glabel" data-cb-u="u-6c850f109c">EUR → <?= $esc($cb_currency !== '' ? $cb_currency : 'INR') ?></span>
      <div class="mono" data-cb-u="u-5360f3178f" data-cb-fx-rate>
        <span data-cb-u="u-6c850f109c">…</span>
      </div>
    </div>
    <div data-cb-u="u-e1612f0331">
      last fetched
      <span class="mono" data-cb-fx-fetched-at>—</span>
    </div>
    <div data-cb-u="u-ca6fc035af">
      <button type="button"
              class="cb-btn ghost"
              data-cb-action="fx-refresh"
              data-cb-target="cb-fx-preview">
        Refresh
      </button>
    </div>
  </div>
  <p data-cb-u="u-9e4546b1ac">
    Source: Frankfurter (cached) · markup of <span class="mono"><?= $esc(number_format($cb_fx_markup, 2)) ?>%</span> applied separately during sync.
  </p>
</div>

<?php if ($cb_flash !== ''): ?>
<div class="cb-card">
  <p><?= $esc($cb_flash) ?></p>
</div>
<?php endif; ?>

<!-- ───────────────────── Term translation ───────────────────── -->
<div class="cb-card">
  <h3>Term translation</h3>
  <p class="cb-card-sub">
    Contabo publishes terms up to 12 months. Any other profile period N is priced
    from the nearest published term at or below N (default) or proportionally from
    the monthly list price. Defaults keep existing catalog prices. WHMCS can bill
    only 1/3/6/12/24/36-month cycles; other N is for headline periods and quotes.
  </p>
  <form method="post" action="<?= $esc($module_link) ?>">
    <input type="hidden" name="action" value="settings-save">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>
    <table class="cb-table">
      <tr>
        <th><label for="cb-term-policy">Base-plan policy</label></th>
        <td>
          <select id="cb-term-policy" name="term_policy">
            <option value="nearest-term"<?= $cb_term_policy === 'nearest-term' ? ' selected' : '' ?>>nearest-term (default — use published rate at or below N)</option>
            <option value="proportional"<?= $cb_term_policy === 'proportional' ? ' selected' : '' ?>>proportional (list monthly × N, no term discount)</option>
          </select>
        </td>
      </tr>
      <tr>
        <th><label for="cb-term-addon-policy">Add-on policy</label></th>
        <td>
          <select id="cb-term-addon-policy" name="term_addon_policy">
            <option value="proportional"<?= $cb_term_addon === 'proportional' ? ' selected' : '' ?>>proportional (default — monthly × N unless a native term exists)</option>
            <option value="nearest-term"<?= $cb_term_addon === 'nearest-term' ? ' selected' : '' ?>>nearest-term (use a native per-term add-on map when present)</option>
          </select>
        </td>
      </tr>
      <tr>
        <th><label for="cb-term-adjust">Extra derived-term adjustment %</label></th>
        <td>
          <input id="cb-term-adjust" name="term_adjust_pct_json" type="text" class="mono" size="40" value="<?= $esc($cb_term_adjust) ?>">
          <div class="muted">JSON object of months → percent off the derived monthly rate, e.g. <span class="mono">{"36":5}</span>. 0–50. Native Contabo terms are never adjusted. Default <span class="mono">{}</span> changes no price.</div>
        </td>
      </tr>
    </table>
    <p><button type="submit" class="cb-btn primary">Save term policy</button></p>
  </form>
</div>

</div>
