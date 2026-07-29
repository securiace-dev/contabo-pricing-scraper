<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Read-only settings view — sectioned cards.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var object   $settings  Settings object (apiBaseUrl, apiToken, defaultSyncStrategy,
 *                          currencyIso, applyGst18, fxMarkupPct, logRetentionDays, moduleLink).
 */

$cb_api_url     = isset($settings->apiBaseUrl) ? (string) $settings->apiBaseUrl : '';
$cb_api_token   = isset($settings->apiToken) ? (string) $settings->apiToken : '';
$cb_sync_strat  = isset($settings->defaultSyncStrategy) ? (string) $settings->defaultSyncStrategy : '';
$cb_currency    = isset($settings->currencyIso) ? (string) $settings->currencyIso : '';
$cb_apply_gst   = !empty($settings->applyGst18);
$cb_fx_markup   = isset($settings->fxMarkupPct) ? (float) $settings->fxMarkupPct : 0.0;
$cb_log_ret     = isset($settings->logRetentionDays) ? (int) $settings->logRetentionDays : 0;
$cb_token_set   = $cb_api_token !== '';
?>

<!-- ───────────────────── Header ───────────────────── -->
<div class="cb-card">
  <h2 class="cb-card-title display" data-cb-u="u-ab79ea2b85">Settings</h2>
  <p class="cb-card-sub" data-cb-u="u-5074b45d61">
    Edit these values from <strong>Setup → Addon Modules → Contabo Pricing Sync → Configure</strong>.
    Values shown here are read-only.
  </p>
</div>

<!-- ───────────────────── API connection ───────────────────── -->
<div class="cb-card">
  <h3>API connection</h3>
  <table class="cb-table" data-cb-u="u-eb2cdaa55e">
    <tr>
      <th data-cb-u="u-e104eb5fa2">API base URL</th>
      <td>
        <?php if ($cb_api_url !== ''): ?>
          <code class="mono" data-cb-u="u-7e6609e5f6"><?= $esc($cb_api_url) ?></code>
        <?php else: ?>
          <span class="cb-pill bad"><span class="dot"></span>not set</span>
        <?php endif; ?>
      </td>
    </tr>
    <tr>
      <th>API token</th>
      <td>
        <?php if ($cb_token_set): ?>
          <span class="cb-pill good"><span class="dot"></span>ENC · encrypted at rest</span>
          <span class="kbd" data-cb-u="u-9d5367afed">stored</span>
        <?php else: ?>
          <span class="cb-pill bad"><span class="dot"></span>not set</span>
        <?php endif; ?>
      </td>
    </tr>
    <tr>
      <th>Connectivity</th>
      <td>
        <button type="button"
                class="cb-btn subtle<?= $cb_token_set && $cb_api_url !== '' ? '' : ' disabled' ?>"
                <?= $cb_token_set && $cb_api_url !== '' ? '' : 'disabled' ?>
                data-cb-action="test-api-connection"
                data-cb-target="cb-api-test-result">
          Test connection
        </button>
        <span data-cb-result="cb-api-test-result" role="status" aria-live="polite"
              data-cb-u="u-a40f776248"></span>
      </td>
    </tr>
  </table>
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
          <span class="cb-pill good"><span class="dot"></span>yes</span>
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
    Source: <span class="mono">/api/v1/fx</span> · markup of <span class="mono"><?= $esc(number_format($cb_fx_markup, 2)) ?>%</span> applied separately during sync.
  </p>
</div>

</div>
