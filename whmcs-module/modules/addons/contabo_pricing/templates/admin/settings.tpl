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
  <h2 class="cb-card-title display" style="margin:0">Settings</h2>
  <p class="cb-card-sub" style="margin:6px 0 0">
    Edit these values from <strong>Setup → Addon Modules → Contabo Pricing Sync → Configure</strong>.
    Values shown here are read-only.
  </p>
</div>

<!-- ───────────────────── API connection ───────────────────── -->
<div class="cb-card">
  <h3>API connection</h3>
  <table class="cb-table" style="margin-top:4px">
    <tr>
      <th style="width:220px">API base URL</th>
      <td>
        <?php if ($cb_api_url !== ''): ?>
          <code class="mono" style="font-size:12.5px"><?= $esc($cb_api_url) ?></code>
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
          <span class="kbd" style="margin-left:8px">stored</span>
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
        <span data-cb-result="cb-api-test-result" style="margin-left:10px; font-size:12.5px; color:var(--muted)"></span>
      </td>
    </tr>
  </table>
</div>

<!-- ───────────────────── Sync behaviour ───────────────────── -->
<div class="cb-card">
  <h3>Sync behaviour</h3>
  <table class="cb-table" style="margin-top:4px">
    <tr>
      <th style="width:220px">Default sync strategy</th>
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
          <span class="cb-pill warn" style="margin-left:8px">logs kept indefinitely</span>
        <?php endif; ?>
      </td>
    </tr>
  </table>
</div>

<!-- ───────────────────── Currency & tax ───────────────────── -->
<div class="cb-card">
  <h3>Currency &amp; tax</h3>
  <table class="cb-table" style="margin-top:4px">
    <tr>
      <th style="width:220px">Base currency</th>
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
          <span style="color:var(--muted); font-size:12px; margin-left:6px">applied on top of mid-market rate</span>
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
       style="display:flex; align-items:baseline; gap:14px; flex-wrap:wrap">
    <div>
      <span class="glabel" style="color:var(--muted-soft)">EUR → <?= $esc($cb_currency !== '' ? $cb_currency : 'INR') ?></span>
      <div class="mono" style="font-size:22px; color:var(--accent); margin-top:2px" data-cb-fx-rate>
        <span style="color:var(--muted-soft)">…</span>
      </div>
    </div>
    <div style="color:var(--muted); font-size:12px">
      last fetched
      <span class="mono" data-cb-fx-fetched-at>—</span>
    </div>
    <div style="margin-left:auto">
      <button type="button"
              class="cb-btn ghost"
              data-cb-action="fx-refresh"
              data-cb-target="cb-fx-preview">
        Refresh
      </button>
    </div>
  </div>
  <p style="margin:10px 0 0; color:var(--muted); font-size:11.5px">
    Source: <span class="mono">/api/v1/fx</span> · markup of <span class="mono"><?= $esc(number_format($cb_fx_markup, 2)) ?>%</span> applied separately during sync.
  </p>
</div>

</div>
