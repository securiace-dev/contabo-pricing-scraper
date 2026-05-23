<?php
/**
 * Currency support diagnostic (0.5.1) — READ-ONLY.
 *
 * Renders CurrencySupportReport::build(). Surfaces whether any non-INR services
 * exist and whether any are on contabo_pricing-mapped products (the condition
 * that makes the INR-only revenue/snapshot guard an ACTIVE risk rather than
 * latent). No forms, no writes.
 *
 * @var \Closure                 $esc
 * @var string                   $module_link
 * @var array<string,mixed>|null $report
 * @var string|null              $error
 */

$cb_verdict_tone = ['no_non_inr' => 'good', 'non_inr_unmapped' => 'warn', 'non_inr_mapped_active_risk' => 'bad'];

$cb_r       = is_array($report) ? $report : null;
$cb_verdict = $cb_r !== null ? (string) ($cb_r['verdict'] ?? '') : '';
$cb_tone    = $cb_verdict_tone[$cb_verdict] ?? 'warn';
$cb_base    = $cb_r !== null ? (string) ($cb_r['base_currency_code'] ?? '') : '';

$cb_strip_data = $cb_r !== null ? [
    ['lbl' => 'Verdict', 'v' => str_replace('_', ' ', $cb_verdict), 'tone' => $cb_tone],
    ['lbl' => 'Non-INR meaningful', 'v' => (string) (int) ($cb_r['non_inr_meaningful_total'] ?? 0), 'sub' => 'Active / Suspended / Pending'],
    ['lbl' => 'Non-INR on mapped products', 'v' => (string) count($cb_r['non_inr_mapped'] ?? []), 'tone' => (count($cb_r['non_inr_mapped'] ?? []) > 0 ? 'bad' : 'good')],
    ['lbl' => 'Live services on mapped products', 'v' => (string) (int) ($cb_r['mapped_live_services_total'] ?? 0)],
] : [];

require __DIR__ . '/_layout_open.tpl';
?>

<div style="display:flex; align-items:baseline; justify-content:space-between; gap:16px; margin: 4px 0 12px;">
  <div>
    <h2 class="cb-card-title">Currency support</h2>
    <p class="cb-card-sub">Read-only diagnostic. The configurable-option pricing &amp; revenue path is <strong><?= $esc($cb_base !== '' ? $cb_base : 'base-currency') ?>-only</strong> in this version; this page shows whether any non-base-currency services are exposed to it.</p>
  </div>
  <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=maintenance">← Maintenance</a>
</div>

<?php if ($error !== null): ?>
  <div class="cb-error"><?= $esc($error) ?></div>
<?php elseif ($cb_r === null): ?>
  <div class="cb-error">No report data.</div>
<?php else: ?>

  <div class="cb-card cb-<?= $esc($cb_tone === 'good' ? 'flash' : 'error') ?>" style="margin-top:0;">
    <strong><?= $esc(ucfirst(str_replace('_', ' ', $cb_verdict))) ?>.</strong>
    <?php
      $cb_msgs = [
        'no_non_inr'                  => 'No non-INR meaningful services found — the multi-currency guard is fully latent.',
        'non_inr_unmapped'            => 'Non-INR services exist, but none are mapped to contabo_pricing products — latent, not an immediate issue.',
        'non_inr_mapped_active_risk'  => 'Non-INR services are mapped to contabo_pricing products — the multi-currency guard is an ACTIVE risk. Do not enable configurable-product billing for those products until the multi-currency pricing path is complete.',
      ];
      echo $esc($cb_msgs[$cb_verdict] ?? '');
    ?>
  </div>

  <!-- Req 4 labeling: be explicit about what the money column means. -->
  <div class="cb-card" style="border-left:3px solid var(--accent);">
    <p style="margin:0; font-size:12.5px; color:var(--muted);">
      <strong>What the amounts mean:</strong>
      “Service amount” below is the WHMCS service recurring amount
      (<code class="mono">tblhosting.amount</code>) — what the customer is billed today.
      It is <strong>not</strong> the authoritative configurable-option pricing base:
      the pricing base is resolved from <code class="mono">tblpricing</code> /
      the configurable-pricing resolver, not from this column.
    </p>
  </div>

  <div class="cb-card">
    <h3>Currencies</h3>
    <table class="cb-table">
      <thead><tr><th>ID</th><th>Code</th><th>Prefix</th><th class="right">Rate</th><th>Base?</th></tr></thead>
      <tbody>
        <?php foreach (($cb_r['currencies'] ?? []) as $c): ?>
          <tr>
            <td class="mono"><?= $esc((string) (int) $c['id']) ?></td>
            <td><?= $esc((string) $c['code']) ?></td>
            <td class="mono"><?= $esc((string) $c['prefix']) ?></td>
            <td class="right mono"><?= $esc(number_format((float) $c['rate'], 5)) ?></td>
            <td><?php if (!empty($c['is_base'])): ?><span class="cb-pill good"><span class="dot"></span> base / supported</span><?php else: ?><span class="cb-pill grey">other</span><?php endif; ?></td>
          </tr>
        <?php endforeach; ?>
      </tbody>
    </table>
  </div>

  <div class="cb-card">
    <h3>Meaningful services by currency &amp; status</h3>
    <p class="cb-card-sub" style="margin-top:-4px;">Active / Suspended / Pending only. Excluded (Cancelled / Terminated / Fraud) non-base services: <strong><?= $esc((string) (int) ($cb_r['excluded_non_inr_total'] ?? 0)) ?></strong>.</p>
    <?php if (empty($cb_r['meaningful_counts'])): ?>
      <div class="cb-empty"><div class="display">No meaningful services.</div></div>
    <?php else: ?>
      <table class="cb-table">
        <thead><tr><th>Currency</th><th>Status</th><th class="right">Count</th></tr></thead>
        <tbody>
          <?php foreach ($cb_r['meaningful_counts'] as $row): ?>
            <tr>
              <td><?= $esc((string) $row['code']) ?> <span class="muted mono">(<?= $esc((string) (int) $row['currency_id']) ?>)</span></td>
              <td><?= $esc((string) $row['status']) ?></td>
              <td class="right mono"><?= $esc((string) (int) $row['count']) ?></td>
            </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    <?php endif; ?>
  </div>

  <?php if (!empty($cb_r['non_inr_mapped'])): ?>
    <div class="cb-card" style="border-left:3px solid var(--bad);">
      <h3 style="color:var(--bad);">Non-base services on mapped products (active risk)</h3>
      <table class="cb-table">
        <thead><tr><th>Service</th><th>Client</th><th>Product</th><th>Status</th><th>Cycle</th><th class="right">Service amount</th><th>Currency</th></tr></thead>
        <tbody>
          <?php foreach ($cb_r['non_inr_mapped'] as $s): ?>
            <tr>
              <td class="mono">#<?= $esc((string) (int) $s['service_id']) ?></td>
              <td class="mono">#<?= $esc((string) (int) $s['client_id']) ?></td>
              <td class="mono"><?= $esc((string) (int) $s['packageid']) ?></td>
              <td><?= $esc((string) $s['status']) ?></td>
              <td><?= $esc((string) $s['billingcycle']) ?></td>
              <td class="right"><span class="price"><?= $esc(number_format((float) $s['amount'], 2)) ?></span></td>
              <td><?= $esc((string) $s['currency_code']) ?></td>
            </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>
  <?php endif; ?>

<?php endif; ?>

</div><?php /* close .cb-wrap from _layout_open.tpl */ ?>
