<?php
/**
 * Tax-mode settings.
 *
 * Read-only table of the 8 modes (active row highlighted) + a write-enabled
 * form for `tax_registration_mode`, vendor tax rate, output tax rate, and
 * the recoverability / inclusivity flags. POST → tax-settings-save which
 * calls check_token() and persists via Capsule::updateOrInsert.
 *
 * Worked example block uses the plan-default numbers:
 *   cost ₹850 + 18% vendor tax + ₹20 buffer = ₹1,023 landed (monthly)
 *   gross ₹14,400 annually − 0% output tax = ₹14,400 net revenue
 *   margin = ₹2,124 on ₹14,400 ⇒ 14.75%
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array{
 *   tax_registration_mode:string,
 *   vendor_tax_rate_pct:string,
 *   vendor_tax_recoverable:string,
 *   charge_output_tax_to_client:string,
 *   prices_include_output_tax:string,
 *   output_tax_rate_pct:string,
 * } $current
 * @var list<string> $modes
 * @var list<array{mode:string,label:string,output_tax_charged:bool,vendor_tax_recoverable:bool,prices_include_output_tax:bool}> $mode_summaries
 */

$cb_active = (string) ($current['tax_registration_mode'] ?? '');
$cb_flash = isset($_REQUEST['flash']) ? (string) $_REQUEST['flash'] : '';

// Strip — surface the 6 active values so the admin can confirm at a glance.
$cb_strip_data = [
    [
        'lbl' => 'Active mode',
        'v'   => $cb_active === '' ? '—' : $cb_active,
        'sub' => '8 modes supported',
        'tone' => $cb_active === 'unregistered_no_output_tax' ? 'grey' : 'good',
    ],
    [
        'lbl' => 'Vendor tax %',
        'v'   => (string) ($current['vendor_tax_rate_pct'] ?? '0.00'),
        'sub' => ((string) ($current['vendor_tax_recoverable'] ?? '0')) === '1' ? 'recoverable' : 'non-recoverable',
        'tone' => '',
    ],
    [
        'lbl' => 'Output tax %',
        'v'   => (string) ($current['output_tax_rate_pct'] ?? '0.00'),
        'sub' => ((string) ($current['charge_output_tax_to_client'] ?? '0')) === '1' ? 'charged to client' : 'not charged',
        'tone' => '',
    ],
    [
        'lbl' => 'Prices include tax',
        'v'   => ((string) ($current['prices_include_output_tax'] ?? '0')) === '1' ? 'yes' : 'no',
        'sub' => 'affects net-revenue math',
        'tone' => '',
    ],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55">Tax settings</h2>
    <p class="cb-card-sub" data-cb-u="u-8c7c145b64">
      Pluggable tax-recovery mode used by <code class="mono">MarginCalculator</code>. Every decision row
      records a <code class="mono">tax_mode_snapshot</code>, so historical margin doesn't change when this is flipped.
    </p>
  </div>
  <div data-cb-u="u-b887bfd543">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=repricing">&larr; Dashboard</a>
  </div>
</header>

<?php if ($cb_flash !== ''): ?>
  <div class="cb-flash"><?= $esc($cb_flash) ?></div>
<?php endif; ?>

<?php /* Modes table ------------------------------------------------------- */ ?>
<div class="cb-card">
  <h3>Supported modes</h3>
  <div data-cb-u="u-e2c857d54b">
    <table class="cb-table" data-cb-u="u-b7aa1ec004">
      <thead>
        <tr>
          <th>Mode</th>
          <th>Description</th>
          <th>Output tax</th>
          <th>Vendor tax recoverable</th>
          <th>Prices include output tax</th>
        </tr>
      </thead>
      <tbody>
        <?php foreach ($mode_summaries as $row):
          $isActive = ($row['mode'] === $cb_active);
        ?>
        <tr class="<?= $isActive ? 'cb-row-active' : '' ?>">
          <td>
            <code class="mono"><?= $esc($row['mode']) ?></code>
            <?php if ($isActive): ?>
              <span class="cb-pill good" data-cb-u="u-451d13ae38"><span class="dot"></span>active</span>
            <?php endif; ?>
          </td>
          <td><?= $esc($row['label']) ?></td>
          <td><?= !empty($row['output_tax_charged']) ? '<span class="cb-pill good">yes</span>' : '<span class="cb-pill grey">no</span>' ?></td>
          <td><?= !empty($row['vendor_tax_recoverable']) ? '<span class="cb-pill good">yes</span>' : '<span class="cb-pill grey">no</span>' ?></td>
          <td><?= !empty($row['prices_include_output_tax']) ? '<span class="cb-pill good">yes</span>' : '<span class="cb-pill grey">no</span>' ?></td>
        </tr>
        <?php endforeach; ?>
      </tbody>
    </table>
  </div>
</div>

<?php /* Worked example --------------------------------------------------- */ ?>
<div class="cb-card">
  <h3>Worked example — unregistered, no output tax</h3>
  <p class="cb-card-sub" data-cb-u="u-2b0e8ce8c2">
    Numbers match the plan's reference scenario: vendor base ₹850 / month, 18% German VAT on the Contabo invoice
    (non-recoverable in India until you register), ₹20 of FX + payment buffer, sold annually at ₹14,400.
  </p>
  <div data-cb-u="u-99d46bf47c">
    <div class="cb-inset" data-cb-u="u-ca724ea71f">
      <div class="cb-card-sub" data-cb-u="u-d886b9a9db">Landed cost (monthly)</div>
      <div class="mono" data-cb-u="u-c680fb2cb2">
        850.00 base<br>
        + 153.00 vendor VAT (18%)<br>
        + 20.00 FX + payment buffer<br>
        <strong>= 1,023.00 / mo</strong>
      </div>
    </div>
    <div class="cb-inset" data-cb-u="u-ca724ea71f">
      <div class="cb-card-sub" data-cb-u="u-d886b9a9db">Landed cost (annually)</div>
      <div class="mono" data-cb-u="u-c680fb2cb2">
        1,023.00 × 12<br>
        <strong>= 12,276.00 / yr</strong>
      </div>
    </div>
    <div class="cb-inset" data-cb-u="u-ca724ea71f">
      <div class="cb-card-sub" data-cb-u="u-d886b9a9db">Revenue (annually)</div>
      <div class="mono" data-cb-u="u-c680fb2cb2">
        14,400.00 gross<br>
        − 0% output tax<br>
        <strong>= 14,400.00 net</strong>
      </div>
    </div>
    <div class="cb-inset" data-cb-u="u-b521a014bd">
      <div class="cb-card-sub" data-cb-u="u-d886b9a9db">Margin</div>
      <div class="mono" data-cb-u="u-c680fb2cb2">
        14,400 − 12,276 = <strong>2,124</strong><br>
        2,124 / 14,400 = <span class="cb-pill good"><strong>14.75%</strong></span>
      </div>
    </div>
  </div>
</div>

<?php /* Write form -------------------------------------------------------- */ ?>
<div class="cb-card">
  <h3>Edit active settings</h3>
  <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-38965f9b18">
    <input type="hidden" name="action" value="tax-settings-save">
    <?= generate_token() ?>

    <div data-cb-u="u-5f28e5f494">
      <div class="cb-field">
        <label for="cb-tax-mode">Tax registration mode</label>
        <select id="cb-tax-mode" name="tax_registration_mode">
          <?php foreach ($modes as $cb_m): ?>
            <option value="<?= $esc($cb_m) ?>"<?= ($cb_active === $cb_m ? ' selected' : '') ?>><?= $esc($cb_m) ?></option>
          <?php endforeach; ?>
        </select>
      </div>

      <div class="cb-field">
        <label for="cb-vtr">Vendor tax rate (%)</label>
        <input id="cb-vtr" type="number" step="0.01" min="0" max="100"
               name="vendor_tax_rate_pct"
               value="<?= $esc((string) ($current['vendor_tax_rate_pct'] ?? '18.00')) ?>">
      </div>

      <div class="cb-field">
        <label for="cb-vtr-rec">Vendor tax recoverable</label>
        <select id="cb-vtr-rec" name="vendor_tax_recoverable">
          <option value="0"<?= (((string) ($current['vendor_tax_recoverable'] ?? '0')) === '0' ? ' selected' : '') ?>>No (lands as cost)</option>
          <option value="1"<?= (((string) ($current['vendor_tax_recoverable'] ?? '0')) === '1' ? ' selected' : '') ?>>Yes (input-tax credit)</option>
        </select>
      </div>

      <div class="cb-field">
        <label for="cb-charge-out">Charge output tax to client</label>
        <select id="cb-charge-out" name="charge_output_tax_to_client">
          <option value="0"<?= (((string) ($current['charge_output_tax_to_client'] ?? '0')) === '0' ? ' selected' : '') ?>>No</option>
          <option value="1"<?= (((string) ($current['charge_output_tax_to_client'] ?? '0')) === '1' ? ' selected' : '') ?>>Yes</option>
        </select>
      </div>

      <div class="cb-field">
        <label for="cb-incl-out">Prices include output tax</label>
        <select id="cb-incl-out" name="prices_include_output_tax">
          <option value="0"<?= (((string) ($current['prices_include_output_tax'] ?? '0')) === '0' ? ' selected' : '') ?>>No (tax added on top)</option>
          <option value="1"<?= (((string) ($current['prices_include_output_tax'] ?? '0')) === '1' ? ' selected' : '') ?>>Yes (gross-inclusive)</option>
        </select>
      </div>

      <div class="cb-field">
        <label for="cb-out-rate">Output tax rate (%)</label>
        <input id="cb-out-rate" type="number" step="0.01" min="0" max="100"
               name="output_tax_rate_pct"
               value="<?= $esc((string) ($current['output_tax_rate_pct'] ?? '0.00')) ?>">
      </div>
    </div>

    <div data-cb-u="u-88cfb290e4">
      <button type="submit" class="cb-btn">Save tax settings</button>
      <span data-cb-u="u-685394115d">
        Saving emits an audit row in <code class="mono">mod_contabo_pricing_action</code> (action_type = phase_changed).
      </span>
    </div>
  </form>
</div>

</div>
