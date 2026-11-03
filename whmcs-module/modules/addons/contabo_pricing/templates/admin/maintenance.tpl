<?php
/**
 * Maintenance & schema-health admin page.
 *
 * Three panels:
 *   1. Schema health — green/red, current vs target schema_version + any
 *      missing required columns (from SchemaHealth::requiredColumnsPresent()).
 *   2. Run migrations now — POST action=maintenance-migrate (CSRF).
 *   2b. Re-quote a paid order — POST action=maintenance-requote (CSRF),
 *      field service_id. Lists paid services that have no sealed snapshot.
 *   3. Purge / reset module data — POST action=maintenance-purge (CSRF),
 *      gated by an "I understand" checkbox + a typed confirmation phrase.
 *
 * ── Template vars expected from AdminController ──────────────────────────────
 * @var \Closure $esc                 HTML-escape helper.
 * @var string   $module_link         Base addon URL (action appended via POST).
 * @var array{
 *   healthy:bool,
 *   missing:list<string>,
 *   schema_version:int,
 *   compatibility_mode?:bool,
 *   optional_missing?:list<string>,
 *   publication_catalog_supported?:bool
 * } $cb_schema_health                 SchemaHealth::requiredColumnsPresent().
 * @var int      $cb_schema_target     Installer::SCHEMA_VERSION (target).
 * @var string   $cb_purge_phrase      SchemaHealth::PURGE_CONFIRMATION_PHRASE.
 *
 * ── Form contract for the orchestrator (AdminController handlers) ────────────
 *   maintenance-migrate:
 *     POST fields: action=maintenance-migrate, token (CSRF).
 *     Handler: check_token(); SchemaHealth::assertOrMigrate(); flash result.
 *
 *   maintenance-purge:
 *     POST fields:
 *       action=maintenance-purge
 *       token                          (CSRF, via generate_token()).
 *       purge_confirm_checkbox=1       (the "I understand" checkbox).
 *       purge_confirmation_phrase      (typed text; MUST equal
 *                                       SchemaHealth::PURGE_CONFIRMATION_PHRASE
 *                                       — validate server-side with
 *                                       SchemaHealth::isPurgeConfirmed()).
 *     Handler MUST (server-side):
 *       - check_token();
 *       - require purge_confirm_checkbox is set;
 *       - SchemaHealth::isPurgeConfirmed($_POST['purge_confirmation_phrase']);
 *       - logActivity() the admin id + intent BEFORE deletion;
 *       - take a backup export of all mod_contabo_* tables first;
 *       - TRUNCATE only mod_contabo_* tables (NEVER WHMCS tables);
 *       - reinstall base schema rows (schema_version) afterwards.
 */

$cb_health  = isset($cb_schema_health) && is_array($cb_schema_health)
    ? $cb_schema_health
    : ['healthy' => false, 'missing' => [], 'schema_version' => 0];
$cb_healthy = !empty($cb_health['healthy']);
$cb_missing = isset($cb_health['missing']) && is_array($cb_health['missing'])
    ? $cb_health['missing']
    : [];
$cb_current = isset($cb_health['schema_version']) ? (int) $cb_health['schema_version'] : 0;
$cb_target  = isset($cb_schema_target) ? (int) $cb_schema_target : $cb_current;
$cb_phrase  = isset($cb_purge_phrase) ? (string) $cb_purge_phrase : 'PURGE CONTABO PRICING DATA';
$cb_compatibility_mode = !empty($cb_health['compatibility_mode']);
$cb_optional_missing = isset($cb_health['optional_missing']) && is_array($cb_health['optional_missing'])
    ? $cb_health['optional_missing']
    : [];

$cb_stuck = isset($cb_stuck_services) && is_array($cb_stuck_services) ? $cb_stuck_services : [];
$cb_ttl   = isset($cb_quote_ttl) ? (int) $cb_quote_ttl : 1800;
$cb_preview = isset($cb_requote_preview) && is_array($cb_requote_preview) ? $cb_requote_preview : null;

$cb_flash = isset($_REQUEST['flash']) ? (string) $_REQUEST['flash'] : '';
$cb_err   = isset($_REQUEST['error']) ? (string) $_REQUEST['error'] : '';

$cb_version_ok = $cb_current >= $cb_target;
$cb_all_green  = $cb_healthy && $cb_version_ok;

$cb_strip_data = [
    [
        'lbl'  => 'Schema health',
        'v'    => $cb_all_green ? 'OK' : 'ATTENTION',
        'sub'  => $cb_all_green ? 'all required columns present' : count($cb_missing) . ' column(s) missing',
        'tone' => $cb_all_green ? 'good' : 'bad',
    ],
    [
        'lbl'  => 'Schema version',
        'v'    => 'v' . $cb_current,
        'sub'  => $cb_version_ok ? 'up to date' : 'target v' . $cb_target,
        'tone' => $cb_version_ok ? 'good' : 'warn',
    ],
    [
        'lbl'  => 'Target version',
        'v'    => 'v' . $cb_target,
        'sub'  => 'Installer::SCHEMA_VERSION',
        'tone' => '',
    ],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55">Maintenance</h2>
    <p class="cb-card-sub" data-cb-u="u-1beb43d512">
      Verify the addon's database schema, run pending migrations, and (carefully)
      reset all Contabo Pricing data. Destructive actions are gated by an explicit
      typed confirmation.
    </p>
  </div>
  <div data-cb-u="u-b887bfd543">
    <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=currency-report">Currency support</a>
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=dashboard">&larr; Dashboard</a>
  </div>
</header>

<?php if ($cb_flash !== ''): ?>
  <div class="cb-flash"><?= $esc($cb_flash) ?></div>
<?php endif; ?>
<?php if ($cb_err !== ''): ?>
  <div class="cb-error"><?= $esc($cb_err) ?></div>
<?php endif; ?>

<?php /* ── Panel 1: Schema health ─────────────────────────────────────────── */ ?>
<div class="cb-card">
  <h3>Schema health</h3>
  <div data-cb-u="u-2af59efcc3">
    <?php if ($cb_all_green): ?>
      <span class="cb-pill good"><span class="dot"></span>Healthy</span>
    <?php else: ?>
      <span class="cb-pill bad"><span class="dot"></span>Needs migration</span>
    <?php endif; ?>
    <span data-cb-u="u-84933fa4aa">
      recorded version <span class="mono">v<?= (int) $cb_current ?></span>
      vs target <span class="mono">v<?= (int) $cb_target ?></span>
    </span>
  </div>

  <?php if ($cb_missing === []): ?>
    <p data-cb-u="u-d83761292e">
      All required runtime columns are present on
      <code class="mono">mod_contabo_mapping</code> and
      <code class="mono">mod_contabo_profile</code>.
    </p>
    <?php if ($cb_compatibility_mode): ?>
      <p class="cb-card-sub" data-cb-u="u-d83761292e">
        Compatibility mode is active: publication/catalog tables are absent on this WHMCS schema,
        so catalog import and mapping publication remain disabled until those addon tables exist.
      </p>
      <?php if ($cb_optional_missing !== []): ?>
        <p class="muted">
          Optional capability gap:
          <code class="mono"><?= $esc(implode(', ', $cb_optional_missing)) ?></code>
        </p>
      <?php endif; ?>
    <?php endif; ?>
  <?php else: ?>
    <p data-cb-u="u-919cd4969c">
      The following required columns are missing. Run migrations to repair:
    </p>
    <ul data-cb-u="u-44c90ade49">
      <?php foreach ($cb_missing as $cb_col): ?>
        <li><code class="mono"><?= $esc((string) $cb_col) ?></code></li>
      <?php endforeach; ?>
    </ul>
  <?php endif; ?>
</div>

<?php /* ── Panel 2: Run migrations now ────────────────────────────────────── */ ?>
<div class="cb-card">
  <h3>Run migrations now</h3>
  <p class="cb-card-sub" data-cb-u="u-2b0e8ce8c2">
    Re-runs forward-only migrations up to <code class="mono">v<?= (int) $cb_target ?></code>.
    Migrations are idempotent — running this when already up to date is a safe no-op.
  </p>
  <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-38965f9b18">
    <input type="hidden" name="action" value="maintenance-migrate">
    <?= generate_token() ?>
    <button class="cb-btn" type="submit">Run migrations now</button>
  </form>
</div>

<?php /* ── Panel 2b: Re-quote a paid order ────────────────────────────────── */ ?>
<div class="cb-card">
  <h3>Re-quote a paid order</h3>
  <p class="cb-card-sub">
    A paid VPS service with no sealed snapshot is refused at Create (Activity Log:
    <code class="mono">Contabo provisioning gate ABORTED</code>). Re-quoting rebuilds
    the snapshot from the customer's <strong>own recorded quote</strong> (their mapping
    version; if that publication is gone, the recorded snapshot after an explicit
    confirmation, because its SKU cannot be checked against anything), binds it to the
    Paid invoice that settled the order (the order's invoice, or a mass-pay, merged or
    re-issued invoice linked to it), seals it, and keeps the old row as superseded.
    If no verifiable recorded quote exists it refuses; the only way forward is an
    explicit override that shows what the current mapping would change (SKU, region,
    image, renewal) and is written to the Activity Log. Every re-quote shows the full diff
    first (options, first payment, recurring, tax, total as well), also against what WHMCS
    holds now; if WHMCS has drifted from the recorded quote the confirmation is required.
    Confirming needs a Full Administrator (or a role listed in
    <code class="mono">snapshot.requote_role_ids</code>). It refuses unless a linked
    invoice is Paid. Unpaid checkouts stay quotable for
    <code class="mono"><?= (int) $cb_ttl ?></code>
    seconds or until the invoice due date, whichever is later
    (<code class="mono">snapshot.quote_ttl_seconds</code>).
  </p>
  <?php if ($cb_preview !== null): ?>
    <div class="cb-card">
      <h4>Service #<?= (int) $cb_preview['service_id'] ?>: <?= $esc((string) $cb_preview['code']) ?></h4>
      <p class="cb-card-sub"><?= $esc((string) $cb_preview['message']) ?></p>
      <?php $cb_kind = (string) ($cb_preview['override_kind'] ?? ''); ?>
      <?php if (!empty($cb_preview['deviations'])): ?>
        <p class="cb-card-sub"><strong>WHMCS no longer matches the customer's recorded quote:</strong>
          <?= $esc(implode('; ', array_map('strval', (array) $cb_preview['deviations']))) ?>.
          The quote that will be sealed is the recorded one, not the live service or invoice.</p>
      <?php endif; ?>
      <?php if ((array) ($cb_preview['diff'] ?? []) !== []): ?>
        <table class="cb-table">
          <thead><tr><th>Field</th><th>Customer's recorded quote<?= in_array($cb_kind, ['recorded_snapshot', 'recorded_deviation', ''], true) ? ' (will be sealed)' : '' ?></th><th><?= $cb_kind === 'current_mapping' ? 'Current mapping ' . $esc((string) $cb_preview['mapping_version']) . ' (will be sealed)' : 'Current mapping' ?></th><th>WHMCS now</th></tr></thead>
          <tbody>
          <?php foreach ((array) $cb_preview['diff'] as $cb_d): ?>
            <tr>
              <td><?= $esc((string) $cb_d['field']) ?></td>
              <td class="mono"><?= $esc((string) $cb_d['old']) ?></td>
              <td class="mono"><?= !empty($cb_d['changed']) ? '<strong>' : '' ?><?= $esc((string) $cb_d['new']) ?><?= !empty($cb_d['changed']) ? '</strong>' : '' ?></td>
              <td class="mono"><?= array_key_exists('live', $cb_d) ? (!empty($cb_d['live_differs']) ? '<strong>' : '') . $esc((string) $cb_d['live']) . (!empty($cb_d['live_differs']) ? '</strong>' : '') : '' ?></td>
            </tr>
          <?php endforeach; ?>
          </tbody>
        </table>
      <?php endif; ?>
      <?php if (!empty($cb_preview['needs_override'])): ?>
        <?php if (!empty($cb_preview['may_confirm'])): ?>
        <form method="post" action="<?= $esc($module_link) ?>">
          <input type="hidden" name="action" value="maintenance-requote">
          <input type="hidden" name="service_id" value="<?= (int) $cb_preview['service_id'] ?>">
          <input type="hidden" name="override" value="1">
          <input type="hidden" name="diff_token" value="<?= $esc((string) $cb_preview['diff_token']) ?>">
          <?= generate_token() ?>
          <button class="cb-btn" type="submit"><?php
            echo $cb_kind === 'recorded_snapshot' ? 'Confirm: seal the recorded quote'
                : ($cb_kind === 'recorded_deviation' ? 'Confirm: seal the recorded quote although WHMCS differs'
                : 'Confirm override with the current mapping');
          ?></button>
        </form>
        <?php else: ?>
        <p class="cb-card-sub"><strong>Your admin role may not confirm this.</strong>
          Ask a Full Administrator (or a role listed in <code class="mono">snapshot.requote_role_ids</code>).</p>
        <?php endif; ?>
      <?php elseif (in_array((string) $cb_preview['code'], ['ready'], true)): ?>
        <form method="post" action="<?= $esc($module_link) ?>">
          <input type="hidden" name="action" value="maintenance-requote">
          <input type="hidden" name="service_id" value="<?= (int) $cb_preview['service_id'] ?>">
          <?= generate_token() ?>
          <button class="cb-btn" type="submit">Re-quote: seal exactly this quote</button>
        </form>
      <?php endif; ?>
    </div>
  <?php endif; ?>
  <?php if ($cb_stuck !== []): ?>
    <table class="cb-table">
      <thead><tr><th>Service</th><th>Order</th><th>Paid invoice</th><th>Snapshot rows</th><th>Status</th><th></th></tr></thead>
      <tbody>
      <?php foreach ($cb_stuck as $cb_row): ?>
        <tr>
          <td class="mono">#<?= (int) $cb_row['service_id'] ?></td>
          <td class="mono">#<?= (int) $cb_row['order_id'] ?></td>
          <td class="mono">#<?= (int) $cb_row['invoice_id'] ?><?= (string) ($cb_row['via'] ?? '') !== '' && $cb_row['via'] !== 'order' ? ' (' . $esc((string) $cb_row['via']) . ')' : '' ?></td>
          <td><?= $esc((string) $cb_row['states']) ?></td>
          <td>
            <?php if (($cb_row['status'] ?? '') === 'refused'): ?>
              <strong>Refused:</strong> <?= $esc((string) ($cb_row['reason'] ?? '')) ?>
            <?php elseif (($cb_row['status'] ?? '') === 'override'): ?>
              <?= in_array(($cb_row['override_kind'] ?? ''), ['recorded_snapshot', 'recorded_deviation'], true) ? 'Needs confirmation' : 'Needs override' ?>: <?= $esc((string) ($cb_row['reason'] ?? '')) ?>
            <?php else: ?>
              Ready (<?= $esc((string) ($cb_row['source'] ?? '')) ?>)
            <?php endif; ?>
          </td>
          <td>
            <?php if (($cb_row['status'] ?? '') !== 'refused'): ?>
            <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=maintenance&amp;preview_service=<?= (int) $cb_row['service_id'] ?>">Review</a>
            <?php endif; ?>
          </td>
        </tr>
      <?php endforeach; ?>
      </tbody>
    </table>
  <?php else: ?>
    <p class="cb-card-sub">No paid service is waiting on a snapshot.</p>
  <?php endif; ?>
  <form method="post" action="<?= $esc($module_link) ?>">
    <input type="hidden" name="action" value="maintenance-requote">
    <?= generate_token() ?>
    <label>Service ID <input type="number" min="1" name="service_id" class="mono" required></label>
    <button class="cb-btn" type="submit">Re-quote service</button>
  </form>
</div>

<?php /* ── Panel 3: Purge / reset module data ─────────────────────────────── */ ?>
<div class="cb-card" data-cb-u="u-3bf30616da">
  <h3 data-cb-u="u-cf716c9f6e">Purge / reset module data</h3>

  <div class="cb-error" data-cb-u="u-1294519fc5">
    <strong>This truncates <code class="mono">mod_contabo_*</code> tables only.</strong>
    It does <strong>NOT</strong> touch WHMCS clients, services, invoices, or transactions.
    A backup export is taken first.
  </div>

  <p data-cb-u="u-f2f2d4e94a">
    Not sure what a purge would remove? <strong>Preview the exact blast radius first</strong> —
    this dry-run deletes nothing and needs no confirmation phrase.
  </p>
  <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-7b8b5d8af2">
    <input type="hidden" name="action" value="maintenance-purge">
    <input type="hidden" name="purge_dry_run" value="1">
    <?= generate_token() ?>
    <button class="cb-btn subtle" type="submit">Preview purge (dry-run)</button>
  </form>

  <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-38965f9b18"
        data-cb-form="maintenance-purge"
        data-cb-confirm="This will delete ALL Contabo Pricing addon data. Continue?">
    <input type="hidden" name="action" value="maintenance-purge">
    <?= generate_token() ?>

    <label data-cb-u="u-8cc56ae6d5">
      <input type="checkbox" name="purge_confirm_checkbox" value="1" required data-cb-u="u-c8590d693a">
      <span>I understand this deletes all Contabo Pricing addon data.</span>
    </label>

    <label data-cb-u="u-8cc56ae6d5">
      <input type="checkbox" name="purge_config_objects" value="1" data-cb-u="u-c8590d693a">
      <span>
        <strong>Also delete the WHMCS configurable options this addon created.</strong>
        Removes only the option groups / options / sub-options / pricing rows recorded
        in the addon's link tables (i.e. created by Apply) and the product links to them.
        It never touches a configurable option the addon didn't create, nor any client,
        service, invoice or order. Leave unticked to keep them and clear only
        <code class="mono">mod_contabo_*</code> data. Runs <em>before</em> the truncate
        so the ownership records are still available.
      </span>
    </label>

    <div class="cb-field" data-cb-u="u-d3d0db89cc">
      <label for="cb-purge-phrase">
        Type <code class="mono"><?= $esc($cb_phrase) ?></code> to confirm
      </label>
      <input id="cb-purge-phrase"
             type="text"
             name="purge_confirmation_phrase"
             autocomplete="off"
             spellcheck="false"
             placeholder="<?= $esc($cb_phrase) ?>"
             required>
    </div>

    <button class="cb-btn danger" type="submit">Purge module data</button>
  </form>
</div>

</div>
