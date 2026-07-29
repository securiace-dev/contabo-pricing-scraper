<?php
/**
 * Maintenance & schema-health admin page.
 *
 * Three panels:
 *   1. Schema health — green/red, current vs target schema_version + any
 *      missing required columns (from SchemaHealth::requiredColumnsPresent()).
 *   2. Run migrations now — POST action=maintenance-migrate (CSRF).
 *   3. Purge / reset module data — POST action=maintenance-purge (CSRF),
 *      gated by an "I understand" checkbox + a typed confirmation phrase.
 *
 * ── Template vars expected from AdminController ──────────────────────────────
 * @var \Closure $esc                 HTML-escape helper.
 * @var string   $module_link         Base addon URL (action appended via POST).
 * @var array{
 *   healthy:bool,
 *   missing:list<string>,
 *   schema_version:int
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
