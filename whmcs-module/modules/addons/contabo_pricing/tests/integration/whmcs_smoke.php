<?php
/**
 * Real-WHMCS integration smoke for the contabo_pricing addon.
 *
 * Run INSIDE the dockerised dev WHMCS container, e.g.:
 *
 *   docker exec -i securiace-vps-platform-whmcs8-php-1 \
 *     php /var/www/html/modules/addons/contabo_pricing/tests/integration/whmcs_smoke.php
 *
 * The unit suite runs against FakeCapsule, which returns arrays. Real WHMCS
 * (real Capsule) returns stdClass, and the container can be running STALE code.
 * Both have repeatedly masked bugs that only surface end-to-end. This script
 * exercises the real apply / observe / drift path against the live dev WHMCS so
 * those slips are caught by one command (scripts/whmcs-integration-smoke.sh).
 *
 * It writes a UNIQUE throwaway profile/product/group per run (ids derived from
 * time()), asserts the exposure + drift + observe behaviour, and then best-effort
 * deletes everything it created so repeat runs stay clean.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion,
 * no named args.
 *
 * @license MIT
 */

declare(strict_types=1);

// ── Bootstrap real WHMCS + the addon's ContaboPricing\ stub autoloader ───────
chdir('/var/www/html');
require 'init.php';

$contaboLibDir = getenv('CONTABO_ADDON_LIB_DIR') ?: '/var/www/html/modules/addons/contabo_pricing/lib';
spl_autoload_register(static function (string $class) use ($contaboLibDir): void {
    if (strpos($class, 'ContaboPricing\\') === 0) {
        $rel  = str_replace(array('ContaboPricing\\', '\\'), array('', '/'), $class);
        $path = $contaboLibDir . '/' . $rel . '.php';
        if (is_file($path)) {
            require_once $path;
        }
    }
}, true, true);

use ContaboPricing\ConfigOptionLinkRepository;
use ContaboPricing\ConfigOptionPricingContext;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\DriftHasher;
use ContaboPricing\Installer;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\ProposalMaker;
use ContaboPricing\SchemaHealth;
use ContaboPricing\Settings;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use WHMCS\Database\Capsule;

// ── Tiny PASS/FAIL harness ───────────────────────────────────────────────────
$GLOBALS['__smoke_failures'] = 0;
$GLOBALS['__smoke_passes']   = 0;

/**
 * @param mixed $cond truthy = pass
 */
function smoke_assert($cond, string $msg): bool
{
    if ($cond) {
        $GLOBALS['__smoke_passes']++;
        echo "  PASS  " . $msg . "\n";
        return true;
    }
    $GLOBALS['__smoke_failures']++;
    echo "  FAIL  " . $msg . "\n";
    return false;
}

function smoke_section(string $title): void
{
    echo "\n== " . $title . " ==\n";
}

function smoke_note(string $msg): void
{
    echo "  ..    " . $msg . "\n";
}

// ── Unique throwaway identity for this run ───────────────────────────────────
$runId      = time();
$rand       = substr(bin2hex(random_bytes(3)), 0, 6);
$profileId  = 900000000 + ($runId % 90000000);     // huge, never collides with real profiles
$productId  = 900000000 + (($runId + 7) % 90000000);
$groupKey   = 'smoke-' . $runId . '-' . $rand;
$groupName  = 'CONTABO SMOKE ' . $runId . ' ' . $rand;
$batchId    = 'smoke-' . $runId . '-' . $rand;

echo "Contabo Pricing — real-WHMCS integration smoke\n";
echo "profile_id=$profileId product_id=$productId group='$groupName'\n";

/** Build a fresh non-dry-run syncer wired to a real adapter + link repo. */
$links = new ConfigOptionLinkRepository();
$makeSyncer = static function (bool $dryRun) use ($batchId, $links): ConfigurableOptionsSyncer {
    $adapter = new WhmcsConfigOptionsAdapter($dryRun, $batchId);
    $audit   = new OptionAuditLog($batchId);
    return new ConfigurableOptionsSyncer($adapter, $audit, $links);
};

// Pricing context: INR base currency (1), positive landed multiplier, simple markup.
$ctx = new ConfigOptionPricingContext(
    WhmcsConfigOptionsAdapter::INR_CURRENCY_ID, // currency id 1 (base/INR)
    90.0,                                       // EUR → local landed multiplier
    'cost_plus_pct',
    15.0,
    'exact_2_decimals'
);

/**
 * The small spec the smoke applies:
 *   - Image: ONE dropdown with an OS sub (visible) + a Panels sub (hidden).
 *            Each value carries a `category` so ExposureResolver can curate the
 *            per-sub hidden flag.
 *   - Networking:Bandwidth: option-level HIDDEN under the Retail preset.
 *   - Region: option-level VISIBLE under the Retail preset.
 */
$specs = array(
    array(
        'dimension_key' => 'Image',
        'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN,
        'values'        => array(
            array('value_key' => 'os:ubuntu-24', 'label' => '[OS] Ubuntu 24.04', 'category' => 'OS',     'monthly_eur_delta' => 0.0,  'is_default' => true,  'sortorder' => 0),
            array('value_key' => 'panel:cpanel', 'label' => '[Panels] cPanel',   'category' => 'Panels', 'monthly_eur_delta' => 5.0,  'is_default' => false, 'sortorder' => 1),
        ),
    ),
    array(
        'dimension_key' => 'Networking:Bandwidth',
        'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN,
        'values'        => array(
            array('value_key' => 'bw:32t', 'label' => '32 TB', 'monthly_eur_delta' => 0.0, 'is_default' => true, 'sortorder' => 0),
        ),
    ),
    array(
        'dimension_key' => 'Region',
        'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN,
        'values'        => array(
            array('value_key' => 'region:eu', 'label' => 'European Union', 'monthly_eur_delta' => 0.0, 'is_default' => true, 'sortorder' => 0),
        ),
    ),
);

// Track ids we touch, for cleanup.
$createdGroupId  = 0;
$imageOptId      = 0;

// ── CLEANUP (registered up front; runs at the very end, best-effort) ─────────
// One-shot: the same closure is invoked explicitly at the end AND wired as a
// shutdown function so it still runs on a fatal/throw. The flag keeps it to a
// single visible pass.
$cleanupRan = false;
$cleanup = static function () use (&$createdGroupId, &$cleanupRan, $profileId, $productId, $groupName, $batchId) {
    if ($cleanupRan) {
        return;
    }
    $cleanupRan = true;
    smoke_section('cleanup (best-effort)');
    try {
        // Resolve the group id even if apply() never recorded it (find by name).
        $gid = (int) $createdGroupId;
        if ($gid <= 0) {
            $g = Capsule::table('tblproductconfiggroups')->where('name', $groupName)->first();
            if ($g !== null) {
                $g   = (array) $g;
                $gid = (int) ($g['id'] ?? 0);
            }
        }

        if ($gid > 0) {
            // Collect option ids in the group, then their sub-option ids.
            $optRows = Capsule::table('tblproductconfigoptions')->where('gid', $gid)->get(array('id'));
            $optIds  = array();
            foreach ($optRows as $o) {
                $o = (array) $o;
                $optIds[] = (int) $o['id'];
            }

            $subIds = array();
            if ($optIds !== array()) {
                $subRows = Capsule::table('tblproductconfigoptionssub')->whereIn('configid', $optIds)->get(array('id'));
                foreach ($subRows as $s) {
                    $s = (array) $s;
                    $subIds[] = (int) $s['id'];
                }
            }

            // tblpricing rows for those sub-options.
            if ($subIds !== array()) {
                Capsule::table('tblpricing')
                    ->where('type', 'configoptions')
                    ->whereIn('relid', $subIds)
                    ->delete();
                Capsule::table('tblproductconfigoptionssub')->whereIn('id', $subIds)->delete();
            }
            if ($optIds !== array()) {
                Capsule::table('tblproductconfigoptions')->whereIn('id', $optIds)->delete();
            }
            Capsule::table('tblproductconfiglinks')->where('gid', $gid)->delete();
            Capsule::table('tblproductconfiggroups')->where('id', $gid)->delete();
            smoke_note('removed WHMCS group #' . $gid . ' (+ options/subs/pricing/links)');
        } else {
            smoke_note('no WHMCS group to remove');
        }

        // Addon-owned link tables (scoped to this run's profile/product).
        $optLinkRows = Capsule::table('mod_contabo_config_option_link')->where('profile_id', $profileId)->get(array('id'));
        $optLinkIds  = array();
        foreach ($optLinkRows as $r) {
            $r = (array) $r;
            $optLinkIds[] = (int) $r['id'];
        }
        if ($optLinkIds !== array()) {
            Capsule::table('mod_contabo_config_option_value_link')->whereIn('option_link_id', $optLinkIds)->delete();
        }
        Capsule::table('mod_contabo_config_option_link')->where('profile_id', $profileId)->delete();
        Capsule::table('mod_contabo_config_group_link')->where('profile_id', $profileId)->where('whmcs_product_id', $productId)->delete();

        // Audit rows from this run's batch.
        Capsule::table('mod_contabo_config_option_audit')->where('sync_batch_id', $batchId)->delete();
        smoke_note('removed addon link / value / group-link / audit rows for profile #' . $profileId);
    } catch (\Throwable $e) {
        smoke_note('cleanup error (ignored): ' . $e->getMessage());
    }
};

// Guarantee cleanup runs even on fatal/throw.
register_shutdown_function($cleanup);

try {
    // ── 1) schema ────────────────────────────────────────────────────────────
    smoke_section('schema');
    $health = SchemaHealth::assertOrMigrate();
    smoke_assert(!empty($health['ok']), 'SchemaHealth::assertOrMigrate() ok (error=' . (string) ($health['error'] ?? '') . ')');
    smoke_assert(
        (int) ($health['to'] ?? -1) === Installer::SCHEMA_VERSION,
        'schema to=' . (int) ($health['to'] ?? -1) . ' === Installer::SCHEMA_VERSION=' . Installer::SCHEMA_VERSION
    );

    // ── 1b) Proposal Studio preview-only safety boundary ────────────────────
    smoke_section('proposal studio preview boundary');
    $proposalSettings = new Settings(
        'http://localhost:8080/api/v1',
        '',
        'notify',
        'INR',
        false,
        0.0,
        365,
        'addonmodules.php?module=contabo_pricing'
    );
    $proposalMaker = new ProposalMaker($proposalSettings);
    $delivery = $proposalMaker->deliveryDecision('smoke-version', 'email', 1, 'smoke@example.invalid');
    smoke_assert($proposalSettings->proposalAiModel === 'gpt-5.6-luna', 'OpenAI profile uses the cost-efficient default model');
    smoke_assert($proposalSettings->proposalAiRequestStyle === 'responses', 'OpenAI profile defaults to Responses API');
    smoke_assert(empty($delivery['allowed']), 'delivery stays blocked without durable proposal persistence');

    // ── 1b) real-schema parity (catches the recurringamount→amount class) ──────
    // FakeCapsule is schemaless, so the unit suite cannot see a wrong column name.
    // Assert the real WHMCS columns here so that divergence fails the gate.
    smoke_section('real-schema parity');
    $schema = Capsule::schema();
    smoke_assert($schema->hasColumn('tblhosting', 'amount'), 'tblhosting.amount exists (real recurring-charge column)');
    smoke_assert($schema->hasColumn('tblhosting', 'firstpaymentamount'), 'tblhosting.firstpaymentamount exists');
    smoke_assert(!$schema->hasColumn('tblhosting', 'recurringamount'), 'tblhosting.recurringamount is NOT a raw column (must never be read/written raw)');
    foreach (array('mod_contabo_config_group_link', 'mod_contabo_config_option_link', 'mod_contabo_config_option_value_link') as $linkTbl) {
        smoke_assert($schema->hasColumn($linkTbl, 'expected_hash'), $linkTbl . '.expected_hash exists (drift baseline, schema v6)');
    }

    // ── 2) apply (exposure) ────────────────────────────────────────────────────
    smoke_section('apply (exposure)');
    $applyResult = $makeSyncer(false)->apply($profileId, $productId, $groupKey, $groupName, $specs, $ctx);

    $group          = (array) ($applyResult['group'] ?? array());
    $createdGroupId = (int) ($group['id'] ?? 0);
    smoke_assert($createdGroupId > 0, 'apply created/linked a WHMCS group (id=' . $createdGroupId . ')');
    smoke_assert((int) ($applyResult['options'] ?? 0) === 3, 'apply traversed 3 options (got ' . (int) ($applyResult['options'] ?? 0) . ')');

    // Read back the live WHMCS option-level hidden flags.
    $imageLink = $links->findOptionLink($profileId, 'Image');
    $bwLink    = $links->findOptionLink($profileId, 'Networking:Bandwidth');
    $imageOptId = (int) ($imageLink['whmcs_option_id'] ?? 0);
    $bwOptId    = (int) ($bwLink['whmcs_option_id'] ?? 0);
    smoke_assert($imageOptId > 0, 'Image option link recorded a whmcs_option_id (' . $imageOptId . ')');
    smoke_assert($bwOptId > 0, 'Bandwidth option link recorded a whmcs_option_id (' . $bwOptId . ')');

    $imageOptRow = Capsule::table('tblproductconfigoptions')->where('id', $imageOptId)->first();
    $bwOptRow    = Capsule::table('tblproductconfigoptions')->where('id', $bwOptId)->first();
    $imageOptRow = $imageOptRow !== null ? (array) $imageOptRow : array();
    $bwOptRow    = $bwOptRow !== null ? (array) $bwOptRow : array();

    smoke_assert((int) ($imageOptRow['hidden'] ?? -1) === 0, 'Image option visible (hidden=0, got ' . (string) ($imageOptRow['hidden'] ?? 'NULL') . ')');
    smoke_assert((int) ($bwOptRow['hidden'] ?? -1) === 1, 'Bandwidth option hidden (hidden=1, got ' . (string) ($bwOptRow['hidden'] ?? 'NULL') . ')');

    // Read back the Image sub-options' hidden flags by their labels.
    $osSub    = Capsule::table('tblproductconfigoptionssub')
        ->where('configid', $imageOptId)->where('optionname', '[OS] Ubuntu 24.04')->first();
    $panelSub = Capsule::table('tblproductconfigoptionssub')
        ->where('configid', $imageOptId)->where('optionname', '[Panels] cPanel')->first();
    $osSub    = $osSub !== null ? (array) $osSub : array();
    $panelSub = $panelSub !== null ? (array) $panelSub : array();

    smoke_assert((int) ($osSub['hidden'] ?? -1) === 0, 'OS image sub visible (hidden=0, got ' . (string) ($osSub['hidden'] ?? 'NULL') . ')');
    smoke_assert((int) ($panelSub['hidden'] ?? -1) === 1, 'Panels image sub hidden (hidden=1, got ' . (string) ($panelSub['hidden'] ?? 'NULL') . ')');

    // ── 3) drift baseline + guard ──────────────────────────────────────────────
    smoke_section('drift baseline + guard');
    $imageLink = $links->findOptionLink($profileId, 'Image'); // re-read for expected_hash
    $expectedHash = (string) ($imageLink['expected_hash'] ?? '');
    smoke_assert($expectedHash !== '', 'Image option link has a non-empty expected_hash baseline');

    // Confirm the baseline actually matches the live row (real stdClass round-trip).
    $liveOptForHash = Capsule::table('tblproductconfigoptions')->where('id', $imageOptId)->first();
    $liveOptForHash = $liveOptForHash !== null ? (array) $liveOptForHash : array();
    smoke_assert(
        DriftHasher::matches($expectedHash, $liveOptForHash, WhmcsConfigOptionsAdapter::OPTION_DRIFT_COLUMNS),
        'baseline hash matches the freshly-written live option (no false drift)'
    );

    // Admin hand-edits the live option out of band.
    Capsule::table('tblproductconfigoptions')->where('id', $imageOptId)->update(array('optionname' => 'X HANDEDITED'));

    // Re-apply identical inputs: the drifted option must be flagged + skipped.
    $applyResult2 = $makeSyncer(false)->apply($profileId, $productId, $groupKey, $groupName, $specs, $ctx);
    $driftSkipped = (int) ($applyResult2['summary']['drift_skipped'] ?? 0);
    smoke_assert($driftSkipped >= 1, 'second apply reports drift_skipped >= 1 (got ' . $driftSkipped . ')');

    $liveAfter = Capsule::table('tblproductconfigoptions')->where('id', $imageOptId)->first();
    $liveAfter = $liveAfter !== null ? (array) $liveAfter : array();
    smoke_assert(
        (string) ($liveAfter['optionname'] ?? '') === 'X HANDEDITED',
        "admin hand-edit NOT clobbered (optionname still 'X HANDEDITED', got '" . (string) ($liveAfter['optionname'] ?? '') . "')"
    );

    // ── 3b) value-level drift (sub-option) ─────────────────────────────────────
    // The Region option is still clean (only Image was hand-edited above), so its
    // value loop runs on re-apply — letting us prove the NEW value-level guard:
    // a hand-edited live sub-option is flagged + skipped, never clobbered.
    smoke_section('value-level drift (sub-option + pricing)');
    $regionLink  = $links->findOptionLink($profileId, 'Region');
    $regionOptId = (int) ($regionLink['whmcs_option_id'] ?? 0);
    $regionSubRow = $regionOptId > 0
        ? Capsule::table('tblproductconfigoptionssub')->where('configid', $regionOptId)->first()
        : null;
    $regionSubRow = $regionSubRow !== null ? (array) $regionSubRow : array();
    $regionSubId  = (int) ($regionSubRow['id'] ?? 0);
    smoke_assert($regionSubId > 0, 'Region sub-option exists (id=' . $regionSubId . ')');

    // Admin hand-edits the live Region SUB-OPTION (the option row stays untouched).
    Capsule::table('tblproductconfigoptionssub')->where('id', $regionSubId)->update(array('optionname' => 'EU HAND-EDITED'));

    $applyResult3  = $makeSyncer(false)->apply($profileId, $productId, $groupKey, $groupName, $specs, $ctx);
    $valueDrift    = (int) ($applyResult3['summary']['drift_skipped'] ?? 0);
    smoke_assert($valueDrift >= 1, 'third apply reports drift_skipped >= 1 incl. value-level (got ' . $valueDrift . ')');

    $liveRegionSub = Capsule::table('tblproductconfigoptionssub')->where('id', $regionSubId)->first();
    $liveRegionSub = $liveRegionSub !== null ? (array) $liveRegionSub : array();
    smoke_assert(
        (string) ($liveRegionSub['optionname'] ?? '') === 'EU HAND-EDITED',
        "value-level drift: hand-edited Region sub NOT clobbered (got '" . (string) ($liveRegionSub['optionname'] ?? '') . "')"
    );

    // ── 4) observe ──────────────────────────────────────────────────────────────
    smoke_section('observe');
    $observeResult = $makeSyncer(true)->observe($profileId, $groupName, $specs, $ctx, $productId);
    $observedOptions = (int) ($observeResult['totals']['options'] ?? 0);
    smoke_assert((bool) ($observeResult['dry_run'] ?? false) === true, 'observe ran in dry-run mode');
    smoke_assert($observedOptions > 0, 'observe totals.options > 0 (got ' . $observedOptions . ')');
} catch (\Throwable $e) {
    $GLOBALS['__smoke_failures']++;
    echo "\n  FAIL  uncaught exception: " . $e->getMessage() . "\n";
    echo "        " . $e->getFile() . ':' . $e->getLine() . "\n";
}

// Cleanup runs now; the shutdown hook is a belt-and-braces guard (one-shot flag
// makes the second invocation a no-op).
$cleanup();

// ── Summary ──────────────────────────────────────────────────────────────────
$passes   = (int) $GLOBALS['__smoke_passes'];
$failures = (int) $GLOBALS['__smoke_failures'];
echo "\n=========================================\n";
echo "Smoke summary: " . $passes . " passed, " . $failures . " failed\n";
if ($failures === 0) {
    echo "RESULT: ALL PASS\n";
    exit(0);
}
echo "RESULT: FAILURES\n";
exit(1);
