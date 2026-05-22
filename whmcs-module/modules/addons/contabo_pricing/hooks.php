<?php
/**
 * WHMCS hook registrations for the Contabo Pricing addon.
 *
 * @license MIT
 */
declare(strict_types=1);

use ContaboPricing\ApiClient;
use ContaboPricing\AuditLog;
use ContaboPricing\ProfileManager;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use WHMCS\Database\Capsule;

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly.');
}

// Stub autoload for environments where vendor/ isn't installed.
$autoload = __DIR__ . '/vendor/autoload.php';
if (file_exists($autoload)) {
    require_once $autoload;
} else {
    spl_autoload_register(static function (string $class): void {
        if (strpos($class, 'ContaboPricing\\') === 0) {
            $rel = str_replace(['ContaboPricing\\', '\\'], ['', '/'], $class);
            $path = __DIR__ . '/lib/' . $rel . '.php';
            if (file_exists($path)) require_once $path;
        }
    });
}

/**
 * Load addon config values out of tbladdonmodules. Hook context doesn't pass
 * the module's settings directly, so we read them ourselves.
 *
 * @return array<string, mixed>
 */
function contabo_pricing_loadModuleVars(): array
{
    $rows = Capsule::table('tbladdonmodules')->where('module', 'contabo_pricing')->get(['setting', 'value']);
    $vars = [];
    foreach ($rows as $r) {
        $vars[$r->setting] = $r->value;
    }
    return $vars;
}

/**
 * Runs once per WHMCS daily cron pass. Triggers a sync if the addon has been
 * active long enough since the last run. Idempotent — SyncEngine short-circuits
 * when /meta reports the same snapshot as the previous successful run.
 */
add_hook('DailyCronJob', 10, static function (): void {
    try {
        $vars = contabo_pricing_loadModuleVars();
        if (empty($vars)) return;  // addon not configured / not activated

        $settings = Settings::fromVars($vars);
        $engine = new SyncEngine(
            $settings,
            new ApiClient($settings),
            new ProfileManager($settings),
        );

        $summary = $engine->run('cron');

        // Notify admin only on changes / failures, never on 'no-change'.
        if (in_array($summary['status'] ?? '', ['succeeded', 'failed'], true)) {
            $msg  = "Contabo Pricing sync ({$summary['status']}): ";
            $msg .= "profiles checked {$summary['profiles_checked']}, changed {$summary['profiles_changed']}, ";
            $msg .= "products updated {$summary['products_updated']}.";
            if (!empty($summary['errors'])) {
                $msg .= "\n\nErrors:\n" . implode("\n", array_map('strval', $summary['errors']));
            }
            sendAdminNotification('system', 'Contabo Pricing sync', $msg);
        }

        // Trim audit log
        (new AuditLog())->prune($settings->logRetentionDays);
    } catch (\Throwable $e) {
        logActivity('Contabo Pricing daily cron failed: ' . $e->getMessage());
    }
});

// ── WHMCS Renewal Pricing Policy Engine — Phase A hooks ─────────────────────
//
// Phase A is OBSERVE-ONLY. The apply-path hook `PreInvoicingGenerateInvoiceItems`
// is NOT registered here; that's Phase B work (see harmonic-popping-hollerith.md
// deliverable 12). We register:
//
//   - DailyCronJob (priority 10, lower than the SyncEngine cron above) → walks
//     mapped services, emits read-only decisions via CronDriver, scans
//     scheduled changes, prunes old audit rows. NEVER writes tblhosting.
//   - InvoiceCreation / InvoiceCreated (priority 99) → watchdogs that compare
//     freshly generated invoice line amounts to the latest applied decision
//     and log any mismatch. They MUST NOT mutate the invoice; that's enforced
//     inside Watchdog (Agent C's class).
//   - AdminClientServicesTabFields → adds a "Contabo Pricing" tab to the
//     admin service profile, rendered by AdminController::servicePricingTabContent().
//   - AdminClientServicesTabFieldsSave → no-op stub for Phase A (the tab is
//     read-only); Phase B will wire policy edits here.
//
// All registrations are guarded by class_exists() so a partial deployment
// (e.g. Agent C's Watchdog not yet shipped) degrades gracefully instead of
// fatalling the cron.

add_hook('DailyCronJob', 20, static function (): void {
    try {
        if (class_exists('\\ContaboPricing\\CronDriver')) {
            (new \ContaboPricing\CronDriver())->runObserveSweep();
        }
    } catch (\Throwable $e) {
        logActivity('Contabo Pricing CronDriver hook failed: ' . $e->getMessage());
    }
});

add_hook('InvoiceCreation', 99, static function ($vars): void {
    try {
        if (class_exists('\\ContaboPricing\\Watchdog')) {
            $invoiceId = (int) ($vars['invoiceid'] ?? 0);
            if ($invoiceId > 0) {
                (new \ContaboPricing\Watchdog())->onInvoiceCreation($invoiceId);
            }
        }
    } catch (\Throwable $e) {
        logActivity('Contabo Pricing Watchdog (InvoiceCreation) failed: ' . $e->getMessage());
    }
});

add_hook('InvoiceCreated', 99, static function ($vars): void {
    try {
        if (class_exists('\\ContaboPricing\\Watchdog')) {
            $invoiceId = (int) ($vars['invoiceid'] ?? 0);
            if ($invoiceId > 0) {
                (new \ContaboPricing\Watchdog())->onInvoiceCreated($invoiceId);
            }
        }
    } catch (\Throwable $e) {
        logActivity('Contabo Pricing Watchdog (InvoiceCreated) failed: ' . $e->getMessage());
    }
});

add_hook('AdminClientServicesTabFields', 10, static function ($vars) {
    try {
        $svcId = (int) ($vars['id'] ?? ($vars['serviceid'] ?? 0));
        if ($svcId <= 0) {
            return [];
        }
        if (!class_exists('\\ContaboPricing\\AdminController')) {
            return [];
        }
        // Build a minimal Settings instance from the addon's stored vars so
        // the controller helper can resolve the templateDir + module link.
        $moduleVars = contabo_pricing_loadModuleVars();
        $moduleVars['modulelink'] = 'addonmodules.php?module=contabo_pricing';
        $settings = Settings::fromVars($moduleVars);
        $tplDir = __DIR__ . '/templates/admin';
        $controller = new \ContaboPricing\AdminController($settings, $tplDir);
        $html = $controller->servicePricingTabContent($svcId);
        if ($html === '') {
            return [];
        }
        return ['Contabo Pricing' => $html];
    } catch (\Throwable $e) {
        logActivity('Contabo Pricing service tab render failed: ' . $e->getMessage());
        return [];
    }
});

add_hook('AdminClientServicesTabFieldsSave', 10, static function ($vars): void {
    // Phase A intentionally has no editable controls in the tab — nothing to
    // save. Phase B will wire policy edits here. Stubbed so the hook contract
    // is stable.
});

/**
 * Surface a tiny "Last sync" widget on the WHMCS admin home page.
 */
add_hook('AdminHomeWidgets', 1, static function () {
    return new class extends \WHMCS\Module\AbstractWidget {
        protected $title = 'Contabo Pricing';
        protected $description = 'Last sync of Contabo VPS/VDS pricing into WHMCS products.';
        protected $weight = 100;
        protected $columns = 1;
        protected $cache = false;

        public function getData(): array
        {
            $last = Capsule::table('mod_contabo_sync_log')->orderByDesc('id')->first();
            $active = (int) Capsule::table('mod_contabo_profile')->where('active', true)->count();
            return [
                'last'   => $last ? (array) $last : null,
                'active' => $active,
            ];
        }

        public function generateOutput($data): string
        {
            $last = $data['last'] ?? null;
            if (!$last) {
                return '<div class="widget-content-padded">No sync runs recorded yet. <a href="addonmodules.php?module=contabo_pricing">Open the addon</a>.</div>';
            }
            $status = htmlspecialchars((string) ($last['status'] ?? ''));
            $when   = htmlspecialchars(substr((string) ($last['started_at'] ?? ''), 0, 16));
            $changed = (int) ($last['profiles_changed'] ?? 0);
            $products = (int) ($last['products_updated'] ?? 0);
            return <<<HTML
                <div class="widget-content-padded">
                  <div><strong>Last sync:</strong> {$status} at {$when}</div>
                  <div>Profiles changed: {$changed} · Products updated: {$products}</div>
                  <div>Active profiles: {$data['active']}</div>
                  <div style="margin-top:8px">
                    <a class="btn btn-default btn-sm" href="addonmodules.php?module=contabo_pricing">Open addon</a>
                  </div>
                </div>
            HTML;
        }
    };
});
