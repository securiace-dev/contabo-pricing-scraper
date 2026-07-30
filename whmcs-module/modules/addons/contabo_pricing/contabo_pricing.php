<?php
/**
 * Contabo Pricing — WHMCS Addon
 *
 * Syncs Contabo VPS/VDS pricing into WHMCS products through versioned profiles.
 * Talks to a contabo-pricing API server (Rust binary, /api/v1/*) and never
 * scrapes Contabo directly.
 *
 * Targets WHMCS 8.x. Requires PHP >= 8.1. Composer autoload is wired via the
 * accompanying composer.json (run `composer install --no-dev` once after
 * uploading the module folder to modules/addons/).
 *
 * @license MIT
 */

declare(strict_types=1);

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly.');
}

// Composer autoload — tolerate missing vendor/ gracefully so the WHMCS UI
// can still load the module and display an actionable error.
$autoload = __DIR__ . '/vendor/autoload.php';
if (file_exists($autoload)) {
    require_once $autoload;
} else {
    // Provide a stub autoloader so the rest of this file parses without fatals.
    spl_autoload_register(static function (string $class): void {
        if (strpos($class, 'ContaboPricing\\') === 0) {
            $rel = str_replace(['ContaboPricing\\', '\\'], ['', '/'], $class);
            $path = __DIR__ . '/lib/' . $rel . '.php';
            if (file_exists($path)) {
                require_once $path;
            }
        }
    });
}

use ContaboPricing\AdminController;
use ContaboPricing\Installer;
use ContaboPricing\Settings;

/**
 * Module configuration shown on the addon's Activate / Settings page.
 *
 * @return array<string, mixed>
 */
function contabo_pricing_config(): array
{
    return [
        'name'        => 'Contabo Pricing Sync',
        'description' => 'Sync Contabo VPS/VDS pricing into WHMCS products via versioned profiles.',
        'version'     => AdminController::VERSION,
        'author'      => 'yashodhank',
        'language'    => 'english',
        'fields' => [
            'api_base_url' => [
                'FriendlyName' => 'API base URL',
                'Type'         => 'text',
                'Size'         => '60',
                'Default'      => 'http://localhost:8080/api/v1',
                'Description'  => 'URL of the contabo-pricing API server (no trailing slash).',
            ],
            'api_token' => [
                'FriendlyName' => 'Bearer token',
                'Type'         => 'password',
                'Size'         => '50',
                'Description'  => 'Required only for the Refresh button. Read endpoints are open.',
            ],
            'default_sync_strategy' => [
                'FriendlyName' => 'Default sync strategy',
                'Type'         => 'dropdown',
                'Options'      => 'manual,notify,auto-apply',
                'Default'      => 'notify',
                'Description'  => 'Per-profile setting overrides this. "notify" emails an admin on drift.',
            ],
            'currency_iso' => [
                'FriendlyName' => 'WHMCS base currency (ISO)',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => 'INR',
                'Description'  => 'Profiles store EUR + FX. Sync converts to this currency.',
            ],
            'apply_gst_18' => [
                'FriendlyName' => 'Apply 18% GST',
                'Type'         => 'yesno',
                'Default'      => 'yes',
                'Description'  => 'India GST. Disable if your WHMCS tax rules already apply GST automatically.',
            ],
            'fx_markup_pct' => [
                'FriendlyName' => 'FX markup %',
                'Type'         => 'text',
                'Size'         => '5',
                'Default'      => '3.5',
                'Description'  => 'Card/bank markup added on top of mid-market EUR→INR.',
            ],
            'log_retention_days' => [
                'FriendlyName' => 'Sync log retention (days)',
                'Type'         => 'text',
                'Size'         => '5',
                'Default'      => '365',
                'Description'  => 'Older sync log rows are pruned by the daily cron.',
            ],
        ],
    ];
}

/**
 * Called when the admin first activates the addon. Creates database tables.
 *
 * @return array{status: string, description: string}
 */
function contabo_pricing_activate(): array
{
    try {
        (new Installer())->install();
        return [
            'status'      => 'success',
            'description' => 'Contabo Pricing tables created. Configure API URL + token, then visit the addon page to manage profiles.',
        ];
    } catch (\Throwable $e) {
        logActivity('Contabo Pricing activate failed: ' . $e->getMessage());
        return [
            'status'      => 'error',
            'description' => 'Activation failed: ' . htmlspecialchars($e->getMessage()),
        ];
    }
}

/**
 * Called when the admin deactivates the addon. Tables are RETAINED so
 * historical profile versions and sync logs survive accidental disables.
 *
 * @return array{status: string, description: string}
 */
function contabo_pricing_deactivate(): array
{
    return [
        'status'      => 'success',
        'description' => 'Addon deactivated. Database tables retained (delete manually if you also want to drop history).',
    ];
}

/**
 * Schema migrations between addon versions.
 *
 * @param array<string, mixed> $vars
 */
function contabo_pricing_upgrade(array $vars): void
{
    (new Installer())->upgrade((string) ($vars['version'] ?? '0.0.0'));
}

/**
 * Renders the admin-area UI for the addon. Delegates to AdminController which
 * does the action-routing + Smarty rendering.
 *
 * @param array<string, mixed> $vars
 */
function contabo_pricing_output(array $vars): void
{
    try {
        $settings = Settings::fromVars($vars);
        $controller = new AdminController($settings, __DIR__ . '/templates/admin');
        $controller->dispatch($_REQUEST);
    } catch (\Throwable $e) {
        echo '<div class="errorbox"><strong>Contabo Pricing error:</strong> '
            . htmlspecialchars($e->getMessage()) . '</div>';
        logActivity('Contabo Pricing output error: ' . $e->getMessage() . ' :: ' . $e->getTraceAsString());
    }
}

/**
 * Sidebar block shown on the addon page in WHMCS admin.
 *
 * WHMCS's `_sidebar` hook expects either a string of HTML, an
 * `\WHMCS\Module\AbstractWidget` instance, or null. Returning a plain PHP
 * array used to print the literal "Array" because WHMCS stringifies the
 * return value. We render proper HTML instead.
 *
 * @param array<string, mixed> $vars
 */
function contabo_pricing_sidebar(array $vars): string
{
    $base  = isset($vars['modulelink']) ? (string) $vars['modulelink'] : 'addonmodules.php?module=contabo_pricing';
    // Primary catalog/profile pages, then a divider, then the renewal-pricing
    // engine pages (Phase A / A.5 / A.5.1). A value of '#divider' renders a
    // non-link separator label.
    $items = [
        'Dashboard'       => '',
        'Profiles'        => '&action=profiles',
        'Mappings'        => '&action=mappings',
        'VPS operations'  => '&action=operations',
        'Sync history'    => '&action=sync-history',
        'Settings'        => '&action=settings',
        'Repricing'       => '#divider',
        'Repricing dashboard' => '&action=repricing',
        'Price decisions' => '&action=price-decisions',
        'Skipped report'  => '&action=skipped-report',
        'Tax settings'    => '&action=tax-settings',
        'Maintenance'     => '&action=maintenance',
    ];
    $html = '<ul class="list-unstyled cb-side-nav">';
    foreach ($items as $label => $qs) {
        if ($qs === '#divider') {
            $html .= '<li class="cb-side-nav__label small text-muted">'
                . htmlspecialchars($label, ENT_QUOTES, 'UTF-8') . '</li>';
            continue;
        }
        $href = htmlspecialchars($base . $qs, ENT_QUOTES, 'UTF-8');
        $html .= '<li class="cb-side-nav__item"><a href="' . $href . '">' . htmlspecialchars($label, ENT_QUOTES, 'UTF-8') . '</a></li>';
    }
    $html .= '</ul>';
    return $html;
}
