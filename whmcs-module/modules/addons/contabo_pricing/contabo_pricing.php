<?php
/**
 * Contabo Pricing — WHMCS Addon
 *
 * Syncs Contabo VPS/VDS pricing into WHMCS products through versioned profiles.
 * Talks to a contabo-pricing API server (Rust binary, /api/v1/*) and never
 * scrapes Contabo directly.
 *
 * Targets WHMCS 8.x and 9.x. Source syntax remains PHP 7.4-compatible;
 * WHMCS 9 installations follow WHMCS's modern PHP runtime requirements.
 * Composer autoload is wired via the
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
                'FriendlyName' => 'Legacy sync: apply 18% GST',
                'Type'         => 'yesno',
                'Default'      => 'yes',
                'Description'  => 'Legacy catalog-sync behavior only. Proposal Studio uses its separate fail-closed output-tax controls below.',
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
            'proposal_provider_tax_charged' => [
                'FriendlyName' => 'Proposal: provider tax charged',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'Provider/vendor cash tax only; this is not Securiace output GST.',
            ],
            'proposal_provider_prices_include_tax' => [
                'FriendlyName' => 'Proposal: provider prices include tax',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'When enabled, Proposal Studio decomposes the provider gross into net base + tax cash before landed-cost recovery logic.',
            ],
            'proposal_provider_tax_rate_pct' => [
                'FriendlyName' => 'Proposal: provider tax rate %',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => '0',
                'Description'  => 'Applied to provider cash cost. Retained in landed cost unless recoverability is verified.',
            ],
            'proposal_provider_tax_recoverable' => [
                'FriendlyName' => 'Proposal: provider tax recoverable',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'Enable only with verified input-tax-credit evidence.',
            ],
            'proposal_payment_buffer_pct' => [
                'FriendlyName' => 'Proposal: payment buffer %',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => '0',
                'Description'  => 'Payment-processing buffer, separate from FX/card markup and owner margin.',
            ],
            'proposal_output_tax_enabled' => [
                'FriendlyName' => 'Proposal: charge Securiace output GST',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'Defaults disabled. Preview fails closed unless registration is verified and commercial mode is GST-exclusive.',
            ],
            'proposal_output_tax_registration_verified' => [
                'FriendlyName' => 'Proposal: GST registration verified',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'Operator attestation that registration evidence has been verified.',
            ],
            'proposal_output_tax_commercial_mode' => [
                'FriendlyName' => 'Proposal: commercial tax mode',
                'Type'         => 'dropdown',
                'Options'      => 'all_inclusive_no_separate_tax,gst_exclusive',
                'Default'      => 'all_inclusive_no_separate_tax',
                'Description'  => 'Must match the current commercial/legal setting. Safe default charges no separate GST.',
            ],
            'proposal_output_tax_rate_pct' => [
                'FriendlyName' => 'Proposal: output GST rate %',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => '18',
                'Description'  => 'Applied after owner margin only when both output-tax gates pass.',
            ],
            'proposal_ai_enabled' => [
                'FriendlyName' => 'Proposal AI narrative',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'Optional narrative-only pass after deterministic preview. Facts, prices and visibility remain authoritative.',
            ],
            'proposal_ai_provider' => [
                'FriendlyName' => 'Proposal AI provider profile',
                'Type'         => 'dropdown',
                'Options'      => 'openai,compatible',
                'Default'      => 'openai',
                'Description'  => 'OpenAI uses Responses API. Compatible endpoints use explicit Chat Completions mode.',
            ],
            'proposal_ai_base_url' => [
                'FriendlyName' => 'Proposal AI base URL',
                'Type'         => 'text',
                'Size'         => '60',
                'Default'      => 'https://api.openai.com/v1',
                'Description'  => 'HTTPS required except loopback development endpoints.',
            ],
            'proposal_ai_api_key' => [
                'FriendlyName' => 'Proposal AI API key',
                'Type'         => 'password',
                'Size'         => '50',
                'Description'  => 'Encrypted using WHMCS encrypt(); excluded from logs, prompts and artifacts.',
            ],
            'proposal_ai_model' => [
                'FriendlyName' => 'Proposal AI model/deployment',
                'Type'         => 'text',
                'Size'         => '32',
                'Default'      => 'gpt-5.6-luna',
                'Description'  => 'Cost-efficient OpenAI default. A compatible provider requires its own explicit model/deployment value.',
            ],
            'proposal_ai_request_style' => [
                'FriendlyName' => 'Proposal AI request style',
                'Type'         => 'dropdown',
                'Options'      => 'responses,chat_completions',
                'Default'      => 'responses',
                'Description'  => 'Normalized by provider profile; OpenAI=Responses, compatible=Chat Completions.',
            ],
            'proposal_ai_structured_output' => [
                'FriendlyName' => 'Proposal AI structured-output capability',
                'Type'         => 'yesno',
                'Default'      => 'yes',
                'Description'  => 'Disable for compatible providers that do not support JSON-schema output; local validation still applies.',
            ],
            'proposal_ai_max_output_tokens' => [
                'FriendlyName' => 'Proposal AI max output tokens',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => '1200',
                'Description'  => 'Clamped to 128–4000.',
            ],
            'proposal_ai_timeout_seconds' => [
                'FriendlyName' => 'Proposal AI timeout seconds',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => '30',
                'Description'  => 'Clamped to 5–60 seconds per attempt.',
            ],
            'proposal_ai_retries' => [
                'FriendlyName' => 'Proposal AI retries',
                'Type'         => 'text',
                'Size'         => '4',
                'Default'      => '1',
                'Description'  => 'Clamped to 0–2; retries only provider/network failures.',
            ],
            'proposal_ai_advisory_budget_usd' => [
                'FriendlyName' => 'Proposal AI advisory budget (USD)',
                'Type'         => 'text',
                'Size'         => '6',
                'Default'      => '0.10',
                'Description'  => 'Advisory metadata only. Output-token and timeout limits are enforced; no monetary cap is claimed without configured model rates.',
            ],
            'proposal_delivery_enabled' => [
                'FriendlyName' => 'Proposal delivery intent',
                'Type'         => 'yesno',
                'Default'      => 'no',
                'Description'  => 'This canonical-source slice still blocks sending until immutable persistence, durable outbox/idempotency and attachment-token hooks land.',
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
        'Proposal Studio' => '&action=proposals',
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
