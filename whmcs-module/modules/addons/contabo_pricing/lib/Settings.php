<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Immutable bag of addon configuration values pulled from the WHMCS addon
 * Settings page (passed in $vars by WHMCS) plus the secrets table.
 *
 * Stays compatible with PHP 7.4 (the version FastPanel ships for older WHMCS
 * installs) as well as 8.x — so: no readonly, no constructor property
 * promotion, no str_starts_with, no named args. Typed properties (7.4 feature)
 * are kept to retain shape safety.
 */
final class Settings
{
    /** @var string */ public $defaultSyncStrategy;
    /** @var string */ public $currencyIso;
    /** @var bool   */ public $applyGst18;
    /** @var float  */ public $fxMarkupPct;
    /** @var int    */ public $logRetentionDays;
    /** @var string */ public $moduleLink;

    public function __construct(
        string $defaultSyncStrategy,
        string $currencyIso,
        bool   $applyGst18,
        float  $fxMarkupPct,
        int    $logRetentionDays,
        string $moduleLink
    ) {
        $this->defaultSyncStrategy = $defaultSyncStrategy;
        $this->currencyIso         = $currencyIso;
        $this->applyGst18          = $applyGst18;
        $this->fxMarkupPct         = $fxMarkupPct;
        $this->logRetentionDays    = $logRetentionDays;
        $this->moduleLink          = $moduleLink;
    }

    /**
     * @param array<string, mixed> $vars
     */
    public static function fromVars(array $vars): self
    {
        return new self(
            (string) ($vars['default_sync_strategy'] ?? 'notify'),
            strtoupper((string) ($vars['currency_iso'] ?? 'INR')),
            ((string) ($vars['apply_gst_18'] ?? 'yes')) === 'yes',
            (float) ($vars['fx_markup_pct'] ?? 3.5),
            (int) ($vars['log_retention_days'] ?? 365),
            (string) ($vars['modulelink'] ?? 'addonmodules.php?module=contabo_pricing')
        );
    }
}
