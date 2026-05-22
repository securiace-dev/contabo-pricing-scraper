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
    /**
     * Prefix used to mark an api_token row in tbladdonmodules as encrypted at
     * rest. Encrypted values look like "ENC:<base64-cipher>". Any value that
     * does NOT start with this prefix is treated as freshly-saved plaintext
     * from the WHMCS Settings form and is migrated on first read.
     */
    private const ENCRYPTED_PREFIX = 'ENC:';

    /** @var string */ public $apiBaseUrl;
    /** @var string */ public $apiToken;
    /** @var string */ public $defaultSyncStrategy;
    /** @var string */ public $currencyIso;
    /** @var bool   */ public $applyGst18;
    /** @var float  */ public $fxMarkupPct;
    /** @var int    */ public $logRetentionDays;
    /** @var string */ public $moduleLink;

    public function __construct(
        string $apiBaseUrl,
        string $apiToken,
        string $defaultSyncStrategy,
        string $currencyIso,
        bool   $applyGst18,
        float  $fxMarkupPct,
        int    $logRetentionDays,
        string $moduleLink
    ) {
        $this->apiBaseUrl          = $apiBaseUrl;
        $this->apiToken            = $apiToken;
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
        $rawToken = (string) ($vars['api_token'] ?? '');
        $apiToken = self::resolveToken($rawToken);

        return new self(
            self::trimUrl((string) ($vars['api_base_url'] ?? 'http://localhost:8080/api/v1')),
            $apiToken,
            (string) ($vars['default_sync_strategy'] ?? 'notify'),
            strtoupper((string) ($vars['currency_iso'] ?? 'INR')),
            ((string) ($vars['apply_gst_18'] ?? 'yes')) === 'yes',
            (float) ($vars['fx_markup_pct'] ?? 3.5),
            (int) ($vars['log_retention_days'] ?? 365),
            (string) ($vars['modulelink'] ?? 'addonmodules.php?module=contabo_pricing')
        );
    }

    /**
     * Returns the plaintext bearer token for use during this request. If the
     * value stored in tbladdonmodules is plaintext (no ENC: prefix) the row is
     * upgraded in-place to its encrypted form using WHMCS's native encrypt()
     * helper before the plaintext is returned to the caller.
     */
    public static function resolveToken(string $raw): string
    {
        if ($raw === '') {
            return '';
        }

        // strpos === 0 instead of str_starts_with() so this works on PHP 7.4.
        if (strpos($raw, self::ENCRYPTED_PREFIX) === 0) {
            $cipher = substr($raw, strlen(self::ENCRYPTED_PREFIX));
            try {
                return (string) decrypt($cipher);
            } catch (\Throwable $e) {
                logActivity('Contabo Pricing: token decrypt failed: ' . $e->getMessage());
                return '';
            }
        }

        // Plaintext on disk → encrypt-at-rest now, but still return the
        // plaintext so this request can use it without a re-read round-trip.
        self::migratePlaintextToEncrypted($raw);
        return $raw;
    }

    private static function migratePlaintextToEncrypted(string $plaintext): void
    {
        try {
            $cipher = self::ENCRYPTED_PREFIX . encrypt($plaintext);
            \WHMCS\Database\Capsule::table('tbladdonmodules')
                ->where(['module' => 'contabo_pricing', 'setting' => 'api_token'])
                ->update(['value' => $cipher]);
            logActivity('Contabo Pricing: bearer token encrypted at rest.');
        } catch (\Throwable $e) {
            logActivity('Contabo Pricing: token encrypt-at-rest failed: ' . $e->getMessage());
        }
    }

    private static function trimUrl(string $url): string
    {
        return rtrim($url, '/');
    }
}
