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
    /** @var bool   */ public $proposalAiEnabled;
    /** @var string */ public $proposalAiBaseUrl;
    /** @var string */ public $proposalAiApiKey;
    /** @var string */ public $proposalAiModel;
    /** @var string */ public $proposalAiRequestStyle;
    /** @var bool   */ public $proposalDeliveryEnabled;

    public function __construct(
        string $apiBaseUrl,
        string $apiToken,
        string $defaultSyncStrategy,
        string $currencyIso,
        bool   $applyGst18,
        float  $fxMarkupPct,
        int    $logRetentionDays,
        string $moduleLink,
        bool   $proposalAiEnabled = false,
        string $proposalAiBaseUrl = '',
        string $proposalAiApiKey = '',
        string $proposalAiModel = 'gpt-5-mini',
        string $proposalAiRequestStyle = 'chat_completions',
        bool   $proposalDeliveryEnabled = false
    ) {
        $this->apiBaseUrl          = $apiBaseUrl;
        $this->apiToken            = $apiToken;
        $this->defaultSyncStrategy = $defaultSyncStrategy;
        $this->currencyIso         = $currencyIso;
        $this->applyGst18          = $applyGst18;
        $this->fxMarkupPct         = $fxMarkupPct;
        $this->logRetentionDays    = $logRetentionDays;
        $this->moduleLink          = $moduleLink;
        $this->proposalAiEnabled   = $proposalAiEnabled;
        $this->proposalAiBaseUrl   = self::trimUrl($proposalAiBaseUrl);
        $this->proposalAiApiKey    = $proposalAiApiKey;
        $this->proposalAiModel     = $proposalAiModel !== '' ? $proposalAiModel : 'gpt-5-mini';
        $this->proposalAiRequestStyle = in_array($proposalAiRequestStyle, ['auto', 'chat_completions', 'responses'], true)
            ? $proposalAiRequestStyle
            : 'chat_completions';
        $this->proposalDeliveryEnabled = $proposalDeliveryEnabled;
    }

    /**
     * @param array<string, mixed> $vars
     */
    public static function fromVars(array $vars): self
    {
        $rawToken = (string) ($vars['api_token'] ?? '');
        $apiToken = self::resolveToken($rawToken);
        $rawAiKey = (string) ($vars['proposal_ai_api_key'] ?? '');
        $aiKey = self::resolveAiKey($rawAiKey);

        return new self(
            self::trimUrl((string) ($vars['api_base_url'] ?? 'http://localhost:8080/api/v1')),
            $apiToken,
            (string) ($vars['default_sync_strategy'] ?? 'notify'),
            strtoupper((string) ($vars['currency_iso'] ?? 'INR')),
            ((string) ($vars['apply_gst_18'] ?? 'yes')) === 'yes',
            (float) ($vars['fx_markup_pct'] ?? 3.5),
            (int) ($vars['log_retention_days'] ?? 365),
            (string) ($vars['modulelink'] ?? 'addonmodules.php?module=contabo_pricing'),
            ((string) ($vars['proposal_ai_enabled'] ?? 'no')) === 'yes',
            self::trimUrl((string) ($vars['proposal_ai_base_url'] ?? '')),
            $aiKey,
            trim((string) ($vars['proposal_ai_model'] ?? 'gpt-5-mini')) ?: 'gpt-5-mini',
            trim((string) ($vars['proposal_ai_request_style'] ?? 'chat_completions')),
            ((string) ($vars['proposal_delivery_enabled'] ?? 'no')) === 'yes'
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
        return self::resolveSecret($raw, 'api_token', 'bearer token');
    }

    public static function resolveAiKey(string $raw): string
    {
        return self::resolveSecret($raw, 'proposal_ai_api_key', 'proposal AI key');
    }

    private static function resolveSecret(string $raw, string $setting, string $label): string
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
                logActivity('Contabo Pricing: ' . $label . ' decrypt failed: ' . $e->getMessage());
                return '';
            }
        }

        // Plaintext on disk → encrypt-at-rest now, but still return the
        // plaintext so this request can use it without a re-read round-trip.
        self::migratePlaintextToEncrypted($raw, $setting, $label);
        return $raw;
    }

    private static function migratePlaintextToEncrypted(string $plaintext, string $setting, string $label): void
    {
        try {
            $cipher = self::ENCRYPTED_PREFIX . encrypt($plaintext);
            \WHMCS\Database\Capsule::table('tbladdonmodules')
                ->where(['module' => 'contabo_pricing', 'setting' => $setting])
                ->update(['value' => $cipher]);
            logActivity('Contabo Pricing: ' . $label . ' encrypted at rest.');
        } catch (\Throwable $e) {
            logActivity('Contabo Pricing: ' . $label . ' encrypt-at-rest failed: ' . $e->getMessage());
        }
    }

    private static function trimUrl(string $url): string
    {
        return rtrim($url, '/');
    }
}
