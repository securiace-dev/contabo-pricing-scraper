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
    /** @var string */ public $proposalAiProvider;
    /** @var string */ public $proposalAiBaseUrl;
    /** @var string */ public $proposalAiApiKey;
    /** @var string */ public $proposalAiModel;
    /** @var string */ public $proposalAiRequestStyle;
    /** @var bool   */ public $proposalAiStructuredOutput;
    /** @var int    */ public $proposalAiMaxOutputTokens;
    /** @var int    */ public $proposalAiTimeoutSeconds;
    /** @var int    */ public $proposalAiRetries;
    /** @var float  */ public $proposalAiAdvisoryBudgetUsd;
    /** @var bool   */ public $proposalDeliveryEnabled;
    /** @var bool   */ public $proposalProviderTaxCharged;
    /** @var bool   */ public $proposalProviderPricesIncludeTax;
    /** @var float  */ public $proposalProviderTaxRatePct;
    /** @var bool   */ public $proposalProviderTaxRecoverable;
    /** @var float  */ public $proposalPaymentBufferPct;
    /** @var bool   */ public $proposalOutputTaxEnabled;
    /** @var bool   */ public $proposalOutputTaxRegistrationVerified;
    /** @var string */ public $proposalOutputTaxCommercialMode;
    /** @var float  */ public $proposalOutputTaxRatePct;

    public function __construct(
        string $apiBaseUrl,
        string $apiToken,
        string $defaultSyncStrategy,
        string $currencyIso,
        bool   $applyGst18,
        float  $fxMarkupPct,
        int    $logRetentionDays,
        string $moduleLink,
        bool $proposalAiEnabled = false,
        string $proposalAiProvider = 'openai',
        string $proposalAiBaseUrl = 'https://api.openai.com/v1',
        string $proposalAiApiKey = '',
        string $proposalAiModel = 'gpt-5.6-luna',
        string $proposalAiRequestStyle = 'responses',
        bool $proposalAiStructuredOutput = true,
        int $proposalAiMaxOutputTokens = 1200,
        int $proposalAiTimeoutSeconds = 30,
        int $proposalAiRetries = 1,
        float $proposalAiAdvisoryBudgetUsd = 0.10,
        bool $proposalDeliveryEnabled = false,
        bool $proposalProviderTaxCharged = false,
        bool $proposalProviderPricesIncludeTax = false,
        float $proposalProviderTaxRatePct = 0.0,
        bool $proposalProviderTaxRecoverable = false,
        float $proposalPaymentBufferPct = 0.0,
        bool $proposalOutputTaxEnabled = false,
        bool $proposalOutputTaxRegistrationVerified = false,
        string $proposalOutputTaxCommercialMode = 'all_inclusive_no_separate_tax',
        float $proposalOutputTaxRatePct = 18.0
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
        $normalizedAiProvider = strtolower(trim($proposalAiProvider));
        $this->proposalAiProvider  = in_array($normalizedAiProvider, ['openai', 'compatible'], true)
            ? $normalizedAiProvider
            : '';
        $this->proposalAiBaseUrl   = self::trimUrl($proposalAiBaseUrl);
        $this->proposalAiApiKey    = $proposalAiApiKey;
        $this->proposalAiModel     = trim($proposalAiModel);
        $this->proposalAiRequestStyle = $this->proposalAiProvider === 'openai'
            ? 'responses'
            : ($this->proposalAiProvider === 'compatible' ? 'chat_completions' : '');
        $this->proposalAiStructuredOutput = $proposalAiStructuredOutput;
        $this->proposalAiMaxOutputTokens = max(128, min(4000, $proposalAiMaxOutputTokens));
        $this->proposalAiTimeoutSeconds = max(5, min(60, $proposalAiTimeoutSeconds));
        $this->proposalAiRetries = max(0, min(2, $proposalAiRetries));
        $this->proposalAiAdvisoryBudgetUsd = max(0.0, min(25.0, $proposalAiAdvisoryBudgetUsd));
        $this->proposalDeliveryEnabled = $proposalDeliveryEnabled;
        $this->proposalProviderTaxCharged = $proposalProviderTaxCharged;
        // Preserve this independently so a contradictory "prices include tax"
        // setting fails closed in ProposalMaker instead of being normalized
        // into an apparently tax-exclusive configuration.
        $this->proposalProviderPricesIncludeTax = $proposalProviderPricesIncludeTax;
        $this->proposalProviderTaxRatePct = max(0.0, min(100.0, $proposalProviderTaxRatePct));
        $this->proposalProviderTaxRecoverable = $proposalProviderTaxCharged && $proposalProviderTaxRecoverable;
        $this->proposalPaymentBufferPct = max(0.0, min(100.0, $proposalPaymentBufferPct));
        $this->proposalOutputTaxEnabled = $proposalOutputTaxEnabled;
        $this->proposalOutputTaxRegistrationVerified = $proposalOutputTaxRegistrationVerified;
        $this->proposalOutputTaxCommercialMode = in_array(
            $proposalOutputTaxCommercialMode,
            ['all_inclusive_no_separate_tax', 'gst_exclusive'],
            true
        ) ? $proposalOutputTaxCommercialMode : 'all_inclusive_no_separate_tax';
        $this->proposalOutputTaxRatePct = max(0.0, min(100.0, $proposalOutputTaxRatePct));
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
            self::yesNo($vars['proposal_ai_enabled'] ?? 'no'),
            strtolower(trim((string) ($vars['proposal_ai_provider'] ?? 'openai'))),
            self::trimUrl((string) ($vars['proposal_ai_base_url'] ?? 'https://api.openai.com/v1')),
            $aiKey,
            trim((string) ($vars['proposal_ai_model'] ?? 'gpt-5.6-luna')),
            strtolower(trim((string) ($vars['proposal_ai_request_style'] ?? 'responses'))),
            self::yesNo($vars['proposal_ai_structured_output'] ?? 'yes'),
            (int) ($vars['proposal_ai_max_output_tokens'] ?? 1200),
            (int) ($vars['proposal_ai_timeout_seconds'] ?? 30),
            (int) ($vars['proposal_ai_retries'] ?? 1),
            (float) ($vars['proposal_ai_advisory_budget_usd'] ?? $vars['proposal_ai_max_cost_usd'] ?? 0.10),
            self::yesNo($vars['proposal_delivery_enabled'] ?? 'no'),
            self::yesNo($vars['proposal_provider_tax_charged'] ?? 'no'),
            self::yesNo($vars['proposal_provider_prices_include_tax'] ?? 'no'),
            (float) ($vars['proposal_provider_tax_rate_pct'] ?? 0),
            self::yesNo($vars['proposal_provider_tax_recoverable'] ?? 'no'),
            (float) ($vars['proposal_payment_buffer_pct'] ?? 0),
            self::yesNo($vars['proposal_output_tax_enabled'] ?? 'no'),
            self::yesNo($vars['proposal_output_tax_registration_verified'] ?? 'no'),
            strtolower(trim((string) ($vars['proposal_output_tax_commercial_mode'] ?? 'all_inclusive_no_separate_tax'))),
            (float) ($vars['proposal_output_tax_rate_pct'] ?? 18)
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

    /** @param mixed $value */
    private static function yesNo($value): bool
    {
        return in_array(strtolower(trim((string) $value)), ['1', 'on', 'true', 'yes'], true);
    }

    private static function trimUrl(string $url): string
    {
        return rtrim($url, '/');
    }
}
