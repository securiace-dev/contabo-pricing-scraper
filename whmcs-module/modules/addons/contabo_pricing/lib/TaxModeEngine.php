<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Pluggable Tax Recovery Mode engine.
 *
 * The current deployment is NOT registered for output GST in India yet pays
 * Contabo's 18% German VAT on the vendor invoice. That vendor tax is
 * non-recoverable — it lives in landed cost. When the business later
 * registers, the active mode changes and decisions emitted AFTER the change
 * use the new mode. Older decision rows carry `tax_mode_snapshot` so historical
 * margin math doesn't silently rewrite itself.
 *
 * Eight modes are recognised; per-mode "summary" returns the three flags that
 * MarginCalculator needs:
 *   - output_tax_charged        — is output tax added on top of sell price?
 *   - vendor_tax_recoverable    — can the business claim input-tax credit?
 *   - prices_include_output_tax — is sell_price gross-inclusive of output tax?
 *
 * PHP 7.4 polyglot: no enums, no readonly, no match. Pure helpers, no state.
 */
final class TaxModeEngine
{
    /** Default mode for fresh installs: business not registered for output GST. */
    public const MODE_UNREGISTERED                       = 'unregistered_no_output_tax';
    public const MODE_REG_EXCLUSIVE_RECOVERABLE          = 'registered_tax_exclusive_recoverable';
    public const MODE_REG_EXCLUSIVE_NON_RECOVERABLE      = 'registered_tax_exclusive_non_recoverable';
    public const MODE_REG_INCLUSIVE_RECOVERABLE          = 'registered_tax_inclusive_recoverable';
    public const MODE_REG_INCLUSIVE_NON_RECOVERABLE      = 'registered_tax_inclusive_non_recoverable';
    public const MODE_NO_TAX_APPLICABLE                  = 'no_tax_applicable';
    public const MODE_TAX_EXEMPT_CUSTOMER                = 'tax_exempt_customer';
    public const MODE_CUSTOM_MANUAL                      = 'custom_manual';

    /**
     * The eight tax-mode identifiers recognised by the engine.
     *
     * @return list<string>
     */
    public static function modes(): array
    {
        return [
            self::MODE_UNREGISTERED,
            self::MODE_REG_EXCLUSIVE_RECOVERABLE,
            self::MODE_REG_EXCLUSIVE_NON_RECOVERABLE,
            self::MODE_REG_INCLUSIVE_RECOVERABLE,
            self::MODE_REG_INCLUSIVE_NON_RECOVERABLE,
            self::MODE_NO_TAX_APPLICABLE,
            self::MODE_TAX_EXEMPT_CUSTOMER,
            self::MODE_CUSTOM_MANUAL,
        ];
    }

    /**
     * Default mode for fresh installs. Current deployment is unregistered.
     */
    public static function defaultMode(): string
    {
        return self::MODE_UNREGISTERED;
    }

    /**
     * Whether the given identifier is one of the eight recognised modes.
     *
     * @param string $mode
     * @return bool
     */
    public static function isValid(string $mode): bool
    {
        return in_array($mode, self::modes(), true);
    }

    /**
     * Per-mode behaviour summary. Returns the three flags MarginCalculator
     * consumes to decide how to compute landed cost and net revenue.
     *
     * Keys returned:
     *   - output_tax_charged        bool — is output tax added on top?
     *   - vendor_tax_recoverable    bool — can input-tax credit be claimed?
     *   - prices_include_output_tax bool — is the sell price gross-inclusive?
     *
     * `custom_manual` returns null-ish defaults; the admin overrides each flag
     * via the tax-settings UI and the resolved values come through Settings.
     *
     * @param string $mode One of the eight identifiers from self::modes().
     * @return array{output_tax_charged:bool, vendor_tax_recoverable:bool, prices_include_output_tax:bool}
     * @throws \InvalidArgumentException when $mode is not recognised.
     */
    public static function summary(string $mode): array
    {
        switch ($mode) {
            case self::MODE_UNREGISTERED:
                return [
                    'output_tax_charged'        => false,
                    'vendor_tax_recoverable'    => false,
                    'prices_include_output_tax' => false,
                ];

            case self::MODE_REG_EXCLUSIVE_RECOVERABLE:
                return [
                    'output_tax_charged'        => true,
                    'vendor_tax_recoverable'    => true,
                    'prices_include_output_tax' => false,
                ];

            case self::MODE_REG_EXCLUSIVE_NON_RECOVERABLE:
                return [
                    'output_tax_charged'        => true,
                    'vendor_tax_recoverable'    => false,
                    'prices_include_output_tax' => false,
                ];

            case self::MODE_REG_INCLUSIVE_RECOVERABLE:
                return [
                    'output_tax_charged'        => true,
                    'vendor_tax_recoverable'    => true,
                    'prices_include_output_tax' => true,
                ];

            case self::MODE_REG_INCLUSIVE_NON_RECOVERABLE:
                return [
                    'output_tax_charged'        => true,
                    'vendor_tax_recoverable'    => false,
                    'prices_include_output_tax' => true,
                ];

            case self::MODE_NO_TAX_APPLICABLE:
                return [
                    'output_tax_charged'        => false,
                    'vendor_tax_recoverable'    => false,
                    'prices_include_output_tax' => false,
                ];

            case self::MODE_TAX_EXEMPT_CUSTOMER:
                return [
                    'output_tax_charged'        => false,
                    'vendor_tax_recoverable'    => false,
                    'prices_include_output_tax' => false,
                ];

            case self::MODE_CUSTOM_MANUAL:
                // Admin overrides each flag via tax-settings; the resolved
                // booleans arrive at MarginCalculator via Settings. Defaults
                // are conservative ("no output tax, nothing recoverable").
                return [
                    'output_tax_charged'        => false,
                    'vendor_tax_recoverable'    => false,
                    'prices_include_output_tax' => false,
                ];

            default:
                throw new \InvalidArgumentException(
                    'TaxModeEngine: unknown tax mode "' . $mode . '"'
                );
        }
    }
}
