<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Shared rounding helper for the Phase A.5 per-cycle pricing engines.
 *
 * Both SyncEngine (Agent B — catalog writes) and RenewalEngine (Agent C —
 * renewal evaluations) MUST funnel every "computed sell price → final stored
 * price" transition through ::apply(). Duplicating this logic in two engines
 * is forbidden — if the rounding rules drift the catalog/renewal contract
 * silently breaks (e.g. catalog rounds 1234 → 1299 while renewal rounds it
 * → 1234, and the same product ends up offering two different prices for the
 * "same" cycle depending on whether you're a new buyer or an existing
 * customer).
 *
 * Supported modes are the values persisted by the WHMCS mapping editor and
 * snapshotted into `metadata_json.rounding_mode` / catalog audit rows:
 *
 *   - 'exact_2_decimals' (default): round half-up to 2 decimal places.
 *     1234.567 → 1234.57. The most common mode and what the engine used in
 *     Phase A before per-cycle pricing landed.
 *
 *   - 'nearest_rupee': round to the nearest whole currency unit.
 *
 *   - 'nearest_9': round UP to the next integer ending in 9.
 *
 *   - 'nearest_99': round UP to the next integer ending in 99.
 *
 *   - 'nearest_100': round to the nearest hundred currency units.
 *
 *   - 'custom': reserved for existing persisted mappings. Until a versioned
 *     custom rule contract exists, it safely degrades to exact two decimals.
 *
 * Any unknown mode falls back to 'exact_2_decimals' AND emits a logActivity()
 * warning so admins notice misconfiguration without breaking the cron.
 *
 * PHP 7.4 polyglot: no match, no readonly, no enums.
 */
final class Rounding
{
    public const MODE_EXACT_2_DECIMALS = 'exact_2_decimals';
    public const MODE_NEAREST_RUPEE    = 'nearest_rupee';
    public const MODE_NEAREST_9        = 'nearest_9';
    public const MODE_NEAREST_99       = 'nearest_99';
    public const MODE_NEAREST_100      = 'nearest_100';
    public const MODE_CUSTOM           = 'custom';

    /**
     * All recognised modes in canonical display order.
     *
     * @return list<string>
     */
    public static function supportedModes(): array
    {
        return [
            self::MODE_EXACT_2_DECIMALS,
            self::MODE_NEAREST_RUPEE,
            self::MODE_NEAREST_9,
            self::MODE_NEAREST_99,
            self::MODE_NEAREST_100,
            self::MODE_CUSTOM,
        ];
    }

    /**
     * Modes administrators may select for new or updated mappings. `custom`
     * remains readable for legacy rows but cannot be newly selected until its
     * versioned rule contract exists.
     *
     * @return list<string>
     */
    public static function selectableModes(): array
    {
        return [
            self::MODE_EXACT_2_DECIMALS,
            self::MODE_NEAREST_RUPEE,
            self::MODE_NEAREST_9,
            self::MODE_NEAREST_99,
            self::MODE_NEAREST_100,
        ];
    }

    /**
     * Whether the given string is a recognised rounding mode.
     */
    public static function isSupportedMode(string $mode): bool
    {
        return in_array($mode, self::supportedModes(), true);
    }

    public static function isSelectableMode(string $mode): bool
    {
        return in_array($mode, self::selectableModes(), true);
    }

    /**
     * Round a price according to the given mode. Non-positive values are
     * normalized to 0.00 so this pure formatter remains total; pricing engines
     * must separately enforce their positive-price write invariant.
     *
     * @param float  $price Pre-round price (typically the output of
     *                      MarginCalculator::sellPriceForCycle()).
     * @param string $mode  One of self::MODE_* — anything else falls back to
     *                      'exact_2_decimals' with a logActivity() warning.
     * @return float        Rounded price; same currency unit as input.
     */
    public static function apply(float $price, string $mode): float
    {
        if ($price <= 0.0) {
            return round(max($price, 0.0), 2);
        }

        switch ($mode) {
            case self::MODE_EXACT_2_DECIMALS:
                return round($price, 2);

            case self::MODE_NEAREST_RUPEE:
                return round($price, 0);

            case self::MODE_NEAREST_9:
                $base = (int) floor($price / 10.0) * 10;
                $candidate = (float) ($base + 9);
                if ($candidate < $price) {
                    $candidate += 10.0;
                }
                return $candidate;

            case self::MODE_NEAREST_99:
                $base = (int) floor($price / 100.0) * 100;
                $candidate = (float) ($base + 99);
                if ($candidate < $price) {
                    $candidate += 100.0;
                }
                return $candidate;

            case self::MODE_NEAREST_100:
                return round($price / 100.0) * 100.0;

            case self::MODE_CUSTOM:
                return round($price, 2);

            default:
                if (function_exists('logActivity')) {
                    \logActivity(
                        'Contabo Pricing Rounding: unknown mode "' . $mode
                        . '" — falling back to exact_2_decimals'
                    );
                }
                return round($price, 2);
        }
    }
}
