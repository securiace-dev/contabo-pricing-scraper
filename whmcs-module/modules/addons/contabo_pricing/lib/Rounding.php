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
 * Supported modes (snapshotted into `metadata_json.rounding_mode` of every
 * decision / catalog_audit row so historical reconstructions can replay):
 *
 *   - 'exact_2_decimals' (default): round half-up to 2 decimal places.
 *     1234.567 → 1234.57. The most common mode and what the engine used in
 *     Phase A before per-cycle pricing landed.
 *
 *   - 'nearest_99': round UP to the next *.99 boundary in the major unit.
 *     1234.00 → 1234.99; 1234.50 → 1234.99; 1235.00 → 1235.99. Useful for
 *     INR/USD storefronts that always quote `.99` prices.
 *
 *   - 'nearest_95': same idea but to `.95`. 1234.00 → 1234.95; 1235.10 → 1235.95.
 *
 *   - 'nearest_50': round UP to the next 50-paise / 50-cent boundary.
 *     1234.00 → 1234.50; 1234.51 → 1235.00. Used in cash-handling
 *     jurisdictions where sub-50 units don't exist.
 *
 *   - 'nearest_integer': round UP to the next whole unit. 1234.01 → 1235.
 *
 * Any unknown mode falls back to 'exact_2_decimals' AND emits a logActivity()
 * warning so admins notice misconfiguration without breaking the cron.
 *
 * PHP 7.4 polyglot: no match, no readonly, no enums.
 */
final class Rounding
{
    public const MODE_EXACT_2_DECIMALS = 'exact_2_decimals';
    public const MODE_NEAREST_99       = 'nearest_99';
    public const MODE_NEAREST_95       = 'nearest_95';
    public const MODE_NEAREST_50       = 'nearest_50';
    public const MODE_NEAREST_INTEGER  = 'nearest_integer';

    /**
     * All recognised modes in canonical display order.
     *
     * @return list<string>
     */
    public static function supportedModes(): array
    {
        return [
            self::MODE_EXACT_2_DECIMALS,
            self::MODE_NEAREST_99,
            self::MODE_NEAREST_95,
            self::MODE_NEAREST_50,
            self::MODE_NEAREST_INTEGER,
        ];
    }

    /**
     * Whether the given string is a recognised rounding mode.
     */
    public static function isSupportedMode(string $mode): bool
    {
        return in_array($mode, self::supportedModes(), true);
    }

    /**
     * Round a price according to the given mode. Negative prices are passed
     * through as-is (the engine should never compute negatives — bug elsewhere
     * if you see one) but we don't crash on them. Zero is always returned as
     * 0.00 regardless of mode (free-cycle gets no rounding surprise).
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

            case self::MODE_NEAREST_99:
                // Round UP to the next *.99 boundary.
                // 1234.00 → 1234.99; 1234.99 → 1234.99; 1235.00 → 1235.99.
                $whole = (int) floor($price);
                $candidate = $whole + 0.99;
                if ($candidate < $price) {
                    $candidate += 1.0;
                }
                return round($candidate, 2);

            case self::MODE_NEAREST_95:
                $whole = (int) floor($price);
                $candidate = $whole + 0.95;
                if ($candidate < $price) {
                    $candidate += 1.0;
                }
                return round($candidate, 2);

            case self::MODE_NEAREST_50:
                // Round UP to the next .00 or .50 boundary.
                $doubled = $price * 2.0;
                $candidate = ceil($doubled - 1e-9) / 2.0;
                return round($candidate, 2);

            case self::MODE_NEAREST_INTEGER:
                $candidate = ceil($price - 1e-9);
                return round($candidate, 2);

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
