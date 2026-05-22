<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Centralised mapping from WHMCS billing-cycle literals to month counts.
 *
 * The engine NEVER compares monthly vendor cost to a `tblhosting.recurringamount`
 * directly — both sides must be normalised through this class. This is the
 * single source of truth for "what does Annually mean in months?" and the only
 * place the WHMCS literal strings appear in arithmetic context.
 *
 * Anything outside the known set (Free Account, One Time, …) returns null and
 * RenewalEngine maps that to skip_reason = 'cycle_unsupported'.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly.
 */
final class CycleNormalizer
{
    /**
     * Canonical WHMCS billing-cycle literals → month counts.
     *
     * The keys are the exact strings WHMCS stores in `tblhosting.billingcycle`.
     * Anything else (e.g. "Free Account", "One Time") is intentionally absent.
     *
     * @var array<string, int>
     */
    private const MAP = [
        'Monthly'       => 1,
        'Quarterly'     => 3,
        'Semi-Annually' => 6,
        'Annually'      => 12,
        'Biennially'    => 24,
        'Triennially'   => 36,
    ];

    /**
     * Return the number of months in the given WHMCS billing-cycle literal, or
     * null if the cycle is not one the engine can price for.
     *
     * @param string $cycle WHMCS billing-cycle literal exactly as stored in
     *                      `tblhosting.billingcycle` (case-sensitive).
     * @return int|null     1, 3, 6, 12, 24, 36 for supported cycles; null for
     *                      Free Account / One Time / anything else.
     */
    public static function monthsForCycle(string $cycle): ?int
    {
        return self::MAP[$cycle] ?? null;
    }

    /**
     * Whether a WHMCS billing-cycle literal is one the engine can price for.
     *
     * @param string $cycle WHMCS billing-cycle literal.
     * @return bool
     */
    public static function isSupported(string $cycle): bool
    {
        return array_key_exists($cycle, self::MAP);
    }

    /**
     * All supported WHMCS billing-cycle literals (Monthly … Triennially).
     *
     * @return list<string>
     */
    public static function supportedCycles(): array
    {
        return array_keys(self::MAP);
    }
}
