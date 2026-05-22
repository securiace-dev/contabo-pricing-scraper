<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Canonical mapping from WHMCS billing-cycle literals to the corresponding
 * `tblpricing` recurring and setup-fee column names, plus the small set of
 * helpers that classify a stored price value (-1 / 0 / >0 / NULL) into
 * 'disabled' / 'free' / 'priced' / 'absent'.
 *
 * `tblpricing` stores per-cycle recurring prices in dedicated columns
 * (`monthly`, `quarterly`, `semiannually`, `annually`, `biennially`,
 * `triennially`) and the matching setup fees in `msetupfee`, `qsetupfee`,
 * `ssetupfee`, `asetupfee`, `bsetupfee`, `tsetupfee`. WHMCS treats a value
 * of -1 as "this cycle is disabled for this product", 0 as "free for this
 * cycle" and >0 as a real price. NULL means the row simply doesn't carry
 * a value for that cycle yet (treated as 'absent' by the engine).
 *
 * Non-recurring cycles ('Free Account', 'One Time') have neither recurring
 * nor setup-fee column; both ::getRecurringColumn() and ::getSetupFeeColumn()
 * return null for them and ::isNonRecurringCycle() returns true.
 *
 * The month-count lookup is delegated to CycleNormalizer to keep a single
 * source of truth.
 *
 * PHP 7.4 polyglot.
 */
final class CyclePricingMap
{
    /**
     * WHMCS cycle literal → tblpricing recurring column name.
     *
     * @var array<string,string>
     */
    private const RECURRING_COLUMN = [
        'Monthly'       => 'monthly',
        'Quarterly'     => 'quarterly',
        'Semi-Annually' => 'semiannually',
        'Annually'      => 'annually',
        'Biennially'    => 'biennially',
        'Triennially'   => 'triennially',
    ];

    /**
     * WHMCS cycle literal → tblpricing setup-fee column name.
     *
     * @var array<string,string>
     */
    private const SETUP_FEE_COLUMN = [
        'Monthly'       => 'msetupfee',
        'Quarterly'     => 'qsetupfee',
        'Semi-Annually' => 'ssetupfee',
        'Annually'      => 'asetupfee',
        'Biennially'    => 'bsetupfee',
        'Triennially'   => 'tsetupfee',
    ];

    /**
     * Non-recurring WHMCS cycle literals. These have no `tblpricing` column
     * mapping and the engine skips them with `skip_reason = cycle_unsupported`.
     *
     * @var list<string>
     */
    private const NON_RECURRING_CYCLES = [
        'Free Account',
        'One Time',
    ];

    /**
     * Return the `tblpricing` recurring column name for the given WHMCS cycle,
     * or null for non-recurring / unknown cycles.
     */
    public static function getRecurringColumn(string $cycle): ?string
    {
        return self::RECURRING_COLUMN[$cycle] ?? null;
    }

    /**
     * Return the `tblpricing` setup-fee column name for the given WHMCS cycle,
     * or null for non-recurring / unknown cycles.
     */
    public static function getSetupFeeColumn(string $cycle): ?string
    {
        return self::SETUP_FEE_COLUMN[$cycle] ?? null;
    }

    /**
     * Return the cycle length in months, delegating to CycleNormalizer.
     */
    public static function getCycleMonths(string $cycle): ?int
    {
        return CycleNormalizer::monthsForCycle($cycle);
    }

    /**
     * Whether the cycle is a supported recurring cycle (Monthly … Triennially).
     */
    public static function isRecurringCycleSupported(string $cycle): bool
    {
        return array_key_exists($cycle, self::RECURRING_COLUMN);
    }

    /**
     * Whether the cycle is a known non-recurring WHMCS cycle literal
     * (Free Account, One Time).
     */
    public static function isNonRecurringCycle(string $cycle): bool
    {
        return in_array($cycle, self::NON_RECURRING_CYCLES, true);
    }

    /**
     * Classify a raw `tblpricing` cell value into one of four price states:
     *
     *   null  → 'absent'   (the row carries no value for this cycle yet)
     *   <0    → 'disabled' (WHMCS convention: -1 means cycle disabled)
     *   == 0  → 'free'     (cycle priced at zero — intentionally free)
     *   >0    → 'priced'   (a real recurring price)
     *
     * Used by SyncEngine to decide whether to skip a cycle for catalog
     * reasons (e.g. `catalog_skip_disabled_cycle`,
     * `catalog_skip_free_cycle`) and by the renewal engine for the same
     * classification on existing services.
     */
    public static function priceStatusFromValue(?float $value): string
    {
        if ($value === null) {
            return 'absent';
        }
        if ($value < 0) {
            return 'disabled';
        }
        if ($value == 0.0) {
            return 'free';
        }
        return 'priced';
    }
}
