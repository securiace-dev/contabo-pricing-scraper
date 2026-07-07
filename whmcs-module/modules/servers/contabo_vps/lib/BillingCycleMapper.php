<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Maps a WHMCS billing cycle to Contabo's REQUIRED create-instance `period`
 * (initial contract period in months; Contabo accepts exactly 1, 3, 6 or 12).
 *
 * Rule: longest Contabo period ≤ the WHMCS cycle, floor 1. Biennial/triennial
 * WHMCS orders therefore get a 12-month Contabo contract — Contabo auto-renews
 * the contract, so billing continuity holds; the mismatch is logged so the
 * admin can see it.
 *
 * Pure class: no DB, no API, fully deterministic. PHP 7.4 polyglot.
 */
final class BillingCycleMapper
{
    /** @var array<string,int> */
    private const CYCLE_TO_PERIOD = [
        'monthly'      => 1,
        'quarterly'    => 3,
        'semiannually' => 6,
        'semi-annually'=> 6,
        'annually'     => 12,
        'biennially'   => 12,
        'triennially'  => 12,
    ];

    public static function toPeriod(string $billingCycle): int
    {
        $cycle = strtolower(trim($billingCycle));
        if (isset(self::CYCLE_TO_PERIOD[$cycle])) {
            $period = self::CYCLE_TO_PERIOD[$cycle];
            if (($cycle === 'biennially' || $cycle === 'triennially') && function_exists('logActivity')) {
                logActivity('Contabo VPS: WHMCS cycle "' . $billingCycle . '" exceeds Contabo\'s longest contract period — using a 12-month Contabo contract (auto-renews).');
            }
            return $period;
        }
        // Free / One Time / unknown → shortest commitment.
        if (function_exists('logActivity')) {
            logActivity('Contabo VPS: unrecognised billing cycle "' . $billingCycle . '" — defaulting to a 1-month Contabo contract period.');
        }
        return 1;
    }
}
