<?php
declare(strict_types=1);

namespace ContaboPricing\Quote;

/**
 * Port of POST /quote from the retired Rust API (src/api/handlers.rs
 * quote()). Pure math: no I/O. Callers supply the plan row and (for INR) the
 * EUR->INR rate.
 *
 * Request keys (Rust wire names): plan_slug, period_months, currency, gst
 * (bool), fx_markup (ratio, 0.035 = 3.5%), fx_rate. Optionally `cycle`
 * (monthly|quarterly|semiannually|annually|biennially|triennially) instead of
 * period_months. Unknown cycles / periods are rejected, never coerced.
 *
 * PHP 7.4 polyglot.
 */
final class QuoteService
{
    public const GST_RATE = 0.18;

    /** @var array<string,int> */
    public const CYCLE_MONTHS = [
        'monthly' => 1,
        'quarterly' => 3,
        'semiannually' => 6,
        'annually' => 12,
        'biennially' => 24,
        'triennially' => 36,
    ];

    /**
     * @param array<string,mixed> $plan plan row (API v1.1 shape: periods[])
     * @param array<string,mixed> $req
     * @return array<string,mixed>
     * @throws \InvalidArgumentException on unknown cycle/period or missing INR rate
     */
    public function quote(array $plan, array $req): array
    {
        $months = $this->resolveMonths($req);

        $row = null;
        foreach ((array) ($plan['periods'] ?? []) as $p) {
            if (is_array($p) && (int) ($p['months'] ?? 0) === $months) {
                $row = $p;
                break;
            }
        }
        if ($row === null) {
            throw new \InvalidArgumentException("period {$months} mo is not offered for this plan");
        }

        $planSlug = (string) ($req['plan_slug'] ?? ($plan['product_slug'] ?? ($plan['slug'] ?? '')));
        $currency = (string) ($req['currency'] ?? 'EUR');
        $gst = !empty($req['gst']);
        $fxMarkup = (float) ($req['fx_markup'] ?? 0.0);

        $base = (float) ($row['effective_monthly'] ?? 0.0);
        $setup = (float) ($row['setup_fee'] ?? 0.0);
        $configured = $base; // selection deltas are not applied (parity with Rust)
        $gstAmt = $gst ? $configured * self::GST_RATE : 0.0;
        $afterGst = $configured + $gstAmt;

        $breakdown = [sprintf('base €%.2f/mo', $base)];
        if ($gst) {
            $breakdown[] = sprintf('+18%% GST €%.2f', $gstAmt);
        }

        $fxRate = null;
        if ($currency === 'INR') {
            if (!isset($req['fx_rate']) || !is_numeric($req['fx_rate']) || (float) $req['fx_rate'] <= 0.0) {
                throw new \InvalidArgumentException('INR quote requires a positive fx_rate');
            }
            $rate = (float) $req['fx_rate'];
            $withMarkup = $rate * (1.0 + $fxMarkup);
            $breakdown[] = sprintf('× EUR→INR %.4f (%d%% markup)', $withMarkup, (int) round($fxMarkup * 100.0));
            $finalMonthly = $afterGst * $withMarkup;
            $fxRate = $rate;
        } else {
            $finalMonthly = $afterGst;
        }

        $finalTotal = $finalMonthly * $months + $setup * ($fxRate !== null ? $fxRate : 1.0);

        return [
            'plan_slug' => $planSlug,
            'period_months' => $months,
            'currency' => $currency,
            'base_monthly_eur' => $base,
            'configured_monthly_eur' => $configured,
            'setup_fee_eur' => $setup,
            'gst_amount_eur' => $gstAmt,
            'fx_rate' => $fxRate,
            'fx_markup' => $fxMarkup,
            'final_monthly' => $finalMonthly,
            'final_total' => $finalTotal,
            'breakdown' => $breakdown,
        ];
    }

    /** @param array<string,mixed> $req */
    private function resolveMonths(array $req): int
    {
        if (isset($req['cycle'])) {
            $key = strtolower((string) $req['cycle']);
            if (!isset(self::CYCLE_MONTHS[$key])) {
                throw new \InvalidArgumentException('unknown billing cycle: ' . (string) $req['cycle']);
            }
            return self::CYCLE_MONTHS[$key];
        }
        $m = (int) ($req['period_months'] ?? 0);
        if (!in_array($m, array_values(self::CYCLE_MONTHS), true)) {
            throw new \InvalidArgumentException('unsupported period_months: ' . $m);
        }
        return $m;
    }
}
