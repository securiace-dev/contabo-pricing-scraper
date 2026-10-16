<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Term translation engine: price ANY billing term of N months from whatever
 * terms Contabo actually publishes. Pure functions, no I/O, PHP 7.4 polyglot.
 *
 * Precedence (first hit wins):
 *   1. NATIVE        — Contabo publishes exactly N months → use it verbatim.
 *   2. DERIVED       — per policy:
 *        'nearest-term'  → effective monthly of the nearest published term AT OR
 *                          BELOW N (never a deeper discount the customer did not
 *                          earn; shortest published term if none is <= N).
 *        'proportional'  → LIST monthly (1-mo effective rate, else plan base
 *                          monthly) with NO term discount; total = monthly x N.
 *   3. If the policy's own source is missing, the other derivation is tried.
 *   4. Nothing usable → null (caller WARNS, never fails the sync).
 *
 * An optional per-term adjustment (percent off the derived MONTHLY rate) can be
 * configured for terms longer than anything Contabo publishes. Default 0: no
 * extrapolated discount is assumed. Adjustments never touch native terms and
 * never touch setup fees.
 *
 * All amounts here are EUR, ex-tax (Contabo's native currency). Tax, FX and the
 * WHMCS rounding mode are applied downstream by the existing pipeline
 * (ProfileVersionInput::toLocalMonthly + Rounding::apply); this class never
 * applies GST.
 *
 * WHMCS can bill only the six tblpricing cycles (1/3/6/12/24/36 months). Any
 * other N is supported for a profile headline period, Proposal Studio quotes
 * and provenance — it cannot become a WHMCS catalog cycle.
 */
final class TermTranslator
{
    public const POLICY_NEAREST_TERM = 'nearest-term';
    public const POLICY_PROPORTIONAL = 'proportional';

    /** @var list<int> */
    public const WHMCS_BILLABLE_MONTHS = [1, 3, 6, 12, 24, 36];

    public const SOURCE_NATIVE               = 'native';
    public const SOURCE_DERIVED_NEAREST      = 'derived_nearest_term';
    public const SOURCE_DERIVED_PROPORTIONAL = 'derived_proportional';

    public const MAX_ADJUST_PCT = 50.0;

    /** @return list<string> */
    public static function policies(): array
    {
        return [self::POLICY_NEAREST_TERM, self::POLICY_PROPORTIONAL];
    }

    public static function normalizePolicy(?string $policy): string
    {
        $p = strtolower(trim((string) $policy));
        return in_array($p, self::policies(), true) ? $p : self::POLICY_NEAREST_TERM;
    }

    /**
     * Index a plan's published terms: months => [monthly, setup].
     *
     * @param array<string,mixed> $plan
     * @return array<int,array{monthly:float,setup:float}>
     */
    public static function publishedTerms(array $plan): array
    {
        $out = [];
        foreach ((array) ($plan['periods'] ?? []) as $p) {
            if (!is_array($p)) {
                continue;
            }
            $m = (int) ($p['months'] ?? 0);
            if ($m <= 0 || !isset($p['effective_monthly']) || !is_numeric($p['effective_monthly'])) {
                continue;
            }
            $monthly = (float) $p['effective_monthly'];
            if (!is_finite($monthly) || $monthly <= 0.0) {
                continue;
            }
            $setup = isset($p['setup_fee']) && is_numeric($p['setup_fee']) ? max(0.0, (float) $p['setup_fee']) : 0.0;
            $out[$m] = ['monthly' => $monthly, 'setup' => $setup];
        }
        ksort($out);
        return $out;
    }

    /**
     * Price one term.
     *
     * @param array<int,array{monthly:float,setup:float}> $published from publishedTerms()
     * @param float|null $listMonthly plan base monthly price (used by 'proportional' when no 1-mo term exists)
     * @param array<int,float> $adjustPct months => percent off derived monthly (0..50)
     * @return array<string,mixed>|null null when nothing can price this term
     *   {months, source, derived, monthly_eur, setup_eur, total_eur, source_months,
     *    formula, policy, adjust_pct}
     */
    public static function resolve(
        array $published,
        int $months,
        string $policy = self::POLICY_NEAREST_TERM,
        ?float $listMonthly = null,
        array $adjustPct = []
    ): ?array {
        if ($months <= 0 || $published === []) {
            return null;
        }
        $policy = self::normalizePolicy($policy);

        if (isset($published[$months])) {
            $t = $published[$months];
            return self::build($months, self::SOURCE_NATIVE, $policy, $t['monthly'], $t['setup'], $months, 0.0,
                sprintf('native %d-mo published rate', $months));
        }

        $adj = isset($adjustPct[$months]) ? max(0.0, min(self::MAX_ADJUST_PCT, (float) $adjustPct[$months])) : 0.0;

        $order = $policy === self::POLICY_PROPORTIONAL
            ? [self::POLICY_PROPORTIONAL, self::POLICY_NEAREST_TERM]
            : [self::POLICY_NEAREST_TERM, self::POLICY_PROPORTIONAL];

        foreach ($order as $how) {
            if ($how === self::POLICY_NEAREST_TERM) {
                if (self::hasTermAtOrBelow($published, $months) === false && ($listMonthly !== null && $listMonthly > 0.0)) {
                    // Term shorter than every published term: a deeper-term
                    // discount was never earned, so skip to the list price.
                    continue;
                }
                $src = self::nearestAtOrBelow($published, $months);
                $t = $published[$src];
                $monthly = $t['monthly'] * (1.0 - $adj / 100.0);
                return self::build($months, self::SOURCE_DERIVED_NEAREST, $policy, $monthly, $t['setup'], $src, $adj,
                    sprintf('monthly(%d-mo) = monthly(%d-mo) x (1 - %s%%); total = monthly x %d',
                        $months, $src, self::pct($adj), $months));
            }
            // proportional: list monthly, no term discount
            $list = null;
            $srcMonths = 1;
            $setup = 0.0;
            if (isset($published[1])) {
                $list = $published[1]['monthly'];
                $setup = $published[1]['setup'];
            } elseif ($listMonthly !== null && $listMonthly > 0.0) {
                $list = $listMonthly;
                $srcMonths = 0; // plan base price, no published term
            }
            if ($list !== null) {
                $monthly = $list * (1.0 - $adj / 100.0);
                return self::build($months, self::SOURCE_DERIVED_PROPORTIONAL, $policy, $monthly, $setup, $srcMonths, $adj,
                    sprintf('monthly(%d-mo) = list monthly x (1 - %s%%); total = list monthly x %d',
                        $months, self::pct($adj), $months));
            }
        }
        return null;
    }

    /**
     * Resolve every requested term; terms that cannot be priced are reported in
     * `missing` (a WARNING for the caller), never thrown.
     *
     * @param list<int> $terms
     * @param array<int,float> $adjustPct
     * @return array{terms:array<int,array<string,mixed>>,missing:list<int>}
     */
    public static function resolveMany(
        array $published,
        array $terms,
        string $policy = self::POLICY_NEAREST_TERM,
        ?float $listMonthly = null,
        array $adjustPct = []
    ): array {
        $out = [];
        $missing = [];
        foreach ($terms as $m) {
            $r = self::resolve($published, (int) $m, $policy, $listMonthly, $adjustPct);
            if ($r === null) {
                $missing[] = (int) $m;
            } else {
                $out[(int) $m] = $r;
            }
        }
        return ['terms' => $out, 'missing' => $missing];
    }

    /**
     * Price one recurring add-on for term N. `$termMonthly` is months => EUR/mo
     * when the add-on is published per term (none are today: Contabo publishes
     * add-ons as a single monthly price, so the common case is the
     * proportional branch). One-time add-on charges are NOT passed through here
     * — they are never multiplied by the term (see oneTime()).
     *
     * @param float $monthlyDelta add-on EUR per month (the published monthly price)
     * @param array<int,float> $termMonthly optional native per-term monthly prices
     * @return array<string,mixed> {months, source, derived, monthly_eur, total_eur, source_months, formula}
     */
    public static function resolveAddon(
        float $monthlyDelta,
        int $months,
        array $termMonthly = [],
        string $policy = self::POLICY_PROPORTIONAL
    ): array {
        $monthlyDelta = max(0.0, $monthlyDelta);
        $policy = self::normalizePolicy($policy);
        if (isset($termMonthly[$months]) && (float) $termMonthly[$months] >= 0.0) {
            return self::build($months, self::SOURCE_NATIVE, $policy, (float) $termMonthly[$months], 0.0, $months, 0.0,
                sprintf('add-on native %d-mo rate', $months));
        }
        if ($termMonthly !== [] && $policy === self::POLICY_NEAREST_TERM) {
            $pub = [];
            foreach ($termMonthly as $m => $v) {
                if ((int) $m > 0 && (float) $v >= 0.0) {
                    $pub[(int) $m] = ['monthly' => (float) $v, 'setup' => 0.0];
                }
            }
            if ($pub !== []) {
                ksort($pub);
                $src = self::nearestAtOrBelow($pub, $months);
                return self::build($months, self::SOURCE_DERIVED_NEAREST, $policy, $pub[$src]['monthly'], 0.0, $src, 0.0,
                    sprintf('add-on monthly(%d-mo) = add-on monthly(%d-mo)', $months, $src));
            }
        }
        // Default: the published monthly price scaled by N, no term discount assumed.
        $isMonthTerm = $months === 1;
        return self::build($months, $isMonthTerm ? self::SOURCE_NATIVE : self::SOURCE_DERIVED_PROPORTIONAL, $policy,
            $monthlyDelta, 0.0, 1, 0.0,
            $isMonthTerm ? 'add-on published monthly rate' : sprintf('add-on total = published monthly x %d (no term discount assumed)', $months));
    }

    /**
     * One-time charge (setup fee, one-time add-on). Never scaled by the term and
     * never discounted by the term adjustment.
     */
    public static function oneTime(float $amountEur): float
    {
        return max(0.0, $amountEur);
    }

    /** Round half up (away from zero), `$dp` decimals. EUR display/provenance only. */
    public static function roundHalfUp(float $v, int $dp = 2): float
    {
        return round($v, $dp, PHP_ROUND_HALF_UP);
    }

    /** @param array<int,mixed> $published */
    private static function hasTermAtOrBelow(array $published, int $months): bool
    {
        foreach (array_keys($published) as $m) {
            if ((int) $m <= $months) {
                return true;
            }
        }
        return false;
    }

    /**
     * Largest published term <= months; the shortest published term when none is.
     *
     * @param array<int,mixed> $published non-empty
     */
    public static function nearestAtOrBelow(array $published, int $months): int
    {
        $best = null;
        foreach (array_keys($published) as $m) {
            $m = (int) $m;
            if ($m <= $months && ($best === null || $m > $best)) {
                $best = $m;
            }
        }
        return $best !== null ? $best : (int) min(array_keys($published));
    }

    /**
     * Compact, JSON-safe provenance record for a resolved term.
     *
     * @param array<string,mixed> $r result of resolve()/resolveAddon()
     * @return array<string,mixed>
     */
    public static function provenance(array $r): array
    {
        return [
            'months'        => (int) $r['months'],
            'source'        => (string) $r['source'],
            'derived'       => (bool) $r['derived'],
            'policy'        => (string) $r['policy'],
            'source_months' => (int) $r['source_months'],
            'adjust_pct'    => (float) $r['adjust_pct'],
            'monthly_eur'   => (float) $r['monthly_eur'],
            'formula'       => (string) $r['formula'],
        ];
    }

    /** @return array<string,mixed> */
    private static function build(
        int $months,
        string $source,
        string $policy,
        float $monthly,
        float $setup,
        int $sourceMonths,
        float $adjustPct,
        string $formula
    ): array {
        $monthly = round($monthly, 4);
        return [
            'months'        => $months,
            'source'        => $source,
            'derived'       => $source !== self::SOURCE_NATIVE,
            'policy'        => $policy,
            'monthly_eur'   => $monthly,
            'setup_eur'     => $setup,
            'total_eur'     => self::roundHalfUp($monthly * $months, 2) + $setup,
            'source_months' => $sourceMonths,
            'adjust_pct'    => $adjustPct,
            'formula'       => $formula,
        ];
    }

    private static function pct(float $v): string
    {
        return rtrim(rtrim(number_format($v, 2, '.', ''), '0'), '.') ?: '0';
    }

    /**
     * Headline-period provenance from a profile version options_snapshot.
     *
     * @param mixed $snapshot array or JSON string
     * @return array<string,mixed>|null
     */
    public static function headlineFromSnapshot($snapshot, int $periodMonths): ?array
    {
        if (is_string($snapshot)) {
            $decoded = json_decode($snapshot, true);
            $snapshot = is_array($decoded) ? $decoded : null;
        }
        if (!is_array($snapshot) || $periodMonths <= 0) {
            return null;
        }
        foreach ((array) ($snapshot['_term_provenance'] ?? []) as $prov) {
            if (!is_array($prov)) {
                continue;
            }
            if ((int) ($prov['months'] ?? 0) === $periodMonths && !empty($prov['derived'])) {
                return $prov;
            }
        }
        return null;
    }

    /**
     * Operator-facing provenance sentence. Names Contabo because this is
     * internal (sync log, Activity Log, Proposal Studio internal artifact).
     *
     * @param array<string,mixed> $provenance
     */
    public static function operatorNote(array $provenance): string
    {
        $src = (int) ($provenance['source_months'] ?? 0);
        $formula = trim((string) ($provenance['formula'] ?? ''));
        $note = $src > 0
            ? sprintf('derived from %d-month Contabo price', $src)
            : 'derived, not an official Contabo price';
        return $formula === '' ? $note : $note . ': ' . $formula;
    }

    /**
     * Client-facing provenance sentence. Must not name the provider.
     *
     * @param array<string,mixed> $provenance
     */
    public static function clientSafeNote(array $provenance): string
    {
        $src = (int) ($provenance['source_months'] ?? 0);
        if ($src > 0) {
            return sprintf(
                'This term uses an estimated rate derived from a %d-month published term.',
                $src
            );
        }
        return 'This term uses an estimated rate derived from a shorter published term.';
    }
}
