<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Pure snapshot differ over normalized plans keyed by slug. Port of the
 * archived differ with the review corrections applied (see
 * docs/test-vectors/differ.json): scrape_failed instead of mass retirement,
 * parser_error for non-numeric spec fields, storage-type ranking.
 *
 * A plan is the PlanNormalizer shape; only product_name, base_monthly_price and
 * specs_parsed are read. Output is sorted by slug, then emission order:
 * cost diff, parser_error, spec_increase, spec_decrease, plan_renamed.
 */
final class PlanDiffer
{
    /** @var list<string> */
    public const NUMERIC_DIMS = ['cpu_count', 'ram_gb', 'storage_primary_gb', 'port_speed_mbps'];

    /** @var array<string,int> lower-cased storage type => rank (higher is better) */
    public const STORAGE_RANK = ['nvme' => 3, 'ssd' => 2, 'hdd' => 1];

    /**
     * @param array<string,array<string,mixed>> $prevPlansBySlug
     * @param array<string,array<string,mixed>> $nextPlansBySlug
     * @return list<array{kind:string,catalog_sku:?string,cost_before:?float,cost_after:?float,spec_before:?array,spec_after:?array,notes:string}>
     */
    public static function diff(array $prevPlansBySlug, array $nextPlansBySlug): array
    {
        if ($nextPlansBySlug === [] && $prevPlansBySlug !== []) {
            return [self::make(
                'scrape_failed',
                null,
                null,
                null,
                null,
                null,
                'next snapshot empty while previous had ' . count($prevPlansBySlug)
                . ' plans; treated as failed scrape, not mass retirement'
            )];
        }

        $skus = [];
        foreach (array_keys($prevPlansBySlug) as $k) {
            $skus[(string) $k] = true;
        }
        foreach (array_keys($nextPlansBySlug) as $k) {
            $skus[(string) $k] = true;
        }
        $skus = array_keys($skus);
        sort($skus, SORT_STRING);

        $out = [];
        foreach ($skus as $sku) {
            $sku = (string) $sku;
            $inPrev = array_key_exists($sku, $prevPlansBySlug) || array_key_exists((int) $sku, $prevPlansBySlug);
            $inNext = array_key_exists($sku, $nextPlansBySlug) || array_key_exists((int) $sku, $nextPlansBySlug);
            $prev = $inPrev ? (is_array($prevPlansBySlug[$sku] ?? null) ? $prevPlansBySlug[$sku] : []) : null;
            $next = $inNext ? (is_array($nextPlansBySlug[$sku] ?? null) ? $nextPlansBySlug[$sku] : []) : null;

            if ($prev === null && $next !== null) {
                $out[] = self::make('new_plan', $sku, null, self::cost($next), null, self::spec($next), '');
                continue;
            }
            if ($prev !== null && $next === null) {
                $out[] = self::make('plan_retired', $sku, self::cost($prev), null, self::spec($prev), null, '');
                continue;
            }
            if ($prev === null || $next === null) {
                continue;
            }
            foreach (self::comparePair($sku, $prev, $next) as $d) {
                $out[] = $d;
            }
        }
        return $out;
    }

    /**
     * @param array<string,mixed> $prev
     * @param array<string,mixed> $next
     * @return list<array<string,mixed>>
     */
    private static function comparePair(string $sku, array $prev, array $next): array
    {
        $out = [];
        $specBefore = self::spec($prev);
        $specAfter = self::spec($next);

        // cost
        $cb = self::cost($prev);
        $ca = self::cost($next);
        if ($cb !== null && $ca !== null) {
            $b = self::micro($cb);
            $a = self::micro($ca);
            if ($a !== $b) {
                $kind = $a > $b ? 'cost_increase' : 'cost_decrease';
                $note = $b === 0
                    ? sprintf('%.4f -> %.4f', $cb, $ca)
                    : sprintf('%+.2f%% (%.4f -> %.4f)', (($a - $b) / $b) * 100.0, $cb, $ca);
                $out[] = self::make($kind, $sku, $cb, $ca, $specBefore, $specAfter, $note);
            }
        }

        // spec
        $ps = is_array($prev['specs_parsed'] ?? null) ? $prev['specs_parsed'] : [];
        $ns = is_array($next['specs_parsed'] ?? null) ? $next['specs_parsed'] : [];
        $errors = [];
        $increase = [];
        $decrease = [];

        foreach (self::NUMERIC_DIMS as $dim) {
            $pv = $ps[$dim] ?? null;
            $nv = $ns[$dim] ?? null;
            $pOk = self::isNum($pv);
            $nOk = self::isNum($nv);
            if (!$pOk && !$nOk) {
                continue;
            }
            if ($pOk !== $nOk) {
                $errors[] = $dim;
                continue;
            }
            if ((float) $pv < (float) $nv) {
                $increase[] = $dim;
            } elseif ((float) $pv > (float) $nv) {
                $decrease[] = $dim;
            }
        }

        $pt = $ps['storage_primary_type'] ?? null;
        $nt = $ns['storage_primary_type'] ?? null;
        $pRank = is_string($pt) ? (self::STORAGE_RANK[strtolower(trim($pt))] ?? null) : null;
        $nRank = is_string($nt) ? (self::STORAGE_RANK[strtolower(trim($nt))] ?? null) : null;
        $pPresent = is_string($pt) && trim($pt) !== '';
        $nPresent = is_string($nt) && trim($nt) !== '';
        if ($pPresent || $nPresent) {
            if ($pPresent !== $nPresent) {
                $errors[] = 'storage_primary_type';
            } elseif (strtolower(trim((string) $pt)) !== strtolower(trim((string) $nt))) {
                if ($pRank === null || $nRank === null) {
                    $errors[] = 'storage_primary_type';
                } elseif ($pRank < $nRank) {
                    $increase[] = 'storage_primary_type';
                } else {
                    $decrease[] = 'storage_primary_type';
                }
            }
        }

        if ($errors !== []) {
            $out[] = self::make(
                'parser_error',
                $sku,
                null,
                null,
                $specBefore,
                $specAfter,
                'spec field missing, null or non-numeric on one side: ' . implode(', ', $errors)
            );
        }
        if ($increase !== []) {
            $out[] = self::make('spec_increase', $sku, null, null, $specBefore, $specAfter, implode(', ', $increase));
        }
        if ($decrease !== []) {
            $out[] = self::make('spec_decrease', $sku, null, null, $specBefore, $specAfter, implode(', ', $decrease));
        }

        // rename: name-only change (no numeric/type dimension moved)
        $pn = (string) ($prev['product_name'] ?? '');
        $nn = (string) ($next['product_name'] ?? '');
        if ($pn !== $nn && $increase === [] && $decrease === []) {
            $out[] = self::make('plan_renamed', $sku, null, null, $specBefore, $specAfter, $pn . ' -> ' . $nn);
        }
        return $out;
    }

    /** @param mixed $v */
    private static function isNum($v): bool
    {
        return is_int($v) || is_float($v);
    }

    private static function micro(float $v): int
    {
        return (int) round($v * 10000);
    }

    /** @param array<string,mixed> $plan */
    private static function cost(array $plan): ?float
    {
        $c = $plan['base_monthly_price'] ?? null;
        if (is_int($c) || is_float($c)) {
            return (float) $c;
        }
        if (is_string($c) && is_numeric($c)) {
            return (float) $c;
        }
        return null;
    }

    /**
     * @param array<string,mixed> $plan
     * @return array{product_name:string,specs_parsed:array<string,mixed>}
     */
    private static function spec(array $plan): array
    {
        return [
            'product_name' => (string) ($plan['product_name'] ?? ''),
            'specs_parsed' => is_array($plan['specs_parsed'] ?? null) ? $plan['specs_parsed'] : [],
        ];
    }

    /**
     * @param array<string,mixed>|null $before
     * @param array<string,mixed>|null $after
     * @return array{kind:string,catalog_sku:?string,cost_before:?float,cost_after:?float,spec_before:?array,spec_after:?array,notes:string}
     */
    private static function make(string $kind, ?string $sku, ?float $cb, ?float $ca, ?array $before, ?array $after, string $notes): array
    {
        return [
            'kind' => $kind,
            'catalog_sku' => $sku,
            'cost_before' => $cb,
            'cost_after' => $ca,
            'spec_before' => $before,
            'spec_after' => $after,
            'notes' => $notes,
        ];
    }
}
