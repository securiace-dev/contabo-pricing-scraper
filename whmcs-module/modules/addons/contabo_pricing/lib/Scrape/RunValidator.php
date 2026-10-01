<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Deterministic, numeric, PHP-only gates over a merged scrape result. No LLM
 * is involved here; JevJudge can only veto afterwards.
 *
 * Attempt shape (RunValidator reads only these keys):
 *   ['family'=>string, 'url'=>string, 'final_url'=>?string, 'plans'=>list<plan>, 'warnings'=>list<string>]
 *
 * Gates (each ['ok'=>bool,'detail'=>mixed]): min_plans_per_family,
 * count_drop_pct, schema_completeness, price_sanity, cross_page_consistency,
 * dom_cross_check, label_guard. Diff buckets come from PlanDiffer +
 * ChangeClassifier against the last good run's plans.
 */
final class RunValidator
{
    /** Effective monthly may not exceed base by more than this factor. */
    public const EFFECTIVE_MAX_FACTOR = 1.01;

    /** Free-text spec fields scanned by label_guard (upstream name/slug/url are identifiers, exempt). */
    public const LABEL_FIELDS = ['display_name', 'cpu', 'ram', 'base_storage', 'snapshots', 'port'];

    /**
     * @param list<array<string,mixed>> $plans merged plans (one per slug)
     * @param list<array<string,mixed>> $attempts
     * @param array<mixed>|null $lastGoodPlans list of plans (or slug map) from the latest succeeded run
     * @return array{gates:array<string,array{ok:bool,detail:mixed}>,anomalies:list<array<string,mixed>>,risky:list<array<string,mixed>>,safe:list<array<string,mixed>>,passed:bool,warnings:list<string>}
     */
    public function evaluate(array $plans, array $attempts, ?array $lastGoodPlans, ScrapeSettings $s): array
    {
        $cur = self::bySlug($plans);
        $prev = self::bySlug($lastGoodPlans ?? []);

        $gates = [
            'min_plans_per_family' => $this->minPlans($cur, $s),
            'count_drop_pct' => $this->countDrop($cur, $prev, $s),
            'schema_completeness' => $this->schema($cur),
            'price_sanity' => $this->priceSanity($cur, $prev, $s),
            'cross_page_consistency' => $this->crossPage($attempts),
            'dom_cross_check' => $this->domCheck($attempts),
            'label_guard' => $this->labels($cur),
        ];

        $buckets = ['anomalies' => [], 'risky' => [], 'safe' => []];
        $map = [
            ChangeClassifier::ANOMALY => 'anomalies',
            ChangeClassifier::RISKY => 'risky',
            ChangeClassifier::SAFE => 'safe',
        ];
        foreach (PlanDiffer::diff($prev, $cur) as $d) {
            $buckets[$map[ChangeClassifier::classify($d)]][] = $d;
        }

        $passed = true;
        foreach ($gates as $g) {
            if (!$g['ok']) {
                $passed = false;
            }
        }
        return [
            'gates' => $gates,
            'anomalies' => $buckets['anomalies'],
            'risky' => $buckets['risky'],
            'safe' => $buckets['safe'],
            'passed' => $passed,
            'warnings' => $this->redirectWarnings($attempts),
        ];
    }

    /**
     * Upstream relaunches redirect old plan URLs (Spike-0: cloud-vps-10 now
     * lands on cloud-vps-core-4 while the blob still holds the legacy slugs).
     * That is informational, never a gate failure.
     *
     * @param list<array<string,mixed>> $attempts
     * @return list<string>
     */
    private function redirectWarnings(array $attempts): array
    {
        $out = [];
        foreach ($attempts as $a) {
            $url = (string) ($a['url'] ?? '');
            $final = (string) ($a['final_url'] ?? '');
            if ($url !== '' && $final !== '' && PlanUrlList::slugFromUrl($url) !== PlanUrlList::slugFromUrl($final)) {
                $out[] = 'upstream redirect: ' . PlanUrlList::slugFromUrl($url) . ' -> ' . PlanUrlList::slugFromUrl($final);
            }
        }
        return array_values(array_unique($out));
    }

    /**
     * @param array<mixed> $plans
     * @return array<string,array<string,mixed>>
     */
    public static function bySlug(array $plans): array
    {
        $out = [];
        foreach ($plans as $k => $p) {
            if (!is_array($p)) {
                continue;
            }
            $slug = isset($p['product_slug']) && is_string($p['product_slug']) ? $p['product_slug'] : (is_string($k) ? $k : '');
            if ($slug !== '') {
                $out[$slug] = $p;
            }
        }
        return $out;
    }

    /** @param array<string,array<string,mixed>> $cur */
    private function minPlans(array $cur, ScrapeSettings $s): array
    {
        $counts = [];
        foreach ($s->minPlansByFamily() as $family => $_min) {
            $counts[$family] = 0;
        }
        foreach ($cur as $p) {
            $f = (string) ($p['family'] ?? 'Unknown');
            $counts[$f] = ($counts[$f] ?? 0) + 1;
        }
        $ok = true;
        $detail = [];
        foreach ($s->minPlansByFamily() as $family => $min) {
            $good = $counts[$family] >= $min;
            $ok = $ok && $good;
            $detail[$family] = ['count' => $counts[$family], 'min' => $min, 'ok' => $good];
        }
        return ['ok' => $ok, 'detail' => $detail];
    }

    /**
     * @param array<string,array<string,mixed>> $cur
     * @param array<string,array<string,mixed>> $prev
     */
    private function countDrop(array $cur, array $prev, ScrapeSettings $s): array
    {
        $n = count($cur);
        $p = count($prev);
        if ($p === 0) {
            return ['ok' => true, 'detail' => ['previous' => 0, 'current' => $n, 'drop_pct' => 0.0, 'note' => 'no baseline']];
        }
        $drop = max(0.0, (($p - $n) / $p) * 100.0);
        $max = $s->maxDropPct();
        // compare on hundredths of a percent to avoid float noise at the boundary
        $ok = (int) round($drop * 100) <= (int) round($max * 100);
        return ['ok' => $ok, 'detail' => ['previous' => $p, 'current' => $n, 'drop_pct' => round($drop, 2), 'max_pct' => $max]];
    }

    /** @param array<string,array<string,mixed>> $cur */
    private function schema(array $cur): array
    {
        $bad = [];
        foreach ($cur as $slug => $p) {
            $why = [];
            if (!isset($p['periods']) || !is_array($p['periods']) || count($p['periods']) < 1) {
                $why[] = 'periods';
            }
            $base = $p['base_monthly_price'] ?? null;
            if (!(is_int($base) || is_float($base)) || $base <= 0) {
                $why[] = 'base_monthly_price';
            }
            $sp = is_array($p['specs_parsed'] ?? null) ? $p['specs_parsed'] : [];
            foreach (['cpu_count', 'ram_gb', 'storage_primary_gb'] as $dim) {
                $v = $sp[$dim] ?? null;
                if (!(is_int($v) || is_float($v)) || $v <= 0) {
                    $why[] = $dim;
                }
            }
            if ($why !== []) {
                $bad[$slug] = $why;
            }
        }
        return ['ok' => $bad === [], 'detail' => ['incomplete' => $bad]];
    }

    /**
     * @param array<string,array<string,mixed>> $cur
     * @param array<string,array<string,mixed>> $prev
     */
    private function priceSanity(array $cur, array $prev, ScrapeSettings $s): array
    {
        $max = $s->maxPriceChangePct();
        $bad = [];
        foreach ($cur as $slug => $p) {
            $base = $p['base_monthly_price'] ?? null;
            if (!(is_int($base) || is_float($base)) || $base <= 0) {
                continue; // reported by schema_completeness
            }
            $baseM = (int) round($base * 10000);
            foreach ((array) ($p['periods'] ?? []) as $per) {
                $eff = is_array($per) ? ($per['effective_monthly'] ?? null) : null;
                if (!(is_int($eff) || is_float($eff))) {
                    $bad[$slug][] = 'effective_monthly missing';
                    continue;
                }
                $effM = (int) round($eff * 10000);
                if ($effM <= 0 || $effM * 100 > $baseM * (int) round(self::EFFECTIVE_MAX_FACTOR * 100)) {
                    $bad[$slug][] = 'effective_monthly ' . $eff . ' outside (0, base*1.01] for base ' . $base;
                }
            }
            $pb = $prev[$slug]['base_monthly_price'] ?? null;
            if ((is_int($pb) || is_float($pb)) && $pb > 0) {
                $pbM = (int) round($pb * 10000);
                $changePct = abs($baseM - $pbM) / $pbM * 100.0;
                if ((int) round($changePct * 100) > (int) round($max * 100)) {
                    $bad[$slug][] = sprintf('base changed %.2f%% (max %.2f%%)', $changePct, $max);
                }
            }
        }
        return ['ok' => $bad === [], 'detail' => ['violations' => $bad]];
    }

    /** @param list<array<string,mixed>> $attempts */
    private function crossPage(array $attempts): array
    {
        $seen = [];
        $conflicts = [];
        foreach ($attempts as $a) {
            foreach ((array) ($a['plans'] ?? []) as $p) {
                if (!is_array($p) || !isset($p['product_slug'])) {
                    continue;
                }
                $slug = (string) $p['product_slug'];
                $sig = json_encode([
                    $p['base_monthly_price'] ?? null,
                    $p['specs_parsed'] ?? null,
                    array_map(static function ($per) {
                        return is_array($per) ? [$per['months'] ?? null, $per['effective_monthly'] ?? null] : null;
                    }, (array) ($p['periods'] ?? [])),
                ]);
                if (isset($seen[$slug]) && $seen[$slug] !== $sig) {
                    $conflicts[$slug] = true;
                }
                $seen[$slug] = $seen[$slug] ?? $sig;
            }
        }
        return ['ok' => $conflicts === [], 'detail' => ['conflicting_slugs' => array_keys($conflicts)]];
    }

    /** @param list<array<string,mixed>> $attempts */
    private function domCheck(array $attempts): array
    {
        $mismatches = [];
        foreach ($attempts as $a) {
            foreach ((array) ($a['warnings'] ?? []) as $w) {
                if (is_string($w) && strpos($w, 'dom_probe mismatch') === 0) {
                    $mismatches[] = $w;
                }
            }
        }
        return ['ok' => $mismatches === [], 'detail' => ['mismatches' => $mismatches]];
    }

    /** @param array<string,array<string,mixed>> $cur */
    private function labels(array $cur): array
    {
        $guard = new LabelGuard();
        $hits = [];
        foreach ($cur as $slug => $p) {
            foreach (self::LABEL_FIELDS as $f) {
                if (isset($p[$f]) && is_string($p[$f])) {
                    $found = $guard->scan($p[$f]);
                    if ($found !== []) {
                        $hits[$slug][$f] = $found;
                    }
                }
            }
        }
        return ['ok' => $hits === [], 'detail' => ['hits' => $hits]];
    }
}
