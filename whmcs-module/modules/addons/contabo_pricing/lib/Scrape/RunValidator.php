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
 *
 * Family governance (FamilyRegistry::reconcile() diff, optional 5th argument):
 * a new, renamed, retired, delisted or reappeared family, a plan that moved
 * between families, and a family whose plan count deviates more than
 * scrape.max_drop_pct from its typical_plan_count are RISKY (-> needs_review,
 * never a rejection). min_plans_per_family then works per registry family.
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
     * @param array<string,mixed>|null $familyDiff FamilyRegistry::reconcile() result for this run
     * @return array{gates:array<string,array{ok:bool,detail:mixed}>,anomalies:list<array<string,mixed>>,risky:list<array<string,mixed>>,safe:list<array<string,mixed>>,passed:bool,warnings:list<string>}
     */
    public function evaluate(array $plans, array $attempts, ?array $lastGoodPlans, ScrapeSettings $s, ?array $familyDiff = null): array
    {
        $cur = self::bySlug($plans);
        $prev = self::bySlug($lastGoodPlans ?? []);

        $gates = [
            'min_plans_per_family' => $familyDiff === null ? $this->minPlans($cur, $s) : $this->minPlansRegistry($cur, $s, $familyDiff),
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

        $warnings = $this->redirectWarnings($attempts);
        if ($familyDiff !== null) {
            foreach ($this->familyFlags($familyDiff) as $flag) {
                $buckets['risky'][] = $flag;
            }
            foreach ((array) ($familyDiff['unapproved'] ?? []) as $u) {
                $warnings[] = 'family not yet approved: ' . (string) ($u['label'] ?? '') . ' (imported, flagged)';
            }
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
            'warnings' => $warnings,
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
     * Per registry family: min = floor(typical x (1 - max_drop_pct)), at least 1.
     * A moderate shrink is a needs_review flag (familyFlags); only a severe one
     * rejects. A family with no learned typical yet falls back to the legacy
     * scrape.min_plans_* setting for its name, then to 1.
     *
     * @param array<string,array<string,mixed>> $cur
     * @param array<string,mixed> $familyDiff
     */
    private function minPlansRegistry(array $cur, ScrapeSettings $s, array $familyDiff): array
    {
        $counts = [];
        foreach ($cur as $p) {
            $k = (string) ($p['category_id'] ?? '');
            $counts[$k] = ($counts[$k] ?? 0) + 1;
        }
        $legacy = $s->minPlansByFamily();
        $ok = true;
        $detail = [];
        foreach ((array) ($familyDiff['families'] ?? []) as $f) {
            $name = (string) ($f['name'] ?? $f['label'] ?? '');
            $typical = (int) ($f['typical_plan_count'] ?? 0);
            if ($typical > 0) {
                $min = max(1, (int) floor($typical * (1.0 - $s->maxDropPct() / 100.0)));
            } else {
                $min = max(1, (int) ($legacy[$name] ?? 1));
                if (!isset($legacy[$name])) {
                    $min = 1;
                }
            }
            $count = $counts[(string) ($f['category_id'] ?? '')] ?? 0;
            $good = $count >= $min;
            $ok = $ok && $good;
            $detail[$name] = ['count' => $count, 'min' => $min, 'typical' => $typical, 'ok' => $good];
        }
        if ($detail === []) {
            $ok = false; // a run that discovers no family at all must never import
            $detail['(none)'] = ['count' => 0, 'min' => 1, 'typical' => 0, 'ok' => false];
        }
        return ['ok' => $ok, 'detail' => $detail];
    }

    /**
     * Governance flags from the registry diff, shaped like PlanDiffer entries.
     *
     * @param array<string,mixed> $diff
     * @return list<array<string,mixed>>
     */
    private function familyFlags(array $diff): array
    {
        $mk = static function (string $kind, string $notes, array $family): array {
            return [
                'kind' => $kind, 'catalog_sku' => null, 'cost_before' => null, 'cost_after' => null,
                'spec_before' => null, 'spec_after' => null, 'notes' => $notes, 'family' => $family,
            ];
        };
        $out = [];
        foreach ((array) ($diff['new'] ?? []) as $f) {
            if (($f['status'] ?? '') === 'hidden') {
                continue; // first seen but not linked from the nav: recorded, nothing is imported from it
            }
            $why = ($f['reason'] ?? '') === 'listed_in_nav' ? 'now linked from the site navigation' : 'first seen';
            $out[] = $mk('family_new', 'New family "' . $f['label'] . '" (' . $why . ', ' . (int) ($f['plans'] ?? 0) . ' plans), awaiting approval', $f);
        }
        foreach ((array) ($diff['renamed'] ?? []) as $f) {
            $how = ($f['kind'] ?? '') === 'id_change' ? 'category id changed (history carried forward)' : 'title changed';
            $out[] = $mk('family_renamed', 'Family renamed "' . $f['from'] . '" -> "' . $f['to'] . '": ' . $how, $f);
        }
        foreach ((array) ($diff['retired'] ?? []) as $f) {
            $out[] = $mk('family_retired', 'Family "' . $f['label'] . '" vanished from the catalogue (was ' . $f['was'] . ')', $f);
        }
        foreach ((array) ($diff['delisted'] ?? []) as $f) {
            $out[] = $mk('family_delisted', 'Family "' . $f['label'] . '" is no longer linked from the site navigation', $f);
        }
        foreach ((array) ($diff['reappeared'] ?? []) as $f) {
            $out[] = $mk('family_reappeared', 'Previously retired family "' . $f['label'] . '" is back', $f);
        }
        foreach ((array) ($diff['count_changes'] ?? []) as $f) {
            $out[] = $mk('family_count_deviation', sprintf(
                'Family "%s" has %d plans vs typical %d (%.1f%% > %.1f%%)',
                $f['label'], (int) $f['current'], (int) $f['typical'], (float) $f['pct'], (float) $f['max_pct']
            ), $f);
        }
        foreach ((array) ($diff['plan_moves'] ?? []) as $f) {
            $out[] = $mk('family_plan_moved', 'Plan ' . $f['slug'] . ' moved from "' . $f['from_label'] . '" to "' . $f['to_label'] . '"', $f);
        }
        return $out;
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
