<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Buckets one PlanDiffer diff as safe | risky | anomaly. Unknown kinds are
 * anomalies (fail closed). mandatory_option_price_change is risky only when it
 * moves strictly more than 10%, or when either cost is missing / the old cost
 * is zero. os_image_removed is intentionally unlisted (unresolved policy) and
 * therefore classifies as anomaly until the owner decides.
 */
final class ChangeClassifier
{
    public const SAFE = 'safe';
    public const RISKY = 'risky';
    public const ANOMALY = 'anomaly';

    public const OPTION_PRICE_THRESHOLD_PCT = 10;

    /** @var list<string> */
    private const SAFE_KINDS = ['new_plan', 'cost_decrease', 'spec_increase', 'region_added', 'os_image_added'];
    /** @var list<string> */
    private const RISKY_KINDS = ['cost_increase', 'spec_decrease', 'plan_retired', 'region_removed', 'plan_renamed', 'option_removed'];
    /** @var list<string> */
    private const ANOMALY_KINDS = ['scrape_failed', 'parser_error', 'forbidden_term_leak', 'unknown_family'];

    /** @param array<string,mixed> $diff */
    public static function classify(array $diff): string
    {
        $kind = (string) ($diff['kind'] ?? '');
        if (in_array($kind, self::ANOMALY_KINDS, true)) {
            return self::ANOMALY;
        }
        if ($kind === 'mandatory_option_price_change') {
            return self::optionPriceBucket($diff);
        }
        if (in_array($kind, self::RISKY_KINDS, true)) {
            return self::RISKY;
        }
        if (in_array($kind, self::SAFE_KINDS, true)) {
            return self::SAFE;
        }
        return self::ANOMALY;
    }

    /** @param array<string,mixed> $diff */
    private static function optionPriceBucket(array $diff): string
    {
        $b = $diff['cost_before'] ?? null;
        $a = $diff['cost_after'] ?? null;
        if (!is_numeric($b) || !is_numeric($a)) {
            return self::RISKY;
        }
        $before = (int) round((float) $b * 10000);
        $after = (int) round((float) $a * 10000);
        if ($before === 0) {
            return self::RISKY;
        }
        return abs($after - $before) * 100 > self::OPTION_PRICE_THRESHOLD_PCT * abs($before) ? self::RISKY : self::SAFE;
    }
}
