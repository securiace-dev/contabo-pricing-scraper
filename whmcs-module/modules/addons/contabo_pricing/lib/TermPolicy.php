<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Operator-configurable term-translation policy, stored in mod_contabo_settings
 * (same table/pattern as Scrape\ScrapeSettings). Every read degrades to the
 * defaults on ANY error, so a missing table or row can never fail a sync.
 *
 *   term.policy           nearest-term (default) | proportional
 *   term.addon_policy     proportional (default) | nearest-term
 *   term.adjust_pct_json  {"24":0,"36":0}  percent off DERIVED monthly rate
 *                         for terms Contabo does not publish. Default 0 = no
 *                         extrapolated discount is assumed.
 *
 * The default (nearest-term, no adjustment) reproduces the pre-existing
 * periodPriceVectorFromPlan() behaviour exactly, so deploying this changes no
 * price by itself.
 */
final class TermPolicy
{
    private const TABLE = 'mod_contabo_settings';

    /** @return array<string,string> */
    public static function defaults(): array
    {
        return [
            'term.policy'          => TermTranslator::POLICY_NEAREST_TERM,
            'term.addon_policy'    => TermTranslator::POLICY_PROPORTIONAL,
            'term.adjust_pct_json' => '{}',
        ];
    }

    private static function get(string $key): string
    {
        $default = self::defaults()[$key] ?? '';
        try {
            $v = Capsule::table(self::TABLE)->where('key', $key)->value('value');
            return $v === null ? $default : (string) $v;
        } catch (\Throwable $e) {
            return $default;
        }
    }

    public static function policy(): string
    {
        return TermTranslator::normalizePolicy(self::get('term.policy'));
    }

    public static function addonPolicy(): string
    {
        $p = self::get('term.addon_policy');
        return TermTranslator::normalizePolicy($p === '' ? TermTranslator::POLICY_PROPORTIONAL : $p);
    }

    /** @return array<int,float> months => percent (0..50) */
    public static function adjustPct(): array
    {
        $decoded = json_decode(self::get('term.adjust_pct_json'), true);
        $out = [];
        if (is_array($decoded)) {
            foreach ($decoded as $m => $pct) {
                if ((int) $m > 0 && is_numeric($pct)) {
                    $out[(int) $m] = max(0.0, min(TermTranslator::MAX_ADJUST_PCT, (float) $pct));
                }
            }
        }
        return $out;
    }

    /** Stored JSON object for the settings form; always a valid object string. */
    public static function adjustPctJson(): string
    {
        $pct = self::adjustPct();
        return $pct === [] ? '{}' : (string) json_encode($pct);
    }

    /**
     * Persist the three term.* keys. Invalid policy enums fall back to the
     * documented defaults; a non-object JSON blob is rejected.
     *
     * @throws \InvalidArgumentException
     */
    public static function save(string $policy, string $addonPolicy, string $adjustJson): void
    {
        $policy = TermTranslator::normalizePolicy($policy);
        $addonPolicy = TermTranslator::normalizePolicy(
            $addonPolicy === '' ? TermTranslator::POLICY_PROPORTIONAL : $addonPolicy
        );
        $adjustJson = trim($adjustJson) === '' ? '{}' : $adjustJson;
        $decoded = json_decode($adjustJson, true);
        if (!is_array($decoded)) {
            throw new \InvalidArgumentException('Derived-term adjustment must be a JSON object such as {"24":0,"36":0}.');
        }
        if ($decoded !== [] && array_values($decoded) === $decoded) {
            throw new \InvalidArgumentException('Derived-term adjustment must be a JSON object keyed by months, not a list.');
        }
        $clean = [];
        foreach ($decoded as $m => $pct) {
            if ((int) $m <= 0 || !is_numeric($pct)) {
                throw new \InvalidArgumentException('Each adjustment key must be a positive month count and each value a number.');
            }
            $clean[(string) (int) $m] = max(0.0, min(TermTranslator::MAX_ADJUST_PCT, (float) $pct));
        }
        $now = date('Y-m-d H:i:s');
        $rows = [
            'term.policy'          => $policy,
            'term.addon_policy'    => $addonPolicy,
            'term.adjust_pct_json' => $clean === [] ? '{}' : (string) json_encode($clean),
        ];
        foreach ($rows as $key => $value) {
            Capsule::table(self::TABLE)->updateOrInsert(
                ['key' => $key],
                ['value' => $value, 'updated_at' => $now]
            );
        }
    }
}
