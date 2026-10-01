<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Port of process_plan() in src/main.rs, minus add-on classification: turns
 * one Contabo `products[*]` record into the base_plan shape the Rust service
 * emits (family, ranks, specs, base_storage, periods, specs_parsed,
 * password_rules, source). All derived numbers are recomputed here from the
 * raw price / discount / setup values — nothing a provider computed is trusted.
 *
 * PHP 7.4 compatible.
 */
final class PlanNormalizer
{
    /** @var PlanUrlList */
    private $urls;

    public function __construct(?PlanUrlList $urls = null)
    {
        $this->urls = $urls ?? new PlanUrlList();
    }

    /** Rust family_from_type(). */
    public static function familyFromType(string $type): string
    {
        switch ($type) {
            case 'vps':
                return PlanUrlList::FAMILY_VPS;
            case 'storage-vps':
                return PlanUrlList::FAMILY_STORAGE;
            case 'vds':
                return PlanUrlList::FAMILY_VDS;
            default:
                return 'Unknown';
        }
    }

    /**
     * Rust round2(): format!("{v:.2}") re-parsed. PHP's %.2F is correctly
     * rounded (ties-to-even on exact binary ties) and was verified identical
     * to Rust on 8025 values including 0.125 -> 0.12 and 2.675 -> 2.67.
     */
    public static function round2(float $v): float
    {
        return (float) sprintf('%.2F', $v);
    }

    /** Rust json_num(): whole floats are emitted as integers. @return int|float */
    public static function jsonNum(float $v)
    {
        if (is_finite($v) && floor($v) === $v && abs($v) < 1.0e15) {
            return (int) $v;
        }
        return $v;
    }

    /** Rust normalize_storage_label(). */
    public static function normalizeStorageLabel(string $label): string
    {
        $s = preg_replace('/\bNVMe SSD\b/i', 'NVMe', $label);
        $s = preg_replace('/\bNVME\b/i', 'NVMe', (string) $s);
        return trim((string) $s);
    }

    public static function parseCpuCount(string $s): ?int
    {
        if (preg_match('/^(\d+)\s+(?:vCPU\s+|Physical\s+)?Cores?/i', $s, $m) !== 1) {
            return null;
        }
        return (int) $m[1];
    }

    /** @return int|float|null */
    public static function parseRamGb(string $s)
    {
        if (preg_match('/^(\d+(?:\.\d+)?)\s*GB/i', $s, $m) !== 1) {
            return null;
        }
        return self::jsonNum((float) $m[1]);
    }

    public static function parsePortSpeedMbps(string $s): ?int
    {
        if (preg_match('/^(\d+(?:\.\d+)?)\s*Mbit\/s/i', $s, $m) === 1) {
            return (int) ((float) $m[1]);
        }
        if (preg_match('/^(\d+(?:\.\d+)?)\s*Gbit\/s/i', $s, $m) === 1) {
            return (int) round(((float) $m[1]) * 1000.0);
        }
        return null;
    }

    public static function parseStorageGb(string $s): ?int
    {
        if (preg_match('/^(\d+(?:\.\d+)?)\s*(GB|TB)/i', $s, $m) !== 1) {
            return null;
        }
        $n = (float) $m[1];
        return strtoupper($m[2]) === 'TB' ? (int) round($n * 1000.0) : (int) $n;
    }

    /** @return array{min_length:int,max_length:int,alphanumeric_only:bool,no_special_chars:bool}|null */
    public static function extractPasswordRules(?string $html): ?array
    {
        if ($html === null || $html === '') {
            return null;
        }
        if (preg_match(
            '/(\d+)-(\d+)\s+alphanumeric\s+characters\s+\(no\s+special\s+characters\)/',
            $html,
            $m
        ) !== 1) {
            return null;
        }
        return [
            'min_length' => (int) $m[1],
            'max_length' => (int) $m[2],
            'alphanumeric_only' => true,
            'no_special_chars' => true,
        ];
    }

    /**
     * @param array<string,mixed> $product one `products[*]` record
     * @param array{min_length:int,max_length:int,alphanumeric_only:bool,no_special_chars:bool}|null $passwordRules
     * @return array<string,mixed> base_plan
     * @throws \InvalidArgumentException when the record cannot yield a trustworthy plan
     */
    public function normalize(array $product, string $fetchedAt, ?array $passwordRules): array
    {
        $slug = isset($product['slug']) && is_string($product['slug']) ? $product['slug'] : '';
        $type = isset($product['type']) && is_string($product['type']) ? $product['type'] : '';
        if ($slug === '' || preg_match('/^[a-z0-9-]{1,60}$/', $slug) !== 1) {
            throw new \InvalidArgumentException('product has no usable slug');
        }
        $family = self::familyFromType($type);
        if ($family === 'Unknown') {
            throw new \InvalidArgumentException($slug . ': unknown product type');
        }
        $title = isset($product['title']) && is_string($product['title']) ? $product['title'] : $slug;

        $base = self::eur($product['price'] ?? null);
        if ($base === null || $base <= 0.0) {
            throw new \InvalidArgumentException($slug . ': missing or non-positive EUR price');
        }

        $specs = isset($product['specs']) && is_array($product['specs']) ? $product['specs'] : [];
        $find = static function (string $specType) use ($specs): ?array {
            foreach ($specs as $sp) {
                if (is_array($sp) && ($sp['type'] ?? null) === $specType) {
                    return $sp;
                }
            }
            return null;
        };
        $titleOf = static function (?array $sp): string {
            return $sp !== null && isset($sp['title']) && is_string($sp['title']) ? $sp['title'] : '';
        };
        $cpuStr = $titleOf($find('cpu'));
        $ramStr = $titleOf($find('ram'));
        $portStr = $titleOf($find('port'));
        $snapStr = $titleOf($find('snapshot'));
        $storageSpec = $find('storage');
        $storageTitle = $storageSpec !== null && isset($storageSpec['title']) && is_string($storageSpec['title'])
            ? $storageSpec['title'] : null;
        $storageSub = $storageSpec !== null && isset($storageSpec['subtitle']) && is_string($storageSpec['subtitle'])
            ? $storageSpec['subtitle'] : null;

        $parts = [];
        if ($storageTitle !== null) {
            $parts[] = self::normalizeStorageLabel($storageTitle);
        }
        if ($storageSub !== null && preg_match('/More storage available/i', $storageSub) !== 1) {
            $parts[] = self::normalizeStorageLabel((string) preg_replace('/^or\s+/i', '', $storageSub));
        }
        $baseStorage = implode(' or ', $parts);

        $productUrl = 'https://contabo.com/en/' . $type . '/' . $slug . '/';

        $periods = [];
        $rawPeriods = isset($product['periods']) && is_array($product['periods']) ? $product['periods'] : [];
        foreach ($rawPeriods as $p) {
            if (!is_array($p)) {
                continue;
            }
            $len = $p['length'] ?? null;
            if (is_float($len) && floor($len) === $len) {
                $len = (int) $len;
            }
            if (!is_int($len) || $len <= 0) {
                continue;
            }
            $disc = self::eur($p['discount'] ?? null) ?? 0.0;
            $setup = self::eur($p['setup'] ?? null) ?? 0.0;
            $gross = $base * $len;
            $total = $gross - $disc + $setup;
            $effective = ($gross - $disc) / $len;
            $periods[] = [
                'months' => $len,
                'is_hidden_from_ui' => $len === 3,
                'effective_monthly' => self::jsonNum(self::round2($effective)),
                'setup_fee' => self::jsonNum(self::round2($setup)),
                'total_period_cost' => self::jsonNum(self::round2($total)),
                'discount_total' => self::jsonNum(self::round2($disc)),
            ];
        }

        return [
            'family' => $family,
            'plan_rank' => $this->urls->rankOf($productUrl),
            'plan_family_rank' => $this->urls->familyRankOf($productUrl),
            'product_name' => $title,
            'product_slug' => $slug,
            'product_url' => $productUrl,
            'fetched_at' => $fetchedAt,
            'cpu' => $cpuStr,
            'ram' => $ramStr,
            'base_storage' => $baseStorage,
            'snapshots' => $snapStr,
            'port' => $portStr,
            'base_monthly_price' => self::jsonNum($base),
            'periods' => $periods,
            'specs_parsed' => [
                'cpu_count' => self::parseCpuCount($cpuStr),
                'ram_gb' => self::parseRamGb($ramStr),
                'port_speed_mbps' => self::parsePortSpeedMbps($portStr),
                'storage_primary_gb' => self::parseStorageGb($storageTitle ?? ''),
                'storage_primary_type' => ($storageTitle !== null && strpos(strtolower($storageTitle), 'nvme') !== false)
                    ? 'NVMe' : 'SSD',
            ],
            'password_rules' => $passwordRules,
            'source' => 'sapper',
        ];
    }

    /** `{EUR: n}` -> float; null/absent/non-numeric -> null (callers treat as 0 like Rust's unwrap_or). */
    private static function eur($v): ?float
    {
        if (!is_array($v) || !isset($v['EUR']) || !(is_int($v['EUR']) || is_float($v['EUR']))) {
            return null;
        }
        return (float) $v['EUR'];
    }
}
