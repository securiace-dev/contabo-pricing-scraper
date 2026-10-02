<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Port of process_plan() in src/main.rs: turns one Contabo `products[*]`
 * record into the base_plan shape the Rust service emitted (family, ranks,
 * specs, base_storage, periods, specs_parsed, password_rules, source) plus the
 * round-2 additions: per-currency prices, previous price, GPU/snapshot/traffic
 * specs and per-plan options. All derived numbers are recomputed here from the
 * raw price / discount / setup values — nothing a provider computed is trusted.
 *
 * The family is NOT derived from the product: the caller (PlanExtractor, from
 * the discovered CatalogStructure) passes it in $ctx. Without it (provider-JSON
 * fallback) the product type is humanised, never mapped through a fixed table.
 *
 * PHP 7.4 compatible.
 */
final class PlanNormalizer
{
    /** Product page URL exactly as the site builds it from the product type. */
    public static function productUrl(string $type, string $slug): string
    {
        return 'https://contabo.com/en/' . ($type !== '' ? $type : 'vps') . '/' . $slug . '/';
    }

    /** `storage-vps` -> `Storage Vps`: only used when no discovered family label exists. */
    public static function humanizeType(string $type): string
    {
        $s = trim(ucwords(str_replace(['-', '_'], ' ', $type)));
        return $s === '' ? 'Unclassified' : $s;
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
        if (preg_match('/^(\d+)\s+(?:(?:vCPU|Physical|Virtual)\s+)?Cores?/i', $s, $m) === 1) {
            return (int) $m[1];
        }
        // dedicated servers: "32 x 3.55 GHz (4.20 max)"
        if (preg_match('/^(\d+)\s*x\s*[\d.]+\s*GHz/i', $s, $m) === 1) {
            return (int) $m[1];
        }
        return null;
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

    /**
     * Primary storage in GB. "2 x 1 TB NVMe" sums the drives (2000); a dual
     * label such as "300GB SSD / 150GB NVMe" yields its FIRST (primary) half.
     */
    public static function parseStorageGb(string $s): ?int
    {
        if (preg_match('/^(?:(\d+)\s*x\s*)?(\d+(?:\.\d+)?)\s*(GB|TB)/i', trim($s), $m) !== 1) {
            return null;
        }
        $drives = $m[1] !== '' ? (int) $m[1] : 1;
        $n = (float) $m[2];
        $gb = strtoupper($m[3]) === 'TB' ? (int) round($n * 1000.0) : (int) $n;
        return $gb * max(1, $drives);
    }

    /** NVMe|SSD|HDD of the primary (first) segment of a storage label; SSD when unstated. */
    public static function parseStorageType(string $s): string
    {
        $primary = (string) preg_split('#\s+/\s+|\s+or\s+#i', trim($s), 2)[0];
        $l = strtolower($primary);
        if (strpos($l, 'nvme') !== false) {
            return 'NVMe';
        }
        if (strpos($l, 'hdd') !== false) {
            return 'HDD';
        }
        return 'SSD';
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
     * @param array<string,mixed> $ctx family (label), family_key (category slug), category_id,
     *        plan_rank, plan_family_rank, object_units (region => EUR per 250 GB)
     * @return array<string,mixed> base_plan
     * @throws \InvalidArgumentException when the record cannot yield a trustworthy plan
     */
    public function normalize(array $product, string $fetchedAt, ?array $passwordRules, array $ctx = []): array
    {
        $slug = isset($product['slug']) && is_string($product['slug']) ? $product['slug'] : '';
        $type = isset($product['type']) && is_string($product['type']) ? $product['type'] : '';
        if ($slug === '' || preg_match('/^[a-z0-9-]{1,60}$/', $slug) !== 1) {
            throw new \InvalidArgumentException('product has no usable slug');
        }
        $family = isset($ctx['family']) && is_string($ctx['family']) && $ctx['family'] !== ''
            ? $ctx['family'] : self::humanizeType($type);
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

        $productUrl = self::productUrl($type, $slug);

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

        $gpu = null;
        $traffic = null;
        foreach ($specs as $sp) {
            if (!is_array($sp) || !isset($sp['title']) || !is_string($sp['title'])) {
                continue;
            }
            $st = $sp['type'] ?? null;
            if ($st === null && $gpu === null && preg_match('/^GPU\s*[-:]\s*(.+)$/i', trim($sp['title']), $gm) === 1) {
                $gpu = trim($gm[1]);
            } elseif ($st === 'traffic' && $traffic === null) {
                $traffic = stripos($sp['title'], 'unlimited') !== false ? 'unlimited' : trim($sp['title']);
            }
        }
        $snapCount = preg_match('/(\d+)\s*Snapshot/i', $snapStr, $sm) === 1 ? (int) $sm[1] : null;

        $parsed = [
            'cpu_count' => self::parseCpuCount($cpuStr),
            'ram_gb' => self::parseRamGb($ramStr),
            'port_speed_mbps' => self::parsePortSpeedMbps($portStr),
            'storage_primary_gb' => self::parseStorageGb($storageTitle ?? ''),
            'storage_primary_type' => self::parseStorageType($storageTitle ?? ''),
            'snapshot_count' => $snapCount,
            'traffic' => $traffic,
        ];
        if ($gpu !== null) {
            $parsed['gpu'] = $gpu;
        }

        $plan = [
            'family' => $family,
            'family_key' => isset($ctx['family_key']) ? (string) $ctx['family_key'] : '',
            'category_id' => isset($ctx['category_id']) ? (string) $ctx['category_id'] : '',
            'plan_rank' => isset($ctx['plan_rank']) ? (int) $ctx['plan_rank'] : 0,
            'plan_family_rank' => isset($ctx['plan_family_rank']) ? (int) $ctx['plan_family_rank'] : 0,
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
            'prices' => self::currencies($product['price'] ?? null),
            'previous_price' => self::currencies($product['previousPrice'] ?? null) ?: null,
            'periods' => $periods,
            'specs_parsed' => $parsed,
            'password_rules' => $passwordRules,
            'source' => 'sapper',
        ];
        if (isset($product['addons']) && is_array($product['addons'])) {
            $units = isset($ctx['object_units']) && is_array($ctx['object_units']) ? $ctx['object_units'] : [];
            $plan['options'] = (new CatalogStructure())->options($product['addons'], $units);
        }
        return $plan;
    }

    /** `{EUR,USD,GBP,...}` -> map of the numeric currencies; anything else -> []. @return array<string,int|float> */
    private static function currencies($v): array
    {
        $out = [];
        if (is_array($v)) {
            foreach ($v as $cur => $amount) {
                if (is_string($cur) && preg_match('/^[A-Z]{3}$/', $cur) === 1 && (is_int($amount) || is_float($amount))) {
                    $out[$cur] = self::jsonNum((float) $amount);
                }
            }
        }
        return $out;
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
