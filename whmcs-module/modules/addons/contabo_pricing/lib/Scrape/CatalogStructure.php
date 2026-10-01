<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Turns the decoded `preloaded[0]` object of a Contabo page (categories,
 * products, navItems ...) into the round-2 scorecard structure:
 *
 *   {families[], legacy[], addon_groups[]}
 *
 * Stable mapping rule (see docs/round2-2026-10-01/catalog-structure.md):
 *  - a FAMILY is a category, keyed by its category SLUG (stable), displayed
 *    with the nav title (fallback: category title), ordered by nav position;
 *  - membership comes from the category's own `products` map, NOT from
 *    product.categoryId (the Performance plans carry categoryId=VPS);
 *  - only allow-listed categories become families; everything else (HP/MP
 *    price-tier mirrors, 2026 re-launch, SP/SC sales, outlet, object storage
 *    categories and the uncategorised legacy records) goes to `legacy`.
 *
 * PHP 7.4 compatible.
 */
final class CatalogStructure
{
    /**
     * slug => [nav link, label fallback, slug regex excluded from the family].
     * Order here is the fallback order when navItems is absent.
     */
    public const FAMILY_ALLOWLIST = [
        'vps' => ['nav' => '/vps/', 'label' => 'Core VPS', 'exclude' => '/-plus-\d+$/'],
        'performance-vps' => ['nav' => '/vps-performance/', 'label' => 'Performance VPS', 'exclude' => ''],
        'vds' => ['nav' => '/vps-dedicated/', 'label' => 'Max Performance VPS', 'exclude' => ''],
        'storage-vps' => ['nav' => '/storage-vps/', 'label' => 'Storage VPS', 'exclude' => ''],
        'dedicated-servers' => ['nav' => '/dedicated-servers/', 'label' => 'Dedicated Servers', 'exclude' => ''],
        'gpu-vps' => ['nav' => '/gpu-vps/', 'label' => 'GPU VPS', 'exclude' => ''],
    ];

    public const OBJECT_STORAGE_STEP_GB = 250;

    /** object-storage product slug => region label used in option names */
    private const OBJECT_REGIONS = [
        'european-union' => 'Europe',
        'singapore' => 'Asia',
        'united-states' => 'United States',
    ];

    public const CLASSES = [
        'region', 'storage_upgrade', 'backup', 'object_storage', 'panel', 'os_image', 'app',
        'monitoring', 'ftp_storage', 'other',
    ];

    /**
     * @param array<string,mixed> $pre decoded preloaded[0]
     * @return array{families:list<array<string,mixed>>,legacy:list<array<string,mixed>>,addon_groups:list<array<string,mixed>>}
     */
    public function build(array $pre): array
    {
        $products = isset($pre['products']) && is_array($pre['products']) ? $pre['products'] : [];
        $categories = isset($pre['categories']) && is_array($pre['categories']) ? $pre['categories'] : [];

        $objectUnits = $this->objectStorageUnits($products);

        // slug => category (id kept)
        $bySlug = [];
        foreach ($categories as $cid => $cat) {
            if (is_array($cat) && isset($cat['slug'])) {
                $cat['id'] = isset($cat['id']) ? $cat['id'] : $cid;
                $bySlug[(string) $cat['slug']] = $cat;
            }
        }

        $navTitles = $this->navTitlesByLink($pre);
        $order = $this->familyOrder($navTitles);

        $families = [];
        $claimed = [];
        foreach ($order as $slug) {
            if (!isset($bySlug[$slug])) {
                continue;
            }
            $cat = $bySlug[$slug];
            $rule = self::FAMILY_ALLOWLIST[$slug];
            $label = isset($navTitles[$rule['nav']]) ? $navTitles[$rule['nav']] : (isset($cat['title']) ? (string) $cat['title'] : $rule['label']);
            $plans = [];
            $members = isset($cat['products']) && is_array($cat['products']) ? array_keys($cat['products']) : [];
            foreach ($members as $pid) {
                if (!isset($products[$pid]) || !is_array($products[$pid])) {
                    continue;
                }
                $p = $products[$pid];
                $pslug = isset($p['slug']) ? (string) $p['slug'] : '';
                if ($rule['exclude'] !== '' && preg_match($rule['exclude'], $pslug) === 1) {
                    continue;
                }
                $claimed[$pid] = $slug;
                $plans[] = $this->plan($p, $objectUnits, $slug);
            }
            $families[] = [
                'key' => $slug,
                'label' => trim($label),
                'category_id' => (string) $cat['id'],
                'category_slug' => $slug,
                'nav_link' => $rule['nav'],
                'lowest_price' => isset($cat['lowestPrice']) && is_array($cat['lowestPrice']) ? $cat['lowestPrice'] : null,
                'plans' => $plans,
            ];
        }

        $categoryOf = [];
        foreach ($bySlug as $slug => $cat) {
            if (isset($cat['products']) && is_array($cat['products'])) {
                foreach (array_keys($cat['products']) as $pid) {
                    if (!isset($categoryOf[$pid])) {
                        $categoryOf[$pid] = $slug;
                    }
                }
            }
        }

        $legacy = [];
        foreach ($products as $pid => $p) {
            if (!is_array($p) || isset($claimed[$pid])) {
                continue;
            }
            $cat = isset($categoryOf[$pid]) ? $categoryOf[$pid] : null;
            $legacy[] = [
                'slug' => isset($p['slug']) ? (string) $p['slug'] : '',
                'title' => isset($p['title']) ? trim((string) $p['title']) : '',
                'category_slug' => $cat,
                'group' => $this->legacyGroup($p, $cat),
                'eur' => isset($p['price']['EUR']) ? $p['price']['EUR'] : null,
            ];
        }

        return [
            'families' => $families,
            'legacy' => $legacy,
            'addon_groups' => $this->addonGroups($products),
        ];
    }

    /** @param array<string,mixed> $product */
    public function plan(array $product, array $objectUnits, string $familySlug): array
    {
        $price = isset($product['price']) && is_array($product['price']) ? $product['price'] : [];
        $prices = [];
        foreach (['EUR', 'USD', 'GBP'] as $c) {
            if (isset($price[$c]) && is_numeric($price[$c])) {
                $prices[$c] = $price[$c] + 0;
            }
        }
        $plan = [
            'title' => trim((string) (isset($product['title']) ? $product['title'] : '')),
            'slug' => (string) (isset($product['slug']) ? $product['slug'] : ''),
            'availability' => $this->availability($product),
            'prices' => $prices,
            'periods' => $this->periods($product),
            'specs' => $this->specs(isset($product['specs']) && is_array($product['specs']) ? $product['specs'] : []),
            'options' => $this->options(isset($product['addons']) && is_array($product['addons']) ? $product['addons'] : [], $objectUnits),
        ];
        if (isset($product['previousPrice']['EUR']) && is_numeric($product['previousPrice']['EUR'])) {
            $plan['previous_price'] = $product['previousPrice']['EUR'] + 0;
        }
        return $plan;
    }

    /** Whole floats become ints so JSON output reads 1000, not 1000.0. */
    private static function num(float $v)
    {
        return floor($v) === $v && abs($v) < 1e15 ? (int) $v : $v;
    }

    /** @param array<string,mixed> $p */
    private function availability(array $p): string
    {
        if (isset($p['unavailable']) && $p['unavailable'] === true) {
            return 'unavailable';
        }
        $o = isset($p['outOfStock']) ? $p['outOfStock'] : null;
        if ($o === true) {
            return 'out_of_stock';
        }
        if (is_string($o) && $o !== '') {
            return $o;
        }
        return 'available';
    }

    /** @return list<array<string,mixed>> amounts in EUR */
    private function periods(array $p): array
    {
        $price = isset($p['price']['EUR']) && is_numeric($p['price']['EUR']) ? (float) $p['price']['EUR'] : null;
        $out = [];
        $periods = isset($p['periods']) && is_array($p['periods']) ? $p['periods'] : [];
        foreach ($periods as $per) {
            if (!is_array($per) || !isset($per['length'])) {
                continue;
            }
            $months = (int) $per['length'];
            $disc = isset($per['discount']['EUR']) && is_numeric($per['discount']['EUR']) ? (float) $per['discount']['EUR'] : 0.0;
            if ($price === null || $months < 1) {
                continue;
            }
            $total = $price * $months - $disc;
            $out[] = [
                'months' => $months,
                'discount' => round($disc, 2),
                'effective_monthly' => round($total / $months, 2),
                'total' => round($total, 2),
            ];
        }
        return $out;
    }

    /**
     * @param list<array<string,mixed>> $specs
     * @return array<string,mixed>
     */
    public function specs(array $specs): array
    {
        $o = [];
        foreach ($specs as $s) {
            if (!is_array($s) || !isset($s['title'])) {
                continue;
            }
            $t = trim((string) $s['title']);
            $type = isset($s['type']) ? (string) $s['type'] : '';
            if ($type === 'cpu' && !isset($o['cpu_cores'])) {
                if (preg_match('/^(\d+)\s*(?:vCPU|Virtual)\s*Cores?/i', $t, $m) === 1 || preg_match('/^(\d+)\s*x\s*[\d.]+\s*GHz/i', $t, $m) === 1) {
                    $o['cpu_cores'] = (int) $m[1];
                }
            } elseif ($type === 'ram' && !isset($o['ram_gb'])) {
                if (preg_match('/(\d+(?:\.\d+)?)\s*(GB|TB)/i', $t, $m) === 1) {
                    $o['ram_gb'] = self::num((strtoupper($m[2]) === 'TB' ? 1000 : 1) * (float) $m[1]);
                }
            } elseif ($type === 'storage' && !isset($o['storage_gb'])) {
                if (preg_match('/(?:(\d+)\s*x\s*)?(\d+(?:\.\d+)?)\s*(GB|TB)\s*(SSD|NVMe|HDD)/i', $t, $m) === 1) {
                    $n = $m[1] !== '' ? (int) $m[1] : 1;
                    $gb = (float) $m[2] * (strtoupper($m[3]) === 'TB' ? 1000 : 1) * $n;
                    $o['storage_gb'] = self::num($gb);
                    $o['storage_type'] = strtoupper($m[4]) === 'NVME' ? 'NVMe' : strtoupper($m[4]);
                }
            } elseif ($type === 'snapshot' && !isset($o['snapshots'])) {
                if (preg_match('/(\d+)\s*Snapshot/i', $t, $m) === 1) {
                    $o['snapshots'] = (int) $m[1];
                }
            } elseif ($type === 'port' && !isset($o['port_mbps'])) {
                if (preg_match('/(\d+(?:\.\d+)?)\s*(Gbit|Mbit)/i', $t, $m) === 1) {
                    $o['port_mbps'] = self::num((strtolower($m[2]) === 'gbit' ? 1000 : 1) * (float) $m[1]);
                }
            } elseif ($type === 'traffic' && !isset($o['traffic'])) {
                $o['traffic'] = stripos($t, 'unlimited') !== false ? 'unlimited' : $t;
            } elseif ($type === '' && !isset($o['gpu']) && preg_match('/^GPU\s*-\s*(.+)$/i', $t, $m) === 1) {
                $o['gpu'] = trim($m[1]);
            }
        }
        return $o;
    }

    /**
     * Object storage unit price per 250 GB step, by region label.
     * @param array<string,mixed> $products
     * @return array<string,float>
     */
    public function objectStorageUnits(array $products): array
    {
        $u = [];
        foreach ($products as $p) {
            if (is_array($p) && isset($p['slug'], $p['price']['EUR']) && isset(self::OBJECT_REGIONS[$p['slug']]) && is_numeric($p['price']['EUR'])) {
                $u[self::OBJECT_REGIONS[$p['slug']]] = (float) $p['price']['EUR'];
            }
        }
        return $u;
    }

    /**
     * @param array<string,mixed> $addons
     * @param array<string,float> $objectUnits
     * @return array<string,mixed>
     */
    public function options(array $addons, array $objectUnits): array
    {
        $o = [
            'regions' => [], 'storage_upgrades' => [], 'backup' => null, 'object_storage' => [],
            'os_images' => [], 'apps' => [], 'panels' => [],
        ];
        $seen = [];
        foreach ($addons as $a) {
            if (!is_array($a) || !isset($a['title']) || !is_string($a['title'])) {
                continue;
            }
            $title = trim($a['title']);
            $class = $this->classifyAddon($a);
            $eur = isset($a['price']['EUR']) && is_numeric($a['price']['EUR']) ? $a['price']['EUR'] + 0 : null;
            $key = $class . '|' . $title;
            if (isset($seen[$key])) {
                continue;
            }
            $seen[$key] = true;
            switch ($class) {
                case 'region':
                    $o['regions'][] = ['name' => $title, 'monthly_price' => $eur === null ? 0 : $eur];
                    break;
                case 'storage_upgrade':
                    if ($eur !== null) {
                        $o['storage_upgrades'][] = ['name' => $title, 'monthly_price' => $eur];
                    }
                    break;
                case 'backup':
                    $o['backup'] = ['name' => $title, 'monthly_price' => $eur === null ? 0 : $eur];
                    break;
                case 'os_image':
                    $o['os_images'][] = ['name' => $title, 'monthly_price' => $eur === null ? 0 : $eur];
                    break;
                case 'app':
                    $o['apps'][] = ['name' => $title, 'monthly_price' => $eur === null ? 0 : $eur];
                    break;
                case 'panel':
                    $o['panels'][] = ['name' => $title, 'monthly_price' => $eur === null ? 0 : $eur];
                    break;
                case 'object_storage':
                    if (preg_match('/^(\d+(?:\.\d+)?)\s*(GB|TB)\s+Object Storage/i', $title, $m) === 1) {
                        $gb = (float) $m[1] * (strtoupper($m[2]) === 'TB' ? 1000 : 1);
                        foreach ($objectUnits as $region => $unit) {
                            $o['object_storage'][] = [
                                'name' => $m[1] . ' ' . strtoupper($m[2]) . ' Object Storage in ' . $region,
                                'size_gb' => self::num($gb),
                                'monthly_price' => round($unit * $gb / self::OBJECT_STORAGE_STEP_GB, 2),
                            ];
                        }
                    }
                    break;
            }
        }
        return $o;
    }

    /** @param array<string,mixed> $a */
    public function classifyAddon(array $a): string
    {
        $t = isset($a['title']) && is_string($a['title']) ? trim($a['title']) : '';
        if ($t === '') {
            return 'other';
        }
        if (isset($a['osId'])) {
            return 'os_image';
        }
        if (preg_match('/Object Storage/i', $t) === 1) {
            return 'object_storage';
        }
        if (preg_match('/FTP Storage/i', $t) === 1) {
            return 'ftp_storage';
        }
        if (preg_match('/Backup/i', $t) === 1) {
            return 'backup';
        }
        if (preg_match('/Monitoring/i', $t) === 1) {
            return 'monitoring';
        }
        if (preg_match('/^(Asia|Australia|United States|United Kingdom|European Union|Canada|Japan|India|Singapore|Vereinigte Staaten)\b/iu', $t) === 1 || strpos($t, 'Location:') === 0) {
            return 'region';
        }
        if (preg_match('/cPanel|Plesk|^Webmin\b(?!\s*\+)/i', $t) === 1) {
            return 'panel';
        }
        if (preg_match('/^\d+(?:\.\d+)?\s*(GB|TB)\s*(SSD|NVMe|HDD)\b/i', $t) === 1) {
            return 'storage_upgrade';
        }
        if (preg_match('/(Server|Node)$|^(Docker|LAMP|Webmin \+ LAMP|DevOps Features)$/i', $t) === 1) {
            return 'app';
        }
        return 'other';
    }

    /**
     * @param array<string,mixed> $products
     * @return list<array<string,mixed>>
     */
    public function addonGroups(array $products): array
    {
        $g = [];
        foreach ($products as $pid => $p) {
            if (!is_array($p) || !isset($p['addons']) || !is_array($p['addons'])) {
                continue;
            }
            foreach ($p['addons'] as $a) {
                if (!is_array($a) || !isset($a['title']) || !is_string($a['title'])) {
                    continue;
                }
                $class = $this->classifyAddon($a);
                $gid = isset($a['groupId']) ? (string) $a['groupId'] : 'none:' . $class;
                if (!isset($g[$gid])) {
                    $g[$gid] = ['group_id' => $gid, 'class' => $class, 'classes' => [], 'titles' => [], 'products' => [], 'eur_min' => null, 'eur_max' => null, 'setup_max' => null];
                }
                $g[$gid]['classes'][$class] = true;
                $g[$gid]['titles'][trim($a['title'])] = true;
                $g[$gid]['products'][$pid] = true;
                if (isset($a['price']['EUR']) && is_numeric($a['price']['EUR'])) {
                    $v = $a['price']['EUR'] + 0;
                    $g[$gid]['eur_min'] = $g[$gid]['eur_min'] === null ? $v : min($g[$gid]['eur_min'], $v);
                    $g[$gid]['eur_max'] = $g[$gid]['eur_max'] === null ? $v : max($g[$gid]['eur_max'], $v);
                }
                if (isset($a['setupPrice']['EUR']) && is_numeric($a['setupPrice']['EUR'])) {
                    $v = $a['setupPrice']['EUR'] + 0;
                    $g[$gid]['setup_max'] = $g[$gid]['setup_max'] === null ? $v : max($g[$gid]['setup_max'], $v);
                }
            }
        }
        $out = [];
        foreach ($g as $row) {
            $row['classes'] = array_keys($row['classes']);
            $row['titles'] = array_keys($row['titles']);
            $row['product_count'] = count($row['products']);
            unset($row['products']);
            $out[] = $row;
        }
        usort($out, function ($a, $b) {
            return strnatcmp($a['group_id'], $b['group_id']);
        });
        return $out;
    }

    /** @param array<string,mixed> $p */
    private function legacyGroup(array $p, $cat): string
    {
        if ($cat !== null) {
            return 'category:' . $cat;
        }
        $slug = isset($p['slug']) ? (string) $p['slug'] : '';
        $title = isset($p['title']) ? (string) $p['title'] : '';
        if (stripos($slug, 'dummy') !== false) {
            return 'dummy placeholder (*-dummy)';
        }
        if (preg_match('/\b(SP|SC)\b/', $title) === 1 || preg_match('/-(sp|sc)\b/', $slug) === 1) {
            return 'sale-variant (SP/SC)';
        }
        if (preg_match('/^ds-\d+$|dummy$|^amd-|^intel-|ryzen|genoa|turin|epyc|Dedicated Server/i', $slug . ' ' . $title) === 1 && preg_match('/^cloud-vps/', $slug) !== 1) {
            return 'dedicated-legacy (ds-*, amd-*, intel-*)';
        }
        if (preg_match('/\(2026\)/', $title) === 1 || preg_match('/nvme-2026$/', $slug) === 1 || preg_match('/^cloud-vps-core-\d+-nvme$/', $slug) === 1) {
            return 'core-nvme (2026)';
        }
        if (preg_match('/^cloud-vps-\d+c(-nvme)?$|^VPS \d+ Cores/i', $slug . '|' . $title) === 1 || preg_match('/\d+C\b|\d+ Cores NVMe/', $title) === 1) {
            return 'C-nvme (core-count, coming-soon)';
        }
        if (preg_match('/\b(I|II|III|IV|V|VI)\b.*\b(SSD|NVMe)\b/', $title) === 1) {
            return 'roman-numeral generation (I-VI SSD/NVMe/AS)';
        }
        if (preg_match('/^storage-vps/', $slug) === 1) {
            return 'storage-vps legacy';
        }
        if (preg_match('/^vds-/', $slug) === 1) {
            return 'vds legacy';
        }
        if (preg_match('/nvme/i', $slug . ' ' . $title) === 1) {
            return 'nvme legacy (-nvme)';
        }
        if (preg_match('/^cloud-vps-\d+$|^cloud-vps-core-\d+$/', $slug) === 1) {
            return 'legacy cloud-vps-N';
        }
        return 'other';
    }

    /** @return array<string,string> nav link => title */
    private function navTitlesByLink(array $pre): array
    {
        $out = [];
        $nav = isset($pre['navItems']) && is_array($pre['navItems']) ? $pre['navItems'] : [];
        foreach ($nav as $top) {
            if (!is_array($top) || !isset($top['children']) || !is_array($top['children'])) {
                continue;
            }
            foreach ($top['children'] as $child) {
                if (is_array($child) && isset($child['fields']['link'], $child['fields']['title']) && is_string($child['fields']['link'])) {
                    if (!isset($out[$child['fields']['link']])) {
                        $out[$child['fields']['link']] = (string) $child['fields']['title'];
                    }
                }
            }
        }
        return $out;
    }

    /**
     * Allow-listed slugs ordered by nav position (nav links not present sort
     * after, in allow-list order).
     * @param array<string,string> $navTitles
     * @return list<string>
     */
    private function familyOrder(array $navTitles): array
    {
        $links = array_keys($navTitles);
        $rank = [];
        $i = 0;
        foreach (self::FAMILY_ALLOWLIST as $slug => $rule) {
            $pos = array_search($rule['nav'], $links, true);
            $rank[$slug] = [$pos === false ? 1000 : $pos, $i++];
        }
        $slugs = array_keys(self::FAMILY_ALLOWLIST);
        usort($slugs, function ($a, $b) use ($rank) {
            return $rank[$a] <=> $rank[$b];
        });
        return $slugs;
    }
}
