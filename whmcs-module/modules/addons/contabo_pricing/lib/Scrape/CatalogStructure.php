<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Turns the decoded `preloaded[0]` object of a Contabo page (categories,
 * products, navItems ...) into the round-2 scorecard structure:
 *
 *   {families[], legacy[], addon_groups[]}
 *
 * NOTHING about the family lineup is hard-coded. Each run DISCOVERS it:
 *  - a FAMILY is a category the site's own navigation links to. A nav entry is
 *    matched to a category by (a) the link's last path segment equalling the
 *    category slug, (b) the normalised nav title equalling the category title,
 *    or (c) the same set of dash-separated tokens (`/vps-performance/` =
 *    `performance-vps`). A tie or no match leaves the nav entry unlinked;
 *  - the family label is the nav title (fallback: category title);
 *  - membership comes from the category's own `products` map, NOT from
 *    product.categoryId (the Performance plans carry categoryId=VPS). A
 *    product that is a member of several linked families is owned by the
 *    most specific one (fewest members, then nav order);
 *  - only plan-shaped products count (EUR price > 0 and a cpu or ram spec), so
 *    object-storage unit-price products never become plans;
 *  - categories the nav does not link to (HP/MP price-tier mirrors, 2026
 *    re-launch, SP/SC sales, outlet ...) and uncategorised records go to
 *    `legacy`.
 * FamilyRegistry persists what discover() reports and tracks its history.
 *
 * PHP 7.4 compatible.
 */
final class CatalogStructure
{
    /** @var array<string,mixed>|null decoded preloaded[0] this instance describes */
    private $pre;

    /** @param array<string,mixed>|null $pre */
    public function __construct(?array $pre = null)
    {
        $this->pre = $pre;
    }

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
     * Discovered categories and nav, keyed by category id.
     *
     * Category keys: category_id, slug, title, nav_title, nav_href (path),
     * nav_position, in_nav, plan_family (linked AND has plan products),
     * members (raw product slugs), plans (owned plan slugs, plan_family only),
     * plan_ids, sample_product_url, lowest_price.
     * Nav keys: title, link, position, category_id (null = unlinked).
     *
     * @param array<string,mixed>|null $pre
     * @return array{categories:array<string,array<string,mixed>>,nav:list<array<string,mixed>>}
     */
    public function discover(?array $pre = null): array
    {
        $pre = $pre ?? $this->pre ?? [];
        $products = isset($pre['products']) && is_array($pre['products']) ? $pre['products'] : [];
        $rawCats = isset($pre['categories']) && is_array($pre['categories']) ? $pre['categories'] : [];

        $cats = [];
        foreach ($rawCats as $key => $c) {
            if (!is_array($c) || !isset($c['slug']) || !is_string($c['slug']) || $c['slug'] === '') {
                continue;
            }
            $id = (string) (isset($c['id']) && is_scalar($c['id']) && (string) $c['id'] !== '' ? $c['id'] : $key);
            $memberIds = isset($c['products']) && is_array($c['products']) ? array_keys($c['products']) : [];
            $members = [];
            foreach ($memberIds as $pid) {
                if (isset($products[$pid]) && is_array($products[$pid]) && isset($products[$pid]['slug']) && is_string($products[$pid]['slug'])) {
                    $members[] = $products[$pid]['slug'];
                }
            }
            $cats[$id] = [
                'category_id' => $id,
                'slug' => (string) $c['slug'],
                'title' => self::clean(isset($c['title']) ? (string) $c['title'] : (string) $c['slug']),
                'nav_title' => null,
                'nav_href' => null,
                'nav_position' => null,
                'in_nav' => false,
                'plan_family' => false,
                'members' => $members,
                'member_ids' => array_map('strval', $memberIds),
                'plans' => [],
                'plan_ids' => [],
                'sample_product_url' => null,
                'lowest_price' => isset($c['lowestPrice']) && is_array($c['lowestPrice']) ? $c['lowestPrice'] : null,
            ];
        }

        // ── nav -> category linking ─────────────────────────────────────────
        $nav = $this->navEntries($pre);
        foreach ($nav as $i => $entry) {
            $best = null;
            $bestScore = 0;
            $tie = false;
            foreach ($cats as $id => $cat) {
                $score = self::matchScore($entry, $cat);
                if ($score > $bestScore) {
                    $best = $id;
                    $bestScore = $score;
                    $tie = false;
                } elseif ($score === $bestScore && $score > 0) {
                    $tie = true;
                }
            }
            $nav[$i]['category_id'] = ($best !== null && !$tie && $bestScore >= 2) ? $best : null;
        }
        foreach ($nav as $entry) {
            $id = $entry['category_id'];
            if ($id === null || $cats[$id]['in_nav']) {
                continue; // first (earliest) nav entry wins
            }
            $cats[$id]['in_nav'] = true;
            $cats[$id]['nav_title'] = $entry['title'];
            $cats[$id]['nav_href'] = $entry['link'];
            $cats[$id]['nav_position'] = $entry['position'];
        }

        // ── plan-shaped members of nav-linked categories; most specific owner ──
        $linked = [];
        foreach ($cats as $id => $cat) {
            if ($cat['in_nav']) {
                $linked[] = $id;
            }
        }
        usort($linked, static function ($a, $b) use ($cats): int {
            return $cats[$a]['nav_position'] <=> $cats[$b]['nav_position'];
        });
        $owner = [];
        foreach ($linked as $id) {
            foreach ($cats[$id]['member_ids'] as $pid) {
                if (!isset($products[$pid]) || !is_array($products[$pid]) || !self::isPlanShaped($products[$pid])) {
                    continue;
                }
                if (!isset($owner[$pid]) || count($cats[$id]['member_ids']) < count($cats[$owner[$pid]]['member_ids'])) {
                    $owner[$pid] = $id;
                }
            }
        }
        foreach ($linked as $id) {
            foreach ($cats[$id]['member_ids'] as $pid) {
                if (($owner[$pid] ?? null) !== $id) {
                    continue;
                }
                $cats[$id]['plans'][] = (string) $products[$pid]['slug'];
                $cats[$id]['plan_ids'][] = $pid;
                if ($cats[$id]['sample_product_url'] === null) {
                    $cats[$id]['sample_product_url'] = PlanNormalizer::productUrl(
                        isset($products[$pid]['type']) && is_string($products[$pid]['type']) ? $products[$pid]['type'] : '',
                        (string) $products[$pid]['slug']
                    );
                }
            }
            $cats[$id]['plan_family'] = $cats[$id]['plans'] !== [];
        }

        return ['categories' => $cats, 'nav' => $nav];
    }

    /**
     * @param array<string,mixed> $pre decoded preloaded[0]
     * @return array{families:list<array<string,mixed>>,legacy:list<array<string,mixed>>,addon_groups:list<array<string,mixed>>}
     */
    public function build(array $pre): array
    {
        $products = isset($pre['products']) && is_array($pre['products']) ? $pre['products'] : [];
        $objectUnits = $this->objectStorageUnits($products);
        $d = $this->discover($pre);

        $families = [];
        $claimed = [];
        foreach (self::familyCategories($d) as $cat) {
            $plans = [];
            foreach ($cat['plan_ids'] as $pid) {
                $claimed[$pid] = $cat['slug'];
                $plans[] = $this->plan($products[$pid], $objectUnits, $cat['slug']);
            }
            $families[] = [
                'key' => $cat['slug'],
                'label' => $cat['nav_title'] !== null && $cat['nav_title'] !== '' ? $cat['nav_title'] : $cat['title'],
                'category_id' => $cat['category_id'],
                'category_slug' => $cat['slug'],
                'nav_link' => $cat['nav_href'],
                'lowest_price' => $cat['lowest_price'],
                'plans' => $plans,
            ];
        }

        $categoryOf = [];
        foreach ($d['categories'] as $cat) {
            foreach ($cat['member_ids'] as $pid) {
                if (!isset($categoryOf[$pid])) {
                    $categoryOf[$pid] = $cat['slug'];
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

    /**
     * Plan-family categories of a discover() result, in nav order.
     * @param array{categories:array<string,array<string,mixed>>,nav:list<array<string,mixed>>} $d
     * @return list<array<string,mixed>>
     */
    public static function familyCategories(array $d): array
    {
        $out = [];
        foreach ($d['categories'] as $cat) {
            if (!empty($cat['plan_family'])) {
                $out[] = $cat;
            }
        }
        usort($out, static function (array $a, array $b): int {
            return $a['nav_position'] <=> $b['nav_position'];
        });
        return $out;
    }

    /** A product that can be sold as a plan: EUR price > 0 and a cpu or ram spec. @param array<string,mixed> $p */
    public static function isPlanShaped(array $p): bool
    {
        if (!isset($p['slug']) || !is_string($p['slug']) || $p['slug'] === '') {
            return false;
        }
        if (!isset($p['price']['EUR']) || !is_numeric($p['price']['EUR']) || $p['price']['EUR'] <= 0) {
            return false;
        }
        foreach ((isset($p['specs']) && is_array($p['specs']) ? $p['specs'] : []) as $s) {
            if (is_array($s) && isset($s['type']) && ($s['type'] === 'cpu' || $s['type'] === 'ram')) {
                return true;
            }
        }
        return false;
    }

    private static function clean(string $s): string
    {
        return trim((string) preg_replace('/\s+/u', ' ', $s));
    }

    private static function norm(string $s): string
    {
        return strtolower((string) preg_replace('/[^a-z0-9]+/i', '', $s));
    }

    /** @return list<string> sorted dash tokens */
    private static function tokens(string $slug): array
    {
        $t = array_values(array_filter(explode('-', strtolower($slug)), static function ($x): bool {
            return $x !== '';
        }));
        sort($t);
        return $t;
    }

    /**
     * @param array<string,mixed> $entry nav entry
     * @param array<string,mixed> $cat category
     */
    private static function matchScore(array $entry, array $cat): int
    {
        $score = 0;
        $seg = trim((string) $entry['link'], '/');
        $seg = strpos($seg, '/') !== false ? substr($seg, (int) strrpos($seg, '/') + 1) : $seg;
        if ($seg !== '' && strtolower($seg) === strtolower((string) $cat['slug'])) {
            $score += 3;
        } elseif ($seg !== '' && self::tokens($seg) === self::tokens((string) $cat['slug'])) {
            $score += 2;
        }
        if (self::norm((string) $entry['title']) !== '' && self::norm((string) $entry['title']) === self::norm((string) $cat['title'])) {
            $score += 3;
        }
        return $score;
    }

    /**
     * Every nav entry that links to a path, in document order (children of
     * groups included), one entry per distinct link.
     *
     * @param array<string,mixed> $pre
     * @return list<array{title:string,link:string,position:int,category_id:?string}>
     */
    private function navEntries(array $pre): array
    {
        $out = [];
        $seen = [];
        $walk = function ($nodes, int $depth) use (&$walk, &$out, &$seen): void {
            if (!is_array($nodes) || $depth > 4) {
                return;
            }
            foreach ($nodes as $n) {
                if (!is_array($n)) {
                    continue;
                }
                $f = isset($n['fields']) && is_array($n['fields']) ? $n['fields'] : [];
                if ($depth > 0 && isset($f['link'], $f['title']) && is_string($f['link']) && is_string($f['title'])) {
                    $path = (string) preg_replace('#^https?://[^/]+#i', '', $f['link']);
                    $path = (string) preg_replace('#^/en(?=/)#', '', $path);
                    $path = '/' . trim($path, '/') . '/';
                    if ($path !== '//' && !isset($seen[$path])) {
                        $seen[$path] = true;
                        $out[] = ['title' => self::clean($f['title']), 'link' => $path, 'position' => count($out), 'category_id' => null];
                    }
                }
                if (isset($n['children'])) {
                    $walk($n['children'], $depth + 1);
                }
            }
        };
        $walk(isset($pre['navItems']) && is_array($pre['navItems']) ? $pre['navItems'] : [], 0);
        return $out;
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
            if (!is_array($p) || !isset($p['slug'], $p['price']['EUR']) || !is_string($p['slug']) || !is_numeric($p['price']['EUR'])) {
                continue;
            }
            $isObject = (isset($p['type']) && $p['type'] === 'object-storage') || isset(self::OBJECT_REGIONS[$p['slug']]);
            if (!$isObject) {
                continue;
            }
            // known slugs keep the customer-facing region name; an unknown region slug
            // (a new object-storage location) is title-cased instead of being dropped
            $label = self::OBJECT_REGIONS[$p['slug']] ?? ucwords(str_replace('-', ' ', $p['slug']));
            $u[$label] = (float) $p['price']['EUR'];
        }
        return $u;
    }

    /**
     * Per-plan add-ons by class, each with monthly and setup price (EUR).
     * Object storage has no price on the add-on: it is derived as
     * (region object-storage product price) x size / 250 GB.
     *
     * @param array<string,mixed> $addons
     * @param array<string,float> $objectUnits
     * @return array<string,mixed>
     */
    public function options(array $addons, array $objectUnits): array
    {
        $o = [
            'regions' => [], 'storage_upgrades' => [], 'backup' => null, 'object_storage' => [],
            'os_images' => [], 'apps' => [], 'panels' => [], 'monitoring' => [], 'other' => [],
        ];
        $seen = [];
        foreach ($addons as $a) {
            if (!is_array($a) || !isset($a['title']) || !is_string($a['title'])) {
                continue;
            }
            $title = trim($a['title']);
            $class = $this->classifyAddon($a);
            $eur = isset($a['price']['EUR']) && is_numeric($a['price']['EUR']) ? $a['price']['EUR'] + 0 : null;
            $setup = isset($a['setupPrice']['EUR']) && is_numeric($a['setupPrice']['EUR']) ? $a['setupPrice']['EUR'] + 0 : 0;
            $key = $class . '|' . $title;
            if (isset($seen[$key])) {
                continue;
            }
            $seen[$key] = true;
            $choice = ['name' => $title, 'monthly_price' => $eur === null ? 0 : $eur, 'setup_price' => $setup];
            switch ($class) {
                case 'region':
                    $o['regions'][] = $choice;
                    break;
                case 'storage_upgrade':
                    if ($eur !== null) {
                        $o['storage_upgrades'][] = $choice;
                    }
                    break;
                case 'backup':
                    $o['backup'] = $choice;
                    break;
                case 'os_image':
                    $o['os_images'][] = $choice;
                    break;
                case 'app':
                    $o['apps'][] = $choice;
                    break;
                case 'panel':
                    $o['panels'][] = $choice;
                    break;
                case 'monitoring':
                    $o['monitoring'][] = $choice;
                    break;
                case 'ftp_storage':
                case 'other':
                    // UI marker add-ons ("None", included-size labels) carry no price: noise
                    if ($eur !== null) {
                        $o['other'][] = $choice;
                    }
                    break;
                case 'object_storage':
                    if (preg_match('/^(\d+(?:\.\d+)?)\s*(GB|TB)\s+Object Storage/i', $title, $m) === 1) {
                        $gb = (float) $m[1] * (strtoupper($m[2]) === 'TB' ? 1000 : 1);
                        foreach ($objectUnits as $region => $unit) {
                            $o['object_storage'][] = [
                                'name' => $m[1] . ' ' . strtoupper($m[2]) . ' Object Storage in ' . $region,
                                'size_gb' => self::num($gb),
                                'monthly_price' => round($unit * $gb / self::OBJECT_STORAGE_STEP_GB, 2),
                                'setup_price' => 0,
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
}
