<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\CatalogStructure;
use ContaboPricing\Scrape\SapperLiteralDecoder;
use PHPUnit\Framework\TestCase;

final class CatalogStructureTest extends TestCase
{
    private const FAMILY_KEYS = ['vps', 'performance-vps', 'vds', 'storage-vps', 'dedicated-servers', 'gpu-vps'];

    /** @return array<string,mixed> */
    private function pre(string $fixture): array
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/' . $fixture);
        $d = (new SapperLiteralDecoder())->decodeFromHtml($html);
        $this->assertArrayHasKey(0, $d['preloaded']);
        return $d['preloaded'][0];
    }

    /** @return array<string,mixed> */
    private function synthetic(): array
    {
        $core = ['id' => 'c4', 'title' => 'Cloud VPS 4', 'slug' => 'cloud-vps-core-4', 'categoryId' => 'CAT_VPS',
            'price' => ['EUR' => 5.5, 'USD' => 6.6, 'GBP' => 5.4], 'previousPrice' => null, 'outOfStock' => null, 'unavailable' => null,
            'periods' => [
                ['length' => 1, 'discount' => ['EUR' => 0]],
                ['length' => 12, 'discount' => ['EUR' => 9.9]],
                ['length' => 24, 'discount' => ['EUR' => 26.4]],
            ],
            'specs' => [
                ['title' => '4 vCPU Cores', 'type' => 'cpu'], ['title' => '8 GB RAM', 'type' => 'ram'],
                ['title' => '400GB SSD / 200GB NVMe', 'type' => 'storage'], ['title' => '1 Snapshot', 'type' => 'snapshot', 'value' => 1],
                ['title' => '1 Gbit/s Port', 'type' => 'port'], ['title' => 'Unlimited Traffic*', 'type' => 'traffic'],
            ],
            'addons' => [
                'a1' => ['id' => 'a1', 'title' => 'Asia (India)', 'groupId' => '2219', 'price' => ['EUR' => 2.4], 'setupPrice' => ['EUR' => 0]],
                'a2' => ['id' => 'a2', 'title' => '200 GB SSD', 'groupId' => '2215', 'price' => ['EUR' => 1.5], 'setupPrice' => ['EUR' => 0]],
                'a3' => ['id' => 'a3', 'title' => 'Auto Backup', 'groupId' => '2214', 'price' => ['EUR' => 1.65], 'setupPrice' => ['EUR' => 0]],
                'a4' => ['id' => 'a4', 'title' => '1 TB Object Storage in {product_location}'],
                'a5' => ['id' => 'a5', 'title' => 'Ubuntu 24.04', 'osId' => 332],
                'a6' => ['id' => 'a6', 'title' => 'cPanel/WHM (5 accounts)', 'groupId' => '1273', 'price' => ['EUR' => 21.75], 'setupPrice' => ['EUR' => 0]],
                'a7' => ['id' => 'a7', 'title' => 'Dokploy Server', 'groupId' => '2359', 'price' => ['EUR' => 0], 'setupPrice' => ['EUR' => 0]],
                'a8' => ['id' => 'a8', 'title' => 'None'],
            ],
        ];
        // Performance plan: member of performance-vps via category.products but categoryId points at VPS.
        $plus = ['id' => 'p4', 'title' => 'Cloud VPS Plus 4', 'slug' => 'cloud-vps-plus-4', 'categoryId' => 'CAT_VPS',
            'price' => ['EUR' => 13.5], 'periods' => [], 'specs' => [['title' => '4 vCPU Cores', 'type' => 'cpu']], 'addons' => []];
        $legacy = ['id' => 'l1', 'title' => 'Cloud VPS 10', 'slug' => 'cloud-vps-10', 'categoryId' => null, 'price' => ['EUR' => 5.5], 'periods' => [], 'specs' => []];
        $objAsia = ['id' => 'o1', 'title' => 'Singapore', 'slug' => 'singapore', 'type' => 'object-storage', 'price' => ['EUR' => 2.99]];
        return [
            'products' => ['c4' => $core, 'p4' => $plus, 'l1' => $legacy, 'o1' => $objAsia],
            'categories' => [
                'CAT_VPS' => ['id' => 'CAT_VPS', 'slug' => 'vps', 'title' => 'VPS', 'products' => ['c4' => ['slug' => 'cloud-vps-core-4'], 'p4' => ['slug' => 'cloud-vps-plus-4']]],
                'CAT_PERF' => ['id' => 'CAT_PERF', 'slug' => 'performance-vps', 'title' => 'Performance VPS', 'products' => ['p4' => ['slug' => 'cloud-vps-plus-4']]],
                'CAT_OBJ' => ['id' => 'CAT_OBJ', 'slug' => 'object-storage', 'title' => 'Object Storage', 'products' => ['o1' => ['slug' => 'singapore']]],
            ],
            'navItems' => [
                ['title' => 'VPS', 'children' => [
                    ['fields' => ['title' => 'Core VPS', 'link' => '/vps/']],
                    ['fields' => ['title' => 'Performance VPS', 'link' => '/vps-performance/']],
                ]],
            ],
        ];
    }

    public function testSyntheticMappingMembershipAndOptions(): void
    {
        $r = (new CatalogStructure())->build($this->synthetic());

        $this->assertSame(['vps', 'performance-vps'], array_column($r['families'], 'key'));
        $this->assertSame('Core VPS', $r['families'][0]['label']);

        // Core VPS excludes the -plus- slug even though category.products lists it; Performance claims it.
        $this->assertSame(['cloud-vps-core-4'], array_column($r['families'][0]['plans'], 'slug'));
        $this->assertSame(['cloud-vps-plus-4'], array_column($r['families'][1]['plans'], 'slug'));

        // Everything else is legacy.
        $slugs = array_column($r['legacy'], 'slug');
        sort($slugs);
        $this->assertSame(['cloud-vps-10', 'singapore'], $slugs);

        $plan = $r['families'][0]['plans'][0];
        $this->assertSame('available', $plan['availability']);
        $this->assertSame(['EUR' => 5.5, 'USD' => 6.6, 'GBP' => 5.4], $plan['prices']);
        $this->assertSame(4, $plan['specs']['cpu_cores']);
        $this->assertSame(400.0, (float) $plan['specs']['storage_gb']);
        $this->assertSame('SSD', $plan['specs']['storage_type']);
        $this->assertSame(1000.0, (float) $plan['specs']['port_mbps']);
        $this->assertSame('unlimited', $plan['specs']['traffic']);

        $p12 = $plan['periods'][1];
        $this->assertSame(12, $p12['months']);
        $this->assertSame(9.9, $p12['discount']);
        $this->assertSame(56.1, $p12['total']);
        $this->assertSame(4.68, $p12['effective_monthly']);

        $o = $plan['options'];
        $this->assertSame([['name' => 'Asia (India)', 'monthly_price' => 2.4, 'setup_price' => 0]], $o['regions']);
        $this->assertSame([['name' => '200 GB SSD', 'monthly_price' => 1.5, 'setup_price' => 0]], $o['storage_upgrades']);
        $this->assertSame(['name' => 'Auto Backup', 'monthly_price' => 1.65, 'setup_price' => 0], $o['backup']);
        $this->assertSame([], $o['monitoring']);
        $this->assertSame([], $o['other'], 'price-less UI markers ("None") are noise');
        $this->assertSame('Ubuntu 24.04', $o['os_images'][0]['name']);
        $this->assertSame('cPanel/WHM (5 accounts)', $o['panels'][0]['name']);
        $this->assertSame('Dokploy Server', $o['apps'][0]['name']);
        // 1 TB in the Asia region = 4 x the 250 GB Singapore unit price, matching the checkout screenshot (14.11 gross).
        $this->assertSame([['name' => '1 TB Object Storage in Asia', 'size_gb' => 1000, 'monthly_price' => 11.96, 'setup_price' => 0]], $o['object_storage']);
    }

    public function testNavLinkMatchingRulesAndOwnership(): void
    {
        $mk = static function (string $id, string $slug, string $title, array $members): array {
            $c = ['id' => $id, 'slug' => $slug, 'title' => $title, 'products' => []];
            foreach ($members as $m) {
                $c['products'][$m] = ['slug' => $m];
            }
            return $c;
        };
        $prod = static function (string $slug, array $over = []): array {
            return array_replace([
                'slug' => $slug, 'title' => $slug, 'type' => 'vps', 'price' => ['EUR' => 5],
                'specs' => [['title' => '2 vCPU Cores', 'type' => 'cpu']],
            ], $over);
        };
        $pre = [
            'products' => ['a' => $prod('a'), 'b' => $prod('b'), 'c' => $prod('c'), 'o' => $prod('o', ['specs' => []])],
            'categories' => [
                'K1' => $mk('K1', 'big-vps', 'Big', ['a', 'b', 'c']),
                'K2' => $mk('K2', 'speedy-vps', 'Fast Lane', ['c']),
                'K3' => $mk('K3', 'misc', 'Misc', ['o']),
                'K4' => $mk('K4', 'tie-one', 'Tie', ['a']),
                'K5' => $mk('K5', 'tie-two', 'Tie', ['b']),
            ],
            'navItems' => [['title' => 'Menu', 'children' => [
                ['fields' => ['title' => 'Everything', 'link' => '/big-vps/']],        // slug equals the last path segment
                ['fields' => ['title' => 'Fast Lane', 'link' => '/lane/']],            // normalised title equals the category title
                ['fields' => ['title' => 'Misc', 'link' => 'https://contabo.com/en/misc/']], // absolute link, /en prefix stripped
                ['fields' => ['title' => 'Tie', 'link' => '/tie/']],                    // ambiguous: two categories share the title
                ['fields' => ['title' => 'Blog', 'link' => '/blog/']],                  // no category at all
            ]]],
        ];
        $d = (new CatalogStructure($pre))->discover();
        $this->assertSame(['/big-vps/', '/lane/', '/misc/', '/tie/', '/blog/'], array_column($d['nav'], 'link'));
        $this->assertSame(['K1', 'K2', 'K3', null, null], array_column($d['nav'], 'category_id'));
        $this->assertSame(['Everything', 'Fast Lane'], [$d['categories']['K1']['nav_title'], $d['categories']['K2']['nav_title']]);
        $this->assertTrue($d['categories']['K1']['in_nav']);
        $this->assertFalse($d['categories']['K4']['in_nav'], 'a tied nav entry links to nothing rather than guessing');
        // c is a member of both linked categories: the more specific (fewer members) one owns it
        $this->assertSame(['a', 'b'], $d['categories']['K1']['plans']);
        $this->assertSame(['c'], $d['categories']['K2']['plans']);
        // K3 is linked but holds no plan-shaped product: not a plan family
        $this->assertTrue($d['categories']['K3']['in_nav']);
        $this->assertFalse($d['categories']['K3']['plan_family']);
        $this->assertSame('https://contabo.com/en/vps/a/', $d['categories']['K1']['sample_product_url']);
        $this->assertSame(['K1', 'K2'], array_column(CatalogStructure::familyCategories($d), 'category_id'));
    }

    public function testTokenSetMatchesReorderedSlug(): void
    {
        $pre = [
            'products' => ['p' => ['slug' => 'p', 'type' => 'vps', 'price' => ['EUR' => 1], 'specs' => [['title' => '1 vCPU Cores', 'type' => 'cpu']]]],
            'categories' => ['P' => ['id' => 'P', 'slug' => 'performance-vps', 'title' => 'Fast', 'products' => ['p' => ['slug' => 'p']]]],
            'navItems' => [['title' => 'x', 'children' => [['fields' => ['title' => 'Speed', 'link' => '/vps-performance/']]]]],
        ];
        $d = (new CatalogStructure($pre))->discover();
        $this->assertSame('P', $d['nav'][0]['category_id'], '/vps-performance/ and performance-vps share the same dash tokens');
    }

    public function testClassifyAddon(): void
    {
        $cs = new CatalogStructure();
        $cases = [
            ['Asia (India)', 'region'], ['European Union', 'region'], ['Vereinigte Staaten (Central)', 'region'],
            ['Auto Backup', 'backup'], ['200 GB SSD', 'storage_upgrade'], ['1.8 TB NVMe', 'storage_upgrade'],
            ['1 TB FTP Storage', 'ftp_storage'], ['500 GB Object Storage in {product_location}', 'object_storage'],
            ['cPanel/WHM (100 accounts)', 'panel'], ['Plesk Obsidian Web Pro Edition', 'panel'], ['Full Monitoring', 'monitoring'],
            ['Nextcloud Server', 'app'], ['Docker', 'app'], ['SSL certificate', 'other'],
        ];
        foreach ($cases as $c) {
            $this->assertSame($c[1], $cs->classifyAddon(['title' => $c[0]]), $c[0]);
        }
        $this->assertSame('os_image', $cs->classifyAddon(['title' => 'Debian 13', 'osId' => 1]));
    }

    public function testStructureOnOlderFixtureAssertsShapeNotCounts(): void
    {
        $pre = $this->pre('cloud_vps_10.html');
        $r = (new CatalogStructure())->build($pre);

        $this->assertArrayHasKey('families', $r);
        $this->assertArrayHasKey('legacy', $r);
        $this->assertArrayHasKey('addon_groups', $r);
        foreach ($r['families'] as $f) {
            $this->assertContains($f['key'], self::FAMILY_KEYS);
            $this->assertNotSame('', $f['label']);
            foreach ($f['plans'] as $p) {
                foreach (['title', 'slug', 'availability', 'prices', 'periods', 'specs', 'options'] as $k) {
                    $this->assertArrayHasKey($k, $p);
                }
            }
        }
        $familyPlans = 0;
        foreach ($r['families'] as $f) {
            $familyPlans += count($f['plans']);
        }
        $this->assertSame(count($pre['products']), $familyPlans + count($r['legacy']), 'every product is either family or legacy');
        $this->assertNotEmpty($r['addon_groups']);
    }

    public function testGoldenRound1Body(): void
    {
        $pre = $this->pre('round1_treg_anyapi.html');
        $r = (new CatalogStructure())->build($pre);

        $this->assertSame(self::FAMILY_KEYS, array_column($r['families'], 'key'));
        $this->assertSame(
            ['Core VPS', 'Performance VPS', 'Max Performance VPS', 'Storage VPS', 'Dedicated Servers', 'GPU VPS'],
            array_column($r['families'], 'label')
        );
        $this->assertSame([6, 6, 5, 5, 4, 1], array_map('count', array_column($r['families'], 'plans')));
        $this->assertCount(168, $pre['products']);
        $this->assertCount(141, $r['legacy']);

        $by = [];
        foreach ($r['families'] as $f) {
            foreach ($f['plans'] as $p) {
                $by[$p['slug']] = $p;
            }
        }
        // Checkout screenshot ground truth (EUR net; screenshot gross = net x 1.18).
        $c4 = $by['cloud-vps-core-4'];
        $this->assertSame(5.5, (float) $c4['prices']['EUR']);
        $this->assertSame(6.49, round(5.5 * 1.18, 2));
        $regions = array_column($c4['options']['regions'], 'monthly_price', 'name');
        $this->assertEquals(2.4, $regions['Asia (India)']);
        $this->assertSame(2.83, round(2.4 * 1.18, 2));
        $storage = array_column($c4['options']['storage_upgrades'], 'monthly_price', 'name');
        $this->assertEquals(1.5, $storage['200 GB SSD']);
        $this->assertEquals(1.65, $c4['options']['backup']['monthly_price']);
        $obj = array_column($c4['options']['object_storage'], 'monthly_price', 'name');
        $this->assertEquals(11.96, $obj['1 TB Object Storage in Asia']);
        $this->assertSame(14.11, round(11.96 * 1.18, 2));
        $this->assertContains('Ubuntu 24.04', array_column($c4['options']['os_images'], 'name'));
        $this->assertSame(['months' => 12, 'discount' => 9.9], array_intersect_key($c4['periods'][3], ['months' => 1, 'discount' => 1]));
        $this->assertSame(26.4, $c4['periods'][4]['discount']);

        $gpu = $by['gpu-vps-plus-18'];
        $this->assertSame(999, $gpu['prices']['EUR']);
        $this->assertSame('NVIDIA RTX 6000', $gpu['specs']['gpu']);
        $this->assertSame(18, $gpu['specs']['cpu_cores']);
        $this->assertSame(96.0, (float) $gpu['specs']['ram_gb']);
        $this->assertSame(900.0, (float) $gpu['specs']['storage_gb']);
        $this->assertSame('NVMe', $gpu['specs']['storage_type']);
        $this->assertSame(5, $gpu['specs']['snapshots']);
        $this->assertSame(1000.0, (float) $gpu['specs']['port_mbps']);
        $this->assertSame(1798.2, $gpu['periods'][3]['discount']);
        $this->assertSame(4795.2, $gpu['periods'][4]['discount']);
        $this->assertSame(1002.0, round($gpu['periods'][3]['effective_monthly'] * 1.18, 2));
        $this->assertSame(943.06, round($gpu['periods'][4]['effective_monthly'] * 1.18, 2));
    }
}
