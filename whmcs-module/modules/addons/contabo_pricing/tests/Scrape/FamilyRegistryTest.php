<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Installer;
use ContaboPricing\Scrape\CatalogStructure;
use ContaboPricing\Scrape\FamilyRegistry;
use ContaboPricing\Scrape\SapperLiteralDecoder;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class FamilyRegistryTest extends TestCase
{
    /** @var FamilyRegistry */
    private $reg;

    protected function setUp(): void
    {
        Capsule::reset();
        (new Installer())->migrateTo16();
        $this->reg = new FamilyRegistry();
    }

    /**
     * A discover()-shaped category.
     *
     * @param list<string> $plans
     * @return array<string,mixed>
     */
    private static function cat(string $id, string $slug, string $title, array $plans, bool $linked = true, int $pos = 0, ?string $nav = null): array
    {
        return [
            'category_id' => $id, 'slug' => $slug, 'title' => $title,
            'nav_title' => $linked ? ($nav ?? $title) : null, 'nav_href' => $linked ? '/' . $slug . '/' : null,
            'nav_position' => $linked ? $pos : null, 'in_nav' => $linked, 'plan_family' => $linked && $plans !== [],
            'members' => $plans, 'member_ids' => [], 'plans' => $linked ? $plans : [], 'plan_ids' => [],
            'sample_product_url' => $linked && $plans !== [] ? 'https://contabo.com/en/vps/' . $plans[0] . '/' : null,
            'lowest_price' => null,
        ];
    }

    /** @param list<array<string,mixed>> $cats @return array<string,mixed> */
    private static function struct(array $cats): array
    {
        $out = [];
        foreach ($cats as $c) {
            $out[$c['category_id']] = $c;
        }
        return ['categories' => $out, 'nav' => []];
    }

    /** @return list<string> */
    private static function slugs(string $prefix, int $n): array
    {
        $o = [];
        for ($i = 1; $i <= $n; $i++) {
            $o[] = $prefix . $i;
        }
        return $o;
    }

    /** @return array<string,mixed> */
    private function base(): array
    {
        return self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('B1', 'perf', 'Perf', self::slugs('p', 4), true, 1),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
    }

    /** @return array<string,array<string,mixed>> */
    private function bySlug(): array
    {
        $o = [];
        foreach ($this->reg->all() as $r) {
            $o[$r['slug']] = $r;
        }
        return $o;
    }

    public function testFirstRunMarksEveryFamilyNewAndHidesWhatTheNavDoesNotLink(): void
    {
        $d = $this->reg->reconcile($this->base(), 1);
        $this->assertSame(['A1', 'B1', 'H1'], array_column($d['new'], 'category_id'));
        $this->assertSame(['new', 'new', 'hidden'], array_column($d['new'], 'status'));
        $rows = $this->bySlug();
        $this->assertSame('new', $rows['core']['status']);
        $this->assertSame('hidden', $rows['mirror']['status']);
        $this->assertSame(0, $rows['core']['approved']);
        $this->assertSame(6, $rows['core']['typical_plan_count']);
        $this->assertSame([6], $rows['core']['count_history']);
        $this->assertSame('Core VPS', $rows['core']['title_history'][0]['title']);
        $this->assertSame(self::slugs('c', 6), $rows['core']['plan_slugs']);
        $this->assertSame('https://contabo.com/en/vps/c1/', $rows['core']['sample_product_url']);
        $this->assertSame([0, 1], [$rows['core']['nav_position'], $rows['perf']['nav_position']]);
        $this->assertSame(['Core VPS', 'Perf'], array_column($d['families'], 'name'));
        $this->assertCount(2, $d['unapproved']);
    }

    public function testSecondIdenticalRunMakesThemActiveAndKeepsApproval(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $core = $this->bySlug()['core'];
        $this->assertTrue($this->reg->approve((int) $core['id']));

        $d = $this->reg->reconcile($this->base(), 2);
        $this->assertSame([], $d['new']);
        $this->assertSame([], $d['renamed']);
        $this->assertSame([], $d['count_changes']);
        $rows = $this->bySlug();
        $this->assertSame(['active', 'active', 'hidden'], [$rows['core']['status'], $rows['perf']['status'], $rows['mirror']['status']]);
        $this->assertSame(1, $rows['core']['approved'], 'a run never overwrites the operator-owned approval');
        $this->assertSame(0, $rows['perf']['approved']);
        $this->assertSame(['perf'], array_column($d['unapproved'], 'slug'));
        $this->assertSame(2, $rows['core']['last_run_id']);
        $this->assertCount(1, $rows['core']['title_history'], 'an unchanged title adds no history entry');
    }

    public function testPreviewWritesNothing(): void
    {
        $d = $this->reg->reconcile($this->base(), 1, false);
        $this->assertCount(3, $d['new']);
        $this->assertFalse($d['persisted']);
        $this->assertSame([], $this->reg->all());
        // and a preview after a real run still reports against the persisted state
        $this->reg->reconcile($this->base(), 1);
        $this->assertSame([], $this->reg->reconcile($this->base(), 2, false)['new']);
        $this->assertSame('new', $this->bySlug()['core']['status'], 'preview did not promote new -> active');
    }

    public function testRenamedTitleIsRecordedInHistoryAndFlagged(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $this->reg->renameDisplay((int) $this->bySlug()['core']['id'], 'My Core');
        $s = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS Plus'),
            self::cat('B1', 'perf', 'Perf', self::slugs('p', 4), true, 1),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($s, 2);
        $this->assertCount(1, $d['renamed']);
        $this->assertSame(['title', 'Core VPS', 'Core VPS Plus'], [$d['renamed'][0]['kind'], $d['renamed'][0]['from'], $d['renamed'][0]['to']]);
        $row = $this->bySlug()['core'];
        $this->assertSame(['Core VPS', 'Core VPS Plus'], array_column($row['title_history'], 'title'));
        $this->assertSame('My Core', $row['display_name'], 'the operator display name survives upstream renames');
        $this->assertSame('My Core', $d['families'][0]['name']);
        $this->assertSame('My Core', FamilyRegistry::effectiveName($row));
    }

    public function testVanishedCategoryIsRetiredNotDeletedAndCanReappear(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $this->reg->reconcile($this->base(), 2);
        $without = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($without, 3);
        $this->assertSame(['B1'], array_column($d['retired'], 'category_id'));
        $this->assertSame('active', $d['retired'][0]['was']);
        $this->assertCount(3, $this->reg->all(), 'rows are never deleted');
        $this->assertSame('retired', $this->bySlug()['perf']['status']);
        $this->assertNotContains('perf', array_column($this->reg->active(), 'slug'));

        $back = $this->reg->reconcile($this->base(), 4);
        $this->assertSame(['B1'], array_column($back['reappeared'], 'category_id'));
        $this->assertSame('active', $this->bySlug()['perf']['status']);
        $this->assertSame([4, 4, 4], $this->bySlug()['perf']['count_history'], 'history survives the retirement (runs 1, 2 and 4)');
    }

    public function testSameSlugUnderANewIdIsLinkedAsSuccessorWithHistoryCarriedForward(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $this->reg->reconcile($this->base(), 2);
        $this->reg->approve((int) $this->bySlug()['perf']['id']);
        $this->reg->renameDisplay((int) $this->bySlug()['perf']['id'], 'Performance');

        $moved = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('B2', 'perf', 'Perf', self::slugs('p', 4), true, 1), // new id, same slug
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($moved, 3);
        $this->assertSame([], $d['new'], 'a successor is not "new"');
        $this->assertSame([], $d['retired'], 'nor is its predecessor reported as a plain retirement');
        $this->assertCount(1, $d['renamed']);
        $this->assertSame(['id_change', 'B1', 'B2'], [$d['renamed'][0]['kind'], $d['renamed'][0]['from_category_id'], $d['renamed'][0]['successor_of'] === 'B1' ? 'B2' : '?']);
        $rows = [];
        foreach ($this->reg->all() as $r) {
            $rows[$r['category_id']] = $r;
        }
        $this->assertSame('retired', $rows['B1']['status']);
        $this->assertSame('B1', $rows['B2']['successor_of']);
        $this->assertSame(1, $rows['B2']['approved'], 'approval carries forward');
        $this->assertSame('Performance', $rows['B2']['display_name']);
        $this->assertSame('active', $rows['B2']['status']);
        $this->assertSame([4, 4, 4], $rows['B2']['count_history']);
        $this->assertSame($rows['B1']['first_seen_at'], $rows['B2']['first_seen_at']);
        $this->assertSame(['plan_moves' => []], ['plan_moves' => $d['plan_moves']], 'the same plans under the successor are not moves');
    }

    public function testSuccessorByMemberOverlapEvenWhenTheSlugChanged(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $this->reg->reconcile($this->base(), 2);
        // 4 of 5 members shared (80 %), different id and slug
        $moved = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('B9', 'perf-2027', 'Perf 2027', ['p1', 'p2', 'p3', 'p4', 'p5'], true, 1),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $this->assertSame(0.8, FamilyRegistry::overlap(self::slugs('p', 4), ['p1', 'p2', 'p3', 'p4', 'p5']));
        $d = $this->reg->reconcile($moved, 3);
        $this->assertSame('B1', $d['renamed'][0]['successor_of']);

        // below 80 %: a plain new family and a plain retirement
        Capsule::reset();
        (new Installer())->migrateTo16();
        $this->reg->reconcile($this->base(), 1);
        $weak = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('B9', 'perf-2027', 'Perf 2027', ['p1', 'p2', 'x1', 'x2'], true, 1),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($weak, 2);
        $this->assertSame(['B9'], array_column($d['new'], 'category_id'));
        $this->assertSame(['B1'], array_column($d['retired'], 'category_id'));
    }

    public function testCountDeviationBeyondTheThresholdIsFlaggedAndCalibrationUsesTheMedian(): void
    {
        for ($run = 1; $run <= 3; $run++) {
            $this->reg->reconcile($this->base(), $run);
        }
        $shrunk = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 3), true, 0, 'Core VPS'), // 3 vs typical 6 = 50 %
            self::cat('B1', 'perf', 'Perf', self::slugs('p', 4), true, 1),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($shrunk, 4, false, 20.0);
        $this->assertCount(1, $d['count_changes']);
        $this->assertSame([6, 3, 50.0], [$d['count_changes'][0]['typical'], $d['count_changes'][0]['current'], $d['count_changes'][0]['pct']]);
        $this->assertSame(6, $this->bySlug()['core']['typical_plan_count'], 'preview does not calibrate');

        // the threshold is strict (>), and per family
        $edge = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('B1', 'perf', 'Perf', self::slugs('p', 5), true, 1), // 5 vs 4: 25 %
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $this->assertSame(['B1'], array_column($this->reg->reconcile($edge, 4, false, 20.0)['count_changes'], 'category_id'));
        $this->assertSame([], $this->reg->reconcile($edge, 4, false, 25.0)['count_changes']);

        // accepted runs calibrate with the median of the last 5 counts: [6,6,6,3] -> 6, [6,6,6,3,3] -> 6, [6,6,3,3,3] -> 3
        $this->reg->reconcile($shrunk, 4);
        $this->assertSame(6, $this->bySlug()['core']['typical_plan_count']);
        $this->reg->reconcile($shrunk, 5);
        $this->assertSame(6, $this->bySlug()['core']['typical_plan_count'], 'one odd run does not move the typical count');
        $this->reg->reconcile($shrunk, 6);
        $row = $this->bySlug()['core'];
        $this->assertSame([6, 6, 3, 3, 3], $row['count_history']);
        $this->assertCount(FamilyRegistry::HISTORY_LIMIT, $row['count_history']);
        $this->assertSame(3, $row['typical_plan_count']);
        $this->assertSame(3, $row['last_plan_count']);
    }

    public function testMedianAndOverlapHelpers(): void
    {
        $this->assertSame(0, FamilyRegistry::median([]));
        $this->assertSame(5, FamilyRegistry::median([5]));
        $this->assertSame(6, FamilyRegistry::median([6, 5]));
        $this->assertSame(5, FamilyRegistry::median([9, 5, 1]));
        $this->assertSame(0.0, FamilyRegistry::overlap([], ['a']));
        $this->assertSame(1.0, FamilyRegistry::overlap(['a', 'b'], ['b', 'a']));
    }

    public function testPlanMovingBetweenFamiliesIsReported(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $this->reg->reconcile($this->base(), 2);
        $p = self::slugs('p', 4);
        $c = self::slugs('c', 6);
        $moved = self::struct([
            self::cat('A1', 'core', 'Core', array_slice($c, 1), true, 0, 'Core VPS'), // c1 left
            self::cat('B1', 'perf', 'Perf', array_merge(['c1'], $p), true, 1),      // ...and joined Perf
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($moved, 3);
        $this->assertCount(1, $d['plan_moves']);
        $this->assertSame(['c1', 'Core VPS', 'Perf'], [$d['plan_moves'][0]['slug'], $d['plan_moves'][0]['from_label'], $d['plan_moves'][0]['to_label']]);
    }

    public function testDelistedFamilyBecomesHiddenAndAdminHideIsSticky(): void
    {
        $this->reg->reconcile($this->base(), 1);
        $this->reg->reconcile($this->base(), 2);
        $unlinked = self::struct([
            self::cat('A1', 'core', 'Core', self::slugs('c', 6), true, 0, 'Core VPS'),
            self::cat('B1', 'perf', 'Perf', self::slugs('p', 4), false),
            self::cat('H1', 'mirror', 'Mirror', self::slugs('m', 3), false),
        ]);
        $d = $this->reg->reconcile($unlinked, 3);
        $this->assertSame(['B1'], array_column($d['delisted'], 'category_id'));
        $this->assertSame('hidden', $this->bySlug()['perf']['status']);
        $this->assertSame(['A1'], array_column($d['families'], 'category_id'));

        // an admin-hidden family stays hidden even though the nav links to it
        $this->assertTrue($this->reg->setHidden((int) $this->bySlug()['core']['id'], true));
        $d = $this->reg->reconcile($this->base(), 4);
        $this->assertSame('hidden', $this->bySlug()['core']['status']);
        $this->assertSame(['B1'], array_column($d['families'], 'category_id'), 'hidden families are not in the importable snapshot');
        $this->assertNotContains('core', array_column($this->reg->active(), 'slug'));
        $this->assertTrue($this->reg->setHidden((int) $this->bySlug()['core']['id'], false));
        $this->assertSame('active', $this->bySlug()['core']['status']);
        $this->assertFalse($this->reg->approve(9999));
    }

    public function testRealRound1BlobFirstRunThenStable(): void
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/round1_treg_anyapi.html');
        $pre = (new SapperLiteralDecoder())->decodeFromHtml($html)['preloaded'][0];
        $d = $this->reg->reconcile(new CatalogStructure($pre), 1);
        $this->assertSame(
            ['Core VPS', 'Performance VPS', 'Max Performance VPS', 'Storage VPS', 'Dedicated Servers', 'GPU VPS'],
            array_column($d['families'], 'name')
        );
        $this->assertSame([6, 6, 5, 5, 4, 1], array_column($d['families'], 'count'));
        $rows = $this->bySlug();
        $this->assertSame('hidden', $rows['vps-hp']['status'], 'present in the blob, absent from the nav');
        $this->assertSame('hidden', $rows['vps-mp']['status']);
        $this->assertSame('https://contabo.com/en/vps/cloud-vps-core-4/', $rows['vps']['sample_product_url']);
        $this->assertSame('/vps-performance/', $rows['performance-vps']['nav_href']);

        $d2 = $this->reg->reconcile(new CatalogStructure($pre), 2);
        $this->assertSame([], $d2['new']);
        $this->assertSame([], $d2['count_changes']);
        $this->assertSame([], $d2['plan_moves']);
    }
}
