<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ApiClient;
use ContaboPricing\CatalogAuditLog;
use ContaboPricing\CycleSet;
use ContaboPricing\ProfileManager;
use ContaboPricing\ProfileVersionInput;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Local audit spy (self-contained so this file runs in isolation, independent of
 * SyncEngine6CycleTest's spy). Captures inserts; no DB I/O.
 */
final class PerCycleAuditSpy extends CatalogAuditLog
{
    /** @var list<array<string,mixed>> */
    public array $rows = [];

    protected function storeRow(array $row): int
    {
        $id = count($this->rows) + 1;
        $row['id'] = $id;
        $this->rows[] = $row;
        return $id;
    }

    protected function fetchRecentBatches(int $limit): array
    {
        return [];
    }
}

/**
 * Phase D — two-layer pricing.
 *
 * The PROFILE sources a per-cycle EUR vector (scraped 1/3/6/12 + projected 24/36
 * and any gaps), and the catalog write prices each cycle off ITS OWN source rate
 * × cycle months. Covers:
 *
 *   - the fallback rule: Source(M) = longest scraped period with months ≤ M
 *       · 24/36 → 12-mo rate
 *       · a missing 3-mo → 1-mo rate (NOT a longer-cycle discount)
 *   - per-cycle catalog pricing off the vector (different rate per cycle)
 *   - the profile publish gate (published_cycles_mask) skips unpublished cycles
 *   - mapping source_overrides_json pins a per-product basis
 */
final class PerCycleSourcePricingTest extends TestCase
{
    /** @var PerCycleAuditSpy */ private $audit;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->audit = new PerCycleAuditSpy();
    }

    // ── Fallback rule (pure) ────────────────────────────────────────────────

    public function testVectorProjects24And36FromTwelveMonthRate(): void
    {
        $vector = SyncEngine::periodPriceVectorFromPlan($this->plan([
            1 => 4.50, 3 => 4.50, 6 => 4.05, 12 => 3.60,
        ]));

        $this->assertSame(4.50, $vector[1]);
        $this->assertSame(4.50, $vector[3]);
        $this->assertSame(4.05, $vector[6]);
        $this->assertSame(3.60, $vector[12]);
        // 24/36 never scraped → take the longest available (12-mo) rate.
        $this->assertSame(3.60, $vector[24]);
        $this->assertSame(3.60, $vector[36]);
    }

    public function testMissingQuarterlyFallsBackToMonthlyNotLongerCycle(): void
    {
        // 3-mo absent. The rule must use the 1-mo rate (longest ≤ 3), NOT the
        // cheaper 6/12-mo discount tiers the customer never qualified for.
        $vector = SyncEngine::periodPriceVectorFromPlan($this->plan([
            1 => 10.00, 6 => 8.00, 12 => 7.00,
        ]));

        $this->assertSame(10.00, $vector[3], 'missing quarterly must use the 1-mo rate');
        $this->assertSame(8.00, $vector[6]);
        $this->assertSame(7.00, $vector[12]);
        $this->assertSame(7.00, $vector[24]);
        $this->assertSame(7.00, $vector[36]);
    }

    public function testEmptyPeriodsYieldEmptyVector(): void
    {
        $this->assertSame([], SyncEngine::periodPriceVectorFromPlan(['periods' => []]));
        $this->assertSame([], SyncEngine::periodPriceVectorFromPlan([]));
    }

    // ── Per-cycle catalog pricing off the vector ────────────────────────────

    public function testEachCyclePricesOffItsOwnSourceRate(): void
    {
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask' => CycleSet::MASK_MAX, // all six enabled for customer
            'respect_disabled_cycles' => 1, 'overwrite_free_cycles' => 0,
            'sync_setup_fees' => 0, 'rounding_mode' => 'exact_2_decimals',
            'markup_overrides_json' => '', // inherit → cost_plus_pct 0 → pass-through
        ]);
        $this->seedTblpricingRow(100, 1, []); // fresh catalog: all cycles absent (unguarded)

        // EUR vector with distinct per-cycle rates; 24/36 already projected = 8.
        $version = $this->versionWithVector([
            1 => 10.0, 3 => 10.0, 6 => 9.0, 12 => 8.0, 24 => 8.0, 36 => 8.0,
        ]);

        $engine = $this->makeEngine();
        // published_cycles_mask all six.
        $profile = ['id' => 1, 'published_cycles_mask' => CycleSet::MASK_MAX];
        $engine->applyCatalogForProfile(1, $profile, $version, 'batch-d-1');

        // EUR passthrough (currency INR but version built EUR-passthrough), markup
        // 0 → sell = sourceRate × cycleMonths.
        $this->assertSame(10.0, $this->writtenPrice('monthly'));      // 10 × 1
        $this->assertSame(30.0, $this->writtenPrice('quarterly'));    // 10 × 3
        $this->assertSame(54.0, $this->writtenPrice('semiannually')); // 9 × 6
        $this->assertSame(96.0, $this->writtenPrice('annually'));     // 8 × 12
        $this->assertSame(192.0, $this->writtenPrice('biennially'));  // 8 × 24
        $this->assertSame(288.0, $this->writtenPrice('triennially')); // 8 × 36
    }

    public function testPublishGateSkipsUnpublishedCycles(): void
    {
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask' => CycleSet::MASK_MAX, // mapping would allow all
            'respect_disabled_cycles' => 1, 'overwrite_free_cycles' => 0,
            'sync_setup_fees' => 0, 'rounding_mode' => 'exact_2_decimals',
            'markup_overrides_json' => '',
        ]);
        $this->seedTblpricingRow(100, 1, []);

        $version = $this->versionWithVector([1 => 10.0, 3 => 10.0, 6 => 9.0, 12 => 8.0, 24 => 8.0, 36 => 8.0]);

        // Profile publishes ONLY Monthly + Annually.
        $profile = ['id' => 1, 'published_cycles_mask' => CycleSet::fromCycles(['Monthly', 'Annually'])->toMask()];

        $engine = $this->makeEngine();
        $engine->applyCatalogForProfile(1, $profile, $version, 'batch-d-2');

        $cols = array_map(static fn ($u) => array_key_first($u['update']), $this->tblpricingUpdates());
        sort($cols);
        $this->assertSame(['annually', 'monthly'], $cols);

        $q = $this->findAuditRow('Quarterly');
        $this->assertNotNull($q);
        $this->assertSame(0, (int) $q['applied']);
        $this->assertSame('cycle_not_published', $q['skipped_reason']);
    }

    public function testSourceOverrideJsonPinsBasis(): void
    {
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask(),
            'respect_disabled_cycles' => 1, 'overwrite_free_cycles' => 0,
            'sync_setup_fees' => 0, 'rounding_mode' => 'exact_2_decimals',
            'markup_overrides_json' => '',
            // Per-product source pin: Monthly basis = 20 EUR (overrides vector 10).
            'source_overrides_json' => json_encode(['Monthly' => ['monthly_eur' => 20.0]]),
        ]);
        $this->seedTblpricingRow(100, 1, []);

        $version = $this->versionWithVector([1 => 10.0, 3 => 10.0, 6 => 9.0, 12 => 8.0, 24 => 8.0, 36 => 8.0]);

        $engine = $this->makeEngine();
        $engine->applyCatalogForProfile(1, ['id' => 1, 'published_cycles_mask' => CycleSet::MASK_MAX], $version, 'batch-d-3');

        $this->assertSame(20.0, $this->writtenPrice('monthly'), 'source override pins the basis');
    }

    public function testLegacyVersionWithoutVectorFallsBackToFinalMonthly(): void
    {
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask' => CycleSet::fromCycles(['Annually'])->toMask(),
            'respect_disabled_cycles' => 1, 'overwrite_free_cycles' => 0,
            'sync_setup_fees' => 0, 'rounding_mode' => 'exact_2_decimals',
            'markup_overrides_json' => '',
        ]);
        $this->seedTblpricingRow(100, 1, []);

        // No vector (legacy v7 row) → finalMonthly basis × months.
        $version = ProfileVersionInput::computed(
            5.0, 5.0, 0.0, [], [], 1.0, 'test', 0.0, false, 'EUR', 'snap'
        );

        $engine = $this->makeEngine();
        $engine->applyCatalogForProfile(1, ['id' => 1, 'published_cycles_mask' => CycleSet::MASK_MAX], $version, 'batch-d-4');

        $this->assertSame(60.0, $this->writtenPrice('annually'), '5 (finalMonthly) × 12');
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /** @param array<int,float> $periods months => effective_monthly */
    private function plan(array $periods): array
    {
        $rows = [];
        foreach ($periods as $m => $rate) {
            $rows[] = ['months' => $m, 'effective_monthly' => $rate];
        }
        return ['periods' => $rows];
    }

    /** @param array<int,float> $vector */
    private function versionWithVector(array $vector): ProfileVersionInput
    {
        // EUR passthrough, GST off → finalMonthly == configured, and
        // toLocalMonthly(sourceEur) == sourceEur, so sell = sourceEur × months.
        return ProfileVersionInput::computed(
            $vector[1] ?? 0.0, $vector[1] ?? 0.0, 0.0, [], [],
            1.0, 'test', 0.0, false, 'EUR', 'snap', $vector
        );
    }

    private function makeEngine(): SyncEngine
    {
        $settings = new Settings(
            'http://localhost:8080/api/v1', '', 'manual', 'INR',
            true, 0.0, 365, ''
        );
        $api = new class extends ApiClient {
            public function __construct() {}
        };
        $profileMgr = new class($settings) extends ProfileManager {
            public function listProfiles(bool $activeOnly = true, bool $includeTrashed = false): array { return []; }
            public function latestVersion(int $profileId): ?array { return null; }
            public function appendVersion(int $profileId, ProfileVersionInput $v): int { return 0; }
        };
        return new SyncEngine($settings, $api, $profileMgr, $this->audit);
    }

    private function seedCurrency(int $id, string $code): void
    {
        if (!isset(Capsule::$tables['tblcurrencies'])) {
            Capsule::$tables['tblcurrencies'] = [];
        }
        Capsule::$tables['tblcurrencies'][] = ['id' => $id, 'code' => $code];
    }

    /** @param array<string,mixed> $row */
    private function seedMapping(array $row): void
    {
        if (!isset(Capsule::$tables['mod_contabo_mapping'])) {
            Capsule::$tables['mod_contabo_mapping'] = [];
        }
        Capsule::$tables['mod_contabo_mapping'][] = $row;
    }

    /** @param array<string,mixed> $cols */
    private function seedTblpricingRow(int $productId, int $currencyId, array $cols): void
    {
        if (!isset(Capsule::$tables['tblpricing'])) {
            Capsule::$tables['tblpricing'] = [];
        }
        Capsule::$tables['tblpricing'][] = array_merge([
            'type' => 'product', 'currency' => $currencyId, 'relid' => $productId,
        ], $cols);
    }

    /** @return list<array<string,mixed>> */
    private function tblpricingUpdates(): array
    {
        return array_values(array_filter(
            Capsule::$calls,
            static fn ($call) => $call['table'] === 'tblpricing'
        ));
    }

    /** @return float|null the value written to a given recurring column */
    private function writtenPrice(string $column): ?float
    {
        foreach ($this->tblpricingUpdates() as $u) {
            if (array_key_exists($column, $u['update'])) {
                return (float) $u['update'][$column];
            }
        }
        return null;
    }

    /** @return array<string,mixed>|null */
    private function findAuditRow(string $cycle): ?array
    {
        foreach ($this->audit->rows as $r) {
            if (($r['cycle'] ?? null) === $cycle) {
                return $r;
            }
        }
        return null;
    }
}
