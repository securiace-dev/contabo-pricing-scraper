<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CatalogAuditLog;
use ContaboPricing\CycleSet;
use ContaboPricing\DailyOperationsDigestBuilder;
use ContaboPricing\ProfileManager;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/** A profile asking for a term Contabo does not publish must price, not fail. */
final class SyncEngineDerivedTermTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testThirtySixMonthProfileIsDerivedNotFailed(): void
    {
        $result = $this->runSync(36, [
            ['months' => 1, 'effective_monthly' => 4.50, 'setup_fee' => 0.0],
            ['months' => 12, 'effective_monthly' => 3.60, 'setup_fee' => 0.0],
        ]);
        $this->assertSame('preview', $result['status']);
        $this->assertSame([], $result['errors']);
        $this->assertSame([], $result['warnings'], json_encode($result['warnings']));
        $this->assertSame(1, $result['profiles_changed']);
        $this->assertSame(3.60, $result['change_list'][0]['new_final']);
        $this->assertCount(1, $result['derived_terms']);
        $terms = $result['derived_terms'][0]['terms'];
        $byMonths = [];
        foreach ($terms as $t) {
            $byMonths[$t['months']] = $t;
        }
        $this->assertTrue($byMonths[36]['derived']);
        $this->assertSame(12, $byMonths[36]['source_months']);
        $this->assertArrayNotHasKey(12, $byMonths);
        $this->assertSame('nearest-term', $result['term_policy']);
    }

    public function testNativeTermProducesNoDerivedMarkerForHeadlinePeriod(): void
    {
        $result = $this->runSync(12, [
            ['months' => 1, 'effective_monthly' => 4.50, 'setup_fee' => 0.0],
            ['months' => 12, 'effective_monthly' => 3.60, 'setup_fee' => 0.0],
        ]);
        $this->assertSame([], $result['errors']);
        foreach ($result['derived_terms'] as $d) {
            foreach ($d['terms'] as $t) {
                $this->assertNotSame(12, $t['months']);
            }
        }
    }

    public function testPlanWithNoPublishedTermsWarnsAndNeverFailsTheRun(): void
    {
        $result = $this->runSync(36, []);
        $this->assertNotSame('failed', $result['status']);
        $this->assertSame([], $result['errors']);
        $this->assertCount(1, $result['warnings']);
        $this->assertStringContainsString('36 mo', $result['warnings'][0]);
    }

    public function testEmptyLocalCatalogFailsWithClearPreflightAndFatalLine(): void
    {
        $result = $this->runSync(12, [], ['snapshot_meta' => ['generated_at' => '', 'plan_count' => 0]]);
        $this->assertSame('failed', $result['status']);
        $this->assertStringContainsString('Preflight failed', (string) $result['error_message']);
        $this->assertContains('Fatal error: ' . $result['error_message'], $result['errors']);

        $digest = (new DailyOperationsDigestBuilder())->build($result, ['status' => 'healthy']);
        $this->assertSame('failed', $digest['severity']);
        $this->assertStringContainsString('Fatal error: Preflight failed', $digest['message']);
        $this->assertSame(1, substr_count($digest['message'], 'Fatal error: Preflight failed'));
    }

    public function testDigestShowsErrorMessageEvenWhenErrorsListIsEmpty(): void
    {
        $digest = (new DailyOperationsDigestBuilder())->build(
            ['status' => 'failed', 'profiles_checked' => 0, 'errors' => [], 'error_message' => 'cURL error 7: connection refused'],
            ['status' => 'healthy']
        );
        $this->assertStringContainsString('Fatal error: cURL error 7: connection refused', $digest['message']);
    }

    public function testDigestWarnsAndListsDerivedTermsWithoutFailing(): void
    {
        $digest = (new DailyOperationsDigestBuilder())->build(
            [
                'status' => 'no-change', 'profiles_checked' => 1, 'errors' => [],
                'warnings' => ['profile x: no published term can price 36 mo for p; skipped'],
                'derived_terms' => [['profile_slug' => 'x', 'plan_slug' => 'p', 'terms' => []]],
                'term_policy' => 'nearest-term',
            ],
            ['status' => 'healthy']
        );
        $this->assertSame('warning', $digest['severity']);
        $this->assertStringContainsString('Warnings: profile x', $digest['message']);
        $this->assertStringContainsString('Derived (NOT official Contabo)', $digest['message']);
    }

    public function testStaleCatalogAndZeroProfilesWarnWithoutFailing(): void
    {
        $old = gmdate('Y-m-d\TH:i:s\Z', time() - (SyncEngine::CATALOG_STALE_DAYS + 5) * 86400);
        $stale = $this->runSync(12, [
            ['months' => 1, 'effective_monthly' => 4.50, 'setup_fee' => 0.0],
        ], ['snapshot_meta' => ['generated_at' => $old, 'plan_count' => 1]]);
        $this->assertNotSame('failed', $stale['status']);
        $this->assertNotEmpty($stale['warnings']);
        $this->assertStringContainsString('days old', $stale['warnings'][0]);

        $empty = $this->runSync(12, [
            ['months' => 1, 'effective_monthly' => 4.50, 'setup_fee' => 0.0],
        ], null, true);
        $this->assertNotSame('failed', $empty['status']);
        $this->assertContains('No active profiles to sync.', $empty['warnings']);
    }

    /**
     * @param list<array<string,mixed>> $periods
     * @param array<string,mixed>|null $meta
     * @return array<string,mixed>
     */
    private function runSync(int $periodMonths, array $periods, ?array $meta = null, bool $noProfiles = false): array
    {
        $profile = [
            'id' => 1, 'slug' => 'vps-test', 'plan_slug' => 'vps-test',
            'period_months' => $periodMonths, 'sync_strategy' => 'manual',
            'published_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask(),
        ];
        $meta = $meta ?? ['scraper_version' => 't', 'snapshot_meta' => ['generated_at' => gmdate('Y-m-d\TH:i:s\Z'), 'plan_count' => 1]];
        $api = new class($periods, $meta) extends StubPlanSource {
            private $periods; private $meta;
            public function __construct(array $periods, array $meta) { $this->periods = $periods; $this->meta = $meta; }
            public function meta(): array { return $this->meta; }
            public function plan(string $slug): array
            {
                return ['slug' => $slug, 'base_monthly_price' => 4.50, 'periods' => $this->periods, 'specs_parsed' => []];
            }
        };
        $rows = $noProfiles ? [] : [$profile];
        $profiles = new class(new Settings('manual', 'EUR', false, 0.0, 365, ''), $rows) extends ProfileManager {
            private $rows;
            public function __construct(Settings $s, array $rows) { parent::__construct($s); $this->rows = $rows; }
            public function listProfiles(bool $activeOnly = true, bool $includeTrashed = false): array { return $this->rows; }
            public function latestVersion(int $profileId): ?array { return null; }
        };
        $engine = new SyncEngine(new Settings('manual', 'EUR', false, 0.0, 365, ''), $api, $profiles, new CatalogAuditLog());
        return $engine->run('manual', true);
    }
}
