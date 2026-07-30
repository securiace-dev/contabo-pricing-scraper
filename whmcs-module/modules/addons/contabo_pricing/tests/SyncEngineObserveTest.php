<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ApiClient;
use ContaboPricing\CatalogAuditLog;
use ContaboPricing\CycleSet;
use ContaboPricing\ProfileManager;
use ContaboPricing\ProfileVersionInput;
use ContaboPricing\Rounding;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * The catalog observation contract is absolute: no database table changes.
 */
final class SyncEngineObserveTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testProfilePreviewReturnsTypedPlanWithoutAnyMutation(): void
    {
        $audit = new ObserveCatalogAuditSpy();
        $engine = $this->makeEngine(
            $audit,
            new ObserveProfileManager($this->settings(), [])
        );
        $this->seedCatalog();
        $before = Capsule::$tables;

        $version = ProfileVersionInput::computed(
            10.0,
            10.0,
            2.0,
            [],
            [],
            1.0,
            'test',
            0.0,
            false,
            'EUR',
            '2026-07-30T00:00:00Z',
            [1 => 10.0]
        );
        $result = $engine->previewCatalogForProfile(
            1,
            ['published_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask()],
            $version,
            'preview-batch'
        );

        $this->assertSame($before, Capsule::$tables);
        $this->assertSame([], Capsule::$calls);
        $this->assertSame([], Capsule::$inserts);
        $this->assertSame([], $audit->rows);
        $this->assertSame(1, $result['cycles_planned']);
        $this->assertSame(0, $result['cycles_applied']);
        $this->assertCount(2, $result['planned_writes']);
        $this->assertSame('recurring', $result['planned_writes'][0]['kind']);
        $this->assertSame('monthly', $result['planned_writes'][0]['column']);
        $this->assertSame(10.0, $result['planned_writes'][0]['new_value']);
        $this->assertSame('setup_fee', $result['planned_writes'][1]['kind']);
        $this->assertSame('msetupfee', $result['planned_writes'][1]['column']);
        $this->assertSame(2.0, $result['planned_writes'][1]['new_value']);
    }

    public function testWholeSyncObservationDoesNotWriteLogsVersionsAuditsOrPrices(): void
    {
        $profile = [
            'id' => 1,
            'slug' => 'vps-test',
            'plan_slug' => 'vps-test',
            'period_months' => 1,
            'sync_strategy' => 'auto-apply',
            'published_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask(),
        ];
        $profiles = new ObserveProfileManager($this->settings(), [$profile]);
        $audit = new ObserveCatalogAuditSpy();
        $engine = $this->makeEngine($audit, $profiles);
        $this->seedCatalog();
        $before = Capsule::$tables;

        $result = $engine->run('manual', true);

        $this->assertSame('preview', $result['status']);
        $this->assertTrue($result['observe_only']);
        $this->assertSame(1, $result['profiles_changed']);
        $this->assertSame(1, $result['products_planned']);
        $this->assertSame(1, $result['cycles_planned']);
        $this->assertCount(2, $result['planned_writes']);
        $this->assertSame(0, $profiles->appendCalls);
        $this->assertSame($before, Capsule::$tables);
        $this->assertSame([], Capsule::$calls);
        $this->assertSame([], Capsule::$inserts);
        $this->assertSame([], $audit->rows);
    }

    private function settings(): Settings
    {
        return new Settings(
            'http://localhost:8080/api/v1',
            '',
            'manual',
            'EUR',
            false,
            0.0,
            365,
            ''
        );
    }

    private function makeEngine(
        CatalogAuditLog $audit,
        ProfileManager $profiles
    ): SyncEngine {
        $api = new class extends ApiClient {
            public function __construct() {}

            public function meta(): array
            {
                return ['snapshot_meta' => ['generated_at' => '2026-07-30T00:00:00Z']];
            }

            public function fx(): array
            {
                return [];
            }

            public function plan(string $slug): array
            {
                return [
                    'slug' => $slug,
                    'periods' => [[
                        'months' => 1,
                        'effective_monthly' => 10.0,
                        'setup_fee' => 2.0,
                    ]],
                    'specs_parsed' => [],
                ];
            }
        };
        return new SyncEngine($this->settings(), $api, $profiles, $audit);
    }

    private function seedCatalog(): void
    {
        Capsule::$tables['tblcurrencies'] = [
            ['id' => 1, 'code' => 'EUR'],
        ];
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id' => 10,
            'profile_id' => 1,
            'product_id' => 100,
            'active' => 1,
            'catalog_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask(),
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles' => 1,
            'sync_setup_fees' => 1,
            'rounding_mode' => Rounding::MODE_EXACT_2_DECIMALS,
            'markup_overrides_json' => '',
        ]];
        Capsule::$tables['tblpricing'] = [[
            'type' => 'product',
            'currency' => 1,
            'relid' => 100,
            'monthly' => 9.0,
            'msetupfee' => 0.0,
        ]];
    }
}

final class ObserveCatalogAuditSpy extends CatalogAuditLog
{
    /** @var list<array<string,mixed>> */
    public $rows = [];

    public function insert(array $row): int
    {
        $this->rows[] = $row;
        return count($this->rows);
    }
}

final class ObserveProfileManager extends ProfileManager
{
    /** @var list<array<string,mixed>> */
    private $profiles;

    /** @var int */
    public $appendCalls = 0;

    /** @param list<array<string,mixed>> $profiles */
    public function __construct(Settings $settings, array $profiles)
    {
        parent::__construct($settings);
        $this->profiles = $profiles;
    }

    public function listProfiles(
        bool $activeOnly = true,
        bool $includeTrashed = false
    ): array {
        return $this->profiles;
    }

    public function latestVersion(int $profileId): ?array
    {
        return null;
    }

    public function appendVersion(int $profileId, ProfileVersionInput $v): int
    {
        $this->appendCalls++;
        return 1;
    }
}
