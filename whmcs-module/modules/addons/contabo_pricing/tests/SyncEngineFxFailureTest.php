<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CatalogAuditLog;
use ContaboPricing\CycleSet;
use ContaboPricing\DailyOperationsDigestBuilder;
use ContaboPricing\Lock;
use ContaboPricing\ProfileManager;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * INR without a usable FX rate must fail loudly and write no prices.
 * The previous swallow stored EUR 3.6 as INR 3.6.
 */
final class SyncEngineFxFailureTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_sync_log'] = [];
        Capsule::$tables['mod_contabo_profile_version'] = [];
        Capsule::$tables['tblpricing'] = [[
            'type' => 'product',
            'currency' => 1,
            'relid' => 100,
            'monthly' => 9.0,
            'msetupfee' => 0.0,
        ]];
        Capsule::$tables['tblcurrencies'] = [['id' => 1, 'code' => 'INR']];
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id' => 10,
            'profile_id' => 1,
            'product_id' => 100,
            'active' => 1,
            'catalog_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask(),
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles' => 1,
            'sync_setup_fees' => 1,
            'rounding_mode' => 'exact_2_decimals',
            'markup_overrides_json' => '',
        ]];
    }

    public function testThrownFxIsFatalAndWritesNoVersionsOrPrices(): void
    {
        $profiles = $this->profiles();
        $engine = $this->engine(
            $profiles,
            new class extends StubPlanSource {
                public function meta(): array
                {
                    return ['scraper_version' => 't', 'snapshot_meta' => ['generated_at' => gmdate('Y-m-d\TH:i:s\Z'), 'plan_count' => 1]];
                }
                public function fx(): array
                {
                    throw new \RuntimeException('FX rates unavailable: HTTP 301 or unparseable body');
                }
                public function plan(string $slug): array
                {
                    throw new \RuntimeException('plan() must not run when FX is missing');
                }
            }
        );

        $pricingBefore = Capsule::$tables['tblpricing'];
        $result = $engine->run('manual', false);

        $this->assertSame('failed', $result['status']);
        $this->assertStringContainsString('FX rate unavailable for INR', (string) $result['error_message']);
        $this->assertStringContainsString('HTTP 301', (string) $result['error_message']);
        $this->assertContains('Fatal error: ' . $result['error_message'], $result['errors']);
        $this->assertSame(0, $profiles->appendCalls);
        $this->assertSame([], Capsule::$tables['mod_contabo_profile_version']);
        $this->assertSame($pricingBefore, Capsule::$tables['tblpricing']);
        $this->assertSame(0, $result['profiles_checked']);

        $digest = (new DailyOperationsDigestBuilder())->build($result, ['status' => 'healthy']);
        $this->assertSame('failed', $digest['severity']);
        $this->assertStringContainsString('Fatal error: FX rate unavailable for INR', $digest['message']);
        $this->assertStringContainsString('HTTP 301', $digest['message']);

        $logLine = 'Contabo Pricing sync failed: ' . (string) $result['error_message']
            . ' | errors: ' . implode(' ; ', $result['errors']);
        $this->assertStringContainsString('FX rate unavailable for INR', $logLine);
        $this->assertStringContainsString('HTTP 301', $logLine);
    }

    public function testEmptyFxResponseIsFatalForInr(): void
    {
        $profiles = $this->profiles();
        $engine = $this->engine(
            $profiles,
            new class extends StubPlanSource {
                public function meta(): array
                {
                    return ['scraper_version' => 't', 'snapshot_meta' => ['generated_at' => gmdate('Y-m-d\TH:i:s\Z'), 'plan_count' => 1]];
                }
                public function fx(): array
                {
                    return [];
                }
                public function plan(string $slug): array
                {
                    return [
                        'slug' => $slug,
                        'base_monthly_price' => 3.60,
                        'periods' => [['months' => 1, 'effective_monthly' => 3.60, 'setup_fee' => 0.0]],
                        'specs_parsed' => [],
                    ];
                }
            }
        );

        $result = $engine->run('manual', true);

        $this->assertSame('failed', $result['status']);
        $this->assertStringContainsString('refusing to store EUR amounts as INR', (string) $result['error_message']);
        $this->assertSame(0, $profiles->appendCalls);
        $this->assertSame(0, $result['profiles_changed']);
    }

    public function testInrWithRateConvertsEurosInsteadOfPassthrough(): void
    {
        $profiles = $this->profiles();
        $engine = $this->engine(
            $profiles,
            new class extends StubPlanSource {
                public function meta(): array
                {
                    return ['scraper_version' => 't', 'snapshot_meta' => ['generated_at' => gmdate('Y-m-d\TH:i:s\Z'), 'plan_count' => 1]];
                }
                public function fx(): array
                {
                    return ['eurInr' => 112.317, 'source' => 'frankfurter'];
                }
                public function plan(string $slug): array
                {
                    return [
                        'slug' => $slug,
                        'base_monthly_price' => 3.60,
                        'periods' => [['months' => 1, 'effective_monthly' => 3.60, 'setup_fee' => 0.0]],
                        'specs_parsed' => [],
                    ];
                }
            }
        );

        $result = $engine->run('manual', true);

        $this->assertSame('preview', $result['status']);
        $this->assertSame([], $result['errors']);
        $this->assertNull($result['error_message']);
        $this->assertSame(1, $result['profiles_changed']);
        $this->assertSame(404.34, $result['change_list'][0]['new_final']);
        $this->assertNotSame(3.6, $result['change_list'][0]['new_final']);
        $this->assertSame(0, $profiles->appendCalls);
    }

    private function settings(): Settings
    {
        return new Settings('manual', 'INR', false, 0.0, 365, '');
    }

    private function profiles(): FxFailProfileManager
    {
        return new FxFailProfileManager($this->settings(), [[
            'id' => 1,
            'slug' => 'vps-test',
            'plan_slug' => 'vps-test',
            'period_months' => 1,
            'sync_strategy' => 'auto-apply',
            'published_cycles_mask' => CycleSet::fromCycles(['Monthly'])->toMask(),
        ]]);
    }

    private function engine(ProfileManager $profiles, StubPlanSource $api): SyncEngine
    {
        $lock = new class extends Lock {
            public function acquire(string $name, int $ttlSeconds): ?string
            {
                return 'tok';
            }
            public function release(string $name, string $token): void
            {
            }
        };
        return new SyncEngine($this->settings(), $api, $profiles, new CatalogAuditLog(), $lock);
    }
}

final class FxFailProfileManager extends ProfileManager
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

    public function listProfiles(bool $activeOnly = true, bool $includeTrashed = false): array
    {
        return $this->profiles;
    }

    public function latestVersion(int $profileId): ?array
    {
        return null;
    }

    public function appendVersion(int $profileId, \ContaboPricing\ProfileVersionInput $v): int
    {
        $this->appendCalls++;
        return 1;
    }
}
