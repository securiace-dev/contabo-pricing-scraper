<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CatalogAuditLog;
use ContaboPricing\CycleSet;
use ContaboPricing\ProfileManager;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * SyncEngine dry-run mode tests.
 *
 * Validates that setDryRun(true) prevents tblpricing mutations while
 * ::preview() returns the writes that would have occurred.
 */
final class SyncEngineDryRunTest extends TestCase
{
    /** @var CatalogAuditLogSpy */ private $audit;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->audit = new CatalogAuditLogSpy();
    }

    public function testDryRunDoesNotMutateTblpricing(): void
    {
        $mask = CycleSet::fromCycles(['Monthly', 'Quarterly', 'Annually', 'Semiannually', 'Biennially', 'Triennially'])->toMask();

        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id'                       => 10,
            'profile_id'               => 1,
            'product_id'               => 100,
            'active'                   => 1,
            'catalog_cycles_mask'      => $mask,
            'respect_disabled_cycles'  => 1,
            'overwrite_free_cycles'    => 0,
            'sync_setup_fees'          => 0,
            'rounding_mode'            => 'exact_2_decimals',
            'markup_overrides_json'    => '',
        ]);
        $this->seedTblpricingRow(100, 1, [
            'monthly' => 1000.00,
        ]);

        $engine = $this->makeEngine();
        $engine->setDryRun(true);

        $version = $this->makeVersion(1200.00, 0.0);
        $stats = $engine->applyCatalogForProfile(
            1, $this->loadMapping(10) + ['sync_strategy' => 'auto-apply'], $version, 'batch-test-dryrun'
        );

        // Validate no tblpricing mutations occurred.
        $updates = $this->tblpricingUpdates();
        $this->assertEmpty($updates, 'dry-run should not mutate tblpricing, got ' . count($updates) . ' updates');

        // Validate preview captures the would-be writes.
        $preview = $engine->preview();
        $this->assertNotEmpty($preview, 'preview should capture would-be writes');
    }

    public function testDryRunPreviewReturnsExpectedColumns(): void
    {
        $mask = CycleSet::fromCycles(['Monthly', 'Annually'])->toMask();

        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id'                       => 10,
            'profile_id'               => 1,
            'product_id'               => 100,
            'active'                   => 1,
            'catalog_cycles_mask'      => $mask,
            'respect_disabled_cycles'  => 1,
            'overwrite_free_cycles'    => 0,
            'sync_setup_fees'          => 0,
            'rounding_mode'            => 'exact_2_decimals',
            'markup_overrides_json'    => '',
        ]);
        $this->seedTblpricingRow(100, 1, [
            'monthly'  => 1000.00,
            'annually' => 10000.00,
        ]);

        $engine = $this->makeEngine();
        $engine->setDryRun(true);

        $version = $this->makeVersion(1200.00, 0.0);
        $engine->applyCatalogForProfile(
            1, $this->loadMapping(10) + ['sync_strategy' => 'auto-apply'], $version, 'batch-test-dryrun-preview'
        );

        $preview = $engine->preview();
        $this->assertCount(2, $preview, 'expected 2 writes (Monthly + Annually)');

        $columns = array_column($preview, 'column');
        $this->assertContains('monthly', $columns);
        $this->assertContains('annually', $columns);

        foreach ($preview as $write) {
            $this->assertArrayHasKey('productId', $write);
            $this->assertArrayHasKey('currencyId', $write);
            $this->assertArrayHasKey('column', $write);
            $this->assertArrayHasKey('value', $write);
            $this->assertGreaterThan(0.0, $write['value'], 'preview write value must be positive');
            $this->assertEquals(100, $write['productId']);
            $this->assertEquals(1, $write['currencyId']);
        }
    }

    public function testDryRunDefaultIsFalse(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();

        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id'                       => 10,
            'profile_id'               => 1,
            'product_id'               => 100,
            'active'                   => 1,
            'catalog_cycles_mask'      => $mask,
            'respect_disabled_cycles'  => 1,
            'overwrite_free_cycles'    => 0,
            'sync_setup_fees'          => 0,
            'rounding_mode'            => 'exact_2_decimals',
            'markup_overrides_json'    => '',
        ]);
        $this->seedTblpricingRow(100, 1, [
            'monthly' => 1000.00,
        ]);

        $engine = $this->makeEngine();
        // Default: dryRun is false, writes should happen.
        $version = $this->makeVersion(1200.00, 0.0);
        $engine->applyCatalogForProfile(
            1, $this->loadMapping(10) + ['sync_strategy' => 'auto-apply'], $version, 'batch-test-default'
        );

        $updates = $this->tblpricingUpdates();
        $this->assertNotEmpty($updates, 'default mode (dryRun=false) should write to tblpricing');

        $preview = $engine->preview();
        $this->assertEmpty($preview, 'preview should be empty when dryRun is false');
    }

    // ── Fixture helpers (mirrors SyncEngine6CycleTest) ─────────────────────

    private function makeEngine(): SyncEngine
    {
        $settings = new Settings(
            apiBaseUrl: 'http://api.local/v1',
            apiToken: '',
            defaultSyncStrategy: 'auto-apply',
            currencyIso: 'INR',
            applyGst18: false,
            fxMarkupPct: 3.5,
            logRetentionDays: 365,
            moduleLink: 'addonmodules.php?module=contabo_pricing',
        );
        $api = new \ContaboPricing\ApiClient($settings, 8, new MockRequestExecutor());
        $profiles = new \ContaboPricing\ProfileManager($settings, $this->audit);
        return new SyncEngine($settings, $api, $profiles, $this->audit);
    }

    /** @return array<string,mixed> */
    private function makeVersion(float $monthlyEur, float $setupEur): \ContaboPricing\ProfileVersionInput
    {
        return \ContaboPricing\ProfileVersionInput::computed(
            $monthlyEur, $monthlyEur, $setupEur,
            [], [], null, null, 3.5, false, 'INR',
            date('c'), [$monthlyEur]
        );
    }

    /** @param array<string,mixed> $row */
    private function seedCurrency(int $id, string $code): void
    {
        Capsule::table('tblcurrencies')->insert(['id' => $id, 'code' => $code]);
    }

    /** @param array<string,scalar|null> $row */
    private function seedMapping(array $row): void
    {
        Capsule::table('mod_contabo_mapping')->insert($row);
    }

    /** @param array<string,float> $cells */
    private function seedTblpricingRow(int $productId, int $currencyId, array $cells): void
    {
        Capsule::table('tblpricing')->insert(
            ['type' => 'product', 'currency' => $currencyId, 'relid' => $productId] + $cells
        );
    }

    /** @return array<string,mixed> */
    private function loadMapping(int $id): array
    {
        return (array) Capsule::table('mod_contabo_mapping')->where('id', $id)->first();
    }

    /** @return list<array<string,mixed>> */
    private function tblpricingUpdates(): array
    {
        return array_values(array_filter(
            Capsule::$calls,
            static fn ($call) => $call['table'] === 'tblpricing'
        ));
    }
}

// Re-use the mock from ApiClientTest so we don't need HTTP in these tests.
if (!class_exists(MockRequestExecutor::class, false)) {
    require_once __DIR__ . '/ApiClientTest.php';
}

// Re-use the CatalogAuditLogSpy from SyncEngine6CycleTest.
if (!class_exists(CatalogAuditLogSpy::class, false)) {
    require_once __DIR__ . '/SyncEngine6CycleTest.php';
}
