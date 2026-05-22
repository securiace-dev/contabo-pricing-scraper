<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CycleSet;
use ContaboPricing\MappingRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5.1 — regression coverage for the schema-v3 mapping write path.
 *
 * MappingRepository is the single guarded sink into mod_contabo_mapping. These
 * tests prove (a) it persists the catalog/renewal masks, and (b) a forbidden
 * legacy apply_to_* key can never reach the persisted row.
 */
final class MappingRepositoryTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testInsertUsesCatalogAndRenewalMasks(): void
    {
        $repo = new MappingRepository();

        $catalog = CycleSet::fromCycles(['Monthly', 'Annually'])->toMask(); // 1 | 8 = 9
        $renewal = CycleSet::fromCycles(['Monthly'])->toMask();             // 1

        $id = $repo->createOrUpdate([
            'profile_id'          => 7,
            'product_id'          => 501,
            'catalog_cycles_mask' => $catalog,
            'renewal_cycles_mask' => $renewal,
            'active'              => true,
        ]);

        $this->assertGreaterThan(0, $id);

        $row = $repo->findByProfileAndProduct(7, 501);
        $this->assertNotNull($row);
        $this->assertSame($catalog, (int) $row['catalog_cycles_mask']);
        $this->assertSame($renewal, (int) $row['renewal_cycles_mask']);
        $this->assertSame(9, (int) $row['catalog_cycles_mask']);
        $this->assertSame(1, (int) $row['renewal_cycles_mask']);
    }

    public function testLegacyFieldsRejectedFromRuntimePayload(): void
    {
        $repo = new MappingRepository();

        $repo->createOrUpdate([
            'profile_id'            => 3,
            'product_id'            => 99,
            'catalog_cycles_mask'   => 5,
            'renewal_cycles_mask'   => 5,
            // Forbidden legacy keys — must be dropped before SQL.
            'apply_to_monthly'      => true,
            'apply_to_semiannually' => true,
            'apply_to_annually'     => true,
        ]);

        $row = $repo->findByProfileAndProduct(3, 99);
        $this->assertNotNull($row);
        $this->assertArrayNotHasKey('apply_to_monthly', $row);
        $this->assertArrayNotHasKey('apply_to_semiannually', $row);
        $this->assertArrayNotHasKey('apply_to_annually', $row);

        // And the every-insert/update record never carried the legacy keys.
        foreach (Capsule::$inserts as $ins) {
            $this->assertArrayNotHasKey('apply_to_monthly', $ins['values']);
            $this->assertArrayNotHasKey('apply_to_semiannually', $ins['values']);
            $this->assertArrayNotHasKey('apply_to_annually', $ins['values']);
        }
        foreach (Capsule::$calls as $call) {
            $this->assertArrayNotHasKey('apply_to_monthly', $call['update']);
            $this->assertArrayNotHasKey('apply_to_semiannually', $call['update']);
            $this->assertArrayNotHasKey('apply_to_annually', $call['update']);
        }
    }

    public function testMasksAreClampedAndBoolsCoerced(): void
    {
        $repo = new MappingRepository();

        $repo->createOrUpdate([
            'profile_id'              => 1,
            'product_id'              => 2,
            'catalog_cycles_mask'     => 999,   // > MASK_MAX → clamp to 63
            'renewal_cycles_mask'     => -5,    // < 0 → clamp to 0
            'respect_disabled_cycles' => 'yes', // truthy → 1
            'overwrite_free_cycles'   => 0,     // falsy  → 0
            'active'                  => true,
        ]);

        $row = $repo->findByProfileAndProduct(1, 2);
        $this->assertNotNull($row);
        $this->assertSame(CycleSet::MASK_MAX, (int) $row['catalog_cycles_mask']);
        $this->assertSame(0, (int) $row['renewal_cycles_mask']);
        $this->assertSame(1, (int) $row['respect_disabled_cycles']);
        $this->assertSame(0, (int) $row['overwrite_free_cycles']);
    }

    public function testJsonArrayIsEncodedToString(): void
    {
        $repo = new MappingRepository();

        $repo->createOrUpdate([
            'profile_id'            => 4,
            'product_id'            => 5,
            'markup_overrides_json' => ['Monthly' => 12.5],
            'setup_fee_overrides_json' => 'not-json',
        ]);

        $row = $repo->findByProfileAndProduct(4, 5);
        $this->assertNotNull($row);
        $this->assertIsString($row['markup_overrides_json']);
        $this->assertSame(['Monthly' => 12.5], json_decode($row['markup_overrides_json'], true));
        // Invalid JSON string collapses to '{}', never stored raw.
        $this->assertSame('{}', $row['setup_fee_overrides_json']);
    }

    public function testCreateOrUpdateUpdatesExistingRow(): void
    {
        $repo = new MappingRepository();

        $firstId = $repo->createOrUpdate([
            'profile_id'          => 8,
            'product_id'          => 8,
            'catalog_cycles_mask' => 1,
            'renewal_cycles_mask' => 1,
        ]);

        $secondId = $repo->createOrUpdate([
            'profile_id'          => 8,
            'product_id'          => 8,
            'catalog_cycles_mask' => 63,
            'renewal_cycles_mask' => 63,
        ]);

        $this->assertSame($firstId, $secondId);
        $this->assertSame(1, Capsule::table('mod_contabo_mapping')
            ->where('profile_id', 8)->where('product_id', 8)->count());

        $row = $repo->findByProfileAndProduct(8, 8);
        $this->assertSame(63, (int) $row['catalog_cycles_mask']);
    }

    public function testFindReturnsNullWhenAbsent(): void
    {
        $repo = new MappingRepository();
        $this->assertNull($repo->findByProfileAndProduct(123, 456));
    }
}
