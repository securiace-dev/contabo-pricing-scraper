<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\MappingRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5.1 regression — the update (UPDATE) path must only write schema-v3
 * columns. Updating a mapping must never reference the dropped apply_to_*
 * columns or it would raise SQLSTATE[42S22] on the migrated schema.
 */
final class MappingUpdateTest extends TestCase
{
    private const WRITABLE = [
        'profile_id', 'product_id', 'product_group_id',
        'catalog_cycles_mask', 'renewal_cycles_mask',
        'markup_overrides_json', 'setup_fee_overrides_json',
        'respect_disabled_cycles', 'overwrite_free_cycles', 'sync_setup_fees',
        'rounding_mode', 'active',
    ];

    private const ALLOWED_EXTRA = ['created_at', 'updated_at', 'id'];

    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testUpdateMappingDoesNotUseLegacyColumns(): void
    {
        $repo = new MappingRepository();

        // Seed an existing row (insert path).
        $repo->createOrUpdate([
            'profile_id'          => 22,
            'product_id'          => 777,
            'catalog_cycles_mask' => 1,
            'renewal_cycles_mask' => 1,
        ]);

        // Discard insert noise; we only assert on the UPDATE that follows.
        Capsule::$calls = [];

        // Same (profile_id, product_id) → triggers the UPDATE branch.
        $repo->createOrUpdate([
            'profile_id'            => 22,
            'product_id'            => 777,
            'catalog_cycles_mask'   => 63,
            'renewal_cycles_mask'   => 9,
            'active'                => false,
            // Hostile extras.
            'apply_to_monthly'      => true,
            'apply_to_semiannually' => true,
            'apply_to_annually'     => true,
            'totally_unknown_field' => 'x',
        ]);

        $this->assertNotEmpty(Capsule::$calls, 'Expected an UPDATE on the existing mapping.');

        $allowed = array_merge(self::WRITABLE, self::ALLOWED_EXTRA);
        foreach (Capsule::$calls as $call) {
            if ($call['table'] !== 'mod_contabo_mapping') {
                continue;
            }
            $writtenCols = array_keys($call['update']);
            $extra = array_diff($writtenCols, $allowed);
            $this->assertSame([], $extra, 'Unexpected columns updated: ' . implode(',', $extra));
            $this->assertArrayNotHasKey('apply_to_monthly', $call['update']);
            $this->assertArrayNotHasKey('apply_to_semiannually', $call['update']);
            $this->assertArrayNotHasKey('apply_to_annually', $call['update']);
        }

        $row = $repo->findByProfileAndProduct(22, 777);
        $this->assertSame(63, (int) $row['catalog_cycles_mask']);
        $this->assertSame(9, (int) $row['renewal_cycles_mask']);
        $this->assertSame(0, (int) $row['active']);
    }
}
