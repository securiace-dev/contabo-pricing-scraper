<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\MappingRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5.1 regression — the create (INSERT) path must only write schema-v3
 * columns. This is a direct guard against the observed
 * SQLSTATE[42S22] Unknown column 'apply_to_monthly' crash.
 */
final class MappingCreateTest extends TestCase
{
    /** The schema-v3 writable column set, mirrored from MappingRepository. */
    private const WRITABLE = [
        'profile_id', 'product_id', 'product_group_id',
        'catalog_cycles_mask', 'renewal_cycles_mask',
        'markup_overrides_json', 'setup_fee_overrides_json',
        'respect_disabled_cycles', 'overwrite_free_cycles', 'sync_setup_fees',
        'rounding_mode', 'active',
    ];

    /** Columns the repository may append automatically. */
    private const ALLOWED_EXTRA = ['created_at', 'updated_at', 'id'];

    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testCreateMappingDoesNotUseLegacyColumns(): void
    {
        $repo = new MappingRepository();

        $repo->createOrUpdate([
            'profile_id'            => 11,
            'product_id'            => 501,
            'catalog_cycles_mask'   => 9,
            'renewal_cycles_mask'   => 1,
            'rounding_mode'         => 'nearest_99',
            'active'                => true,
            // Hostile extra inputs that must never reach SQL.
            'apply_to_monthly'      => true,
            'apply_to_semiannually' => true,
            'apply_to_annually'     => true,
            'totally_unknown_field' => 'x',
        ]);

        $this->assertNotEmpty(Capsule::$inserts, 'Expected an INSERT for a new mapping.');

        $allowed = array_merge(self::WRITABLE, self::ALLOWED_EXTRA);
        foreach (Capsule::$inserts as $ins) {
            if ($ins['table'] !== 'mod_contabo_mapping') {
                continue;
            }
            $writtenCols = array_keys($ins['values']);
            // Every written column is a subset of WRITABLE (+ timestamps/id).
            $extra = array_diff($writtenCols, $allowed);
            $this->assertSame([], $extra, 'Unexpected columns written: ' . implode(',', $extra));
            // Belt-and-braces: no legacy keys at all.
            $this->assertArrayNotHasKey('apply_to_monthly', $ins['values']);
            $this->assertArrayNotHasKey('apply_to_semiannually', $ins['values']);
            $this->assertArrayNotHasKey('apply_to_annually', $ins['values']);
        }
    }
}
