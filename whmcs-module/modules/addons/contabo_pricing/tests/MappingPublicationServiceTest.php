<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\MappingPublicationService;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use WHMCS\Database\Capsule;

final class MappingPublicationServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id' => 7,
            'profile_id' => 11,
            'product_id' => 22,
            'product_group_id' => 33,
            'catalog_cycles_mask' => 63,
            'renewal_cycles_mask' => 31,
            'rounding_mode' => 'exact_2_decimals',
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles' => 0,
            'sync_setup_fees' => 1,
            'markup_overrides_json' => '{"annually":"12.50"}',
            'setup_fee_overrides_json' => '{}',
            'source_overrides_json' => '{}',
            'mapping_state' => 'draft',
            'published_mapping_version' => null,
        ]];
        Capsule::$tables['mod_contabo_catalog_versions'] = [[
            'id' => 44,
            'catalog_version' => 'catalog-2026-07-30',
            'state' => 'observed',
            'payload_hash' => str_repeat('a', 64),
            'source_observed_at' => '2026-07-30 10:00:00',
        ]];
        Capsule::$tables['mod_contabo_mapping_publications'] = [];
        Capsule::$tables['mod_contabo_publication_approvals'] = [];
        Capsule::$tables['mod_contabo_repricing_lock'] = [];
    }

    public function testPreviewIsIdempotentAndDoesNotAdvanceMapping(): void
    {
        $service = new MappingPublicationService();
        $first = $service->preview(7, $this->selection());
        $second = $service->preview(7, $this->selection());

        $this->assertSame($first['mapping_version'], $second['mapping_version']);
        $this->assertSame($first['preview_hash'], $second['preview_hash']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_mapping_publications']);
        $this->assertNull(Capsule::$tables['mod_contabo_mapping'][0]['published_mapping_version']);
        $this->assertSame('draft', Capsule::$tables['mod_contabo_mapping'][0]['mapping_state']);
    }

    public function testApprovalAtomicallyAdvancesPublishedPointer(): void
    {
        $service = new MappingPublicationService();
        $preview = $service->preview(7, $this->selection());

        $result = $service->approve(
            (string) $preview['mapping_version'],
            (string) $preview['preview_hash'],
            9,
            'PUBLISH MAPPING',
            'Reviewed provider identifiers.'
        );

        $this->assertSame('published', $result['state']);
        $mapping = Capsule::$tables['mod_contabo_mapping'][0];
        $this->assertSame($preview['mapping_version'], $mapping['published_mapping_version']);
        $this->assertSame('V45', $mapping['provider_sku_id']);
        $this->assertSame('catalog-2026-07-30', $mapping['rust_catalog_version']);
        $this->assertSame('published', $mapping['mapping_state']);
        $this->assertSame($preview['preview_hash'], $mapping['mapping_payload_hash']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_publication_approvals']);

        // Repeating the exact approved request is a no-op, not a second
        // approval or a new publication.
        $again = $service->approve(
            (string) $preview['mapping_version'],
            (string) $preview['preview_hash'],
            9,
            'PUBLISH MAPPING'
        );
        $this->assertSame('published', $again['state']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_publication_approvals']);
    }

    public function testChangedPreviewHashCannotBeApproved(): void
    {
        $service = new MappingPublicationService();
        $preview = $service->preview(7, $this->selection());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('preview changed');
        $service->approve(
            (string) $preview['mapping_version'],
            str_repeat('b', 64),
            9,
            'PUBLISH MAPPING'
        );
    }

    public function testTypedConfirmationIsRequired(): void
    {
        $service = new MappingPublicationService();
        $preview = $service->preview(7, $this->selection());

        $this->expectException(InvalidArgumentException::class);
        $service->approve(
            (string) $preview['mapping_version'],
            (string) $preview['preview_hash'],
            9,
            'yes'
        );
    }

    /**
     * @return array<string,string>
     */
    private function selection(): array
    {
        return [
            'rust_catalog_version' => 'catalog-2026-07-30',
            'provider_sku_id' => 'V45',
            'region_id' => 'EU',
            'image_id' => 'image-ubuntu-2404',
            'management_code' => 'self_managed',
        ];
    }
}
