<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionCompatibilityRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Compatibility-matrix chokepoint (design §5). Exercises upsert idempotency,
 * the *_json LONGTEXT round-trip, and validateCombination over the documented
 * outcomes. Runs against FakeCapsule (no DB).
 */
final class ConfigOptionCompatibilityRepositoryTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testUpsertInsertsThenIdempotentlyUpdates(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();

        $a = $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'incompatible_with'  => ['region:us-east'],
            'min_value'          => 1,
            'max_value'          => 1,
            'source_snapshot_id' => 42,
        ]);
        $this->assertSame('cloud-vps-10', (string) $a['plan_slug']);
        $this->assertSame('Image', (string) $a['dimension_key']);
        $this->assertSame('os:windows', (string) $a['value_key']);
        $this->assertArrayHasKey('id', $a);
        $this->assertNotNull($a['last_verified_at']);
        $firstId = (int) $a['id'];

        // Same (plan, dimension, value) → update in place, not a new row.
        $b = $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'max_value' => 2,
        ]);
        $this->assertSame($firstId, (int) $b['id'], 'idempotent: same row reused');
        $this->assertSame(2, (int) $b['max_value']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_option_compatibility']);
    }

    public function testDifferentValueKeyIsADistinctRow(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', []);
        $repo->upsertRule('cloud-vps-10', 'Image', 'os:ubuntu', []);
        $this->assertCount(2, Capsule::$tables['mod_contabo_option_compatibility']);
    }

    public function testJsonColumnsRoundTrip(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();

        $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'compatible_with'   => ['region:eu-central', 'region:eu-west'],
            'incompatible_with' => ['region:us-east'],
            'required_values'   => ['license:windows'],
        ]);

        $row = $repo->find('cloud-vps-10', 'Image', 'os:windows');
        $this->assertNotNull($row);

        // Stored as JSON-array strings in the LONGTEXT columns.
        $this->assertSame(['region:eu-central', 'region:eu-west'], json_decode((string) $row['compatible_with_json'], true));
        $this->assertSame(['region:us-east'], json_decode((string) $row['incompatible_with_json'], true));
        $this->assertSame(['license:windows'], json_decode((string) $row['required_values_json'], true));
    }

    public function testFindReturnsNullWhenAbsent(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $this->assertNull($repo->find('cloud-vps-10', 'Image', 'nope'));
    }

    public function testValidateAllCompatibleIsValid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Image', 'os:ubuntu', [
            'compatible_with' => ['region:eu-central'],
        ]);
        $repo->upsertRule('cloud-vps-10', 'Region', 'region:eu-central', []);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:ubuntu'],
            ['dimension_key' => 'Region', 'value_key' => 'region:eu-central'],
        ]);

        $this->assertTrue($result['valid']);
        $this->assertSame([], $result['violations']);
    }

    public function testValidateNoRulesIsValid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:ubuntu'],
            ['dimension_key' => 'Region', 'value_key' => 'region:eu-central'],
        ]);
        $this->assertTrue($result['valid']);
        $this->assertSame([], $result['violations']);
    }

    public function testValidateIncompatiblePairIsInvalid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'incompatible_with' => ['region:us-east'],
        ]);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:windows'],
            ['dimension_key' => 'Region', 'value_key' => 'region:us-east'],
        ]);

        $this->assertFalse($result['valid']);
        $this->assertCount(1, $result['violations']);
        $v = $result['violations'][0];
        $this->assertSame('incompatible', $v['reason']);
        $this->assertSame('Image', $v['dimension_key']);
        $this->assertSame('os:windows', $v['value_key']);
        $this->assertStringContainsString('region:us-east', $v['detail']);
    }

    public function testValidateMissingRequiredValueIsInvalid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'required_values' => ['license:windows'],
        ]);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:windows'],
            // license:windows deliberately absent
        ]);

        $this->assertFalse($result['valid']);
        $this->assertCount(1, $result['violations']);
        $this->assertSame('missing_required', $result['violations'][0]['reason']);
        $this->assertStringContainsString('license:windows', $result['violations'][0]['detail']);
    }

    public function testValidateRequiredValuePresentIsValid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'required_values' => ['license:windows'],
        ]);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:windows'],
            ['dimension_key' => 'License', 'value_key' => 'license:windows'],
        ]);

        $this->assertTrue($result['valid']);
    }

    public function testValidateQtyAboveMaxIsInvalid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Networking:IPv4', 'ipv4:extra', [
            'min_value' => 0,
            'max_value' => 2,
        ]);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Networking:IPv4', 'value_key' => 'ipv4:extra', 'qty' => 5],
        ]);

        $this->assertFalse($result['valid']);
        $this->assertCount(1, $result['violations']);
        $this->assertSame('qty_out_of_range', $result['violations'][0]['reason']);
        $this->assertStringContainsString('maximum 2', $result['violations'][0]['detail']);
    }

    public function testValidateQtyBelowMinIsInvalid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Networking:IPv4', 'ipv4:extra', [
            'min_value' => 2,
            'max_value' => 4,
        ]);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Networking:IPv4', 'value_key' => 'ipv4:extra', 'qty' => 1],
        ]);

        $this->assertFalse($result['valid']);
        $this->assertSame('qty_out_of_range', $result['violations'][0]['reason']);
        $this->assertStringContainsString('minimum 2', $result['violations'][0]['detail']);
    }

    public function testValidateQtyWithinRangeIsValid(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Networking:IPv4', 'ipv4:extra', [
            'min_value' => 1,
            'max_value' => 4,
        ]);

        $result = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Networking:IPv4', 'value_key' => 'ipv4:extra', 'qty' => 3],
        ]);

        $this->assertTrue($result['valid']);
    }
}
