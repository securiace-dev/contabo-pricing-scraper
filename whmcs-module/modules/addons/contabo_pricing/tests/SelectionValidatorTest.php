<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionCapabilityRepository;
use ContaboPricing\ConfigOptionCompatibilityRepository;
use ContaboPricing\SelectionValidator;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * SelectionValidator — the single facade composing the §5 compatibility gate
 * (hard violations) with the §4 capability matrix (destructive-change warnings).
 *
 * Seeds the REAL repositories against FakeCapsule (no DB), exactly as the
 * sibling compatibility / capability repo tests do, so the composition is
 * exercised end-to-end rather than against hand-rolled stubs.
 */
final class SelectionValidatorTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /**
     * (a) All-compatible, no capability rows → valid, empty violations + warnings.
     */
    public function testAllCompatibleNoCapabilityIsValidWithNoWarnings(): void
    {
        $compat = new ConfigOptionCompatibilityRepository();
        $compat->upsertRule('cloud-vps-10', 'Image', 'os:ubuntu', [
            'compatible_with' => ['region:eu-central'],
        ]);
        $compat->upsertRule('cloud-vps-10', 'Region', 'region:eu-central', []);

        $validator = new SelectionValidator($compat, new ConfigOptionCapabilityRepository());

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:ubuntu'],
            ['dimension_key' => 'Region', 'value_key' => 'region:eu-central'],
        ]);

        $this->assertTrue($result['valid']);
        $this->assertSame([], $result['violations']);
        $this->assertSame([], $result['capability_warnings']);
    }

    /**
     * A selection with no rules AND no capability rows at all is still valid.
     */
    public function testNoRulesNoCapabilitiesIsValid(): void
    {
        $validator = new SelectionValidator();

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:ubuntu'],
            ['dimension_key' => 'Region', 'value_key' => 'region:eu-central'],
        ]);

        $this->assertTrue($result['valid']);
        $this->assertSame([], $result['violations']);
        $this->assertSame([], $result['capability_warnings']);
    }

    /**
     * (b) An incompatible pair → valid=false and the compat violation surfaced
     * verbatim. No capability rows → no warnings.
     */
    public function testIncompatiblePairIsInvalidAndSurfacesViolation(): void
    {
        $compat = new ConfigOptionCompatibilityRepository();
        $compat->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'incompatible_with' => ['region:us-east'],
        ]);

        $validator = new SelectionValidator($compat, new ConfigOptionCapabilityRepository());

        $result = $validator->validate('cloud-vps-10', [
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
        $this->assertSame([], $result['capability_warnings']);
    }

    /**
     * (c) A destructive selection → valid=true (warnings don't block) and one
     * capability_warning carrying the capability_source and the flags.
     */
    public function testDestructiveSelectionWarnsButDoesNotBlock(): void
    {
        $cap = new ConfigOptionCapabilityRepository();
        $cap->upsertCapability('cloud-vps-10', 'Storage', 'ssd:400', [
            'destructive_change'      => true,
            'data_loss_expected'      => true,
            'requires_backup_warning' => true,
            'requires_admin_approval' => true,
            'capability_source'       => ConfigOptionCapabilityRepository::SOURCE_SCRAPE_VERIFIED,
        ]);

        // No compatibility rule for the value → compat says valid.
        $validator = new SelectionValidator(new ConfigOptionCompatibilityRepository(), $cap);

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Storage', 'value_key' => 'ssd:400'],
        ]);

        // Warnings never block.
        $this->assertTrue($result['valid']);
        $this->assertSame([], $result['violations']);

        $this->assertCount(1, $result['capability_warnings']);
        $w = $result['capability_warnings'][0];
        $this->assertSame('Storage', $w['dimension_key']);
        $this->assertSame('ssd:400', $w['value_key']);
        $this->assertSame('destructive', $w['kind']);
        $this->assertTrue($w['requires_backup_warning']);
        $this->assertTrue($w['requires_admin_approval']);
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_SCRAPE_VERIFIED,
            $w['capability_source']
        );
    }

    /**
     * A non-destructive capability row produces no warning.
     */
    public function testNonDestructiveCapabilityProducesNoWarning(): void
    {
        $cap = new ConfigOptionCapabilityRepository();
        $cap->upsertCapability('cloud-vps-10', 'Storage', 'ssd:400', [
            'destructive_change' => false,
            'allowed_on_upgrade' => true,
        ]);

        $validator = new SelectionValidator(new ConfigOptionCompatibilityRepository(), $cap);

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Storage', 'value_key' => 'ssd:400'],
        ]);

        $this->assertTrue($result['valid']);
        $this->assertSame([], $result['capability_warnings']);
    }

    /**
     * (d) A qty-out-of-range compatibility violation is surfaced and blocks.
     */
    public function testQtyOutOfRangeViolationIsSurfaced(): void
    {
        $compat = new ConfigOptionCompatibilityRepository();
        $compat->upsertRule('cloud-vps-10', 'Networking:IPv4', 'ipv4:extra', [
            'min_value' => 0,
            'max_value' => 2,
        ]);

        $validator = new SelectionValidator($compat, new ConfigOptionCapabilityRepository());

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Networking:IPv4', 'value_key' => 'ipv4:extra', 'qty' => 5],
        ]);

        $this->assertFalse($result['valid']);
        $this->assertCount(1, $result['violations']);
        $this->assertSame('qty_out_of_range', $result['violations'][0]['reason']);
        $this->assertStringContainsString('maximum 2', $result['violations'][0]['detail']);
        $this->assertSame([], $result['capability_warnings']);
    }

    /**
     * A hard violation and a destructive warning can co-exist: the violation
     * blocks (valid=false) while the destructive warning is still reported.
     */
    public function testViolationAndWarningCoexist(): void
    {
        $compat = new ConfigOptionCompatibilityRepository();
        $compat->upsertRule('cloud-vps-10', 'Image', 'os:windows', [
            'incompatible_with' => ['region:us-east'],
        ]);

        $cap = new ConfigOptionCapabilityRepository();
        $cap->upsertCapability('cloud-vps-10', 'Image', 'os:windows', [
            'destructive_change' => true,
            'capability_source'  => ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION,
        ]);

        $validator = new SelectionValidator($compat, $cap);

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:windows'],
            ['dimension_key' => 'Region', 'value_key' => 'region:us-east'],
        ]);

        $this->assertFalse($result['valid']);
        $this->assertCount(1, $result['violations']);
        $this->assertSame('incompatible', $result['violations'][0]['reason']);

        $this->assertCount(1, $result['capability_warnings']);
        $this->assertSame('os:windows', $result['capability_warnings'][0]['value_key']);
        $this->assertSame('destructive', $result['capability_warnings'][0]['kind']);
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION,
            $result['capability_warnings'][0]['capability_source']
        );
    }

    /**
     * Constructor defaults to real repo instances (no injection) and still works
     * end-to-end against seeded FakeCapsule tables.
     */
    public function testDefaultConstructorUsesRealRepos(): void
    {
        (new ConfigOptionCompatibilityRepository())->upsertRule(
            'cloud-vps-10',
            'Image',
            'os:windows',
            ['required_values' => ['license:windows']]
        );
        (new ConfigOptionCapabilityRepository())->upsertCapability(
            'cloud-vps-10',
            'Image',
            'os:windows',
            [
                'destructive_change' => true,
                'capability_source'  => ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED,
            ]
        );

        $validator = new SelectionValidator();

        $result = $validator->validate('cloud-vps-10', [
            ['dimension_key' => 'Image', 'value_key' => 'os:windows'],
            // license:windows deliberately absent → missing_required violation
        ]);

        $this->assertFalse($result['valid']);
        $this->assertSame('missing_required', $result['violations'][0]['reason']);
        $this->assertCount(1, $result['capability_warnings']);
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED,
            $result['capability_warnings'][0]['capability_source']
        );
    }
}
