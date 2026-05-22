<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\OptionTypeMapper;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.1 — OptionTypeMapper.
 *
 * Verifies the canonical normalised-dimension → WHMCS optiontype mapping
 * (0=dropdown, 1=radio, 2=yes/no, 3=qty, 4=text) from
 * PHASE_A52_DESIGN_IMPACT.md, including the IPv4 quantity and Private
 * Networking yes/no special cases.
 */
final class OptionTypeMapperTest extends TestCase
{
    public function testImageIsDropdown(): void
    {
        $this->assertSame(0, OptionTypeMapper::mapFor('Image'));
    }

    public function testRegionIsRadio(): void
    {
        $this->assertSame(1, OptionTypeMapper::mapFor('Region'));
    }

    public function testStorageTypeIsRadio(): void
    {
        $this->assertSame(1, OptionTypeMapper::mapFor('Storage Type'));
    }

    public function testDataProtectionIsYesNo(): void
    {
        $this->assertSame(2, OptionTypeMapper::mapFor('Data Protection'));
    }

    public function testNetworkingBandwidthIsDropdown(): void
    {
        $this->assertSame(0, OptionTypeMapper::mapFor('Networking:Bandwidth'));
    }

    public function testNetworkingIpv4IsQuantity(): void
    {
        $this->assertSame(3, OptionTypeMapper::mapFor('Networking:IPv4'));
    }

    public function testNetworkingPrivateIsYesNo(): void
    {
        $this->assertSame(2, OptionTypeMapper::mapFor('Networking:Private Networking'));
    }

    public function testUnknownDimensionFallsBackToDropdown(): void
    {
        $this->assertSame(0, OptionTypeMapper::mapFor('SomethingNew'));
    }

    public function testIsQuantityTrueOnlyForIpv4(): void
    {
        $this->assertTrue(OptionTypeMapper::isQuantity('Networking:IPv4'));
        $this->assertFalse(OptionTypeMapper::isQuantity('Networking:Private Networking'));
        $this->assertFalse(OptionTypeMapper::isQuantity('Image'));
        $this->assertFalse(OptionTypeMapper::isQuantity('Region'));
    }

    public function testIsYesNoTrueForPrivateNetworkingAndDataProtection(): void
    {
        $this->assertTrue(OptionTypeMapper::isYesNo('Networking:Private Networking'));
        $this->assertTrue(OptionTypeMapper::isYesNo('Data Protection'));
        $this->assertFalse(OptionTypeMapper::isYesNo('Networking:IPv4'));
        $this->assertFalse(OptionTypeMapper::isYesNo('Region'));
    }

    public function testDataProtectionDegradesToRadioWhenNotExactlyTwoValues(): void
    {
        // Verified data has exactly None + Auto Backup → yes/no.
        $this->assertSame(2, OptionTypeMapper::mapForWithValueCount('Data Protection', 2));
        // A hypothetical 3-tier backup offering can't be a yes/no.
        $this->assertSame(1, OptionTypeMapper::mapForWithValueCount('Data Protection', 3));
    }

    public function testRegionUpgradesToDropdownWhenManyValues(): void
    {
        // 9 verified regions → still a radio.
        $this->assertSame(1, OptionTypeMapper::mapForWithValueCount('Region', 9));
        // A future region explosion (> threshold) → dropdown.
        $this->assertSame(0, OptionTypeMapper::mapForWithValueCount('Region', 20));
    }

    public function testValueCountVariantLeavesQuantityAndDropdownUntouched(): void
    {
        $this->assertSame(3, OptionTypeMapper::mapForWithValueCount('Networking:IPv4', 2));
        $this->assertSame(0, OptionTypeMapper::mapForWithValueCount('Image', 34));
        $this->assertSame(0, OptionTypeMapper::mapForWithValueCount('Networking:Bandwidth', 3));
    }

    public function testPrivateNetworkingDegradesToRadioWhenNotTwoValues(): void
    {
        $this->assertSame(2, OptionTypeMapper::mapForWithValueCount('Networking:Private Networking', 2));
        $this->assertSame(1, OptionTypeMapper::mapForWithValueCount('Networking:Private Networking', 4));
    }

    public function testTableCoversAllSevenNormalisedDimensions(): void
    {
        $table = OptionTypeMapper::table();
        $this->assertCount(7, $table);
        $this->assertSame(
            ['Image', 'Region', 'Storage Type', 'Data Protection', 'Networking:Bandwidth', 'Networking:IPv4', 'Networking:Private Networking'],
            array_keys($table)
        );
    }
}
