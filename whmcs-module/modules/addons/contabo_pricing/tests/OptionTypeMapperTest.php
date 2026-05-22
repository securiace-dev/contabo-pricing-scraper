<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\OptionTypeMapper;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.1 — OptionTypeMapper.
 *
 * Verifies the canonical normalised-dimension → WHMCS optiontype mapping
 * (1=dropdown, 2=radio, 3=yes/no, 4=quantity — empirically confirmed in the
 * A.6 preflight; WHMCS has no type 0 and no text type), including the IPv4
 * quantity and Private Networking yes/no special cases.
 *
 * Assertions use the named constants so the dimension→type table can't drift,
 * and {@see testWhmcsLiteralValues} pins those constants to the WHMCS integer
 * literals so the off-by-one regression that mis-mapped IPv4 to a yes/no
 * (which would undercharge) can never recur.
 */
final class OptionTypeMapperTest extends TestCase
{
    public function testWhmcsLiteralValues(): void
    {
        // These are WHMCS' actual tblproductconfigoptions.optiontype values.
        $this->assertSame(1, OptionTypeMapper::TYPE_DROPDOWN);
        $this->assertSame(2, OptionTypeMapper::TYPE_RADIO);
        $this->assertSame(3, OptionTypeMapper::TYPE_YESNO);
        $this->assertSame(4, OptionTypeMapper::TYPE_QUANTITY);
    }

    public function testImageIsDropdown(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, OptionTypeMapper::mapFor('Image'));
    }

    public function testRegionIsRadio(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_RADIO, OptionTypeMapper::mapFor('Region'));
    }

    public function testStorageTypeIsRadio(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_RADIO, OptionTypeMapper::mapFor('Storage Type'));
    }

    public function testDataProtectionIsYesNo(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_YESNO, OptionTypeMapper::mapFor('Data Protection'));
    }

    public function testNetworkingBandwidthIsDropdown(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, OptionTypeMapper::mapFor('Networking:Bandwidth'));
    }

    public function testNetworkingIpv4IsQuantity(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_QUANTITY, OptionTypeMapper::mapFor('Networking:IPv4'));
    }

    public function testNetworkingPrivateIsYesNo(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_YESNO, OptionTypeMapper::mapFor('Networking:Private Networking'));
    }

    public function testUnknownDimensionFallsBackToDropdown(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, OptionTypeMapper::mapFor('SomethingNew'));
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
        $this->assertSame(OptionTypeMapper::TYPE_YESNO, OptionTypeMapper::mapForWithValueCount('Data Protection', 2));
        // A hypothetical 3-tier backup offering can't be a yes/no.
        $this->assertSame(OptionTypeMapper::TYPE_RADIO, OptionTypeMapper::mapForWithValueCount('Data Protection', 3));
    }

    public function testRegionUpgradesToDropdownWhenManyValues(): void
    {
        // 9 verified regions → still a radio.
        $this->assertSame(OptionTypeMapper::TYPE_RADIO, OptionTypeMapper::mapForWithValueCount('Region', 9));
        // A future region explosion (> threshold) → dropdown.
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, OptionTypeMapper::mapForWithValueCount('Region', 20));
    }

    public function testValueCountVariantLeavesQuantityAndDropdownUntouched(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_QUANTITY, OptionTypeMapper::mapForWithValueCount('Networking:IPv4', 2));
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, OptionTypeMapper::mapForWithValueCount('Image', 34));
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, OptionTypeMapper::mapForWithValueCount('Networking:Bandwidth', 3));
    }

    public function testPrivateNetworkingDegradesToRadioWhenNotTwoValues(): void
    {
        $this->assertSame(OptionTypeMapper::TYPE_YESNO, OptionTypeMapper::mapForWithValueCount('Networking:Private Networking', 2));
        $this->assertSame(OptionTypeMapper::TYPE_RADIO, OptionTypeMapper::mapForWithValueCount('Networking:Private Networking', 4));
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
