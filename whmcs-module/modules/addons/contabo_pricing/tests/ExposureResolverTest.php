<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\DimensionParser;
use ContaboPricing\ExposureResolver;
use ContaboPricing\RetailVpsMinimalPreset;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.1 — ExposureResolver.
 *
 * The resolver bridges DimensionParser dimension keys (Image, Region,
 * Storage Type, Data Protection, Networking:Bandwidth, Networking:IPv4,
 * Networking:Private Networking) to RetailVpsMinimalPreset exposure decisions,
 * and answers exposure for image categories (OS/Panels/Apps/Blockchain).
 *
 * Conservative-by-default contract: an unknown dimension or category is never
 * exposed and always hidden.
 */
final class ExposureResolverTest extends TestCase
{
    public function testPresetName(): void
    {
        $this->assertSame('Retail VPS Minimal', ExposureResolver::presetName());
        $this->assertSame(RetailVpsMinimalPreset::name(), ExposureResolver::presetName());
    }

    /* ---- Per-dimension exposure (the key bridge) -------------------------- */

    public function testImageDimensionExposedAndVisible(): void
    {
        $d = ExposureResolver::decideForDimension(DimensionParser::DIM_IMAGE);
        $this->assertTrue($d['expose_to_customer']);
        $this->assertFalse($d['hidden']);
    }

    public function testRegionDimensionExposedAndVisible(): void
    {
        $d = ExposureResolver::decideForDimension(DimensionParser::DIM_REGION);
        $this->assertTrue($d['expose_to_customer']);
        $this->assertFalse($d['hidden']);
    }

    public function testDataProtectionDimensionExposedAndVisible(): void
    {
        // Data Protection (Backup) is exposed/visible under Retail Minimal.
        $d = ExposureResolver::decideForDimension(DimensionParser::DIM_DATA_PROTECTION);
        $this->assertTrue($d['expose_to_customer']);
        $this->assertFalse($d['hidden']);
    }

    public function testStorageTypeDimensionHidden(): void
    {
        $d = ExposureResolver::decideForDimension(DimensionParser::DIM_STORAGE_TYPE);
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testBandwidthDimensionHidden(): void
    {
        $d = ExposureResolver::decideForDimension('Networking:Bandwidth');
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testPrivateNetworkingDimensionHidden(): void
    {
        $d = ExposureResolver::decideForDimension('Networking:Private Networking');
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testIpv4DimensionMatchesPreset(): void
    {
        // IPv4 exposure follows the preset (v1: exposed because unit price known).
        $presetIpv4 = RetailVpsMinimalPreset::exposureFor()[RetailVpsMinimalPreset::DIM_IPV4];
        $d = ExposureResolver::decideForDimension('Networking:IPv4');
        $this->assertSame((bool) $presetIpv4['expose_to_customer'], $d['expose_to_customer']);
        $this->assertSame((bool) $presetIpv4['hidden'], $d['hidden']);
    }

    public function testEveryMappedDimensionMatchesPresetExposure(): void
    {
        // The bridge must faithfully reflect the preset for every known key.
        $cases = [
            DimensionParser::DIM_IMAGE           => RetailVpsMinimalPreset::DIM_IMAGE,
            DimensionParser::DIM_REGION          => RetailVpsMinimalPreset::DIM_REGION,
            DimensionParser::DIM_STORAGE_TYPE    => RetailVpsMinimalPreset::DIM_STORAGE,
            DimensionParser::DIM_DATA_PROTECTION => RetailVpsMinimalPreset::DIM_BACKUP,
            'Networking:IPv4'                    => RetailVpsMinimalPreset::DIM_IPV4,
            'Networking:Bandwidth'               => RetailVpsMinimalPreset::DIM_BANDWIDTH,
            'Networking:Private Networking'      => RetailVpsMinimalPreset::DIM_PRIVATE_NETWORKING,
        ];
        $exposure = RetailVpsMinimalPreset::exposureFor();
        foreach ($cases as $dimKey => $presetKey) {
            $d = ExposureResolver::decideForDimension($dimKey);
            $this->assertSame(
                (bool) $exposure[$presetKey]['expose_to_customer'],
                $d['expose_to_customer'],
                "expose mismatch for {$dimKey}"
            );
            $this->assertSame(
                (bool) $exposure[$presetKey]['hidden'],
                $d['hidden'],
                "hidden mismatch for {$dimKey}"
            );
        }
    }

    public function testDimensionResultShapeIsExactlyTwoBoolKeys(): void
    {
        $d = ExposureResolver::decideForDimension(DimensionParser::DIM_IMAGE);
        $this->assertSame(['expose_to_customer', 'hidden'], array_keys($d));
        $this->assertIsBool($d['expose_to_customer']);
        $this->assertIsBool($d['hidden']);
    }

    public function testUnknownDimensionIsConservativeHidden(): void
    {
        $d = ExposureResolver::decideForDimension('Totally Unknown Dimension');
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testRawNetworkingKeyWithoutSubConcernIsUnknownAndConservative(): void
    {
        // Bare 'Networking' is NOT a DimensionParser dimension key; treat it as
        // unknown rather than guessing a sub-concern.
        $d = ExposureResolver::decideForDimension('Networking');
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testPresetSnakeCaseKeyIsNotAcceptedAsDimensionKey(): void
    {
        // Passing the preset's own snake_case key must NOT resolve — the resolver
        // only speaks DimensionParser keys, so this is conservative.
        $d = ExposureResolver::decideForDimension('storage_type');
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testPlanSlugIsAcceptedAndYieldsSameShape(): void
    {
        $a = ExposureResolver::decideForDimension(DimensionParser::DIM_IMAGE, 'cloud-vps-10');
        $b = ExposureResolver::decideForDimension(DimensionParser::DIM_IMAGE);
        $this->assertSame(array_keys($a), array_keys($b));
        $this->assertSame($a, $b); // plan-agnostic in v1
    }

    /* ---- Image category exposure ----------------------------------------- */

    public function testImageCategoryOsVisible(): void
    {
        $d = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_OS);
        $this->assertTrue($d['expose_to_customer']);
        $this->assertFalse($d['hidden']);
    }

    public function testImageCategoryPanelsHidden(): void
    {
        $d = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_PANELS);
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testImageCategoryAppsHidden(): void
    {
        $d = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_APPS);
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testImageCategoryBlockchainHidden(): void
    {
        $d = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_BLOCKCHAIN);
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testImageCategoryResultShapeIsExactlyTwoBoolKeys(): void
    {
        $d = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_OS);
        $this->assertSame(['expose_to_customer', 'hidden'], array_keys($d));
        $this->assertIsBool($d['expose_to_customer']);
        $this->assertIsBool($d['hidden']);
    }

    public function testUnknownImageCategoryIsConservativeHidden(): void
    {
        $d = ExposureResolver::decideForImageCategory('Quantum');
        $this->assertFalse($d['expose_to_customer']);
        $this->assertTrue($d['hidden']);
    }

    public function testImageCategoryPlanSlugAcceptedAndYieldsSameShape(): void
    {
        $a = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_OS, 'cloud-vps-10');
        $b = ExposureResolver::decideForImageCategory(RetailVpsMinimalPreset::IMG_CAT_OS);
        $this->assertSame($a, $b);
    }
}
