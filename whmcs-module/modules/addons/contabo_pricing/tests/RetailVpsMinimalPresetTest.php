<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\RetailVpsMinimalPreset;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.1 — RetailVpsMinimalPreset (amendment 8, default exposure).
 *
 * Verifies the per-dimension exposure flags and the per-category Image
 * visibility map:
 *   Image=OS only (Panels/Apps/Blockchain hidden); Region exposed; Storage
 *   hidden; Backup exposed (optional); IPv4 qty exposed; Bandwidth + Private
 *   Networking hidden.
 */
final class RetailVpsMinimalPresetTest extends TestCase
{
    public function testNameIsRetailVpsMinimal(): void
    {
        $this->assertSame('Retail VPS Minimal', RetailVpsMinimalPreset::name());
    }

    public function testExposureCoversEverySevenDimension(): void
    {
        $map = RetailVpsMinimalPreset::exposureFor();
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_IMAGE, $map);
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_REGION, $map);
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_STORAGE, $map);
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_BACKUP, $map);
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_IPV4, $map);
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_BANDWIDTH, $map);
        $this->assertArrayHasKey(RetailVpsMinimalPreset::DIM_PRIVATE_NETWORKING, $map);
        $this->assertCount(7, $map);
    }

    public function testEveryEntryHasContractKeys(): void
    {
        foreach (RetailVpsMinimalPreset::exposureFor() as $key => $entry) {
            $this->assertArrayHasKey('expose_to_customer', $entry, $key);
            $this->assertArrayHasKey('hidden', $entry, $key);
            $this->assertArrayHasKey('default_value', $entry, $key);
            $this->assertArrayHasKey('note', $entry, $key);
            $this->assertIsBool($entry['expose_to_customer']);
            $this->assertIsBool($entry['hidden']);
            $this->assertNotSame('', $entry['note']);
        }
    }

    public function testImageExposedWithOsOnlyCategoryVisibility(): void
    {
        $image = RetailVpsMinimalPreset::exposureFor()[RetailVpsMinimalPreset::DIM_IMAGE];

        $this->assertTrue($image['expose_to_customer']);
        $this->assertFalse($image['hidden']);
        $this->assertArrayHasKey('image_category_visibility', $image);

        $vis = $image['image_category_visibility'];
        // OS visible.
        $this->assertTrue($vis[RetailVpsMinimalPreset::IMG_CAT_OS]['expose_to_customer']);
        $this->assertFalse($vis[RetailVpsMinimalPreset::IMG_CAT_OS]['hidden']);
        // Panels / Apps / Blockchain hidden.
        $this->assertFalse($vis[RetailVpsMinimalPreset::IMG_CAT_PANELS]['expose_to_customer']);
        $this->assertTrue($vis[RetailVpsMinimalPreset::IMG_CAT_PANELS]['hidden']);
        $this->assertFalse($vis[RetailVpsMinimalPreset::IMG_CAT_APPS]['expose_to_customer']);
        $this->assertTrue($vis[RetailVpsMinimalPreset::IMG_CAT_APPS]['hidden']);
        $this->assertFalse($vis[RetailVpsMinimalPreset::IMG_CAT_BLOCKCHAIN]['expose_to_customer']);
        $this->assertTrue($vis[RetailVpsMinimalPreset::IMG_CAT_BLOCKCHAIN]['hidden']);
    }

    public function testImageCategoryVisibilityHelper(): void
    {
        $vis = RetailVpsMinimalPreset::imageCategoryVisibility();
        $this->assertTrue($vis[RetailVpsMinimalPreset::IMG_CAT_OS]['expose_to_customer']);
        $this->assertTrue($vis[RetailVpsMinimalPreset::IMG_CAT_PANELS]['hidden']);
    }

    public function testRegionExposed(): void
    {
        $region = RetailVpsMinimalPreset::exposureFor()[RetailVpsMinimalPreset::DIM_REGION];
        $this->assertTrue($region['expose_to_customer']);
        $this->assertFalse($region['hidden']);
    }

    public function testStorageHidden(): void
    {
        $storage = RetailVpsMinimalPreset::exposureFor()[RetailVpsMinimalPreset::DIM_STORAGE];
        $this->assertFalse($storage['expose_to_customer']);
        $this->assertTrue($storage['hidden']);
    }

    public function testBackupExposedOptional(): void
    {
        $backup = RetailVpsMinimalPreset::exposureFor()[RetailVpsMinimalPreset::DIM_BACKUP];
        $this->assertTrue($backup['expose_to_customer']);
        $this->assertFalse($backup['hidden']);
    }

    public function testIpv4QtyExposedWhenUnitPriceKnown(): void
    {
        $ipv4 = RetailVpsMinimalPreset::exposureFor()[RetailVpsMinimalPreset::DIM_IPV4];
        $this->assertTrue($ipv4['expose_to_customer']);
        $this->assertFalse($ipv4['hidden']);
    }

    public function testBandwidthAndPrivateNetworkingHidden(): void
    {
        $map = RetailVpsMinimalPreset::exposureFor();

        $bw = $map[RetailVpsMinimalPreset::DIM_BANDWIDTH];
        $this->assertFalse($bw['expose_to_customer']);
        $this->assertTrue($bw['hidden']);

        $priv = $map[RetailVpsMinimalPreset::DIM_PRIVATE_NETWORKING];
        $this->assertFalse($priv['expose_to_customer']);
        $this->assertTrue($priv['hidden']);
    }

    public function testExposeAndHiddenAreConsistentlyOpposite(): void
    {
        // For every non-Image dimension, hidden is the negation of exposure.
        foreach (RetailVpsMinimalPreset::exposureFor() as $key => $entry) {
            $this->assertSame(
                !$entry['expose_to_customer'],
                $entry['hidden'],
                "exposure/hidden inconsistent for {$key}"
            );
        }
    }

    public function testPlanSlugArgumentAccepted(): void
    {
        // Plan-agnostic in v1 — passing a slug yields the same shape.
        $a = RetailVpsMinimalPreset::exposureFor('cloud-vps-10');
        $b = RetailVpsMinimalPreset::exposureFor();
        $this->assertSame(array_keys($a), array_keys($b));
    }
}
