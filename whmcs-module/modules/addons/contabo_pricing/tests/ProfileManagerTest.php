<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProfileManager;
use PHPUnit\Framework\TestCase;

final class ProfileManagerTest extends TestCase
{
    public function testBuildSlugLowercasesRegionAndOsButPreservesPlanSlugCase(): void
    {
        // Production behavior: only region/os are lowercased — the plan_slug
        // is trusted to already be slug-shaped, so its case is preserved.
        $slug = ProfileManager::buildSlug('Cloud-VPS-10', 12, 'EU', 'Ubuntu-22');
        $this->assertSame('Cloud-VPS-10-12mo-eu-ubuntu-22', $slug);
    }

    public function testBuildSlugWithAlreadyLowercasePlanSlug(): void
    {
        $slug = ProfileManager::buildSlug('cloud-vps-10', 12, 'EU', 'Ubuntu-22');
        $this->assertSame('cloud-vps-10-12mo-eu-ubuntu-22', $slug);
    }

    public function testBuildSlugOmitsEmptyRegion(): void
    {
        $slug = ProfileManager::buildSlug('vps-1', 1, '', 'ubuntu');
        $this->assertSame('vps-1-1mo-ubuntu', $slug);
    }

    public function testBuildSlugOmitsEmptyOs(): void
    {
        $slug = ProfileManager::buildSlug('vps-1', 1, 'eu', '');
        $this->assertSame('vps-1-1mo-eu', $slug);
    }

    public function testBuildSlugOmitsBothWhenEmpty(): void
    {
        $slug = ProfileManager::buildSlug('vps-1', 6, '', '');
        $this->assertSame('vps-1-6mo', $slug);
    }

    public function testBuildSlugStripsSpecialCharacters(): void
    {
        // Spaces, slashes, ampersands collapse into '-' runs which the regex
        // currently keeps as single dashes per replacement; trim('-') strips edges.
        $slug = ProfileManager::buildSlug('cloud vps', 1, 'eu/de', 'ubuntu&server');
        // Each non-alnum/non-dash run is replaced by '-', so 'cloud vps' -> 'cloud-vps'.
        $this->assertSame('cloud-vps-1mo-eu-de-ubuntu-server', $slug);
    }

    public function testBuildSlugTrimsTrailingDashesFromEmptyTail(): void
    {
        // If region/os are empty, the result should not end with a stray '-'.
        $slug = ProfileManager::buildSlug('plan', 3, '', '');
        $this->assertStringEndsNotWith('-', $slug);
        $this->assertSame('plan-3mo', $slug);
    }

    public function testBuildSlugPreservesNumericPeriod(): void
    {
        $slug = ProfileManager::buildSlug('vps', 24, 'us', 'debian');
        $this->assertStringContainsString('24mo', $slug);
    }
}
