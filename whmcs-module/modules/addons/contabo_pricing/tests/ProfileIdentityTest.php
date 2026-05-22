<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProfileIdentityResolver;
use PHPUnit\Framework\TestCase;

final class ProfileIdentityTest extends TestCase
{
    /**
     * @return array<string,mixed>
     */
    private function fixedInput(): array
    {
        return [
            'profile_mode'  => ProfileIdentityResolver::MODE_FIXED,
            'plan_slug'     => 'cloud-vps-10',
            'period_months' => 12,
            'region'        => 'EU',
            'os'            => 'Ubuntu 24.04',
            'sync_strategy' => 'notify',
            'options'       => [
                'Image:OS'  => ['label' => 'Ubuntu 24.04', 'monthly' => 0.0],
                'Region'    => ['label' => 'EU', 'monthly' => 0.0],
                'Storage'   => ['label' => '200 GB NVMe', 'monthly' => 2.0],
                'Backup'    => ['label' => 'Auto Backup', 'monthly' => 1.0],
                'Networking'=> ['label' => '1 IPv4', 'monthly' => 0.0],
            ],
        ];
    }

    public function testFixedProfileFingerprintIncludesFixedImageAndRegion(): void
    {
        $base = $this->fixedInput();
        $fpBase = ProfileIdentityResolver::buildFingerprint($base);

        // Changing region MUST change the fingerprint (region is identity).
        $diffRegion = $base;
        $diffRegion['region'] = 'US';
        $diffRegion['options']['Region'] = ['label' => 'US', 'monthly' => 0.0];
        $this->assertNotSame(
            $fpBase,
            ProfileIdentityResolver::buildFingerprint($diffRegion),
            'Changing region must change the fingerprint'
        );

        // Changing the image (OS) MUST change the fingerprint.
        $diffImage = $base;
        $diffImage['os'] = 'Debian 12';
        $diffImage['options']['Image:OS'] = ['label' => 'Debian 12', 'monthly' => 0.0];
        $this->assertNotSame(
            $fpBase,
            ProfileIdentityResolver::buildFingerprint($diffImage),
            'Changing the fixed image must change the fingerprint'
        );

        // Changing an IRRELEVANT field (display name, tags) must NOT change it.
        $irrelevant = $base;
        $irrelevant['name'] = 'A totally different display name';
        $irrelevant['tags'] = 'prod,billing,whatever';
        $this->assertSame(
            $fpBase,
            ProfileIdentityResolver::buildFingerprint($irrelevant),
            'Cosmetic fields (name/tags) must not affect the fingerprint'
        );
    }

    public function testImageCollapsesMutuallyExclusiveCategoriesToOneValue(): void
    {
        // OS / App / Control Panel / Blockchain are categories of ONE image.
        // The projection must NOT emit four separate keys.
        $input = $this->fixedInput();
        $input['options'] = [
            'Image:ControlPanel' => ['label' => 'cPanel'],
            'Region'             => ['label' => 'EU'],
        ];
        $projection = ProfileIdentityResolver::identityProjection($input);

        $this->assertArrayHasKey('image', $projection, 'projection must have a single image key');
        $this->assertArrayNotHasKey('os', $projection);
        $this->assertArrayNotHasKey('app', $projection);
        $this->assertArrayNotHasKey('controlpanel', $projection);
        $this->assertArrayNotHasKey('blockchain', $projection);
        $this->assertStringContainsStringIgnoringCase('cpanel', (string) $projection['image']);
    }

    public function testProjectionIsKsortedForStability(): void
    {
        $projection = ProfileIdentityResolver::identityProjection($this->fixedInput());
        $keys = array_keys($projection);
        $sorted = $keys;
        sort($sorted);
        $this->assertSame($sorted, $keys, 'identity projection must be ksort-ed for stable hashing');
    }

    public function testConfigurableProfileFingerprintExcludesSelectableValuesPlaceholder(): void
    {
        // The reserved mode's projection is SHAPE-based, not value-based.
        $input = [
            'profile_mode' => ProfileIdentityResolver::MODE_CONFIGURABLE,
            'plan_slug'    => 'cloud-vps-10',
            'product_id'   => 501,
            'exposed_options' => [
                'os'     => ['Ubuntu 24.04', 'Debian 12', 'AlmaLinux 9'],
                'region' => ['EU', 'US', 'SG'],
            ],
            'default_values' => ['os' => 'Ubuntu 24.04', 'region' => 'EU'],
            // A concrete customer-time selection that MUST be ignored:
            'options'      => [
                'Image:OS' => ['label' => 'Debian 12'],
                'Region'   => ['label' => 'US'],
            ],
            'region'       => 'US',
            'os'           => 'Debian 12',
        ];

        $projection = ProfileIdentityResolver::identityProjection($input);

        // No concrete selectable values leak into the configurable projection.
        $this->assertArrayNotHasKey('image', $projection);
        $this->assertArrayNotHasKey('region', $projection);
        $this->assertArrayNotHasKey('storage', $projection);

        // It IS schema-shaped (hashes of exposed options + defaults).
        $this->assertArrayHasKey('exposed_option_schema_hash', $projection);
        $this->assertArrayHasKey('default_values_hash', $projection);
        $this->assertArrayHasKey('product_scope', $projection);
        $this->assertSame(ProfileIdentityResolver::MODE_CONFIGURABLE, $projection['profile_mode']);

        // The serialized projection must NOT contain the concrete selected
        // values "Debian 12" / "US" (they're hashed away into shape).
        $json = (string) json_encode($projection);
        $this->assertStringNotContainsString('Image:OS', $json);
        $this->assertStringNotContainsString('US"', $json);
    }

    public function testConfigurableFingerprintStableAcrossDifferentCustomerSelections(): void
    {
        // Same exposed schema + defaults → same fingerprint, even if the
        // (ignored) concrete customer selections differ.
        $a = [
            'profile_mode'    => ProfileIdentityResolver::MODE_CONFIGURABLE,
            'plan_slug'       => 'cloud-vps-10',
            'product_id'      => 501,
            'exposed_options' => ['os' => ['Ubuntu 24.04', 'Debian 12']],
            'default_values'  => ['os' => 'Ubuntu 24.04'],
            'options'         => ['Image:OS' => ['label' => 'Ubuntu 24.04']],
        ];
        $b = $a;
        $b['options'] = ['Image:OS' => ['label' => 'Debian 12']]; // customer chose differently

        $this->assertSame(
            ProfileIdentityResolver::buildFingerprint($a),
            ProfileIdentityResolver::buildFingerprint($b),
            'Configurable identity must not depend on customer-time selections'
        );
    }

    public function testBuildSlugMovedToResolverMatchesLegacyOutput(): void
    {
        $slug = ProfileIdentityResolver::buildSlug([
            'plan_slug'     => 'Cloud-VPS-10',
            'period_months' => 12,
            'region'        => 'EU',
            'os'            => 'Ubuntu-22',
        ]);
        $this->assertSame('Cloud-VPS-10-12mo-eu-ubuntu-22', $slug);
    }
}
