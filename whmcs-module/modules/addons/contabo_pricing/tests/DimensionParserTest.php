<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\DimensionParser;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.1 — DimensionParser.
 *
 * Proves the split rules from PHASE_A52_DESIGN_IMPACT.md §2: Image → 1 spec,
 * Networking → 3 specs (Bandwidth / IPv4 / Private Networking), Region /
 * Storage Type / Data Protection → 1 spec each; single-value dimensions are
 * omitted. Also covers the amendment-1 negative-delta clamp helper.
 *
 * Fixtures mirror the verified cloud-vps-10 `options` object shape from
 * data/output/contabo_configs.json (full Networking/Region/Storage/Data
 * Protection sets; a representative Image subset).
 */
final class DimensionParserTest extends TestCase
{
    /**
     * A faithful (trimmed) cloud-vps-10 `options` object: every dimension, with
     * the full Networking 7 rows, full Data Protection 2, full Storage 4, a
     * 3-row Region slice, and a 6-row Image slice spanning all 4 categories.
     *
     * @return array<string,array<int,array<string,mixed>>>
     */
    private function options(): array
    {
        return [
            'Image' => [
                ['dimension' => 'Image', 'category' => 'OS', 'option_label' => 'Ubuntu 24.04', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
                ['dimension' => 'Image', 'category' => 'OS', 'option_label' => 'Debian 12', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
                ['dimension' => 'Image', 'category' => 'Panels', 'option_label' => 'cPanel/WHM (5 accounts)', 'monthly_price_delta' => 21.75, 'setup_fee_delta' => 0],
                ['dimension' => 'Image', 'category' => 'Panels', 'option_label' => 'Plesk Pro Edition', 'monthly_price_delta' => 9.5, 'setup_fee_delta' => 0],
                ['dimension' => 'Image', 'category' => 'Apps', 'option_label' => 'Docker', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
                ['dimension' => 'Image', 'category' => 'Blockchain', 'option_label' => 'IPFS Node', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
            ],
            'Networking' => [
                ['dimension' => 'Networking', 'category' => 'Bandwidth', 'option_label' => '10 TB Out + Unlimited In', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
                ['dimension' => 'Networking', 'category' => 'Bandwidth', 'option_label' => '32 TB Out + Unlimited In', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
                ['dimension' => 'Networking', 'category' => 'Bandwidth', 'option_label' => 'Unlimited Traffic', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
                ['dimension' => 'Networking', 'category' => 'IPv4', 'option_label' => '1 IP Address', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
                ['dimension' => 'Networking', 'category' => 'IPv4', 'option_label' => 'Additional IP Address', 'monthly_price_delta' => 3.5, 'setup_fee_delta' => 0],
                ['dimension' => 'Networking', 'category' => 'Private Networking', 'option_label' => 'No Private Networking', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
                ['dimension' => 'Networking', 'category' => 'Private Networking', 'option_label' => 'Private Networking Enabled', 'monthly_price_delta' => 2.29, 'setup_fee_delta' => 0],
            ],
            'Region' => [
                ['dimension' => 'Region', 'category' => 'America', 'option_label' => 'United States (Central)', 'monthly_price_delta' => 0.95, 'setup_fee_delta' => 0, 'region_group' => 'America', 'country' => 'United States (Central)', 'country_code' => 'US', 'subregion' => 'Central'],
                ['dimension' => 'Region', 'category' => 'Europe', 'option_label' => 'European Union', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true, 'region_group' => 'Europe', 'country' => 'Germany', 'country_code' => 'DE', 'subregion' => ''],
                ['dimension' => 'Region', 'category' => 'Asia', 'option_label' => 'India (Central)', 'monthly_price_delta' => 1.4, 'setup_fee_delta' => 0, 'region_group' => 'Asia', 'country' => 'India', 'country_code' => 'IN', 'subregion' => 'Central'],
            ],
            'Storage Type' => [
                ['dimension' => 'Storage Type', 'category' => 'NVMe', 'option_label' => '75 GB NVMe', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
                ['dimension' => 'Storage Type', 'category' => 'NVMe', 'option_label' => '150 GB NVMe', 'monthly_price_delta' => 1.85, 'setup_fee_delta' => 0],
                ['dimension' => 'Storage Type', 'category' => 'SSD', 'option_label' => '150 GB SSD', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
                ['dimension' => 'Storage Type', 'category' => 'SSD', 'option_label' => '300 GB SSD', 'monthly_price_delta' => 1.55, 'setup_fee_delta' => 0],
            ],
            'Data Protection' => [
                ['dimension' => 'Data Protection', 'category' => 'Auto Backup', 'option_label' => 'Auto Backup', 'monthly_price_delta' => 1.5, 'setup_fee_delta' => 0],
                ['dimension' => 'Data Protection', 'category' => 'None', 'option_label' => 'No Data Protection', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
            ],
        ];
    }

    /**
     * @param list<array<string,mixed>> $specs
     * @return array<string,array<string,mixed>>
     */
    private function indexByDimensionKey(array $specs): array
    {
        $out = [];
        foreach ($specs as $spec) {
            $out[$spec['dimension_key']] = $spec;
        }
        return $out;
    }

    public function testImageBecomesExactlyOneSpec(): void
    {
        $result = DimensionParser::parse($this->options());
        $byKey  = $this->indexByDimensionKey($result['specs']);

        $images = array_filter(
            $result['specs'],
            static fn (array $s): bool => $s['dimension_key'] === 'Image'
        );
        $this->assertCount(1, $images);
        $this->assertArrayHasKey('Image', $byKey);
        // Delegated to the normalizer: 6 input rows → 6 sub-values, one option.
        $this->assertCount(6, $byKey['Image']['values']);
        $this->assertSame('OS:Ubuntu 24.04', $byKey['Image']['default_value_key']);
    }

    public function testNetworkingSplitsIntoThreeSpecs(): void
    {
        $result = DimensionParser::parse($this->options());
        $keys   = array_column($result['specs'], 'dimension_key');

        $this->assertContains('Networking:Bandwidth', $keys);
        $this->assertContains('Networking:IPv4', $keys);
        $this->assertContains('Networking:Private Networking', $keys);

        // And NO bare 'Networking' spec leaks through.
        $this->assertNotContains('Networking', $keys);
    }

    public function testNetworkingConcernsCarryOnlyTheirOwnRows(): void
    {
        $result = DimensionParser::parse($this->options());
        $byKey  = $this->indexByDimensionKey($result['specs']);

        $this->assertCount(3, $byKey['Networking:Bandwidth']['values']);
        $this->assertCount(2, $byKey['Networking:IPv4']['values']);
        $this->assertCount(2, $byKey['Networking:Private Networking']['values']);

        // Bandwidth spec must contain no IPv4 rows.
        foreach ($byKey['Networking:Bandwidth']['values'] as $v) {
            $this->assertSame('Bandwidth', $v['category']);
        }
    }

    public function testNetworkingOptiontypes(): void
    {
        $result = DimensionParser::parse($this->options());
        $byKey  = $this->indexByDimensionKey($result['specs']);

        $this->assertSame(0, $byKey['Networking:Bandwidth']['optiontype']);             // dropdown
        $this->assertSame(3, $byKey['Networking:IPv4']['optiontype']);                  // qty
        $this->assertSame(2, $byKey['Networking:Private Networking']['optiontype']);    // yes/no
    }

    public function testRegionStorageDataProtectionEachOneSpec(): void
    {
        $result = DimensionParser::parse($this->options());
        $keys   = array_column($result['specs'], 'dimension_key');

        $this->assertSame(1, count(array_keys($keys, 'Region', true)));
        $this->assertSame(1, count(array_keys($keys, 'Storage Type', true)));
        $this->assertSame(1, count(array_keys($keys, 'Data Protection', true)));
    }

    public function testSingleChoiceOptiontypes(): void
    {
        $result = DimensionParser::parse($this->options());
        $byKey  = $this->indexByDimensionKey($result['specs']);

        $this->assertSame(1, $byKey['Region']['optiontype']);          // radio (3 values)
        $this->assertSame(1, $byKey['Storage Type']['optiontype']);    // radio (4 values)
        $this->assertSame(2, $byKey['Data Protection']['optiontype']); // yes/no (exactly 2)
    }

    public function testTotalSpecCount(): void
    {
        // Image(1) + Networking(3) + Region(1) + Storage(1) + DataProtection(1) = 7
        $result = DimensionParser::parse($this->options());
        $this->assertCount(7, $result['specs']);
        $this->assertSame([], $result['omitted']);
    }

    public function testSingleValueDimensionIsOmitted(): void
    {
        $options = $this->options();
        // Collapse Data Protection to a single value → no real choice.
        $options['Data Protection'] = [
            ['dimension' => 'Data Protection', 'category' => 'None', 'option_label' => 'No Data Protection', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true],
        ];

        $result = DimensionParser::parse($options);
        $keys   = array_column($result['specs'], 'dimension_key');

        $this->assertNotContains('Data Protection', $keys);
        $this->assertCount(6, $result['specs']);

        $omittedKeys = array_column($result['omitted'], 'dimension_key');
        $this->assertContains('Data Protection', $omittedKeys);
        $this->assertSame('single_value', $result['omitted'][0]['reason']);
    }

    public function testSingleValueNetworkingConcernOmitted(): void
    {
        $options = $this->options();
        // Drop the second IPv4 row so IPv4 has a single value.
        $options['Networking'] = array_values(array_filter(
            $options['Networking'],
            static fn (array $r): bool => $r['option_label'] !== 'Additional IP Address'
        ));

        $result = DimensionParser::parse($options);
        $keys   = array_column($result['specs'], 'dimension_key');

        $this->assertNotContains('Networking:IPv4', $keys);
        $this->assertContains('Networking:Bandwidth', $keys);
        $this->assertContains('Networking:Private Networking', $keys);

        $omittedKeys = array_column($result['omitted'], 'dimension_key');
        $this->assertContains('Networking:IPv4', $omittedKeys);
    }

    public function testDeltasPassedThroughUnclamped(): void
    {
        $result = DimensionParser::parse($this->options());
        $byKey  = $this->indexByDimensionKey($result['specs']);

        $ipv4 = $byKey['Networking:IPv4']['values'];
        $additional = array_values(array_filter(
            $ipv4,
            static fn (array $v): bool => $v['label'] === 'Additional IP Address'
        ))[0];
        // Raw EUR delta preserved; clamping is the syncer's job.
        $this->assertSame(3.5, $additional['monthly_eur_delta']);
    }

    public function testUnknownDimensionStillSurfacesAsSpec(): void
    {
        $options = $this->options();
        $options['Future Dimension'] = [
            ['dimension' => 'Future Dimension', 'category' => 'X', 'option_label' => 'A', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0],
            ['dimension' => 'Future Dimension', 'category' => 'X', 'option_label' => 'B', 'monthly_price_delta' => 1.0, 'setup_fee_delta' => 0],
        ];

        $result = DimensionParser::parse($options);
        $keys   = array_column($result['specs'], 'dimension_key');
        $this->assertContains('Future Dimension', $keys);
    }

    public function testNonArrayDimensionIgnored(): void
    {
        $options                 = $this->options();
        $options['Broken']       = 'not-an-array';
        $result                  = DimensionParser::parse($options);
        $keys                    = array_column($result['specs'], 'dimension_key');
        $this->assertNotContains('Broken', $keys);
        $this->assertCount(7, $result['specs']);
    }

    // ── clampDelta (amendment 1) ────────────────────────────────────────────

    public function testClampDeltaFloorsNegativeToZero(): void
    {
        $this->assertSame(0.0, DimensionParser::clampDelta(-5.0));
    }

    public function testClampDeltaZeroStaysZero(): void
    {
        $this->assertSame(0.0, DimensionParser::clampDelta(0.0));
    }

    public function testClampDeltaPositivePassesThrough(): void
    {
        $this->assertSame(7.5, DimensionParser::clampDelta(7.5));
    }
}
