<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ImageOptionNormalizer;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.1 — ImageOptionNormalizer.
 *
 * Proves the headline rule from PHASE_A52_DESIGN_IMPACT.md §2: the 34-row
 * Image dimension collapses to exactly ONE WHMCS option (never four), with
 * prefixed labels + category-clustered sortorder (amendment 2) and a single
 * provisioning round-trip value.
 *
 * Fixtures are a representative subset drawn verbatim from
 * data/output/contabo_configs.json (cloud-vps-10): all four categories are
 * present, including the one priced Panels image (cPanel) and the verified
 * default (Ubuntu 24.04).
 */
final class ImageOptionNormalizerTest extends TestCase
{
    /**
     * Representative slice of the 34 cloud-vps-10 Image rows: 5 OS (one of
     * which is the default), 2 Panels (one priced), 2 Apps, 2 Blockchain.
     * Enough to exercise every code path without hardcoding all 34.
     *
     * @return array<int,array<string,mixed>>
     */
    private function imageRows(): array
    {
        return [
            ['dimension' => 'Image', 'category' => 'OS', 'option_label' => 'Ubuntu 24.04', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'is_default' => true, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'OS', 'option_label' => 'Debian 12', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'OS', 'option_label' => 'AlmaLinux 9', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'OS', 'option_label' => 'Windows Server 2022', 'monthly_price_delta' => 12.0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'Panels', 'option_label' => 'cPanel/WHM (5 accounts)', 'monthly_price_delta' => 21.75, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'Panels', 'option_label' => 'Plesk Pro Edition', 'monthly_price_delta' => 9.5, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'Apps', 'option_label' => 'Docker', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'Apps', 'option_label' => 'GitLab Server', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'Blockchain', 'option_label' => 'IPFS Node', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
            ['dimension' => 'Image', 'category' => 'Blockchain', 'option_label' => 'Flux Node', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0, 'currency' => 'EUR', 'plan_sku' => 'cloud-vps-10'],
        ];
    }

    public function testCollapsesManyRowsToOneOptionWithAllValues(): void
    {
        $rows = $this->imageRows();
        $spec = ImageOptionNormalizer::normalize($rows);

        // ONE option spec — not a list of four.
        $this->assertSame('Image', $spec['dimension_key']);
        $this->assertArrayHasKey('values', $spec);
        // Every input row with a label survives as exactly one sub-value.
        $this->assertCount(count($rows), $spec['values']);
    }

    public function testNeverReturnsFourOptions(): void
    {
        $spec = ImageOptionNormalizer::normalize($this->imageRows());

        // The whole point: a single associative spec, not a numeric list of
        // per-category options. dimension_key proves it is one Image option.
        $this->assertArrayHasKey('dimension_key', $spec);
        $this->assertSame('Image', $spec['dimension_key']);
        // It is NOT a list of specs keyed 0..3.
        $this->assertArrayNotHasKey(0, $spec);
    }

    public function testAllFourCategoriesPresentInGroups(): void
    {
        $spec = ImageOptionNormalizer::normalize($this->imageRows());
        // Retail order: OS, Panels, Apps, Blockchain.
        $this->assertSame(['OS', 'Panels', 'Apps', 'Blockchain'], $spec['category_groups']);
    }

    public function testOptiontypeIsDropdown(): void
    {
        $spec = ImageOptionNormalizer::normalize($this->imageRows());
        $this->assertSame(0, $spec['optiontype']); // dropdown
    }

    public function testLabelsArePrefixedByCategory(): void
    {
        $spec  = ImageOptionNormalizer::normalize($this->imageRows());
        $byKey = $this->indexByValueKey($spec['values']);

        $this->assertSame('[OS] Ubuntu 24.04', $byKey['OS:Ubuntu 24.04']['label']);
        $this->assertSame('[Panel] cPanel/WHM (5 accounts)', $byKey['Panels:cPanel/WHM (5 accounts)']['label']);
        $this->assertSame('[App] Docker', $byKey['Apps:Docker']['label']);
        $this->assertSame('[Blockchain] Flux Node', $byKey['Blockchain:Flux Node']['label']);
    }

    public function testSortorderClustersByCategoryInRetailOrder(): void
    {
        $spec = ImageOptionNormalizer::normalize($this->imageRows());

        // Walk the emitted values: the category sequence must be monotonic in
        // retail rank (all OS, then all Panels, then Apps, then Blockchain)
        // and sortorder must be strictly increasing.
        $rank      = ['OS' => 0, 'Panels' => 1, 'Apps' => 2, 'Blockchain' => 3];
        $lastRank  = -1;
        $lastOrder = -1;
        foreach ($spec['values'] as $value) {
            $this->assertGreaterThanOrEqual($lastRank, $rank[$value['category']]);
            $this->assertGreaterThan($lastOrder, $value['sortorder']);
            $lastRank  = $rank[$value['category']];
            $lastOrder = $value['sortorder'];
        }
    }

    public function testWithinCategorySortedAlphabetically(): void
    {
        $spec   = ImageOptionNormalizer::normalize($this->imageRows());
        $osOnly = array_values(array_filter(
            $spec['values'],
            static fn (array $v): bool => $v['category'] === 'OS'
        ));
        $labels = array_column($osOnly, 'label');
        $sorted = $labels;
        sort($sorted, SORT_STRING | SORT_FLAG_CASE);
        $this->assertSame($sorted, $labels);
    }

    public function testDefaultDetected(): void
    {
        $spec = ImageOptionNormalizer::normalize($this->imageRows());
        $this->assertSame('OS:Ubuntu 24.04', $spec['default_value_key']);

        $byKey = $this->indexByValueKey($spec['values']);
        $this->assertTrue($byKey['OS:Ubuntu 24.04']['is_default']);
        $this->assertFalse($byKey['Apps:Docker']['is_default']);
    }

    public function testNoDefaultWhenNoneFlagged(): void
    {
        $rows = $this->imageRows();
        foreach ($rows as &$r) {
            unset($r['is_default']);
        }
        unset($r);

        $spec = ImageOptionNormalizer::normalize($rows);
        $this->assertNull($spec['default_value_key']);
    }

    public function testPricedPanelDeltaPreserved(): void
    {
        $spec  = ImageOptionNormalizer::normalize($this->imageRows());
        $byKey = $this->indexByValueKey($spec['values']);
        // cPanel carries its raw EUR delta (not clamped here — pricing lives
        // in the syncer).
        $this->assertSame(21.75, $byKey['Panels:cPanel/WHM (5 accounts)']['monthly_eur_delta']);
        $this->assertSame(0.0, $byKey['OS:Ubuntu 24.04']['monthly_eur_delta']);
    }

    public function testProvisioningValueReturnsOneStringNotFour(): void
    {
        $spec = ImageOptionNormalizer::normalize($this->imageRows());

        // Pick the cPanel value and round-trip it: provisioning gets exactly
        // ONE category:label identifier.
        $selected = 'Panels:cPanel/WHM (5 accounts)';
        $prov     = ImageOptionNormalizer::provisioningValue($selected);

        $this->assertIsString($prov);
        $this->assertSame('Panels:cPanel/WHM (5 accounts)', $prov);
        // Sanity: the chosen value_key actually exists in the option.
        $this->assertArrayHasKey($selected, $this->indexByValueKey($spec['values']));
    }

    public function testRowWithEmptyLabelIsSkipped(): void
    {
        $rows   = $this->imageRows();
        $rows[] = ['dimension' => 'Image', 'category' => 'OS', 'option_label' => '', 'monthly_price_delta' => 0, 'setup_fee_delta' => 0];

        $spec = ImageOptionNormalizer::normalize($rows);
        // The empty-label row is dropped, so the count matches the original.
        $this->assertCount(count($this->imageRows()), $spec['values']);
    }

    public function testValueKeyAndPrefixedLabelHelpers(): void
    {
        $this->assertSame('Panels:Webmin', ImageOptionNormalizer::valueKey('Panels', 'Webmin'));
        $this->assertSame('[Panel] Webmin', ImageOptionNormalizer::prefixedLabel('Panels', 'Webmin'));
        // Unknown category passes through with no prefix.
        $this->assertSame('Mystery', ImageOptionNormalizer::prefixedLabel('Nope', 'Mystery'));
    }

    /**
     * @param list<array<string,mixed>> $values
     * @return array<string,array<string,mixed>>
     */
    private function indexByValueKey(array $values): array
    {
        $out = [];
        foreach ($values as $value) {
            $out[$value['value_key']] = $value;
        }
        return $out;
    }
}
