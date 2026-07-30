<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\PriceInvariant;
use ContaboPricing\Rounding;
use PHPUnit\Framework\TestCase;

/**
 * Canonical tests for the rounding values persisted by WHMCS mappings.
 */
final class RoundingTest extends TestCase
{
    public function testSupportedModesMatchPersistedContract(): void
    {
        $this->assertSame([
            'exact_2_decimals',
            'nearest_rupee',
            'nearest_9',
            'nearest_99',
            'nearest_100',
            'custom',
        ], Rounding::supportedModes());

        $this->assertSame([
            'exact_2_decimals',
            'nearest_rupee',
            'nearest_9',
            'nearest_99',
            'nearest_100',
        ], Rounding::selectableModes());
        $this->assertTrue(Rounding::isSupportedMode(Rounding::MODE_CUSTOM));
        $this->assertFalse(Rounding::isSelectableMode(Rounding::MODE_CUSTOM));
    }

    /**
     * @dataProvider roundingCases
     */
    public function testPersistedModeSemantics(float $input, string $mode, float $expected): void
    {
        $this->assertSame($expected, Rounding::apply($input, $mode));
    }

    /** @return list<array{0:float,1:string,2:float}> */
    public static function roundingCases(): array
    {
        return [
            [1234.567, Rounding::MODE_EXACT_2_DECIMALS, 1234.57],
            [1234.40, Rounding::MODE_NEAREST_RUPEE, 1234.0],
            [1234.60, Rounding::MODE_NEAREST_RUPEE, 1235.0],
            [1234.00, Rounding::MODE_NEAREST_9, 1239.0],
            [1239.00, Rounding::MODE_NEAREST_9, 1239.0],
            [1239.01, Rounding::MODE_NEAREST_9, 1249.0],
            [1234.00, Rounding::MODE_NEAREST_99, 1299.0],
            [1299.00, Rounding::MODE_NEAREST_99, 1299.0],
            [1299.01, Rounding::MODE_NEAREST_99, 1399.0],
            [1249.00, Rounding::MODE_NEAREST_100, 1200.0],
            [1250.00, Rounding::MODE_NEAREST_100, 1300.0],
            [1234.567, Rounding::MODE_CUSTOM, 1234.57],
            [1234.567, 'unknown-mode', 1234.57],
        ];
    }

    public function testFormatterNormalizesNonPositiveValuesButWriteInvariantRejectsThem(): void
    {
        foreach (Rounding::supportedModes() as $mode) {
            $this->assertEquals(0.0, Rounding::apply(-10.0, $mode));
            $this->assertEquals(0.0, Rounding::apply(0.0, $mode));
        }

        $this->assertFalse(PriceInvariant::isPositiveFinite(-10.0));
        $this->assertFalse(PriceInvariant::isPositiveFinite(0.0));
        $this->assertFalse(PriceInvariant::isPositiveFinite(INF));
        $this->assertFalse(PriceInvariant::isPositiveFinite(NAN));
        $this->assertTrue(PriceInvariant::isPositiveFinite(0.01));
    }
}
