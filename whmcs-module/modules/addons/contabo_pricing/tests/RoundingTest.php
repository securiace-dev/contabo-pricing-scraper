<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Rounding;
use PHPUnit\Framework\TestCase;

/**
 * Rounding clamp and invariants tests.
 *
 * Validates that negative, zero, and edge-case prices are handled
 * correctly across all supported rounding modes. This is the single
 * gate that every sell price passes through before hitting a DB write.
 */
final class RoundingTest extends TestCase
{
    public function testNegativePriceClampedToZero(): void
    {
        $modes = Rounding::supportedModes();

        $negatives = [
            -0.01,
            -1.0,
            -3.60,
            -1234.56,
        ];

        foreach ($negatives as $price) {
            foreach ($modes as $mode) {
                $result = Rounding::apply($price, $mode);
                $this->assertEquals(
                    0.00,
                    $result,
                    "negative {$price} with mode {$mode} should clamp to 0.00, got {$result}"
                );
            }
        }
    }

    public function testZeroPreservedAcrossModes(): void
    {
        $modes = Rounding::supportedModes();

        foreach ($modes as $mode) {
            $result = Rounding::apply(0.0, $mode);
            $this->assertEquals(
                0.00,
                $result,
                "zero should be 0.00 under mode {$mode}, got {$result}"
            );
        }

        // -0.0 is also zero in PHP (PHP treats -0.0 == 0.0 as true).
        foreach ($modes as $mode) {
            $result = Rounding::apply(-0.0, $mode);
            $this->assertEquals(
                0.00,
                $result,
                "-0.0 should be 0.00 under mode {$mode}, got {$result}"
            );
        }
    }

    public function testExactTwoDecimalsRoundsHalfUp(): void
    {
        $this->assertEquals(3.60, Rounding::apply(3.595, Rounding::MODE_EXACT_2_DECIMALS));
        $this->assertEquals(3.60, Rounding::apply(3.604, Rounding::MODE_EXACT_2_DECIMALS));
        $this->assertEquals(1234.57, Rounding::apply(1234.567, Rounding::MODE_EXACT_2_DECIMALS));
    }

    public function testNearest99RoundsUp(): void
    {
        $this->assertEquals(1234.99, Rounding::apply(1234.00, Rounding::MODE_NEAREST_99));
        $this->assertEquals(1234.99, Rounding::apply(1234.50, Rounding::MODE_NEAREST_99));
        $this->assertEquals(1235.99, Rounding::apply(1235.00, Rounding::MODE_NEAREST_99));
        $this->assertEquals(1234.99, Rounding::apply(1234.99, Rounding::MODE_NEAREST_99));
    }

    public function testNearest95RoundsUp(): void
    {
        $this->assertEquals(1234.95, Rounding::apply(1234.00, Rounding::MODE_NEAREST_95));
        $this->assertEquals(1235.95, Rounding::apply(1235.10, Rounding::MODE_NEAREST_95));
    }

    public function testNearest50RoundsUp(): void
    {
        $this->assertEquals(1234.50, Rounding::apply(1234.01, Rounding::MODE_NEAREST_50));
        $this->assertEquals(1235.00, Rounding::apply(1234.51, Rounding::MODE_NEAREST_50));
        $this->assertEquals(1234.50, Rounding::apply(1234.50, Rounding::MODE_NEAREST_50));
        // Already on boundary — stays where it is.
        $this->assertEquals(1234.00, Rounding::apply(1234.00, Rounding::MODE_NEAREST_50));
    }

    public function testNearestIntegerRoundsUp(): void
    {
        $this->assertEquals(1235.0, Rounding::apply(1234.01, Rounding::MODE_NEAREST_INTEGER));
        $this->assertEquals(1234.0, Rounding::apply(1234.00, Rounding::MODE_NEAREST_INTEGER));
    }

    public function testSmallPositiveValuesAreNotClamped(): void
    {
        // Values just above zero should round normally, not be clamped.
        $result = Rounding::apply(0.001, Rounding::MODE_EXACT_2_DECIMALS);
        $this->assertEquals(0.00, $result, '0.001 rounds to 0.00 in exact_2_decimals');

        // 0.005 rounds up to 0.01 (half-up)
        $result = Rounding::apply(0.005, Rounding::MODE_EXACT_2_DECIMALS);
        $this->assertEquals(0.01, $result);
    }

    public function testSupportedModesReturnsAllFive(): void
    {
        $modes = Rounding::supportedModes();
        $this->assertCount(5, $modes);
        $this->assertContains('exact_2_decimals', $modes);
        $this->assertContains('nearest_99', $modes);
        $this->assertContains('nearest_95', $modes);
        $this->assertContains('nearest_50', $modes);
        $this->assertContains('nearest_integer', $modes);
    }

    public function testIsSupportedMode(): void
    {
        $this->assertTrue(Rounding::isSupportedMode('exact_2_decimals'));
        $this->assertTrue(Rounding::isSupportedMode('nearest_99'));
        $this->assertFalse(Rounding::isSupportedMode('unknown_mode'));
        $this->assertFalse(Rounding::isSupportedMode(''));
    }
}
