<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Shared last-line validation for values eligible for monetary writes.
 */
final class PriceInvariant
{
    public static function isPositiveFinite(float $value): bool
    {
        return is_finite($value) && $value > 0.0;
    }

    public static function isNonNegativeFinite(float $value): bool
    {
        return is_finite($value) && $value >= 0.0;
    }

    public static function requirePositiveFinite(
        float $value,
        string $reason,
        string $context
    ): void {
        if (!self::isPositiveFinite($value)) {
            throw new PricingInvariantViolation(
                $reason,
                $context . ' must be a positive finite amount'
            );
        }
    }

    public static function requireNonNegativeFinite(
        float $value,
        string $reason,
        string $context
    ): void {
        if (!self::isNonNegativeFinite($value)) {
            throw new PricingInvariantViolation(
                $reason,
                $context . ' must be a non-negative finite amount'
            );
        }
    }
}
