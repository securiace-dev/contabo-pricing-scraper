<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ServiceRevenueResolver;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase C.2 — ServiceRevenueResolver discount calculations.
 *
 * Covers the fetchDiscounts() path introduced in Phase C Feature 2:
 *   - percentage recurring promos
 *   - fixed_amount promos
 *   - recurnextcycle promos already applied (>1 order) → skipped
 *   - client-specific recurring discounts (tblclientdiscounts)
 *   - greater-of logic when both promo and client discount exist
 *   - schema guard when tblpromotions lacks the `recurring` column
 *   - non-INR service early exit
 *
 * All tests drive resolveForService() end-to-end and assert on
 * breakdown.discount_breakdown so the full integration path is exercised.
 *
 * PHP 7.4 polyglot.
 */
final class ServiceRevenueResolverDiscountTest extends TestCase
{
    /** INR currency id (mirrors WhmcsConfigOptionsAdapter::INR_CURRENCY_ID). */
    private const INR = 1;

    protected function setUp(): void
    {
        Capsule::reset();
        // Register tblpromotions columns with `recurring` present by default.
        // Tests that need the schema-guard failure omit this in their own setup.
        Capsule::$columns['tblpromotions'] = ['id', 'type', 'value', 'recurring', 'recurnextcycle', 'uses'];
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Seed the minimum rows needed so resolveForService() completes without
     * exceptions. Returns nothing; all state lives in Capsule::$tables.
     *
     * @param int   $serviceId
     * @param float $base       catalog recurring price for the cycle
     * @param int   $userId     owning client id
     * @param int   $currency   client currency (1 = INR, other = non-INR)
     */
    private function seedMinimalService(
        int $serviceId,
        float $base,
        int $userId,
        int $currency = self::INR
    ): void {
        $packageId = 9000 + $serviceId;
        Capsule::table('tblhosting')->insert([
            'id'           => $serviceId,
            'userid'       => $userId,
            'packageid'    => $packageId,
            'billingcycle' => 'monthly',
            'amount'       => $base,
        ]);
        Capsule::table('tblpricing')->insert([
            'type'         => 'product',
            'relid'        => $packageId,
            'currency'     => self::INR,
            'monthly'      => $base,
            'quarterly'    => 0.0,
            'semiannually' => 0.0,
            'annually'     => 0.0,
            'biennially'   => 0.0,
            'triennially'  => 0.0,
        ]);
        Capsule::table('tblclients')->insert([
            'id'       => $userId,
            'currency' => $currency,
        ]);
    }

    /**
     * Seed an order linking a service to a promo.
     */
    private function seedOrder(int $orderId, int $serviceId, int $userId, int $promoId): void
    {
        Capsule::table('tblorders')->insert([
            'id'        => $orderId,
            'serviceid' => $serviceId,
            'userid'    => $userId,
            'promoid'   => $promoId,
        ]);
    }

    /**
     * Seed a promotion row.
     *
     * @param array<string,mixed> $overrides
     */
    private function seedPromo(int $promoId, array $overrides = []): void
    {
        Capsule::table('tblpromotions')->insert(array_merge([
            'id'             => $promoId,
            'type'           => 'percentage',
            'value'          => 0.0,
            'recurring'      => 1,
            'recurnextcycle' => 0,
        ], $overrides));
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /**
     * A 10% recurring percentage promo applied to a 500 INR service must
     * produce discounts = 50.00.
     */
    public function testPercentageRecurringPromo(): void
    {
        $this->seedMinimalService(1, 500.0, 10);
        $this->seedPromo(1, ['type' => 'percentage', 'value' => 10.0]);
        $this->seedOrder(1, 1, 10, 1);

        $r = (new ServiceRevenueResolver())->resolveForService(1);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(50.0, $disc['amount']);
        $this->assertSame('percentage', $disc['type']);
        $this->assertSame('promo', $disc['source']);
        $this->assertFalse($disc['partial']);
        $this->assertSame(50.0, $r['breakdown']['discounts']);
    }

    /**
     * A fixed_amount promo of 100 on a 500 base → discounts = 100.
     * A fixed_amount promo larger than base is capped at base.
     */
    public function testFixedAmountPromo(): void
    {
        $this->seedMinimalService(2, 500.0, 11);
        $this->seedPromo(2, ['type' => 'fixed_amount', 'value' => 100.0]);
        $this->seedOrder(2, 2, 11, 2);

        $r = (new ServiceRevenueResolver())->resolveForService(2);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(100.0, $disc['amount']);
        $this->assertSame('fixed_amount', $disc['type']);
        $this->assertSame('promo', $disc['source']);

        // Verify cap: fixed_amount > base → capped at base.
        Capsule::reset();
        Capsule::$columns['tblpromotions'] = ['id', 'type', 'value', 'recurring', 'recurnextcycle'];
        $this->seedMinimalService(3, 80.0, 12);
        $this->seedPromo(3, ['type' => 'fixed_amount', 'value' => 200.0]);
        $this->seedOrder(3, 3, 12, 3);

        $r2 = (new ServiceRevenueResolver())->resolveForService(3);
        $this->assertSame(80.0, $r2['breakdown']['discount_breakdown']['amount'],
            'fixed_amount promo must be capped at baseAmount');
    }

    /**
     * A recurnextcycle promo that has already been applied to >1 orders for
     * the same user must be skipped — discounts = 0.
     */
    public function testNextCycleOnlyPromoSkipped(): void
    {
        $this->seedMinimalService(4, 500.0, 13);
        $this->seedPromo(4, ['type' => 'percentage', 'value' => 20.0, 'recurnextcycle' => 1]);
        // Two orders with the same promo+user — already applied more than once.
        Capsule::table('tblorders')->insert(['id' => 10, 'serviceid' => 4, 'userid' => 13, 'promoid' => 4]);
        Capsule::table('tblorders')->insert(['id' => 11, 'serviceid' => 4, 'userid' => 13, 'promoid' => 4]);

        $r = (new ServiceRevenueResolver())->resolveForService(4);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(0.0, $disc['amount'],
            'recurnextcycle promo already used >1 time must be skipped');
        $this->assertSame('none', $disc['source']);
    }

    /**
     * No promo, but the client has a 15% tblclientdiscounts entry.
     * Discounts must equal 15% of base.
     */
    public function testClientDiscountApplied(): void
    {
        $this->seedMinimalService(5, 400.0, 14);
        // No order / promo rows — only a client discount.
        Capsule::table('tblclientdiscounts')->insert([
            'id'       => 1,
            'clientid' => 14,
            'value'    => 15.0,
            'expiry'   => null,
        ]);

        $r = (new ServiceRevenueResolver())->resolveForService(5);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(60.0, $disc['amount']); // 15% of 400
        $this->assertSame('percentage', $disc['type']);
        $this->assertSame('client_discount', $disc['source']);
    }

    /**
     * 20% promo vs 10% client discount on a 500 base → promo wins (100 > 50).
     */
    public function testPromoBeatsClientDiscount(): void
    {
        $this->seedMinimalService(6, 500.0, 15);
        $this->seedPromo(5, ['type' => 'percentage', 'value' => 20.0]);
        $this->seedOrder(5, 6, 15, 5);
        Capsule::table('tblclientdiscounts')->insert([
            'id'       => 2,
            'clientid' => 15,
            'value'    => 10.0,
            'expiry'   => null,
        ]);

        $r = (new ServiceRevenueResolver())->resolveForService(6);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(100.0, $disc['amount']); // 20% of 500
        $this->assertSame('promo', $disc['source'],
            'promo (100) should beat client discount (50)');
    }

    /**
     * When tblpromotions lacks the `recurring` column (schema gap), fetchDiscounts
     * must return partial=true and amount=0 without throwing.
     */
    public function testPartialFallbackOnSchemaGap(): void
    {
        // Reset and register tblpromotions WITHOUT the `recurring` column.
        Capsule::reset();
        Capsule::$columns['tblpromotions'] = ['id', 'type', 'value', 'uses'];

        $this->seedMinimalService(7, 300.0, 16);
        $this->seedOrder(6, 7, 16, 6);

        $r = (new ServiceRevenueResolver())->resolveForService(7);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(0.0, $disc['amount']);
        $this->assertSame('schema_guard', $disc['source']);
        $this->assertTrue($disc['partial'],
            'partial must be true when tblpromotions.recurring column is absent');
        $this->assertTrue($r['breakdown']['discounts_partial']);
    }

    /**
     * A service whose client uses a non-INR currency must produce
     * amount=0 and source='non_inr_skipped' immediately.
     */
    public function testNonInrServiceSkipped(): void
    {
        // Currency 2 = USD (non-INR).
        $this->seedMinimalService(8, 500.0, 17, 2);
        $this->seedPromo(7, ['type' => 'percentage', 'value' => 10.0]);
        $this->seedOrder(7, 8, 17, 7);

        $r = (new ServiceRevenueResolver())->resolveForService(8);
        $disc = $r['breakdown']['discount_breakdown'];

        $this->assertSame(0.0, $disc['amount']);
        $this->assertSame('non_inr_skipped', $disc['source']);
        $this->assertFalse($disc['partial']);
        // Verify the currency_supported flag is also false on the breakdown.
        $this->assertFalse($r['breakdown']['currency_supported']);
    }
}
