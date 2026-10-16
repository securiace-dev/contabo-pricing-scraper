<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Quote\QuoteService;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class QuoteServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /** @return array<string,mixed> */
    private function fixture(string $n): array
    {
        return json_decode((string) file_get_contents(__DIR__ . '/fixtures/api/' . $n . '.json'), true);
    }

    public function testMatchesRustQuoteFixture(): void
    {
        $plan = $this->fixture('plans')[0];
        $golden = $this->fixture('quote');
        $got = (new QuoteService())->quote($plan, [
            'plan_slug' => 'cloud-vps-10',
            'period_months' => 12,
            'currency' => 'INR',
            'gst' => true,
            'fx_markup' => 0.035,
            'fx_rate' => 112.317,
        ]);
        foreach (['base_monthly_eur', 'configured_monthly_eur', 'setup_fee_eur', 'gst_amount_eur',
                  'fx_rate', 'fx_markup', 'final_monthly', 'final_total'] as $k) {
            $this->assertEqualsWithDelta($golden[$k], $got[$k], 1e-9, $k);
        }
        $this->assertSame($golden['plan_slug'], $got['plan_slug']);
        $this->assertSame($golden['period_months'], $got['period_months']);
        $this->assertSame($golden['currency'], $got['currency']);
        $this->assertSame($golden['breakdown'], $got['breakdown']);
        foreach (array_keys($golden) as $k) {
            $this->assertArrayHasKey($k, $got);
        }
        $this->assertFalse($got['term_derived']);
        $this->assertSame('native', $got['term_source']);
    }

    public function testEurWithoutGstHasNoFxRate(): void
    {
        $got = (new QuoteService())->quote($this->fixture('plans')[0], ['period_months' => 12]);
        $this->assertNull($got['fx_rate']);
        $this->assertSame(3.6, $got['final_monthly']);
        $this->assertSame(['base €3.60/mo'], $got['breakdown']);
        $this->assertEqualsWithDelta(43.2, $got['final_total'], 1e-9);
    }

    public function testCycleNamesMapToMonths(): void
    {
        $plan = ['periods' => []];
        foreach (QuoteService::CYCLE_MONTHS as $name => $m) {
            $plan['periods'][] = ['months' => $m, 'effective_monthly' => 2.0, 'setup_fee' => 0.0];
        }
        $svc = new QuoteService();
        $exp = ['monthly' => 1, 'quarterly' => 3, 'semiannually' => 6, 'annually' => 12, 'biennially' => 24, 'triennially' => 36];
        foreach ($exp as $name => $m) {
            $this->assertSame($m, $svc->quote($plan, ['cycle' => $name])['period_months']);
        }
    }

    public function testUnknownCycleRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new QuoteService())->quote($this->fixture('plans')[0], ['cycle' => 'fortnightly']);
    }

    public function testZeroPeriodRejectedAndUnpublishedTermsAreDerived(): void
    {
        $svc = new QuoteService();
        $plan = $this->fixture('plans')[0];
        try {
            $svc->quote($plan, ['period_months' => 0]);
            $this->fail('period 0 should be rejected');
        } catch (\InvalidArgumentException $e) {
            $this->addToAssertionCount(1);
        }
        $five = $svc->quote($plan, ['period_months' => 5]);
        $this->assertTrue($five['term_derived']);
        $this->assertSame(5, $five['period_months']);
        $thirtySix = $svc->quote($plan, ['period_months' => 36]);
        $this->assertTrue($thirtySix['term_derived']);
        $this->assertSame(12, $thirtySix['term_source_months']);
        $this->assertSame(3.6, $thirtySix['base_monthly_eur']);
    }

    public function testInrRequiresRate(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new QuoteService())->quote($this->fixture('plans')[0], ['period_months' => 12, 'currency' => 'INR']);
    }
}
