<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProfileVersionInput;
use PHPUnit\Framework\TestCase;

final class ProfileVersionInputTest extends TestCase
{
    private const DELTA = 0.01;
    private const NOW   = '2026-05-21T00:00:00Z';

    /**
     * @return array<string, array{0: float, 1: float, 2: bool, 3: string, 4: ?float, 5: float, 6: float, 7: float}>
     */
    public static function mathProvider(): array
    {
        // [baseEur, setupEur, applyGst, currency, fxRate, fxMarkupPct, expectMonthly, expectSetup]
        return [
            'EUR no GST-FX (gst=true, fx=null collapses to EUR path)' => [
                3.60, 0.00, true,  'EUR', null,    0.0,   4.248,   0.0,
            ],
            'INR no GST with markup' => [
                3.60, 4.50, false, 'INR', 112.317, 3.5,   418.49,  523.12,
            ],
            'INR with GST + markup' => [
                3.60, 4.50, true,  'INR', 112.317, 3.5,   493.82,  617.28,
            ],
            'INR with GST, no markup, higher base' => [
                14.00, 0.00, true, 'INR', 112.317, 0.0,   1855.48, 0.0,
            ],
        ];
    }

    /**
     * @dataProvider mathProvider
     */
    public function testComputedMath(
        float  $base,
        float  $setup,
        bool   $applyGst,
        string $currency,
        ?float $fxRate,
        float  $fxMarkupPct,
        float  $expectMonthly,
        float  $expectSetup
    ): void {
        $vi = ProfileVersionInput::computed(
            baseMonthlyEur: $base,
            configuredMonthlyEur: $base,
            setupFeeEur: $setup,
            optionsSnapshot: [],
            specsSnapshot: [],
            fxRate: $fxRate,
            fxSource: 'test',
            fxMarkupPct: $fxMarkupPct,
            applyGst18: $applyGst,
            currencyIso: $currency,
            snapshotGeneratedAt: self::NOW,
        );

        $this->assertEqualsWithDelta($expectMonthly, $vi->finalMonthly, self::DELTA, 'monthly');
        $this->assertEqualsWithDelta($expectSetup,   $vi->finalSetup,   self::DELTA, 'setup');
        $this->assertSame($currency, $vi->currencyIso);
        $this->assertEqualsWithDelta($applyGst ? 18.0 : 0.0, $vi->gstPct, 1e-9);
    }

    public function testComputedEurPathIgnoresFxRateWhenCurrencyIsEur(): void
    {
        $vi = ProfileVersionInput::computed(
            baseMonthlyEur: 10.0,
            configuredMonthlyEur: 10.0,
            setupFeeEur: 5.0,
            optionsSnapshot: [],
            specsSnapshot: [],
            fxRate: 99.0, // should be IGNORED because currency=EUR
            fxSource: 'x',
            fxMarkupPct: 50.0,
            applyGst18: false,
            currencyIso: 'EUR',
            snapshotGeneratedAt: self::NOW,
        );

        $this->assertEqualsWithDelta(10.0, $vi->finalMonthly, 1e-9);
        $this->assertEqualsWithDelta(5.0,  $vi->finalSetup,   1e-9);
    }

    public function testComputedStoresFxMarkupAndSource(): void
    {
        $vi = ProfileVersionInput::computed(
            baseMonthlyEur: 1.0,
            configuredMonthlyEur: 1.0,
            setupFeeEur: 0.0,
            optionsSnapshot: ['ssh_keys' => ['id-1']],
            specsSnapshot: ['cpu_cores' => 4],
            fxRate: 100.0,
            fxSource: 'manual',
            fxMarkupPct: 2.0,
            applyGst18: false,
            currencyIso: 'INR',
            snapshotGeneratedAt: self::NOW,
        );

        $this->assertSame(2.0, $vi->fxMarkupPct);
        $this->assertSame('manual', $vi->fxSource);
        $this->assertSame(['ssh_keys' => ['id-1']], $vi->optionsSnapshot);
        $this->assertSame(['cpu_cores' => 4], $vi->specsSnapshot);
    }

    public function testDiffersFromReturnsTrueWhenPreviousNull(): void
    {
        $vi = $this->sampleInput(currency: 'INR', monthly: 100.0);
        $this->assertTrue($vi->differsFrom(null));
    }

    public function testDiffersFromReturnsFalseForSubCentNoise(): void
    {
        $vi = $this->sampleInput(currency: 'INR', monthly: 100.00);

        $prev = [
            'base_monthly_eur'       => 1.0,
            'configured_monthly_eur' => 1.0,
            'setup_fee_eur'          => 0.0,
            'final_monthly'          => 100.003, // < 0.005 difference
            'final_setup'            => 0.0,
            'currency_iso'           => 'INR',
        ];

        $this->assertFalse($vi->differsFrom($prev));
    }

    public function testDiffersFromReturnsTrueWhenMonthlyDriftsBeyondHalfCent(): void
    {
        $vi = $this->sampleInput(currency: 'INR', monthly: 100.00);

        $prev = [
            'base_monthly_eur'       => 1.0,
            'configured_monthly_eur' => 1.0,
            'setup_fee_eur'          => 0.0,
            'final_monthly'          => 100.02, // > 0.005 difference
            'final_setup'            => 0.0,
            'currency_iso'           => 'INR',
        ];

        $this->assertTrue($vi->differsFrom($prev));
    }

    public function testDiffersFromReturnsTrueWhenCurrencyChanges(): void
    {
        $vi = $this->sampleInput(currency: 'INR', monthly: 100.00);

        $prev = [
            'base_monthly_eur'       => 1.0,
            'configured_monthly_eur' => 1.0,
            'setup_fee_eur'          => 0.0,
            'final_monthly'          => 100.0,
            'final_setup'            => 0.0,
            'currency_iso'           => 'EUR', // currency changed
        ];

        $this->assertTrue($vi->differsFrom($prev));
    }

    public function testDiffersFromReturnsTrueWhenBaseEurChanges(): void
    {
        $vi = $this->sampleInput(currency: 'INR', monthly: 100.00, baseEur: 2.0);

        $prev = [
            'base_monthly_eur'       => 1.0, // differs
            'configured_monthly_eur' => 1.0,
            'setup_fee_eur'          => 0.0,
            'final_monthly'          => 100.0,
            'final_setup'            => 0.0,
            'currency_iso'           => 'INR',
        ];

        $this->assertTrue($vi->differsFrom($prev));
    }

    private function sampleInput(string $currency, float $monthly, float $baseEur = 1.0): ProfileVersionInput
    {
        return new ProfileVersionInput(
            baseMonthlyEur: $baseEur,
            configuredMonthlyEur: $baseEur,
            setupFeeEur: 0.0,
            optionsSnapshot: [],
            specsSnapshot: [],
            fxRate: 100.0,
            fxSource: 't',
            fxMarkupPct: 0.0,
            gstPct: 0.0,
            currencyIso: $currency,
            finalMonthly: $monthly,
            finalSetup: 0.0,
            snapshotGeneratedAt: self::NOW,
        );
    }
}
