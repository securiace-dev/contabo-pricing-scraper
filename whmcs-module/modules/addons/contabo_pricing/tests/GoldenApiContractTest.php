<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Golden API contract tests for the WHMCS addon.
 *
 * Validates that the PHP-side understanding of the API response shapes
 * matches the pre-captured golden fixtures. These tests are read-only;
 * they load fixture JSON from disk and assert expected fields exist.
 *
 * Fixtures are at: ../../../tests/fixtures/*.golden.json
 */
final class GoldenApiContractTest extends TestCase
{
    /**
     * Resolve the fixtures directory relative to this test file.
     * tests/GoldenApiContractTest.php -> ../../ -> ../../tests/fixtures/
     */
    private function fixturesDir(): string
    {
        return __DIR__ . '/../../../../../tests/fixtures';
    }

    /**
     * @return array<string, mixed>
     */
    private function loadFixture(string $name): array
    {
        $path = $this->fixturesDir() . '/' . $name;
        if (!file_exists($path)) {
            $this->markTestSkipped("golden fixture $name not found at $path");
            return []; // never reached
        }
        $data = json_decode(file_get_contents($path), true);
        if (!is_array($data)) {
            $this->fail("fixture $name is not valid JSON or not an array");
        }
        return $data;
    }

    // ─── Plans contract ────────────────────────────────────────────────────

    public function testPlansResponseShapeMatchesSchema(): void
    {
        $plans = $this->loadFixture('plans.golden.json');
        $this->assertIsArray($plans);
        $this->assertNotEmpty($plans, 'plans fixture is empty');

        $required = ['product_slug', 'product_name', 'family', 'base_monthly_price'];
        $stringFields = ['product_slug', 'product_name', 'family'];

        foreach ($plans as $plan) {
            $this->assertIsArray($plan);
            $slug = $plan['product_slug'] ?? '?';

            foreach ($required as $field) {
                $this->assertArrayHasKey($field, $plan, "plan $slug missing $field");
            }

            foreach ($stringFields as $field) {
                if (isset($plan[$field])) {
                    $this->assertIsString($plan[$field], "plan $slug: $field should be string");
                }
            }

            $this->assertIsNumeric($plan['base_monthly_price'], "plan $slug: base_monthly_price should be numeric");

            if (isset($plan['periods']) && is_array($plan['periods'])) {
                foreach ($plan['periods'] as $p) {
                    $this->assertIsArray($p);
                    $this->assertArrayHasKey('months', $p);
                    $this->assertArrayHasKey('effective_monthly', $p);
                    $validMonths = [1, 3, 6, 12, 24, 36];
                    $this->assertContains($p['months'], $validMonths, "plan $slug: unexpected months {$p['months']}");
                }
            }
        }
    }

    // ─── Meta contract ─────────────────────────────────────────────────────

    public function testMetaResponseHasRequiredFields(): void
    {
        $meta = $this->loadFixture('meta.golden.json');
        $this->assertIsArray($meta);

        $required = ['scraper_version', 'schema_version', 'snapshot_meta', 'data_dir'];
        foreach ($required as $field) {
            $this->assertArrayHasKey($field, $meta, "meta missing $field");
        }

        $this->assertIsString($meta['scraper_version']);
        $this->assertIsString($meta['schema_version']);
        $this->assertNotEmpty($meta['scraper_version']);
    }

    // ─── FX contract ───────────────────────────────────────────────────────

    public function testFxResponseHasExpectedCurrencyCodes(): void
    {
        $fx = $this->loadFixture('fx.golden.json');
        $this->assertIsArray($fx);

        $this->assertArrayHasKey('base', $fx);
        $this->assertEquals('EUR', $fx['base']);

        if (isset($fx['rates']) && is_array($fx['rates'])) {
            foreach ($fx['rates'] as $code => $rate) {
                $this->assertIsString($code);
                $this->assertMatchesRegularExpression('/^[A-Z]{3}$/', $code, "currency code $code should be 3 uppercase letters");
                $this->assertIsNumeric($rate, "rate for $code should be numeric");
            }
        }
    }

    // ─── Quote request/response contract ───────────────────────────────────

    public function testQuoteResponseShapeMatchesApi(): void
    {
        $quote = $this->loadFixture('quote.golden.json');
        $this->assertIsArray($quote);

        $required = [
            'plan_slug', 'period_months', 'currency',
            'base_monthly_eur', 'configured_monthly_eur', 'setup_fee_eur',
            'gst_amount_eur', 'fx_rate', 'fx_markup',
            'final_monthly', 'final_total', 'breakdown',
        ];

        foreach ($required as $field) {
            $this->assertArrayHasKey($field, $quote, "quote missing $field");
        }

        $this->assertIsString($quote['plan_slug']);
        $this->assertIsInt($quote['period_months']);
        $this->assertMatchesRegularExpression('/^[A-Z]{3}$/', $quote['currency']);

        $this->assertIsArray($quote['breakdown']);
        $this->assertNotEmpty($quote['breakdown']);

        foreach ($quote['breakdown'] as $entry) {
            $this->assertIsString($entry);
        }

        $this->assertGreaterThan(0.0, $quote['final_monthly'], 'final_monthly must be positive');
        $this->assertGreaterThan(0.0, $quote['final_total'], 'final_total must be positive');
    }

    public function testQuoteRequestContractMatchesServer(): void
    {
        $quote = $this->loadFixture('quote.golden.json');

        // The QuoteRequest struct in Rust expects:
        // plan_slug, period_months, currency, selections, gst, fx_markup, fx_rate
        // The QuoteResponse includes all of PlanSlug, PeriodMonths, Currency
        // so a valid request+response round-trip confirms the contract.

        $this->assertArrayHasKey('plan_slug', $quote);
        $this->assertArrayHasKey('period_months', $quote);
        $this->assertArrayHasKey('currency', $quote);
        $this->assertArrayHasKey('base_monthly_eur', $quote);
        $this->assertArrayHasKey('final_monthly', $quote);
        $this->assertArrayHasKey('breakdown', $quote);
    }

    // ─── Configurator contract ─────────────────────────────────────────────

    public function testConfiguratorDimensionsHaveRequiredFields(): void
    {
        $configurator = $this->loadFixture('configurator.golden.json');
        $this->assertIsArray($configurator);

        $required = ['slug', 'options', 'base_monthly_price'];
        foreach ($required as $field) {
            $this->assertArrayHasKey($field, $configurator, "configurator missing $field");
        }

        $this->assertIsArray($configurator['options']);
        $this->assertNotEmpty($configurator['options'], 'configurator options should not be empty');

        foreach ($configurator['options'] as $dimKey => $dimOptions) {
            $this->assertIsString($dimKey);
            $this->assertIsArray($dimOptions);

            foreach ($dimOptions as $opt) {
                $this->assertIsArray($opt);
                $this->assertArrayHasKey('option_label', $opt, "dimension $dimKey: option missing option_label");
                $this->assertArrayHasKey('dimension', $opt, "dimension $dimKey: option missing dimension");
                $this->assertArrayHasKey('monthly_price_delta', $opt, "dimension $dimKey: option missing monthly_price_delta");
            }
        }
    }

    // ─── Pricing invariants on golden data ─────────────────────────────────

    public function testNoNegativePricesInGoldenFixtures(): void
    {
        foreach (['plans.golden.json', 'quote.golden.json'] as $name) {
            $data = $this->loadFixture($name);
            $this->assertNoNegativePriceValues($data, $name);
        }
    }

    /**
     * Recursively scan for negative numeric values.
     *
     * @param array<string,mixed>|list<mixed> $data
     */
    private function assertNoNegativePriceValues($data, string $source): void
    {
        foreach ($data as $key => $value) {
            if (is_float($value) || is_int($value)) {
                $this->assertGreaterThanOrEqual(
                    0.0,
                    (float) $value,
                    "$source: negative value at $key = $value"
                );
            } elseif (is_array($value)) {
                $this->assertNoNegativePriceValues($value, "$source.$key");
            }
        }
    }

    // ─── Currency format validation ────────────────────────────────────────

    public function testCurrencyFieldsAreThreeCharUppercase(): void
    {
        $quote = $this->loadFixture('quote.golden.json');
        $this->assertMatchesRegularExpression('/^[A-Z]{3}$/', $quote['currency']);

        $fx = $this->loadFixture('fx.golden.json');
        if (isset($fx['rates']) && is_array($fx['rates'])) {
            foreach (array_keys($fx['rates']) as $code) {
                $this->assertMatchesRegularExpression('/^[A-Z]{3}$/', $code, "FX code $code");
            }
        }
    }
}
