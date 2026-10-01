<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Catalog\CatalogReader;
use ContaboPricing\CatalogImportService;
use ContaboPricing\Fx\FxService;
use ContaboPricing\LocalPlanSource;
use ContaboPricing\PlanSource;
use ContaboPricing\PlanSourceFactory;
use ContaboPricing\Quote\QuoteService;
use ContaboPricing\RequestExecutor;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class LocalPlanSourceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
        Capsule::$tables['mod_contabo_settings'] = [];
    }

    private function source(): LocalPlanSource
    {
        $exec = new class implements RequestExecutor {
            public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
            {
                return [200, '{"base":"EUR","rates":{"INR":112.317}}', 0, ''];
            }
        };
        return new LocalPlanSource(new CatalogReader(), new FxService($exec), new QuoteService());
    }

    private function importFixture(): void
    {
        $c = json_decode((string) file_get_contents(__DIR__ . '/fixtures/api/catalog.json'), true);
        (new CatalogImportService())->import($c, 1);
    }

    public function testFactoryReturnsLocalSource(): void
    {
        $this->assertInstanceOf(PlanSource::class, PlanSourceFactory::fromSettings());
        $this->assertInstanceOf(LocalPlanSource::class, PlanSourceFactory::fromSettings());
    }

    public function testUnknownPlanThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->source()->plan('nope');
    }

    public function testQuoteAcceptsAdminUiKeysAndFetchesFx(): void
    {
        $this->importFixture();
        $s = $this->source();
        $this->assertSame('cloud-vps-10', $s->plan('cloud-vps-10')['product_slug']);
        $q = $s->quote([
            'plan_slug' => 'cloud-vps-10',
            'period_months' => 12,
            'currency_iso' => 'INR',
            'apply_gst' => true,
            'fx_markup_pct' => 3.5,
        ]);
        $golden = json_decode((string) file_get_contents(__DIR__ . '/fixtures/api/quote.json'), true);
        $this->assertEqualsWithDelta($golden['final_monthly'], $q['final_monthly'], 1e-9);
        $this->assertEqualsWithDelta(112.317, $s->fx()['eurInr'], 1e-9);
    }

    public function testConfiguratorCarriesContractPeriodsFromPlan(): void
    {
        $this->importFixture();
        $cfg = $this->source()->configurator('cloud-vps-10');
        $this->assertSame(12, $cfg['contract_periods'][0]['months']);
        $this->assertSame('Cloud VPS 10', $cfg['title']);
    }
}
