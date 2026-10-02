<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Installer;
use ContaboPricing\Scrape\DataSourcesForm;
use ContaboPricing\Scrape\ScrapeSettings;
use ContaboPricing\Scrape\SourceConfigRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class DataSourcesFormTest extends TestCase
{
    /** @var ScrapeSettings */
    private $s;
    /** @var SourceConfigRepository */
    private $repo;

    protected function setUp(): void
    {
        Capsule::reset();
        (new Installer())->migrateTo15();
        $this->s = new ScrapeSettings();
        $this->repo = new SourceConfigRepository();
    }

    protected function tearDown(): void
    {
        unset($GLOBALS['__cb_token_fail'], $_SERVER['REQUEST_METHOD']);
    }

    private function form(): DataSourcesForm
    {
        return new DataSourcesForm($this->s, $this->repo);
    }

    public function testKeysAreSealedOnSaveAndNeverStoredInPlaintext(): void
    {
        $r = $this->form()->apply([
            'src' => ['alterlab' => ['enabled' => '1', 'api_key' => 'sk-live-ABCD1234']],
            'jev_api_key' => 'jev-live-WXYZ9876',
        ]);
        $this->assertTrue($r['ok']);
        $this->assertSame(['alterlab', 'jev'], $r['keys_replaced']);
        $row = (array) $this->repo->find('alterlab');
        $this->assertStringStartsWith('ENC:', (string) $row['api_key_enc']);
        $this->assertStringNotContainsString('ABCD1234', (string) $row['api_key_enc']);
        $this->assertSame('sk-live-ABCD1234', $this->repo->toConfig($row)->apiKey);
        $stored = (string) Capsule::table('mod_contabo_settings')->where('key', 'scrape.jev_api_key')->value('value');
        $this->assertStringStartsWith('ENC:', $stored);
        $this->assertStringNotContainsString('WXYZ9876', $stored);
        $this->assertSame('jev-live-WXYZ9876', $this->s->jevApiKey());
        foreach (Capsule::$tables as $rows) {
            foreach ($rows as $rowData) {
                foreach ($rowData as $v) {
                    $this->assertNotSame('sk-live-ABCD1234', $v);
                }
            }
        }
    }

    public function testBlankKeyKeepsTheStoredKey(): void
    {
        $this->form()->apply(['src' => ['alterlab' => ['api_key' => 'sk-keep-1111']], 'jev_api_key' => 'jev-keep-2222']);
        $r = $this->form()->apply(['src' => ['alterlab' => ['enabled' => '1', 'api_key' => '', 'priority' => '3']], 'jev_api_key' => '']);
        $this->assertSame([], $r['keys_replaced']);
        $this->assertSame('sk-keep-1111', $this->repo->toConfig((array) $this->repo->find('alterlab'))->apiKey);
        $this->assertSame('jev-keep-2222', $this->s->jevApiKey());
        $this->assertSame(3, (int) $this->repo->find('alterlab')['priority']);
    }

    public function testNumericCoercion(): void
    {
        $this->form()->apply([
            'monthly_budget_usd' => '0.75',
            'per_run_cap_usd' => '-5',
            'min_plans_cloud_vps' => '999',
            'min_plans_storage_vps' => 'abc',
            'max_drop_pct' => '150',
            'max_price_change_pct' => '12.5',
            'jev_confidence_min' => '1.5',
            'src' => ['alterlab' => [
                'monthly_budget_usd' => '2', 'per_run_cap_usd' => 'garbage', 'priority' => 'x',
            ]],
        ]);
        $this->assertSame(750000, $this->s->monthlyBudgetMicro());
        $this->assertSame(0, $this->s->perRunCapMicro(), 'negative clamps to zero');
        $this->assertSame(['Cloud VPS' => 100, 'Storage VPS' => 5, 'Cloud VDS' => 5], $this->s->minPlansByFamily(), 'clamped / non-numeric keeps the old value');
        $this->assertSame(100.0, $this->s->maxDropPct());
        $this->assertSame(12.5, $this->s->maxPriceChangePct());
        $this->assertSame(1.0, $this->s->jevConfidenceMin());
        $row = (array) $this->repo->find('alterlab');
        $this->assertSame(2000000, (int) $row['monthly_budget_micro']);
        $this->assertSame(50000, (int) $row['per_run_cap_micro'], 'garbage keeps the seeded value');
        $this->assertNull($row['priority']);
    }

    public function testCheckboxesAndEnums(): void
    {
        $this->form()->apply(['scrape_enabled' => '1', 'rank_mode' => 'manual', 'plan_source' => 'native', 'jev_enabled' => '1']);
        $this->assertTrue($this->s->enabled());
        $this->assertSame('manual', $this->s->rankMode());
        $this->assertSame('native', $this->s->planSource());
        $this->assertTrue($this->s->jevEnabled());
        $this->assertTrue($this->s->requireHumanReview() === false, 'unchecked box means off');
        $this->form()->apply(['rank_mode' => 'bogus', 'plan_source' => 'bogus', 'require_human_review' => '1']);
        $this->assertFalse($this->s->enabled());
        $this->assertSame('auto', $this->s->rankMode());
        $this->assertSame('local', $this->s->planSource());
        $this->assertTrue($this->s->requireHumanReview());
    }

    public function testInvalidRowsAreRejectedWithoutPartialWrites(): void
    {
        $r = $this->form()->apply(['src' => [
            'alterlab' => ['enabled' => '1', 'base_url' => 'http://insecure.example', 'api_key' => 'sk-should-not-save'],
            'treg_litescrape' => ['enabled' => '1', 'options_json' => '{not json'],
            'ghost' => ['enabled' => '1'],
            'tinyfish_fetch' => ['enabled' => '1', 'api_key' => "bad key\n"],
        ]]);
        $this->assertFalse($r['ok']);
        $this->assertCount(4, $r['errors']);
        $this->assertSame(0, (int) $this->repo->find('alterlab')['enabled']);
        $this->assertSame('', (string) ($this->repo->find('alterlab')['api_key_enc'] ?? ''));
        $this->assertSame(0, (int) $this->repo->find('treg_litescrape')['enabled']);
        $this->assertSame(0, (int) $this->repo->find('tinyfish_fetch')['enabled']);
    }

    public function testOptionsJsonRoundTrip(): void
    {
        $this->form()->apply(['src' => ['tinyfish_agent' => ['options_json' => '{"max_steps": 25}']]]);
        $this->assertSame(25, $this->repo->toConfig((array) $this->repo->find('tinyfish_agent'))->options['max_steps']);
    }

    public function testUsdToMicro(): void
    {
        $this->assertSame(700, DataSourcesForm::usdToMicro('0.0007'));
        $this->assertSame(0, DataSourcesForm::usdToMicro('-1'));
        $this->assertNull(DataSourcesForm::usdToMicro(''));
        $this->assertNull(DataSourcesForm::usdToMicro('1e'));
    }
}
