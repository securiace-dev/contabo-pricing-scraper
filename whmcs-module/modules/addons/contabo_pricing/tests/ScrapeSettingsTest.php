<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Scrape\PlanUrlList;
use ContaboPricing\Scrape\ScrapeSettings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class ScrapeSettingsTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testDefaults(): void
    {
        $s = new ScrapeSettings();
        $this->assertFalse($s->enabled());
        $this->assertSame('auto', $s->rankMode());
        $this->assertSame(500000, $s->monthlyBudgetMicro());
        $this->assertSame(50000, $s->perRunCapMicro());
        $this->assertSame(
            ['Cloud VPS' => 6, 'Storage VPS' => 5, 'Cloud VDS' => 5],
            $s->minPlansByFamily()
        );
        $this->assertSame(20.0, $s->maxDropPct());
        $this->assertSame(50.0, $s->maxPriceChangePct());
        $this->assertTrue($s->requireHumanReview());
        $this->assertFalse($s->jevEnabled());
        $this->assertSame('jev-1.13.0', $s->jevModel());
        $this->assertSame(0.80, $s->jevConfidenceMin());
        $this->assertSame('local', $s->planSource());
        $this->assertSame(PlanUrlList::DEFAULT_URLS, $s->planUrls());
        $this->assertCount(16, $s->planUrls());
    }

    public function testSetPersistsAndUpserts(): void
    {
        $s = new ScrapeSettings();
        $s->set('scrape.enabled', '1');
        $s->set('scrape.enabled', '1');
        $s->set('scrape.max_drop_pct', '35');
        $this->assertTrue($s->enabled());
        $this->assertSame(35.0, $s->maxDropPct());
        $this->assertSame(1, Capsule::table('mod_contabo_settings')->where('key', 'scrape.enabled')->count());
    }

    public function testUnknownKeyRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new ScrapeSettings())->set('scrape.bogus', 'x');
    }

    public function testJevKeyIsSealedAndNeverInAll(): void
    {
        $s = new ScrapeSettings();
        $s->setJevApiKey('jev-key-ABCDEFGH');
        $stored = (string) Capsule::table('mod_contabo_settings')->where('key', 'scrape.jev_api_key')->value('value');
        $this->assertStringStartsWith('ENC:', $stored);
        $this->assertStringNotContainsString('jev-key-ABCDEFGH', $stored);
        $this->assertSame('jev-key-ABCDEFGH', $s->jevApiKey());
        $this->assertTrue($s->hasJevApiKey());
        $this->assertSame('', $s->all()['scrape.jev_api_key']);

        $s->setJevApiKey('');
        $this->assertSame('jev-key-ABCDEFGH', $s->jevApiKey(), 'blank keeps the existing key');
    }

    public function testPlanUrlsFallbackAndFiltering(): void
    {
        $s = new ScrapeSettings();
        $s->set('scrape.plan_urls_json', '{"not":"a list"}');
        $this->assertSame(PlanUrlList::DEFAULT_URLS, $s->planUrls());
        $s->set('scrape.plan_urls_json', json_encode(['https://contabo.com/en/vps/cloud-vps-10/', 'http://evil.example/x']));
        $this->assertSame(['https://contabo.com/en/vps/cloud-vps-10/'], $s->planUrls());
    }
}
