<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\AlterLabSource;
use ContaboPricing\Scrape\FixtureSource;
use ContaboPricing\Scrape\SourceConfig;
use ContaboPricing\Scrape\SourceException;
use ContaboPricing\Scrape\TinyFishAgentSource;
use ContaboPricing\Scrape\TinyFishFetchSource;
use ContaboPricing\Scrape\TregRoutedSource;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/FakeHeaderExecutor.php';

final class ProviderAdaptersTest extends TestCase
{
    private const URL = 'https://contabo.com/en/vps/cloud-vps-10/';

    /** @var FakeHeaderExecutor */
    private $http;
    /** @var list<int> */
    private $sleeps = [];

    protected function setUp(): void
    {
        $this->http = new FakeHeaderExecutor();
        $this->sleeps = [];
    }

    private function sleeper(): callable
    {
        return function (int $s): void {
            $this->sleeps[] = $s;
        };
    }

    private function header(int $call, string $name): ?string
    {
        foreach ($this->http->calls[$call]['headers'] as $h) {
            if (stripos($h, $name . ':') === 0) {
                return trim(substr($h, strlen($name) + 1));
            }
        }
        return null;
    }

    // ── AlterLab ────────────────────────────────────────────────────────────

    public function testAlterLabHappyPathSendsAuthAndBody(): void
    {
        $this->http->push(200, json_encode([
            'job_id' => 'j1', 'final_url' => 'https://contabo.com/en/vps/cloud-vps-core-4/', 'status_code' => 200,
            'content' => ['html' => '<html>ok</html>', 'markdown' => 'x'],
            'billing' => ['final_cost_microcents' => 4000, 'tier_used' => 4],
        ]));
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'KEY-AL-1', []), $this->http, 30, $this->sleeper());
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertSame('<html>ok</html>', $r->html);
        $this->assertSame(4000, $r->costMicro);
        $this->assertSame('https://contabo.com/en/vps/cloud-vps-core-4/', $r->finalUrl);
        $this->assertTrue($src->supportsSapper());
        $this->assertSame(200, $r->httpStatus);
        $this->assertSame('POST', $this->http->calls[0]['method']);
        $this->assertSame('https://api.alterlab.io/api/v1/scrape', $this->http->calls[0]['url']);
        $this->assertSame('KEY-AL-1', $this->header(0, 'X-API-Key'));
        $this->assertSame(['url' => self::URL, 'mode' => 'js'], json_decode((string) $this->http->calls[0]['body'], true));
        $this->assertSame(30, $this->http->calls[0]['timeout']);
    }

    public function testAlterLabPriorCostAndExtractionSchema(): void
    {
        $this->http->push(200, json_encode(['html' => '<p>x</p>']));
        $src = new AlterLabSource(
            new SourceConfig('alterlab', 'https://al.example', 'K', ['extraction_schema' => ['type' => 'object']]),
            $this->http
        );
        $r = $src->fetchFamilyPage(self::URL);
        $this->assertSame(4000, $r->costMicro, 'falls back to the tier-4 prior');
        $body = json_decode((string) $this->http->calls[0]['body'], true);
        $this->assertSame(['type' => 'object'], $body['extraction_schema']);
        $this->assertSame('https://al.example/api/v1/scrape', $this->http->calls[0]['url']);
    }

    public function testAlterLabModeOptionOverrides(): void
    {
        $this->http->push(200, json_encode(['content' => ['html' => '<p>x</p>']]));
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'K', ['mode' => 'auto']), $this->http);
        $src->fetchFamilyPage(self::URL);
        $this->assertSame('auto', json_decode((string) $this->http->calls[0]['body'], true)['mode']);
    }

    public function testAlterLab202ThenPoll(): void
    {
        $this->http->push(202, json_encode(['job_id' => 'job-9']));
        $this->http->push(200, json_encode(['status' => 'running']));
        $this->http->push(200, json_encode(['status' => 'completed', 'result' => ['html' => '<html>done</html>'], 'cost_usd' => 0.0004]));
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'K', []), $this->http, 30, $this->sleeper());
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertSame('<html>done</html>', $r->html);
        $this->assertSame(400, $r->costMicro);
        $this->assertSame([5, 5], $this->sleeps);
        $this->assertSame('GET', $this->http->calls[1]['method']);
        $this->assertSame('https://api.alterlab.io/api/v1/jobs/job-9', $this->http->calls[1]['url']);
        $this->assertSame('K', $this->header(1, 'X-API-Key'));
    }

    public function testAlterLabPollTimeoutAfter85Seconds(): void
    {
        $this->http->push(202, json_encode(['id' => 'j']));
        for ($i = 0; $i < 20; $i++) {
            $this->http->push(200, json_encode(['status' => 'running']));
        }
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'K', []), $this->http, 30, $this->sleeper());
        try {
            $src->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertStringContainsString('85s', $e->getMessage());
        }
        $this->assertSame(17, count($this->sleeps));
        $this->assertSame(85, array_sum($this->sleeps));
    }

    public function testAlterLabNon2xxThrowsWithoutLeakingKey(): void
    {
        $this->http->push(401, 'bad key SECRET-KEY-VALUE');
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'SECRET-KEY-VALUE', []), $this->http);
        try {
            $src->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertSame(401, $e->httpStatus());
            $this->assertStringNotContainsString('SECRET-KEY-VALUE', $e->getMessage());
        }
    }

    public function testAlterLabFailedJobThrows(): void
    {
        $this->http->push(202, json_encode(['job_id' => 'j']));
        $this->http->push(200, json_encode(['status' => 'failed', 'error' => 'blocked']));
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'K', []), $this->http, 30, $this->sleeper());
        $this->expectException(SourceException::class);
        $src->fetchFamilyPage(self::URL);
    }

    public function testNonHttpsBaseUrlRejected(): void
    {
        $src = new AlterLabSource(new SourceConfig('alterlab', 'http://169.254.169.254', 'K', []), $this->http);
        $this->expectException(SourceException::class);
        $src->fetchFamilyPage(self::URL);
    }

    public function testTransportErrorMapsToSourceException(): void
    {
        $this->http->push(0, '', [], 28, 'Operation timed out');
        $src = new AlterLabSource(new SourceConfig('alterlab', '', 'K', []), $this->http);
        $this->expectException(SourceException::class);
        $this->expectExceptionMessageMatches('/transport error/');
        $src->fetchFamilyPage(self::URL);
    }

    // ── TinyFish Fetch ──────────────────────────────────────────────────────

    public function testTinyFishFetchHappyPath(): void
    {
        $this->http->push(200, json_encode(['results' => [['url' => self::URL, 'final_url' => self::URL, 'title' => 'Cloud VPS 4', 'text' => 'Cloud VPS 4 EUR 4.50', 'format' => 'html']], 'errors' => []]));
        $src = new TinyFishFetchSource(new SourceConfig('tinyfish_fetch', '', 'TF-KEY', []), $this->http);
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertSame('Cloud VPS 4 EUR 4.50', $r->html);
        $this->assertFalse($src->supportsSapper());
        $this->assertSame(0, $r->costMicro);
        $this->assertSame('https://api.fetch.tinyfish.ai', $this->http->calls[0]['url']);
        $this->assertSame('TF-KEY', $this->header(0, 'X-API-Key'));
        $this->assertSame(
            ['urls' => [self::URL], 'format' => 'html', 'ttl' => 0],
            json_decode((string) $this->http->calls[0]['body'], true)
        );
    }

    public function testTinyFishFetchPerUrlErrorThrows(): void
    {
        $this->http->push(200, json_encode(['results' => [], 'errors' => [['url' => self::URL, 'error' => 'blocked']]]));
        $src = new TinyFishFetchSource(new SourceConfig('tinyfish_fetch', '', 'K', []), $this->http);
        $this->expectException(SourceException::class);
        $this->expectExceptionMessageMatches('/blocked/');
        $src->fetchFamilyPage(self::URL);
    }

    public function testTinyFishFetchJsonFormatIsDropped(): void
    {
        $src = new TinyFishFetchSource(new SourceConfig('tinyfish_fetch', '', 'K', []), $this->http);
        $this->expectException(SourceException::class);
        $src->fetchFamilyPage(self::URL, ['format' => 'json']);
    }

    public function testTinyFishFetch500Throws(): void
    {
        $this->http->push(500, 'oops');
        $src = new TinyFishFetchSource(new SourceConfig('tinyfish_fetch', '', 'K', []), $this->http);
        $this->expectException(SourceException::class);
        $src->fetchFamilyPage(self::URL);
    }

    // ── Treg ────────────────────────────────────────────────────────────────

    public function testTregAnyapiDefaultsMatchSpike0(): void
    {
        $this->http->push(
            200,
            json_encode(['output' => ['data' => ['html' => '<html><script>__SAPPER__=1</script></html>']]]),
            ['x-treg-cost-micro' => '700']
        );
        $src = new TregRoutedSource(new SourceConfig('treg_anyapi', '', 'TREG-TOKEN', []), $this->http);
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertStringContainsString('__SAPPER__', (string) $r->html);
        $this->assertSame(700, $r->costMicro);
        $this->assertNull($r->servedBy);
        $this->assertTrue($src->supportsSapper());
        $this->assertSame('https://treg.to/call/anyapi.web.scrape', $this->http->calls[0]['url']);
        $this->assertSame('TREG-TOKEN', $this->header(0, 'X-Treg-Token'));
        $this->assertSame(
            ['url' => self::URL, 'formats' => ['html'], 'onlyMainContent' => false, 'waitFor' => 3000],
            json_decode((string) $this->http->calls[0]['body'], true)
        );
    }

    public function testTregRoutedIdReadsServedByHeaderAndRoutingHeaders(): void
    {
        $this->http->push(
            200,
            json_encode(['output' => ['pages' => [['html' => '<html>routed</html>']]]]),
            ['x-treg-cost-micro' => '2500', 'x-treg-served-by' => 'tinyfish.web.fetch']
        );
        $src = new TregRoutedSource(
            new SourceConfig('treg_anyapi', '', 'T', [
                'endpoint_id' => 'treg.web.extract', 'route_prefer' => 'litescrape', 'route_exclude' => ['badco', 'x'], 'max_cost' => 5000,
            ]),
            $this->http
        );
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertSame('<html>routed</html>', $r->html);
        $this->assertSame('tinyfish.web.fetch', $r->servedBy);
        $this->assertSame(2500, $r->costMicro);
        $this->assertSame('https://treg.to/call/treg.web.extract', $this->http->calls[0]['url']);
        $this->assertSame(['url' => self::URL], json_decode((string) $this->http->calls[0]['body'], true));
        $this->assertSame('litescrape', $this->header(0, 'X-Treg-Route-Prefer'));
        $this->assertSame('badco,x', $this->header(0, 'X-Treg-Route-Exclude'));
        $this->assertSame('5000', $this->header(0, 'X-Treg-Max-Cost'));
    }

    public function testTregLitescrapeRowOptionsAndRawFallback(): void
    {
        $this->http->push(200, json_encode(['raw' => '<html>raw</html>', '_treg' => ['served_by' => 'litescrape']]));
        $src = new TregRoutedSource(
            new SourceConfig('treg_litescrape', '', 'T', [
                'endpoint_id' => 'litescrape.web.fetch.post',
                'query' => ['timeout' => 90],
                'body_params' => ['respond_with' => 'html', 'engine' => 'browser', 'page_timeout' => 60],
                'prior_cost_micro' => 150,
            ]),
            $this->http
        );
        $r = $src->fetchFamilyPage(self::URL);
        $this->assertSame('<html>raw</html>', $r->html);
        $this->assertSame('litescrape', $r->servedBy);
        $this->assertSame(150, $r->costMicro, 'falls back to the configured prior when the cost header is absent');
        $this->assertSame('https://treg.to/call/litescrape.web.fetch.post?timeout=90', $this->http->calls[0]['url']);
        $this->assertSame(
            ['respond_with' => 'html', 'engine' => 'browser', 'page_timeout' => 60, 'url' => self::URL],
            json_decode((string) $this->http->calls[0]['body'], true)
        );
    }

    public function testTregPathOptionOverridesDefault(): void
    {
        $this->http->push(200, json_encode(['output' => '<p>x</p>']));
        $src = new TregRoutedSource(new SourceConfig('treg_anyapi', '', 'T', ['path' => 'v1/run/web.fetch']), $this->http);
        $src->fetchFamilyPage(self::URL);
        $this->assertSame('https://treg.to/v1/run/web.fetch', $this->http->calls[0]['url']);
    }

    public function testTregNoContentThrows(): void
    {
        $this->http->push(200, json_encode(['ok' => true]));
        $src = new TregRoutedSource(new SourceConfig('treg_anyapi', '', 'T', []), $this->http);
        $this->expectException(SourceException::class);
        $src->fetchFamilyPage(self::URL);
    }

    // ── TinyFish Agent ──────────────────────────────────────────────────────

    public function testTinyFishAgentIsManualOnlyAndPricesPerStep(): void
    {
        $this->http->push(200, json_encode(['status' => 'completed', 'result' => ['products' => []] + ['x' => 1], 'steps' => 12]));
        $src = new TinyFishAgentSource(
            new SourceConfig('tinyfish_agent', '', 'AG-KEY', ['output_schema' => ['type' => 'object']]),
            $this->http
        );
        $this->assertTrue($src->manualOnly());
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertSame(12 * 16000, $r->costMicro);
        $this->assertSame('provider-json', $r->strategyHint);
        $this->assertNull($r->html);
        $this->assertSame('https://agent.tinyfish.ai/v1/automation/run', $this->http->calls[0]['url']);
        $this->assertSame('AG-KEY', $this->header(0, 'X-API-Key'));
        $body = json_decode((string) $this->http->calls[0]['body'], true);
        $this->assertSame(40, $body['max_steps']);
        $this->assertSame(['type' => 'object'], $body['output_schema']);
    }

    public function testTinyFishAgentTestConnectionDoesNotCallOut(): void
    {
        $src = new TinyFishAgentSource(new SourceConfig('tinyfish_agent', '', 'K', []), $this->http);
        $this->assertTrue($src->testConnection()['ok']);
        $this->assertSame([], $this->http->calls);
    }

    public function testNonAgentSourcesAreNotManualOnly(): void
    {
        foreach ([AlterLabSource::class, TinyFishFetchSource::class, TregRoutedSource::class] as $cls) {
            $src = new $cls(new SourceConfig('x', '', 'k', []), $this->http);
            $this->assertFalse($src->manualOnly(), $cls);
            $this->assertGreaterThan(0.0, $src->priorSuccessRate());
        }
    }

    public function testTestConnectionReportsFailureWithoutThrowing(): void
    {
        $this->http->push(403, 'nope');
        $src = new TinyFishFetchSource(new SourceConfig('tinyfish_fetch', '', 'K', []), $this->http);
        $res = $src->testConnection();
        $this->assertFalse($res['ok']);
        $this->assertStringContainsString('403', $res['message']);
    }

    // ── Fixture ─────────────────────────────────────────────────────────────

    public function testFixtureSourceServesRecordedHtml(): void
    {
        $src = new FixtureSource(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        $r = $src->fetchFamilyPage(self::URL);
        $this->assertStringContainsString('__SAPPER__', (string) $r->html);
        $this->assertSame('fixture', $r->provider);

        $dir = new FixtureSource(__DIR__ . '/../fixtures/scrape');
        $this->assertNotNull($dir->fetchFamilyPage(self::URL)->html);
        $this->expectException(SourceException::class);
        $dir->fetchFamilyPage('https://contabo.com/en/vps/cloud-vps-20/');
    }
}
