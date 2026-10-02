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

    /** @param array<string,mixed> $opts */
    private function agent(array $opts = []): TinyFishAgentSource
    {
        return new TinyFishAgentSource(new SourceConfig('tinyfish_agent', '', 'AG-KEY', $opts), $this->http, 60, $this->sleeper());
    }

    public function testTinyFishAgentRunsAsyncPollsAndPricesPerStep(): void
    {
        $this->http->push(200, json_encode(['run_id' => 'run_abc123']));
        // while RUNNING num_of_steps is a LIST of step objects
        $this->http->push(200, json_encode(['status' => 'PENDING', 'num_of_steps' => []]));
        $this->http->push(200, json_encode(['status' => 'RUNNING', 'num_of_steps' => [['a' => 1], ['a' => 2], ['a' => 3]]]));
        $this->http->push(200, json_encode(['status' => 'COMPLETED', 'num_of_steps' => 12, 'result' => ['products' => [['slug' => 'x']]]]));
        $src = $this->agent([
            'output_schema' => [
                '$schema' => 'http://json-schema.org/draft-07/schema#', 'type' => 'object', 'title' => 'Plans',
                'properties' => ['products' => ['type' => 'array', 'description' => 'd', 'items' => ['type' => 'object', 'properties' => ['slug' => ['type' => 'string']]]]],
                'required' => ['products', 'ghost'],
            ],
        ]);
        $this->assertTrue($src->manualOnly());
        $r = $src->fetchFamilyPage(self::URL);

        $this->assertSame(12 * 16000, $r->costMicro, 'num_of_steps (int on a terminal run) x $0.016');
        $this->assertSame('provider-json', $r->strategyHint);
        $this->assertNull($r->html);
        $this->assertSame('tinyfish-run:run_abc123', $r->servedBy, 'the run id is kept so a run can be recovered');
        $this->assertSame(['POST', 'GET', 'GET', 'GET'], array_column($this->http->calls, 'method'));
        $this->assertSame('https://agent.tinyfish.ai/v1/automation/run-async', $this->http->calls[0]['url']);
        $this->assertSame('https://agent.tinyfish.ai/v1/runs/run_abc123', $this->http->calls[1]['url']);
        $this->assertSame([10, 10], $this->sleeps, 'polled every 10 s');
        $this->assertSame('AG-KEY', $this->header(0, 'X-API-Key'));
        $body = json_decode((string) $this->http->calls[0]['body'], true);
        $this->assertSame(40, $body['max_steps']);
        $this->assertSame(self::URL, $body['url']);
        $this->assertSame(
            ['type' => 'object', 'properties' => ['products' => ['type' => 'array', 'items' => ['type' => 'object', 'properties' => ['slug' => ['type' => 'string']]]]], 'required' => ['products']],
            $body['output_schema'],
            'rejected keys stripped, unknown required names dropped'
        );
    }

    public function testTinyFishAgentNeverRetriesATransportTimeoutOrA5xx(): void
    {
        // a retried POST /run launched duplicate paid runs (4 billed instead of 2)
        $this->http->push(0, '', [], 28, 'Operation timed out after 60001 milliseconds');
        $src = $this->agent();
        try {
            $src->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertStringContainsString('transport error', $e->getMessage());
        }
        $this->assertCount(1, $this->http->calls, 'exactly one launch attempt, no retry');
        $this->assertSame([], $this->sleeps);

        $this->http->calls = [];
        $this->http->push(503, 'upstream down');
        try {
            $this->agent()->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertSame(503, $e->httpStatus());
        }
        $this->assertCount(1, $this->http->calls);
        $this->assertSame('POST', $this->http->calls[0]['method']);

        // and a launch that answers without a run id is not retried either
        $this->http->calls = [];
        $this->http->push(200, json_encode(['status' => 'ok']));
        try {
            $this->agent()->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertStringContainsString('no usable run_id', $e->getMessage());
        }
        $this->assertCount(1, $this->http->calls);
    }

    public function testTinyFishAgentFailedAndCancelledRunsCarryTheRunIdAndSpend(): void
    {
        foreach (['FAILED', 'CANCELLED'] as $state) {
            $this->http->calls = [];
            $this->http->push(200, json_encode(['run_id' => 'r-9']));
            $this->http->push(200, json_encode(['status' => $state, 'num_of_steps' => 7, 'error' => 'blocked AG-KEY']));
            try {
                $this->agent()->fetchFamilyPage(self::URL);
                $this->fail('expected SourceException');
            } catch (SourceException $e) {
                $this->assertStringContainsString(strtolower($state), $e->getMessage());
                $this->assertStringNotContainsString('AG-KEY', $e->getMessage());
                $this->assertSame('r-9', $e->providerRunId());
                $this->assertSame(7 * 16000, $e->costMicro());
            }
            $this->assertCount(2, $this->http->calls, 'no cancel call for a run that already ended');
        }
    }

    public function testTinyFishAgentCancelsWhenMaxStepsOrThePollBudgetIsExceeded(): void
    {
        $steps = [];
        for ($i = 0; $i < 5; $i++) {
            $steps[] = ['n' => $i];
        }
        $this->http->push(200, json_encode(['run_id' => 'r1']));
        $this->http->push(200, json_encode(['status' => 'RUNNING', 'num_of_steps' => $steps]));
        $this->http->push(200, json_encode(['status' => 'CANCELLED']));
        try {
            $this->agent(['max_steps' => 3])->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertStringContainsString('exceeded max_steps (5 > 3)', $e->getMessage());
            $this->assertSame('r1', $e->providerRunId());
            $this->assertSame(5 * 16000, $e->costMicro());
        }
        $this->assertSame('https://agent.tinyfish.ai/v1/runs/r1/cancel', $this->http->calls[2]['url']);
        $this->assertSame('POST', $this->http->calls[2]['method']);

        // poll budget: 25 s budget at a 10 s interval -> polls at 0, 10, 20, 30 s then cancel
        $this->http->calls = [];
        $this->sleeps = [];
        $this->http->push(200, json_encode(['run_id' => 'r2']));
        for ($i = 0; $i < 4; $i++) {
            $this->http->push(200, json_encode(['status' => 'RUNNING', 'num_of_steps' => []]));
        }
        $this->http->push(200, '{}'); // cancel
        try {
            $this->agent(['poll_budget_sec' => 25])->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertStringContainsString('after 25 s; cancelled', $e->getMessage());
            $this->assertSame('r2', $e->providerRunId());
        }
        $this->assertSame([10, 10, 10], $this->sleeps);
        $last = $this->http->calls[count($this->http->calls) - 1];
        $this->assertSame(['POST', 'https://agent.tinyfish.ai/v1/runs/r2/cancel'], [$last['method'], $last['url']]);
        $this->assertCount(1, array_filter($this->http->calls, static function ($c) { return $c['method'] === 'POST' && strpos($c['url'], '/automation/') !== false; }), 'the run was launched once');
    }

    public function testTinyFishAgentToleratesTransientPollFailuresButNotForever(): void
    {
        $this->http->push(200, json_encode(['run_id' => 'r3']));
        $this->http->push(0, '', [], 28, 'timed out');
        $this->http->push(502, 'bad gateway');
        $this->http->push(200, json_encode(['status' => 'COMPLETED', 'num_of_steps' => 4, 'result' => ['a' => 1]]));
        $r = $this->agent()->fetchFamilyPage(self::URL);
        $this->assertSame(4 * 16000, $r->costMicro);
        $this->assertSame(['POST', 'GET', 'GET', 'GET'], array_column($this->http->calls, 'method'), 'polling does not relaunch');

        $this->http->calls = [];
        $this->http->push(200, json_encode(['run_id' => 'r4']));
        $this->http->push(0, '', [], 28, 'timed out');
        $this->http->push(0, '', [], 28, 'timed out');
        $this->http->push(0, '', [], 28, 'timed out');
        $this->http->push(200, '{}'); // cancel
        try {
            $this->agent()->fetchFamilyPage(self::URL);
            $this->fail('expected SourceException');
        } catch (SourceException $e) {
            $this->assertSame('r4', $e->providerRunId());
        }
        $this->assertSame('/cancel', substr($this->http->calls[4]['url'], -7));
    }

    public function testSanitizeSchemaStripsRejectedKeysAtEveryLevelButKeepsPropertyNames(): void
    {
        $in = [
            '$schema' => 'x', '$id' => 'y', 'title' => 'T', 'description' => 'D', 'examples' => [1], 'default' => 1, '$comment' => 'c',
            'type' => 'object',
            'properties' => [
                'title' => ['type' => 'string', 'description' => 'a property called title', 'default' => 'x', 'enum' => ['title', 'description']],
                'description' => ['type' => 'string'],
                'default' => ['type' => 'number', '$comment' => 'zz'],
                'plans' => [
                    'type' => 'array', 'title' => 'P',
                    'items' => ['type' => 'object', 'description' => 'd', 'properties' => ['slug' => ['type' => 'string', 'examples' => ['a']], 'price' => ['type' => 'number']], 'required' => ['slug', 'nope']],
                ],
                'either' => ['anyOf' => [['type' => 'string', 'title' => 'S'], ['type' => 'null', '$comment' => 'n']]],
            ],
            'required' => ['title', 'plans', 'missing'],
            'additionalProperties' => ['type' => 'string', 'description' => 'x'],
        ];
        $out = TinyFishAgentSource::sanitizeSchema($in);

        foreach (['$schema', '$id', 'title', 'description', 'examples', 'default', '$comment'] as $k) {
            $this->assertArrayNotHasKey($k, $out, $k . ' at the root');
        }
        $this->assertSame(['title', 'description', 'default', 'plans', 'either'], array_keys($out['properties']), 'property NAMES survive');
        $this->assertSame(['type' => 'string', 'enum' => ['title', 'description']], $out['properties']['title'], 'enum literals are never rewritten');
        $this->assertSame(['type' => 'number'], $out['properties']['default']);
        $this->assertSame(['type' => 'array', 'items' => ['type' => 'object', 'properties' => ['slug' => ['type' => 'string'], 'price' => ['type' => 'number']], 'required' => ['slug']]], $out['properties']['plans']);
        $this->assertSame([['type' => 'string'], ['type' => 'null']], $out['properties']['either']['anyOf']);
        $this->assertSame(['title', 'plans'], $out['required'], 'every required name must exist in properties');
        $this->assertSame(['type' => 'string'], $out['additionalProperties']);

        // `required` with nothing declared disappears; empty properties stay an object
        $this->assertSame(['type' => 'object'], TinyFishAgentSource::sanitizeSchema(['type' => 'object', 'required' => ['a']]));
        $empty = TinyFishAgentSource::sanitizeSchema(['type' => 'object', 'properties' => []]);
        $this->assertSame('{"type":"object","properties":{}}', json_encode($empty));
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
