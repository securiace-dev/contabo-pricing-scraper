<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\CatalogImportService;
use ContaboPricing\Installer;
use ContaboPricing\Lock;
use ContaboPricing\Scrape\CostLedger;
use ContaboPricing\Scrape\FetchResult;
use ContaboPricing\Scrape\FixtureSource;
use ContaboPricing\Scrape\RunRepository;
use ContaboPricing\Scrape\ScrapeRunService;
use ContaboPricing\Scrape\ScrapeSettings;
use ContaboPricing\Scrape\SourceException;
use ContaboPricing\Scrape\SourceInterface;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

require_once __DIR__ . '/FakeHeaderExecutor.php';

/** Scriptable source wrapping the recorded fixture. */
final class ScriptedSource implements SourceInterface
{
    /** @var string */ private $id;
    /** @var int */ private $price;
    /** @var string */ private $mode;
    /** @var bool */ private $manual;
    /** @var int */ public $calls = 0;

    /** @param string $mode ok|throw|garbage */
    public function __construct(string $id, int $price, string $mode, bool $manual = false)
    {
        $this->id = $id;
        $this->price = $price;
        $this->mode = $mode;
        $this->manual = $manual;
    }

    public function id(): string { return $this->id; }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $this->calls++;
        if ($this->mode === 'throw') {
            throw new SourceException($this->id . ' HTTP 503: boom', 503);
        }
        if ($this->mode === 'garbage') {
            return new FetchResult($this->id, null, '<html><body>captcha wall</body></html>', null, $this->price, 40, 200);
        }
        $r = (new FixtureSource(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html'))->fetchFamilyPage($url);
        return new FetchResult($this->id, 'served-' . $this->id, $r->html, null, $this->price, 120, 200, null, 'https://contabo.com/en/vps/cloud-vps-core-4/');
    }

    public function testConnection(): array { return ['ok' => true, 'latency_ms' => 0, 'message' => '', 'cost_micro' => 0]; }
    public function priorSuccessRate(): float { return 0.9; }
    public function priceMicroPerPage(): int { return $this->price; }
    public function manualOnly(): bool { return $this->manual; }
    public function supportsSapper(): bool { return true; }
}

/** Lock that is always held by someone else. */
final class ContendedLock extends Lock
{
    protected function tryGetLock(string $name): ?bool
    {
        return false;
    }
}

final class ScrapeRunServiceTest extends TestCase
{
    /** @var ScrapeSettings */
    private $s;
    /** @var FakeHeaderExecutor */
    private $http;

    protected function setUp(): void
    {
        Capsule::reset();
        (new Installer())->migrateTo15();
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
        $this->s = new ScrapeSettings();
        $this->s->set('scrape.require_human_review', '0');
        $this->http = new FakeHeaderExecutor();
    }

    private function svc(?Lock $lock = null): ScrapeRunService
    {
        return new ScrapeRunService($this->s, $this->http, new CostLedger(), new CatalogImportService(), $lock);
    }

    /** @return array<string,mixed> */
    private function attemptRows(int $runId): array
    {
        return (new RunRepository())->attempts($runId);
    }

    public function testWaterfallFallsThroughToSecondSourceAndRecordsAttempts(): void
    {
        $a = new ScriptedSource('src_a', 700, 'throw');
        $b = new ScriptedSource('src_b', 700, 'ok');
        $r = $this->svc()->run('manual', 7, ['sources' => [$a, $b], 'dry_run' => true]);

        $this->assertSame('dry_run', $r['state']);
        $this->assertSame(16, $r['plan_count']);
        $this->assertSame(1, $a->calls);
        $this->assertSame(1, $b->calls, 'one page carries every family, so no further fetches');
        $rows = $this->attemptRows($r['run_id']);
        $this->assertCount(2, $rows);
        $this->assertSame(['src_a', 0, 503], [$rows[0]['source_id'], (int) $rows[0]['ok'], (int) $rows[0]['http_status']]);
        $this->assertSame(['src_b', 1, 'served-src_b'], [$rows[1]['source_id'], (int) $rows[1]['ok'], $rows[1]['served_by']]);
        $this->assertSame('sapper', $rows[1]['strategy']);
        $this->assertSame(64, strlen((string) $rows[1]['html_sha256']));
        $this->assertSame('https://contabo.com/en/vps/cloud-vps-core-4/', $rows[1]['final_url']);
        $this->assertSame(700, $r['total_cost_micro']);
        $this->assertSame(['Cloud VPS' => 6, 'Storage VPS' => 5, 'Cloud VDS' => 5], array_map(static function ($f) { return $f['count']; }, (new RunRepository())->find($r['run_id'])['families_json']));
    }

    public function testGarbagePageFallsThroughAndCountsAsFailure(): void
    {
        $a = new ScriptedSource('src_a', 0, 'garbage');
        $b = new ScriptedSource('src_b', 0, 'ok');
        $r = $this->svc()->run('manual', 1, ['sources' => [$a, $b], 'dry_run' => true]);
        $rows = $this->attemptRows($r['run_id']);
        $this->assertSame([0, 1], [(int) $rows[0]['ok'], (int) $rows[1]['ok']]);
        $this->assertSame(16, $r['plan_count']);
    }

    public function testAllSourcesFailingEndsFailedAndNotifies(): void
    {
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('a', 0, 'throw'), new ScriptedSource('b', 0, 'throw')]]);
        $this->assertSame('failed', $r['state']);
        $this->assertStringContainsString('every provider attempt failed', $r['error']);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());
        $this->assertCount(6, $this->attemptRows($r['run_id']), 'both sources tried for each of the three families');
    }

    public function testPerRunCapStopsPaidCalls(): void
    {
        $this->s->set('scrape.per_run_cap_micro', '1000');
        $a = new ScriptedSource('src_a', 700, 'garbage');
        $b = new ScriptedSource('src_b', 700, 'ok');
        $r = $this->svc()->run('manual', 1, ['sources' => [$a, $b], 'dry_run' => true]);
        $this->assertSame(1, $a->calls);
        $this->assertSame(0, $b->calls, '700 + 700 > 1000: the second paid call is never made');
        $this->assertSame('rejected', $r['outcome']);
    }

    public function testFreeCallsAreNotBlockedByCaps(): void
    {
        $this->s->set('scrape.per_run_cap_micro', '0');
        $r = $this->svc()->run('manual', 1, ['sources' => [new ScriptedSource('free', 0, 'ok')], 'dry_run' => true]);
        $this->assertSame(16, $r['plan_count']);
    }

    public function testMonthlyBudgetRejectsBeforeAnyFetch(): void
    {
        Capsule::table('mod_contabo_scrape_run_attempts')->insert([
            'run_id' => 1, 'source_id' => 'x', 'ok' => 1, 'cost_micro' => 500000, 'latency_ms' => 1, 'created_at' => date('Y-m-d H:i:s'),
        ]);
        $a = new ScriptedSource('src_a', 700, 'ok');
        $r = $this->svc()->run('cron', 0, ['sources' => [$a]]);
        $this->assertSame('rejected', $r['state']);
        $this->assertSame('budget_exhausted', $r['reason']);
        $this->assertSame(0, $a->calls);
        $this->assertCount(0, $this->attemptRows($r['run_id']));
    }

    public function testDryRunNeverImports(): void
    {
        $r = $this->svc()->run('manual', 1, ['sources' => [new ScriptedSource('s', 0, 'ok')], 'dry_run' => true]);
        $this->assertSame('dry_run', $r['state']);
        $this->assertSame('auto_import', $r['outcome']);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());
        $run = (new RunRepository())->find($r['run_id']);
        $this->assertSame(1, (int) $run['dry_run']);
        $this->assertNotEmpty($run['envelope_json']['payload_hash']);
    }

    public function testAutoImportCallsRealCatalogImportService(): void
    {
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')]]);
        $this->assertSame('succeeded', $r['state']);
        $this->assertSame('auto_import', $r['outcome']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertSame($r['catalog_version'], Capsule::$tables['mod_contabo_catalog_versions'][0]['catalog_version']);
        $this->assertSame(16, Capsule::table('mod_contabo_catalog_items')->count());
        $run = (new RunRepository())->find($r['run_id']);
        $this->assertSame($r['catalog_version'], $run['catalog_version']);
        $this->assertNotNull($run['decision_id']);
        $this->assertNotSame('', (string) $this->s->get('scrape.last_run_at'));

        // second identical run diffs against the succeeded baseline: all gates pass, nothing risky
        $r2 = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')], 'dry_run' => true]);
        $this->assertSame(0, $r2['risky']);
        $this->assertSame(0, $r2['anomalies']);
        $this->assertSame(0, $r2['safe']);
    }

    public function testRequireHumanReviewThenImportStored(): void
    {
        $this->s->set('scrape.require_human_review', '1');
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')]]);
        $this->assertSame('needs_review', $r['state']);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());

        $res = $this->svc()->importStored($r['run_id'], 5);
        $this->assertTrue($res['created']);
        $this->assertSame('succeeded', (new RunRepository())->find($r['run_id'])['state']);
        $this->assertSame(1, Capsule::table('mod_contabo_catalog_versions')->count());
        $dec = Capsule::table('mod_contabo_decisions')->where('run_id', $r['run_id'])->get();
        $this->assertCount(2, $dec);
        $this->assertSame('admin', ((array) $dec[1])['decided_by']);
    }

    public function testImportStoredRefusesTamperedEnvelopeAndWrongState(): void
    {
        $this->s->set('scrape.require_human_review', '1');
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')]]);
        $run = (new RunRepository())->find($r['run_id']);
        $env = $run['envelope_json'];
        $env['plans'][0]['base_monthly_price'] = 0.01;
        (new RunRepository())->update($r['run_id'], ['envelope_json' => $env]);
        try {
            $this->svc()->importStored($r['run_id'], 5);
            $this->fail('tampered envelope must be refused');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('integrity', $e->getMessage());
        }
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());

        $ok = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')], 'dry_run' => true]);
        $this->expectException(\RuntimeException::class);
        $this->svc()->importStored($ok['run_id'], 5);
    }

    public function testLockContention(): void
    {
        $a = new ScriptedSource('s', 0, 'ok');
        $r = $this->svc(new ContendedLock())->run('cron', 0, ['sources' => [$a]]);
        $this->assertSame('skipped_locked', $r['state']);
        $this->assertSame(0, $a->calls);
        $this->assertSame('skipped_locked', (new RunRepository())->find($r['run_id'])['state']);
    }

    public function testLockIsReleasedAfterARun(): void
    {
        $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'throw')]]);
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')], 'dry_run' => true]);
        $this->assertSame('dry_run', $r['state']);
    }

    public function testManualOnlySourceNeverRunsOnCron(): void
    {
        $agent = new ScriptedSource('tinyfish_agent', 0, 'ok', true);
        $r = $this->svc()->run('cron', 0, ['sources' => [$agent]]);
        $this->assertSame(0, $agent->calls);
        $this->assertSame('rejected', $r['state']);
        $this->assertSame('no_usable_source', $r['reason']);
    }

    public function testJevVetoDowngradesAutoImportToNeedsReview(): void
    {
        $this->s->set('scrape.jev_enabled', '1');
        $this->s->setJevApiKey('jev-key-0000');
        $this->http->push(200, json_encode(['answers' => ['is_pricing_page' => ['choice' => 'no', 'confidence' => 0.93]]]));
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')]]);
        $this->assertSame('needs_review', $r['state']);
        $dec = (new \ContaboPricing\Scrape\DecisionRecorder())->find((int) (new RunRepository())->find($r['run_id'])['decision_id']);
        $this->assertSame('jev', $dec['decided_by']);
        $this->assertSame(true, $dec['jev']['veto']);
        $this->assertStringNotContainsString('jev-key-0000', (string) $dec['jev_json']);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());
    }

    public function testJevOutageDoesNotBlockAutoImport(): void
    {
        $this->s->set('scrape.jev_enabled', '1');
        $this->s->setJevApiKey('jev-key-0000');
        $this->http->push(500, 'down');
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')]]);
        $this->assertSame('succeeded', $r['state']);
        $this->assertSame('skipped', $r['jev']['status']);
    }

    public function testValidatorFailureRejectsAndNeverImports(): void
    {
        $this->s->set('scrape.min_plans_cloud_vps', '7'); // fixture only has 6
        $r = $this->svc()->run('cron', 0, ['sources' => [new ScriptedSource('s', 0, 'ok')]]);
        $this->assertSame('rejected', $r['state']);
        $this->assertFalse($r['gates']['min_plans_per_family']['ok']);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());
    }

    public function testSourcesFromTableNeedKeysAndUseExecutor(): void
    {
        // treg_anyapi is seeded enabled but keyless => skipped, no HTTP made
        $r = $this->svc()->run('cron', 0, []);
        $this->assertSame('no_usable_source', $r['reason']);
        $this->assertSame([], $this->http->calls);

        (new \ContaboPricing\Scrape\SourceConfigRepository())->save('treg_anyapi', ['api_key' => 'T-KEY']);
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        $this->http->push(200, json_encode(['output' => ['data' => ['html' => $html]]]), ['x-treg-cost-micro' => '700']);
        $r = $this->svc()->run('cron', 0, ['dry_run' => true]);
        $this->assertSame(16, $r['plan_count']);
        $this->assertSame(700, $r['total_cost_micro']);
        $this->assertSame('https://treg.to/call/anyapi.web.scrape', $this->http->calls[0]['url']);
        $row = (new \ContaboPricing\Scrape\SourceConfigRepository())->find('treg_anyapi');
        $this->assertSame(0, (int) $row['consecutive_failures']);
    }
}
