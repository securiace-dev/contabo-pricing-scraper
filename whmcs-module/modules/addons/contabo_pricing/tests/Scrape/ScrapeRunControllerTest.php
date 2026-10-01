<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\AdminController;
use ContaboPricing\Installer;
use ContaboPricing\Scrape\HeaderAwareExecutor;
use ContaboPricing\Scrape\RunRepository;
use ContaboPricing\Scrape\ScrapeSettings;
use ContaboPricing\Scrape\SourceConfigRepository;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

require_once __DIR__ . '/FakeHeaderExecutor.php';

/** AdminController with the network and redirect seams replaced. */
final class SeamedAdminController extends AdminController
{
    /** @var FakeHeaderExecutor */
    public $http;
    /** @var list<array{0:string,1:array<string,mixed>}> */
    public $redirects = [];

    protected function scrapeExecutor(): HeaderAwareExecutor
    {
        return $this->http;
    }

    protected function go(string $action, array $extra = []): void
    {
        $this->redirects[] = [$action, $extra];
    }
}

final class ScrapeRunControllerTest extends TestCase
{
    /** @var SeamedAdminController */
    private $c;
    /** @var FakeHeaderExecutor */
    private $http;
    /** @var string */
    private $html;

    protected function setUp(): void
    {
        Capsule::reset();
        (new Installer())->migrateTo15();
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
        Capsule::$tables['mod_contabo_settings'] = array_merge(Capsule::$tables['mod_contabo_settings'] ?? [], [
            ['key' => 'schema_version', 'value' => '15'],
        ]);
        $this->http = new FakeHeaderExecutor();
        $this->c = new SeamedAdminController(
            new Settings('notify', 'INR', false, 3.5, 365, 'addonmodules.php?module=contabo_pricing'),
            __DIR__ . '/../../templates/admin'
        );
        $this->c->http = $this->http;
        $this->html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        (new SourceConfigRepository())->save('treg_anyapi', ['enabled' => 1, 'api_key' => 'TREG-SECRET-KEY-9999']);
        (new ScrapeSettings())->set('scrape.require_human_review', '0');
    }

    protected function tearDown(): void
    {
        unset($GLOBALS['__cb_token_fail'], $_SERVER['REQUEST_METHOD']);
        http_response_code(200);
    }

    /** @param array<string,mixed> $req */
    private function dispatch(array $req): string
    {
        ob_start();
        $this->c->dispatch($req);
        return (string) ob_get_clean();
    }

    private function pushPage(): void
    {
        $this->http->push(200, (string) json_encode(['output' => ['data' => ['html' => $this->html]]]), ['x-treg-cost-micro' => '700']);
    }

    private function lastRunId(): int
    {
        return (int) (new RunRepository())->listRuns(1)[0]['id'];
    }

    /** @return list<string> */
    public static function mutators(): array
    {
        return ['data-sources-save', 'scrape-run', 'scrape-run-import', 'scrape-agent-run'];
    }

    public function testGetToAMutatorIsRejectedAndDoesNothing(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'GET';
        foreach (self::mutators() as $action) {
            $out = $this->dispatch(['action' => $action, 'mode' => 'live', 'id' => '1', 'scrape_enabled' => '1']);
            $this->assertStringContainsString('requires a POST request', $out, $action);
        }
        $this->assertSame(405, http_response_code());
        $this->assertSame([], $this->http->calls);
        $this->assertSame([], Capsule::$tables['mod_contabo_scrape_runs'] ?? []);
        $this->assertFalse((new ScrapeSettings())->enabled());
    }

    public function testBadCsrfTokenIsRejected(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $GLOBALS['__cb_token_fail'] = true;
        foreach (self::mutators() as $action) {
            $out = $this->dispatch(['action' => $action, 'scrape_enabled' => '1', 'mode' => 'live']);
            $this->assertStringContainsString('Invalid security token', $out, $action);
        }
        $this->assertSame([], $this->http->calls);
        $this->assertFalse((new ScrapeSettings())->enabled());
        $this->assertSame([], $this->c->redirects);
    }

    public function testDataSourcesSaveSealsKeyAndRedirects(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $out = $this->dispatch([
            'action' => 'data-sources-save', 'scrape_enabled' => '1',
            'src' => ['alterlab' => ['enabled' => '1', 'api_key' => 'AL-SECRET-KEY-4242']],
        ]);
        $this->assertSame('', $out);
        $this->assertSame('data-sources', $this->c->redirects[0][0]);
        $this->assertSame('Data sources saved.', $this->c->redirects[0][1]['flash']);
        $this->assertTrue((new ScrapeSettings())->enabled());
        $row = (array) (new SourceConfigRepository())->find('alterlab');
        $this->assertStringStartsWith('ENC:', (string) $row['api_key_enc']);
    }

    public function testDataSourcesViewShowsPillAndMaskedTailButNeverTheKey(): void
    {
        $out = $this->dispatch(['action' => 'data-sources']);
        $this->assertStringContainsString('set &middot; ENC', $out);
        $this->assertStringContainsString('9999', $out, 'masked tail is shown');
        $this->assertStringNotContainsString('TREG-SECRET-KEY-9999', $out);
        $this->assertStringNotContainsString('ENC:', $out, 'the sealed blob is not rendered either');
        $this->assertStringContainsString('data-cb-action="test-source"', $out);
        $this->assertStringContainsString('data-source-id="treg_anyapi"', $out);
    }

    public function testDryRunRendersSummaryAndNeverImports(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $this->pushPage();
        $out = $this->dispatch(['action' => 'scrape-run', 'mode' => 'dry']);

        $this->assertStringContainsString('Scrape run #', $out);
        $this->assertStringContainsString('dry run (nothing imported)', $out);
        $this->assertStringContainsString('dry_run', $out);
        $this->assertStringContainsString('price_sanity', $out);
        $this->assertStringContainsString('treg_anyapi', $out);
        $this->assertStringContainsString('$0.0007', $out);
        $this->assertStringNotContainsString('TREG-SECRET-KEY-9999', $out);
        $this->assertCount(1, $this->http->calls);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());
        $run = (new RunRepository())->listRuns(1)[0];
        $this->assertSame('dry_run', $run['state']);
        $this->assertSame(16, (int) $run['plan_count']);
    }

    public function testRunsListAndDetailRender(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $this->pushPage();
        $this->dispatch(['action' => 'scrape-run', 'mode' => 'dry']);
        unset($_SERVER['REQUEST_METHOD']);

        $list = $this->dispatch(['action' => 'scrape-runs']);
        $id = $this->lastRunId();
        $this->assertStringContainsString('action=scrape-run-detail&amp;id=' . $id, $list);
        $detail = $this->dispatch(['action' => 'scrape-run-detail', 'id' => (string) $id]);
        $this->assertStringContainsString('Changes vs last good run (none yet)', $detail);
        $this->assertStringContainsString('new_plan', $detail);
        $this->assertStringNotContainsString('Import this run', $detail);
        $this->assertStringContainsString('Scrape run not found', $this->dispatch(['action' => 'scrape-run-detail', 'id' => '9999']));
    }

    public function testNeedsReviewRunCanBeImportedAfterRehash(): void
    {
        (new ScrapeSettings())->set('scrape.require_human_review', '1');
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $this->pushPage();
        $this->dispatch(['action' => 'scrape-run', 'mode' => 'live']);
        $id = $this->lastRunId();
        $this->assertSame('needs_review', (new RunRepository())->find($id)['state']);
        $this->assertSame(0, Capsule::table('mod_contabo_catalog_versions')->count());

        $out = $this->dispatch(['action' => 'scrape-run-detail', 'id' => (string) $id]);
        $this->assertStringContainsString('Import this run', $out);

        $this->dispatch(['action' => 'scrape-run-import', 'id' => (string) $id]);
        $this->assertSame('scrape-run-detail', $this->c->redirects[0][0]);
        $this->assertStringStartsWith('Imported catalog', $this->c->redirects[0][1]['flash']);
        $this->assertSame(1, Capsule::table('mod_contabo_catalog_versions')->count());
        $this->assertSame('succeeded', (new RunRepository())->find($id)['state']);
    }

    public function testAjaxSourceTestReportsOutcomeAndRecordsSpend(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $this->pushPage();
        $out = $this->dispatch(['action' => 'ajax-source-test', 'source_id' => 'treg_anyapi']);
        $j = json_decode($out, true);
        $this->assertSame(true, $j['ok']);
        $this->assertSame(true, $j['sapper_present']);
        $this->assertSame(16, $j['plan_count']);
        $this->assertSame(700, $j['cost_micro']);
        $this->assertArrayHasKey('latency_ms', $j);
        $this->assertArrayHasKey('served_by', $j);
        $this->assertStringNotContainsString('TREG-SECRET-KEY-9999', $out);
        $rows = Capsule::table('mod_contabo_scrape_run_attempts')->get();
        $this->assertCount(1, $rows);
        $this->assertSame(700, (int) ((array) $rows[0])['cost_micro']);
    }

    public function testAjaxSourceTestFailureAndGuards(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $this->http->push(401, 'bad key TREG-SECRET-KEY-9999');
        $this->http->push(401, 'bad key');
        $j = json_decode($this->dispatch(['action' => 'ajax-source-test', 'source_id' => 'treg_anyapi']), true);
        $this->assertFalse($j['ok']);
        $this->assertStringContainsString('HTTP 401', $j['error']);
        $this->assertStringNotContainsString('TREG-SECRET-KEY-9999', (string) json_encode($j));
        $this->assertArrayHasKey('connection', $j);

        $nokey = json_decode($this->dispatch(['action' => 'ajax-source-test', 'source_id' => 'alterlab']), true);
        $this->assertFalse($nokey['ok']);
        $this->assertStringContainsString('No API key', $nokey['error']);
        $this->assertSame('Unknown data source', json_decode($this->dispatch(['action' => 'ajax-source-test', 'source_id' => 'nope']), true)['error']);

        $_SERVER['REQUEST_METHOD'] = 'GET';
        $this->assertSame('POST required', json_decode($this->dispatch(['action' => 'ajax-source-test', 'source_id' => 'treg_anyapi']), true)['error']);
    }

    public function testAgentRunShowsMaxCostBeforeConfirmAndOnlyRunsOnConfirm(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        $repo = new SourceConfigRepository();
        $repo->save('tinyfish_agent', ['enabled' => 1, 'api_key' => 'AG-SECRET', 'monthly_budget_micro' => 1000000, 'per_run_cap_micro' => 1000000]);
        (new ScrapeSettings())->set('scrape.monthly_budget_micro', '1000000');

        $out = $this->dispatch(['action' => 'scrape-agent-run', 'mode' => 'dry']);
        $this->assertStringContainsString('Confirm agent run', $out);
        $this->assertStringContainsString('$0.640', $out, '40 steps x $0.016');
        $this->assertSame([], $this->http->calls, 'nothing runs before confirmation');

        $this->http->push(200, (string) json_encode(['status' => 'completed', 'result' => ['products' => []], 'steps' => 5]));
        $out = $this->dispatch(['action' => 'scrape-agent-run', 'mode' => 'dry', 'confirm' => '1']);
        $this->assertCount(1, $this->http->calls);
        $this->assertSame('https://agent.tinyfish.ai/v1/automation/run', $this->http->calls[0]['url']);
        $this->assertStringContainsString('Scrape run #', $out);
        $this->assertStringContainsString('tinyfish_agent', $out);
        $this->assertSame(80000, (int) ((array) Capsule::table('mod_contabo_scrape_run_attempts')->get()[0])['cost_micro']);
    }

    public function testAgentRunIsBlockedByBudgetsWhenWorstCaseExceedsThem(): void
    {
        $_SERVER['REQUEST_METHOD'] = 'POST';
        (new SourceConfigRepository())->save('tinyfish_agent', ['enabled' => 1, 'api_key' => 'AG-SECRET']);
        $this->dispatch(['action' => 'scrape-agent-run', 'mode' => 'dry', 'confirm' => '1']);
        $this->assertSame([], $this->http->calls, '$0.64 worst case exceeds the default $0.50 monthly budget');
    }
}
