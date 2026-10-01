<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\JevJudge;
use ContaboPricing\Scrape\ScrapeSettings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class JevJudgeTest extends TestCase
{
    /** @var ScrapeSettings */
    private $s;
    /** @var FakeHeaderExecutor */
    private $http;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->s = new ScrapeSettings();
        $this->s->setJevApiKey('jev-secret-KEY123456');
        $this->s->set('scrape.jev_confidence_min', '0.80');
        $this->http = new FakeHeaderExecutor();
    }

    private function answer(string $choice, float $conf): string
    {
        return (string) json_encode(['answers' => ['is_pricing_page' => [
            'choice' => $choice, 'probabilities' => ['yes' => 0.1, 'no' => 0.9], 'confidence' => $conf,
        ]]]);
    }

    public function testRequestShapeAndAuth(): void
    {
        $this->http->push(200, $this->answer('yes', 0.95));
        $r = (new JevJudge($this->s, $this->http))->judge('<html><script>var a=1</script><body><h1>VPS  10</h1>  $5 / mo</body></html>');
        $call = $this->http->calls[0];
        $this->assertSame('POST', $call['method']);
        $this->assertSame('https://api.typesafe.ai/v1/systemone', $call['url']);
        $this->assertContains('Authorization: Bearer jev-secret-KEY123456', $call['headers']);
        $body = json_decode((string) $call['body'], true);
        $this->assertSame('jev-1.13.0', $body['model']);
        $this->assertSame('VPS 10 $5 / mo', $body['state']);
        $this->assertSame(['yes', 'no'], array_keys($body['questions']['is_pricing_page']['criteria']));
        $this->assertSame('choice', $body['questions']['is_pricing_page']['type']);
        $this->assertArrayNotHasKey('same_product_list', $body['questions']);
        $this->assertSame('ok', $r['status']);
        $this->assertFalse($r['veto']);
    }

    public function testSecondSourceAddsQuestion(): void
    {
        $this->http->push(200, (string) json_encode(['answers' => [
            'is_pricing_page' => ['choice' => 'yes', 'confidence' => 0.9],
            'same_product_list' => ['choice' => 'no', 'confidence' => 0.85],
        ]]));
        $r = (new JevJudge($this->s, $this->http))->judge('<p>a</p>', '<p>b</p>');
        $body = json_decode((string) $this->http->calls[0]['body'], true);
        $this->assertArrayHasKey('same_product_list', $body['questions']);
        $this->assertStringContainsString('=== SECOND SOURCE ===', $body['state']);
        $this->assertTrue($r['veto']);
    }

    public function testBelowThresholdIsIgnored(): void
    {
        $this->http->push(200, $this->answer('no', 0.79));
        $r = (new JevJudge($this->s, $this->http))->judge('<p>x</p>');
        $this->assertSame('skipped', $r['status']);
        $this->assertSame('auto_import', JevJudge::apply('auto_import', $r));
    }

    public function testAtThresholdIsAccepted(): void
    {
        $this->http->push(200, $this->answer('no', 0.80));
        $r = (new JevJudge($this->s, $this->http))->judge('<p>x</p>');
        $this->assertSame('ok', $r['status']);
        $this->assertSame('needs_review', JevJudge::apply('auto_import', $r));
    }

    public function testHttpErrorTimeoutAndGarbageAreSkipped(): void
    {
        $this->http->push(500, 'oops')->push(0, '', [], 28, 'timeout')->push(200, 'not json')->push(200, '{"answers":{}}');
        $j = new JevJudge($this->s, $this->http);
        foreach (['HTTP 500', 'transport error', 'unparseable', 'malformed'] as $needle) {
            $r = $j->judge('<p>x</p>');
            $this->assertSame('skipped', $r['status']);
            $this->assertStringContainsString($needle, $r['reason']);
        }
    }

    public function testMissingKeySkipsWithoutCalling(): void
    {
        Capsule::reset();
        $r = (new JevJudge(new ScrapeSettings(), $this->http))->judge('<p>x</p>');
        $this->assertSame('skipped', $r['status']);
        $this->assertSame([], $this->http->calls);
    }

    public function testCannotUpgradeAnyOutcome(): void
    {
        $yes = ['status' => 'ok', 'veto' => false, 'answers' => []];
        $no = ['status' => 'ok', 'veto' => true, 'answers' => []];
        foreach (['rejected', 'needs_review', 'auto_import'] as $o) {
            $this->assertSame($o, JevJudge::apply($o, $yes));
        }
        $this->assertSame('rejected', JevJudge::apply('rejected', $no));
        $this->assertSame('needs_review', JevJudge::apply('needs_review', $no));
        $this->assertSame('needs_review', JevJudge::apply('auto_import', $no));
    }

    public function testStateIsCappedAndKeyNeverLeaks(): void
    {
        $this->http->push(500, 'x');
        $big = '<p>' . str_repeat('word ', 40000) . '</p>';
        $r = (new JevJudge($this->s, $this->http))->judge($big);
        $body = json_decode((string) $this->http->calls[0]['body'], true);
        $this->assertLessThanOrEqual(90000, strlen($body['state']));
        $this->assertStringNotContainsString('KEY123456', json_encode($r));
    }

    public function testVisibleTextOfRealMegabytePageSurvivesScriptStripping(): void
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        $text = JevJudge::visibleText($html);
        $this->assertGreaterThan(500, strlen($text));
        $this->assertStringNotContainsString('__SAPPER__', $text);
        $this->assertStringContainsString('Cloud VPS', $text);
        $this->assertSame('a b', JevJudge::visibleText('<p>a</p><SCRIPT type="x">var q="</p>"</SCRIPT><!-- c --><style>.x{}</style><b>b</b>'));
    }
}
