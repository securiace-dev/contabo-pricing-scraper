<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Fx\FxService;
use ContaboPricing\RequestExecutor;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class FxServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_settings'] = [];
    }

    private function body(): string
    {
        return (string) file_get_contents(__DIR__ . '/fixtures/api/frankfurter.json');
    }

    private function exec(array $responses): object
    {
        return new class($responses) implements RequestExecutor {
            public $calls = [];
            private $q;
            public function __construct(array $q) { $this->q = $q; }
            public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
            {
                $this->calls[] = $url;
                return array_shift($this->q) ?? [0, '', 7, 'down'];
            }
        };
    }

    public function testMissFetchesAndNormalises(): void
    {
        $e = $this->exec([[200, $this->body(), 0, '']]);
        $now = 1785406830;
        $r = (new FxService($e, static function () use ($now) { return $now; }))->rates();
        $this->assertSame('https://api.frankfurter.app/latest?base=EUR&symbols=INR', $e->calls[0]);
        $this->assertSame(112.317, $r['eurInr']);
        $this->assertSame(112.317, $r['rate']);
        $this->assertSame(112.317, $r['mid']);
        $this->assertSame('frankfurter', $r['source']);
        $this->assertSame(0, $r['age_minutes']);
        $this->assertArrayNotHasKey('stale', $r);
        $this->assertSame('EUR', $r['base']);
        $this->assertSame(112.317, $r['rates']['INR']);
        $this->assertNotSame('', $r['fetched_at']);
    }

    public function testHitWithinTtlDoesNotFetchAndReportsAge(): void
    {
        $e = $this->exec([[200, $this->body(), 0, '']]);
        $t = 1000000;
        $svc = new FxService($e, static function () use (&$t) { return $t; });
        $svc->rates();
        $t += 240;
        $r = $svc->rates();
        $this->assertCount(1, $e->calls);
        $this->assertSame(4, $r['age_minutes']);
        $this->assertSame(112.317, $r['eurInr']);
    }

    public function testExpiredCacheRefetches(): void
    {
        $e = $this->exec([
            [200, $this->body(), 0, ''],
            [200, '{"base":"EUR","rates":{"INR":120.5}}', 0, ''],
        ]);
        $t = 1000000;
        $svc = new FxService($e, static function () use (&$t) { return $t; });
        $svc->rates();
        $t += 301;
        $this->assertSame(120.5, $svc->rates()['eurInr']);
        $this->assertCount(2, $e->calls);
    }

    public function testStaleFallbackWhenUpstreamDown(): void
    {
        $e = $this->exec([[200, $this->body(), 0, ''], [0, '', 28, 'timeout']]);
        $t = 1000000;
        $svc = new FxService($e, static function () use (&$t) { return $t; });
        $svc->rates();
        $t += 900;
        $r = $svc->rates();
        $this->assertTrue($r['stale']);
        $this->assertSame(112.317, $r['eurInr']);
        $this->assertSame(15, $r['age_minutes']);
    }

    public function testNoCacheAndUpstreamDownThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        (new FxService($this->exec([[500, 'x', 0, '']])))->rates();
    }

    public function testGarbageBodyIsNotCached(): void
    {
        $e = $this->exec([[200, '{"rates":{}}', 0, ''], [200, $this->body(), 0, '']]);
        $svc = new FxService($e, static function () { return 5000; });
        try {
            $svc->rates();
            $this->fail('expected exception');
        } catch (\RuntimeException $ex) {
            $this->assertSame(112.317, $svc->rates()['eurInr']);
        }
    }
}
