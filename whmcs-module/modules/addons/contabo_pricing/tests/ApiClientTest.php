<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ApiClient;
use ContaboPricing\RequestExecutor;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;

/**
 * In-memory RequestExecutor that returns canned responses keyed by URL or in
 * FIFO order, and records every request seen for assertions.
 */
final class MockRequestExecutor implements RequestExecutor
{
    /** @var list<array{0: int, 1: string, 2: int, 3: string}> */
    public array $queue = [];

    /** @var list<array{method: string, url: string, headers: array<int, string>, body: ?string, timeout: int}> */
    public array $calls = [];

    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $this->calls[] = [
            'method'  => $method,
            'url'     => $url,
            'headers' => $headers,
            'body'    => $body,
            'timeout' => $timeoutSec,
        ];
        $resp = array_shift($this->queue);
        if ($resp === null) {
            // Default: 200 with empty JSON object so tests that forget to enqueue
            // a response fail loudly via the "non-JSON" path instead of looping.
            return [200, '{}', 0, ''];
        }
        return $resp;
    }
}

final class ApiClientTest extends TestCase
{
    private function settings(string $token = '', string $base = 'http://api.local/v1'): Settings
    {
        return new Settings(
            apiBaseUrl: $base,
            apiToken: $token,
            defaultSyncStrategy: 'notify',
            currencyIso: 'INR',
            applyGst18: true,
            fxMarkupPct: 3.5,
            logRetentionDays: 365,
            moduleLink: 'addonmodules.php?module=contabo_pricing',
        );
    }

    public function testGetHappyPathDecodesJsonBody(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '{"hello":"world","n":42}', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);
        $out = $client->meta();

        $this->assertSame(['hello' => 'world', 'n' => 42], $out);
        $this->assertCount(1, $mock->calls);
        $this->assertSame('GET', $mock->calls[0]['method']);
        $this->assertSame('http://api.local/v1/meta', $mock->calls[0]['url']);
        $this->assertNull($mock->calls[0]['body']);
        $this->assertContains('Accept: application/json', $mock->calls[0]['headers']);
    }

    public function testGetSendsBearerTokenWhenConfigured(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '{"ok":true}', 0, ''];

        $client = new ApiClient($this->settings(token: 'tok-abc'), 8, $mock);
        $client->meta();

        $this->assertContains('Authorization: Bearer tok-abc', $mock->calls[0]['headers']);
    }

    public function testGetOmitsBearerWhenTokenEmpty(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '{"ok":true}', 0, ''];

        $client = new ApiClient($this->settings(token: ''), 8, $mock);
        $client->meta();

        foreach ($mock->calls[0]['headers'] as $h) {
            $this->assertStringStartsNotWith('Authorization:', $h);
        }
    }

    public function testQuotePostsEncodedJsonBodyAndContentType(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '{"quoted":true}', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);
        $client->quote(['plan_slug' => 'vps-10', 'period_months' => 12]);

        $this->assertSame('POST', $mock->calls[0]['method']);
        $this->assertSame('http://api.local/v1/quote', $mock->calls[0]['url']);
        $this->assertNotNull($mock->calls[0]['body']);
        $this->assertSame(
            ['plan_slug' => 'vps-10', 'period_months' => 12],
            json_decode((string) $mock->calls[0]['body'], true)
        );
        $this->assertContains('Content-Type: application/json', $mock->calls[0]['headers']);
    }

    public function testRefreshRequiresAuthTokenAndThrowsWhenMissing(): void
    {
        $client = new ApiClient($this->settings(token: ''), 8, new MockRequestExecutor());

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('API token not configured');
        $client->refresh();
    }

    public function testRefreshHappyPathReturnsJobIdAndStatus(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '{"job_id":"abc-123","status":"queued"}', 0, ''];

        $client = new ApiClient($this->settings(token: 'tok'), 8, $mock);
        $out = $client->refresh();

        $this->assertSame(['job_id' => 'abc-123', 'status' => 'queued'], $out);
    }

    public function testUnauthorized401MappedToRuntimeException(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [401, '{"error":"nope"}', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('unauthorized');
        $client->meta();
    }

    public function testForbidden403MappedToUnauthorizedException(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [403, '{"error":"forbidden"}', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('unauthorized');
        $client->meta();
    }

    public function testNotFound404MappedToRuntimeException(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [404, '{"error":"missing"}', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('not found');
        $client->plan('does-not-exist');
    }

    public function testServerError5xxIncludesStatusCodeInMessage(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [502, 'upstream gone', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);

        try {
            $client->meta();
            $this->fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('HTTP 502', $e->getMessage());
            $this->assertStringContainsString('upstream gone', $e->getMessage());
        }
    }

    public function testGenericClientError4xxIncludesStatusCodeInMessage(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [422, '{"error":"validation"}', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);

        try {
            $client->meta();
            $this->fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('HTTP 422', $e->getMessage());
            $this->assertStringContainsString('client error', $e->getMessage());
        }
    }

    public function testNonJsonBodyThrows(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '<html>not json</html>', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('non-JSON body');
        $client->meta();
    }

    public function testCurlErrorSurfacedToCaller(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [0, '', 28, 'Connection timed out after 8001 ms'];

        $client = new ApiClient($this->settings(), 8, $mock);

        try {
            $client->meta();
            $this->fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('errno 28', $e->getMessage());
            $this->assertStringContainsString('Connection timed out', $e->getMessage());
        }
    }

    public function testPlansAppendsFamilyQueryParam(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '[]', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);
        $client->plans('cloud vps');

        $this->assertSame('http://api.local/v1/plans?family=cloud%20vps', $mock->calls[0]['url']);
    }

    public function testPlansWithoutFamilyOmitsQuery(): void
    {
        $mock = new MockRequestExecutor();
        $mock->queue[] = [200, '[]', 0, ''];

        $client = new ApiClient($this->settings(), 8, $mock);
        $client->plans(null);

        $this->assertSame('http://api.local/v1/plans', $mock->calls[0]['url']);
    }
}
