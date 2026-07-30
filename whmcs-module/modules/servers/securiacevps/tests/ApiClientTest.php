<?php
declare(strict_types=1);

use SecuriAceVps\ContaboApiClient;
use SecuriAceVps\ContaboAuth;
use SecuriAceVps\ContaboProvisioningException;
use SecuriAceVps\Tests\FakeHttpExecutor;
use PHPUnit\Framework\TestCase;

final class ApiClientTest extends TestCase
{
    private FakeHttpExecutor $http;

    /** @var list<int> */
    private array $sleeps = [];

    protected function setUp(): void
    {
        $this->http = new FakeHttpExecutor();
        $this->http->stubToken();
        $this->sleeps = [];
    }

    private function client(): ContaboApiClient
    {
        $auth = new ContaboAuth('cid', 'secret', 'user@example.com', 'pw', $this->http);
        $sleeps = &$this->sleeps;
        return new ContaboApiClient($auth, $this->http, function (int $s) use (&$sleeps): void {
            $sleeps[] = $s;
        });
    }

    public function testGetReturnsDecodedBody(): void
    {
        $this->http->queue('GET api.contabo.com', 200, ['data' => [['instanceId' => 42]]]);
        $resp = $this->client()->get('/v1/compute/instances/42');
        $this->assertSame(42, $resp['data'][0]['instanceId']);
    }

    public function testStaleTokenIsRefreshedOnceAndReplayed(): void
    {
        $this->http->queue('GET api.contabo.com', 401, ['message' => 'expired']);
        $this->http->queue('GET api.contabo.com', 200, ['data' => []]);

        $resp = $this->client()->get('/v1/compute/instances');
        $this->assertSame([], $resp['data']);
        // Two token fetches (initial + forced refresh), two API calls.
        $this->assertCount(2, $this->http->callsMatching('auth.contabo.com'));
        $this->assertCount(2, $this->http->callsMatching('GET https://api.contabo.com'));
        $this->assertSame([], $this->sleeps, 'a 401 replay must not sleep');
    }

    public function testSecond401ThrowsInsteadOfLooping(): void
    {
        $this->http->queue('GET api.contabo.com', 401, ['message' => 'bad creds']);
        $this->http->queue('GET api.contabo.com', 401, ['message' => 'bad creds']);

        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('HTTP 401');
        $this->client()->get('/v1/compute/instances');
    }

    public function testRateLimitRetriesWithLinearBackoffThenSucceeds(): void
    {
        $this->http->queue('GET api.contabo.com', 429, []);
        $this->http->queue('GET api.contabo.com', 429, []);
        $this->http->queue('GET api.contabo.com', 200, ['data' => ['ok' => true]]);

        $resp = $this->client()->get('/v1/compute/instances');
        $this->assertTrue($resp['data']['ok']);
        $this->assertSame([1, 2], $this->sleeps);
    }

    public function testServerErrorsRetryThenThrowWhenExhausted(): void
    {
        foreach ([500, 502, 503, 500] as $code) {
            $this->http->queue('GET api.contabo.com', $code, ['message' => 'boom']);
        }
        try {
            $this->client()->get('/v1/compute/instances');
            $this->fail('expected exception');
        } catch (ContaboProvisioningException $e) {
            $this->assertStringContainsString('HTTP 500', $e->getMessage());
        }
        // Exponential 1, 2, then capped by the 6s total budget (1+2+3=6? cap→3).
        $this->assertSame(3, count($this->sleeps));
        $this->assertLessThanOrEqual(6, array_sum($this->sleeps), 'total backoff must respect the cap');
    }

    public function testMutationServerErrorReturnsAmbiguousOutcomeWithoutReplay(): void
    {
        $this->http->queue('POST api.contabo.com', 503, ['message' => 'upstream uncertain']);
        $this->http->queue('POST api.contabo.com', 201, ['data' => [['instanceId' => 9999]]]);

        try {
            $this->client()->postWithIdentity(
                '/v1/compute/instances',
                ['productId' => 'V45'],
                'durable-create-command'
            );
            $this->fail('expected ambiguous provider exception');
        } catch (ContaboProvisioningException $e) {
            $this->assertTrue($e->hasAmbiguousOutcome());
            $this->assertSame('provider_http_503', $e->safeCode());
            $this->assertSame('transient', $e->retryClassification());
        }

        $this->assertCount(
            1,
            $this->http->callsMatching('POST https://api.contabo.com/v1/compute/instances')
        );
        $this->assertSame([], $this->sleeps);
    }

    public function testMutationRateLimitIsReconciledInsteadOfReplayed(): void
    {
        $this->http->queue('DELETE api.contabo.com', 429, ['message' => 'rate limited']);

        try {
            $this->client()->deleteWithIdentity(
                '/v1/compute/instances/9001/snapshots/snap-1',
                'durable-delete-command'
            );
            $this->fail('expected ambiguous provider exception');
        } catch (ContaboProvisioningException $e) {
            $this->assertTrue($e->hasAmbiguousOutcome());
            $this->assertSame('provider_http_429', $e->safeCode());
        }

        $this->assertCount(
            1,
            $this->http->callsMatching(
                'DELETE https://api.contabo.com/v1/compute/instances/9001/snapshots/snap-1'
            )
        );
        $this->assertSame([], $this->sleeps);
    }

    public function testTransportErrorThrows(): void
    {
        $this->http->queue('GET api.contabo.com', 0, '', 28, 'Connection timed out');
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('transport error');
        $this->client()->get('/v1/compute/instances');
    }

    public function testPutAndDeleteUseCorrectVerbs(): void
    {
        $client = $this->client();
        $client->put('/v1/compute/instances/1', ['imageId' => 'x']);
        $client->delete('/v1/secrets/9');
        $client->deleteWithIdentity('/v1/compute/instances/1/snapshots/snap-1', 'durable-command');

        $this->assertCount(1, $this->http->callsMatching('PUT https://api.contabo.com/v1/compute/instances/1'));
        $this->assertCount(1, $this->http->callsMatching('DELETE https://api.contabo.com/v1/secrets/9'));
        $this->assertCount(
            1,
            $this->http->callsMatching(
                'DELETE https://api.contabo.com/v1/compute/instances/1/snapshots/snap-1'
            )
        );
    }

    public function testTimeoutOverridePropagatesToExecutor(): void
    {
        $client = $this->client();
        $client->setTimeout(10);
        $client->get('/v1/compute/instances');
        $calls = $this->http->callsMatching('GET https://api.contabo.com');
        $this->assertSame(10, $calls[0]['timeout']);
    }

    public function testEveryRequestCarriesARequestId(): void
    {
        $this->client()->get('/v1/compute/instances');
        $calls = $this->http->callsMatching('GET https://api.contabo.com');
        $found = false;
        foreach ($calls[0]['headers'] as $header) {
            if (preg_match('/^x-request-id: [0-9a-f-]{36}$/', $header) === 1) {
                $found = true;
            }
        }
        $this->assertTrue($found, 'x-request-id UUID header missing');
    }

    public function testDurableIdentityMapsToStableUuidRequestId(): void
    {
        $identity = str_repeat('a', 64);
        $expected = ContaboApiClient::requestIdForIdentity($identity);
        $this->assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $expected
        );
        $this->assertSame($expected, ContaboApiClient::requestIdForIdentity($identity));

        $this->client()->postWithIdentity('/v1/compute/instances/1/actions/start', [], $identity);
        $calls = $this->http->callsMatching(
            'POST https://api.contabo.com/v1/compute/instances/1/actions/start'
        );
        $this->assertContains('x-request-id: ' . $expected, $calls[0]['headers']);
    }
}
