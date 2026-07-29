<?php
declare(strict_types=1);

use SecuriAceVps\Tests\Harness;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class TerminateFlowTest extends TestCase
{
    private Harness $h;

    protected function setUp(): void
    {
        Harness::reset();
        $this->h = new Harness();
        Harness::seedWhmcs();
        $this->h->linkService('9001');
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testCancelsWithDocumentedEmptyBodyAndCleansSecrets(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->h->http->queue('POST /v1/compute/instances/9001/cancel', 201, ['data' => [['cancelDate' => '2026-08-01']]]);
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => [
            ['secretId' => 700, 'name' => 'whmcs-svc-300-root', 'type' => 'password'],
        ]]);

        $result = securiacevps_TerminateAccount(Harness::params());

        $this->assertSame('success', $result);
        $cancels = $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances/9001/cancel');
        $this->assertCount(1, $cancels);
        $this->assertSame('{}', $cancels[0]['body'], 'cancel body must be the documented empty object — no terminationDate');
        $this->assertCount(1, $this->h->http->callsMatching('DELETE https://api.contabo.com/v1/secrets/700'));

        // cancelDate surfaced in the activity log; custom-field link kept for audit.
        $joined = implode(' | ', $GLOBALS['__activity_log']);
        $this->assertStringContainsString('2026-08-01', $joined);
        $this->assertSame('9001', Capsule::$tables['tblcustomfieldsvalues'][0]['value']);
    }

    public function testTagMismatchBlocksCancellation(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [
            ['instanceId' => 9001, 'displayName' => 'production-db', 'status' => 'running'],
        ]]);

        $result = securiacevps_TerminateAccount(Harness::params());

        $this->assertStringContainsString('Refusing to cancel', $result);
        $this->assertCount(0, $this->h->http->callsMatching('/cancel'));
    }

    public function testAlreadyAbsentInstanceTerminatesCleanly(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 404, ['message' => 'not found']);
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);

        $result = securiacevps_TerminateAccount(Harness::params());

        $this->assertSame('success', $result);
        $this->assertCount(0, $this->h->http->callsMatching('/cancel'));
    }

    public function testUnlinkedServiceIsAnExplicitError(): void
    {
        Capsule::$tables['tblcustomfieldsvalues'] = [];

        $result = securiacevps_TerminateAccount(Harness::params());

        $this->assertStringContainsString('no linked Contabo instance', $result);
    }
}
