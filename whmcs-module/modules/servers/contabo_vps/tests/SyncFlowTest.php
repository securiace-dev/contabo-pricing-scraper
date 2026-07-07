<?php
declare(strict_types=1);

use ContaboVps\Tests\Harness;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class SyncFlowTest extends TestCase
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

    public function testSyncWritesDedicatedAndAssignedIps(): void
    {
        $this->h->stubTaggedInstance('9001');

        $this->assertSame('success', contabo_vps_buttonSync(Harness::params()));

        $row = Capsule::$tables['tblhosting'][0];
        $this->assertSame('203.0.113.10', $row['dedicatedip']);
        $this->assertSame('203.0.113.11', $row['assignedips']);
    }

    public function testSyncWritesIpv6AlongsideIpv4(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [[
            'instanceId'  => 9001,
            'displayName' => 'whmcs-300 vps.example.com',
            'status'      => 'running',
            'ipConfig'    => [
                'v4' => [['ip' => '203.0.113.10']],
                'v6' => [['ip' => '2a02:c207::1']],
            ],
        ]]]);

        $out = contabo_vps_ClientArea(Harness::params());
        $this->assertSame(['203.0.113.10'], $out['vars']['ipv4']);
        $this->assertSame(['2a02:c207::1'], $out['vars']['ipv6']);

        $row = Capsule::$tables['tblhosting'][0];
        $this->assertSame('203.0.113.10', $row['dedicatedip']);
        $this->assertSame('2a02:c207::1', $row['assignedips'], 'IPv6 must land in assignedips');
    }

    public function testUnchangedIpsAreNotRewritten(): void
    {
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '203.0.113.10';
        Capsule::$tables['tblhosting'][0]['assignedips'] = '203.0.113.11';
        $this->h->stubTaggedInstance('9001');

        contabo_vps_buttonSync(Harness::params());

        $hostingWrites = [];
        foreach (Capsule::$calls as $call) {
            if ($call['table'] === 'tblhosting') {
                $hostingWrites[] = $call;
            }
        }
        $this->assertSame([], $hostingWrites, 'no tblhosting write expected when nothing changed');
    }

    public function testDriftedDisplayNameTagIsRestored(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [[
            'instanceId'  => 9001,
            'displayName' => 'renamed in panel',
            'status'      => 'running',
            'ipConfig'    => ['v4' => [['ip' => '203.0.113.10']]],
        ]]]);

        contabo_vps_buttonSync(Harness::params());

        $patches = $this->h->http->callsMatching('PATCH https://api.contabo.com/v1/compute/instances/9001');
        $this->assertCount(1, $patches);
        $body = json_decode((string) $patches[0]['body'], true);
        $this->assertSame('whmcs-300 vps.example.com', $body['displayName']);
    }

    public function testAdminTabDegradesGracefullyWhenApiIsDown(): void
    {
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '198.51.100.5';
        $this->h->http->setDefault(503, ['message' => 'maintenance']);

        $fields = contabo_vps_AdminServicesTabFields(Harness::params());

        $this->assertArrayHasKey('Status', $fields);
        $this->assertStringContainsString('Live status unavailable', $fields['Status']);
        $this->assertStringContainsString('198.51.100.5', $fields['IPv4']);
    }

    public function testClientAreaDegradesToStaleView(): void
    {
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '198.51.100.5';
        $this->h->http->setDefault(503, ['message' => 'maintenance']);

        $out = contabo_vps_ClientArea(Harness::params());

        $this->assertSame('clientarea', $out['templatefile']);
        $this->assertTrue($out['vars']['stale']);
        $this->assertSame(['198.51.100.5'], $out['vars']['ipv4']);
        $this->assertSame('unavailable', $out['vars']['status']);
    }

    public function testClientAreaRendersLiveSnapshot(): void
    {
        $this->h->stubTaggedInstance('9001');

        $out = contabo_vps_ClientArea(Harness::params());

        $this->assertSame('clientarea', $out['templatefile']);
        $this->assertSame('running', $out['vars']['status']);
        $this->assertSame(['203.0.113.10', '203.0.113.11'], $out['vars']['ipv4']);
        $this->assertArrayNotHasKey('stale', $out['vars']);
    }

    public function testSuspendMapsToStopAndUnsuspendToStart(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->assertSame('success', contabo_vps_SuspendAccount(Harness::params()));
        $this->assertSame('success', contabo_vps_UnsuspendAccount(Harness::params()));
        $this->assertCount(1, $this->h->http->callsMatching('actions/stop'));
        $this->assertCount(1, $this->h->http->callsMatching('actions/start'));
    }
}
