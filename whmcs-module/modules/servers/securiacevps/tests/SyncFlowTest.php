<?php
declare(strict_types=1);

use SecuriAceVps\Tests\Harness;
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

        $this->assertSame('success', securiacevps_buttonSync(Harness::params()));

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

        $this->assertSame('success', securiacevps_buttonSync(Harness::params()));

        $row = Capsule::$tables['tblhosting'][0];
        $this->assertSame('203.0.113.10', $row['dedicatedip']);
        $this->assertSame('2a02:c207::1', $row['assignedips'], 'IPv6 must land in assignedips');
    }

    public function testUnchangedIpsAreNotRewritten(): void
    {
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '203.0.113.10';
        Capsule::$tables['tblhosting'][0]['assignedips'] = '203.0.113.11';
        $this->h->stubTaggedInstance('9001');

        securiacevps_buttonSync(Harness::params());

        $hostingWrites = [];
        foreach (Capsule::$calls as $call) {
            if ($call['table'] === 'tblhosting') {
                $hostingWrites[] = $call;
            }
        }
        $this->assertSame([], $hostingWrites, 'no tblhosting write expected when nothing changed');
    }

    public function testDriftedDisplayNameTagIsRejectedWithoutProviderMutation(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [[
            'instanceId'  => 9001,
            'displayName' => 'renamed in panel',
            'status'      => 'running',
            'ipConfig'    => ['v4' => [['ip' => '203.0.113.10']]],
        ]]]);

        $result = securiacevps_buttonSync(Harness::params());

        $patches = $this->h->http->callsMatching('PATCH https://api.contabo.com/v1/compute/instances/9001');
        $this->assertStringContainsString('ownership could not be verified', $result);
        $this->assertCount(0, $patches);
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['dedicatedip']);
    }

    public function testAdminTabDegradesGracefullyWhenApiIsDown(): void
    {
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '198.51.100.5';
        $this->h->http->setDefault(503, ['message' => 'maintenance']);

        $fields = securiacevps_AdminServicesTabFields(Harness::params());

        $this->assertArrayHasKey('Status', $fields);
        $this->assertStringContainsString('Local VPS projection is unavailable', $fields['Status']);
        $this->assertStringContainsString('198.51.100.5', $fields['IPv4']);
    }

    public function testClientAreaDegradesToStaleView(): void
    {
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '198.51.100.5';
        $this->h->http->setDefault(503, ['message' => 'maintenance']);

        $out = securiacevps_ClientArea(Harness::params());

        $this->assertSame('clientarea', $out['templatefile']);
        $this->assertSame(['198.51.100.5'], $out['vars']['ipv4']);
        $this->assertSame('unavailable', $out['vars']['status']);
    }

    public function testClientAreaRendersLiveSnapshot(): void
    {
        $this->seedLocalProjection();
        $this->h->http->setDefault(503, ['message' => 'maintenance']);
        $callsBefore = count($this->h->http->calls);

        $out = securiacevps_ClientArea(Harness::params());

        $this->assertSame('clientarea', $out['templatefile']);
        $this->assertSame('running', $out['vars']['status']);
        $this->assertSame(['203.0.113.10', '203.0.113.11'], $out['vars']['ipv4']);
        $this->assertSame('verified', $out['vars']['ownership_state']);
        $this->assertSame($callsBefore, count($this->h->http->calls), 'render must not call Contabo');
    }

    private function seedLocalProjection(): void
    {
        foreach ([
            'mod_securiacevps_schema',
            'mod_securiacevps_order_snapshots',
            'mod_securiacevps_resources',
            'mod_securiacevps_operations',
            'mod_securiacevps_operation_attempts',
            'mod_securiacevps_provider_requests',
            'mod_securiacevps_service_locks',
            'mod_securiacevps_capabilities',
            'mod_securiacevps_reconciliation',
            'mod_securiacevps_adoption',
            'mod_securiacevps_billing_sagas',
            'mod_securiacevps_audit_events',
            'mod_securiacevps_operator_commands',
            'mod_securiacevps_secrets',
            'mod_securiacevps_communications',
        ] as $table) {
            Capsule::$columns[$table] = ['id'];
            Capsule::$tables[$table] = [];
        }
        $account = hash('sha256', 'contabo|0|');
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'schema_version', 'value' => '3'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
            ['key' => 'provider_writes_enabled', 'value' => '0'],
        ];
        Capsule::$tables['mod_securiacevps_resources'] = [[
            'id' => 1,
            'service_id' => 300,
            'provider_account_id' => $account,
            'provider_resource_id' => '9001',
            'provider_state' => 'running',
            'provisioning_state' => 'ready',
            'ownership_state' => 'verified',
            'last_observed_at' => '2026-07-30 12:00:00',
        ]];
        Capsule::$tables['mod_securiacevps_adoption'] = [[
            'id' => 1,
            'service_id' => 300,
            'provider_account_id' => $account,
            'provider_resource_id' => '9001',
            'state' => 'verified',
            'confidence' => '1.0000',
        ]];
        Capsule::$tables['tblhosting'][0]['dedicatedip'] = '203.0.113.10';
        Capsule::$tables['tblhosting'][0]['assignedips'] = '203.0.113.11';
    }
}
