<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\CanonicalJson;
use SecuriAceVps\Runtime;
use WHMCS\Database\Capsule;

final class NativeLifecycleTest extends TestCase
{
    /** @var Harness */
    private $harness;

    protected function setUp(): void
    {
        Harness::reset();
        $this->harness = new Harness();
        Runtime::swapLifecycle(null);
        Harness::seedWhmcs();
        $this->seedSuite();
        $this->seedSealedSnapshot();
        $this->seedCapability('create');
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testCreateWaitsForReadinessAndNeverSubmitsTwice(): void
    {
        $this->harness->http->stub(
            'GET /v1/compute/instances?',
            200,
            ['data' => [], '_pagination' => ['totalElements' => 0]]
        );
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 77]]]);
        $this->harness->http->queue('POST /v1/compute/instances', 201, [
            'data' => [['instanceId' => 9001]],
        ]);
        $pending = [
            'data' => [[
                'instanceId' => 9001,
                'displayName' => 'whmcs-300 vps.example.com',
                'status' => 'provisioning',
                'region' => 'EU',
                'imageId' => 'image-1',
                'ipConfig' => ['v4' => []],
            ]],
        ];
        // verifyReady performs an ownership read and then a sync read.
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $pending);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $pending);

        $first = securiacevps_CreateAccount(Harness::params());

        $this->assertStringContainsString('still in progress', $first);
        $this->assertSame(
            'provider_pending',
            Capsule::$tables['mod_securiacevps_operations'][0]['state']
        );

        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [[
                'instanceId' => 9001,
                'displayName' => 'whmcs-300 vps.example.com',
                'status' => 'running',
                'region' => 'EU',
                'imageId' => 'image-1',
                'createdDate' => '2026-07-30T00:00:00Z',
                'ipConfig' => ['v4' => [['ip' => '203.0.113.10']]],
            ]],
        ]);
        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] = date('Y-m-d H:i:s', time() - 1);

        $second = securiacevps_CreateAccount(Harness::params());

        $this->assertSame('success', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching('POST https://api.contabo.com/v1/compute/instances')
        );
        $this->assertSame('succeeded', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['password']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_secrets']);
    }

    public function testProviderWriteKillSwitchBlocksBeforeSubmission(): void
    {
        Capsule::table('mod_securiacevps_schema')
            ->where('key', 'provider_writes_enabled')
            ->update(['value' => '0']);

        $result = securiacevps_CreateAccount(Harness::params());

        $this->assertStringContainsString('administrator review', $result);
        $this->assertCount(
            0,
            $this->harness->http->callsMatching('POST https://api.contabo.com/v1/compute/instances')
        );
    }

    private function seedCapability(string $capability): void
    {
        Capsule::$tables['mod_securiacevps_capabilities'][] = [
            'id' => count(Capsule::$tables['mod_securiacevps_capabilities']) + 1,
            'provider_account_id' => hash('sha256', 'contabo|0|'),
            'capability' => $capability,
            'state' => 'requires_polling',
        ];
    }

    private function seedSealedSnapshot(): void
    {
        $configuration = [
            'billing_cycle' => 'Monthly',
            'image_id' => 'image-1',
            'region' => 'EU',
        ];
        $pricing = [
            'billing_cycle' => 'Monthly',
            'currency' => 'INR',
            'recurring' => '1999.00',
            'setup' => '0.00',
        ];
        $whmcs = [
            'invoice_id' => 12,
            'order_id' => 23,
            'service_label' => 'vps.example.com',
            'total_due' => '1999.00',
        ];
        Capsule::$tables['mod_securiacevps_order_snapshots'][] = [
            'id' => 1,
            'snapshot_uuid' => 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'service_id' => 300,
            'state' => 'sealed',
            'sealed_at' => '2026-07-30 01:00:00',
            'configuration_hash' => hash('sha256', CanonicalJson::encode($configuration)),
            'price_hash' => hash('sha256', CanonicalJson::encode($pricing)),
            'cart_total_hash' => hash('sha256', CanonicalJson::encode($whmcs)),
            'payload_json' => CanonicalJson::encode([
                'provider' => [
                    'sku_id' => 'V45',
                    'region_id' => 'EU',
                    'image_id' => 'image-1',
                    'add_ons' => [],
                ],
                'configuration' => $configuration,
                'pricing' => $pricing,
                'whmcs' => $whmcs,
            ]),
        ];
    }

    private function seedSuite(): void
    {
        $tables = [
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
        ];
        foreach ($tables as $table) {
            Capsule::$columns[$table] = ['id'];
            Capsule::$tables[$table] = [];
        }
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'schema_version', 'value' => '2'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
            ['key' => 'provider_writes_enabled', 'value' => '1'],
            ['key' => 'operation_lease_seconds', 'value' => '120'],
        ];
    }
}
