<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\CanonicalJson;
use WHMCS\Database\Capsule;

final class QueueRecoveryTest extends TestCase
{
    protected function setUp(): void
    {
        Harness::reset();
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
            'mod_securiacevps_snapshot_inventory',
        ] as $table) {
            Capsule::$columns[$table] = ['id'];
            Capsule::$tables[$table] = [];
        }
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'schema_version', 'value' => '5'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
            ['key' => 'provider_writes_enabled', 'value' => '1'],
            ['key' => 'operator_command_lease_seconds', 'value' => '300'],
        ];
    }

    public function testExpiredOperatorCommandClaimIsRecovered(): void
    {
        $payload = ['enabled' => false];
        Capsule::$tables['mod_securiacevps_operator_commands'][] = [
            'id' => 91,
            'command_uuid' => 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'command_type' => 'set_global_write_state',
            'service_id' => null,
            'operation_uuid' => null,
            'requested_by_admin_id' => 7,
            'state' => 'claimed',
            'payload_hash' => hash('sha256', CanonicalJson::encode($payload)),
            'payload_json' => CanonicalJson::encode($payload),
            'safe_error_code' => null,
            'claim_token' => 'expired-worker-token',
            'claim_expires_at' => date('Y-m-d H:i:s', time() - 5),
            'claimed_at' => date('Y-m-d H:i:s', time() - 600),
            'completed_at' => null,
            'created_at' => date('Y-m-d H:i:s', time() - 700),
            'updated_at' => date('Y-m-d H:i:s', time() - 600),
        ];

        _securiacevps_process_operator_commands();

        $row = Capsule::$tables['mod_securiacevps_operator_commands'][0];
        $this->assertSame('completed', $row['state']);
        $this->assertNull($row['claim_token']);
        $this->assertNull($row['claim_expires_at']);
        $this->assertSame(
            '0',
            Capsule::table('mod_securiacevps_schema')
                ->where('key', 'provider_writes_enabled')
                ->value('value')
        );
    }

    public function testExpiredOperatorWorkerCannotFinishNewerClaim(): void
    {
        Capsule::$tables['mod_securiacevps_operator_commands'][] = [
            'id' => 92,
            'command_uuid' => 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
            'command_type' => 'set_global_write_state',
            'service_id' => null,
            'operation_uuid' => null,
            'requested_by_admin_id' => 7,
            'state' => 'claimed',
            'payload_hash' => hash('sha256', CanonicalJson::encode(['enabled' => false])),
            'payload_json' => CanonicalJson::encode(['enabled' => false]),
            'claim_token' => 'new-worker-token',
            'claim_expires_at' => date('Y-m-d H:i:s', time() + 300),
        ];

        _securiacevps_finish_operator_command(
            92,
            'rejected',
            'stale_worker_result',
            'expired-worker-token'
        );

        $row = Capsule::$tables['mod_securiacevps_operator_commands'][0];
        $this->assertSame('claimed', $row['state']);
        $this->assertSame('new-worker-token', $row['claim_token']);
        $this->assertArrayNotHasKey('safe_error_code', $row);
    }
}
