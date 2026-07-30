<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\CapabilityRegistry;
use SecuriAceVps\CanonicalJson;
use SecuriAceVps\ContaboProvisioningException;
use SecuriAceVps\OneTimeSecretStore;
use SecuriAceVps\OperationRepository;
use SecuriAceVps\OrderSnapshotRepository;
use SecuriAceVps\SchemaGuard;
use WHMCS\Database\Capsule;

final class NativeFoundationTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        $this->seedSuiteSchema();
    }

    public function testSchemaGuardFailsClosedWhenSuiteTablesAreAbsent(): void
    {
        Capsule::reset();
        $this->expectException(ContaboProvisioningException::class);
        SchemaGuard::assertReady();
    }

    public function testCapabilityRequiresCertificationAndBothKillSwitches(): void
    {
        $account = 'provider-account';
        Capsule::$tables['mod_securiacevps_capabilities'] = [[
            'id' => 1,
            'provider_account_id' => $account,
            'capability' => 'create',
            'state' => 'supported',
        ]];

        $this->expectException(ContaboProvisioningException::class);
        (new CapabilityRegistry())->assertWriteAllowed($account, 'create');
    }

    public function testCertifiedCapabilityIsAllowedWhenWritesAreEnabled(): void
    {
        Capsule::table('mod_securiacevps_schema')
            ->where('key', 'provider_writes_enabled')
            ->update(['value' => '1']);
        Capsule::$tables['mod_securiacevps_capabilities'] = [[
            'id' => 1,
            'provider_account_id' => 'provider-account',
            'capability' => 'create',
            'state' => 'requires_polling',
        ]];
        Capsule::$tables['mod_securiacevps_schema'][] = [
            'key' => 'capability.create.enabled',
            'value' => '1',
        ];

        (new CapabilityRegistry())->assertWriteAllowed('provider-account', 'create');
        $this->addToAssertionCount(1);
    }

    public function testSealedSnapshotHashesAreVerified(): void
    {
        $configuration = ['image_id' => 'image-1', 'product_id' => 'V45', 'region' => 'EU'];
        $pricing = ['currency' => 'INR', 'recurring' => '1999.00', 'setup' => '0.00'];
        $whmcs = ['invoice_id' => 12, 'order_id' => 23, 'total_due' => '1999.00'];
        Capsule::$tables['mod_securiacevps_order_snapshots'] = [[
            'id' => 1,
            'snapshot_uuid' => 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'service_id' => 44,
            'state' => 'sealed',
            'sealed_at' => '2026-07-30 01:00:00',
            'configuration_hash' => hash('sha256', CanonicalJson::encode($configuration)),
            'price_hash' => hash('sha256', CanonicalJson::encode($pricing)),
            'cart_total_hash' => hash('sha256', CanonicalJson::encode($whmcs)),
            'payload_json' => CanonicalJson::encode([
                'provider' => ['sku_id' => 'V45'],
                'configuration' => $configuration,
                'pricing' => $pricing,
                'whmcs' => $whmcs,
            ]),
        ]];

        $snapshot = (new OrderSnapshotRepository())->sealedForService(44);

        $this->assertSame('V45', $snapshot['payload']['provider']['sku_id']);
    }

    public function testSameCreateCommandReturnsExistingOperation(): void
    {
        $repo = new OperationRepository();
        $first = $repo->accept(
            44,
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'provider-account',
            'create',
            ['sku_id' => 'V45']
        );
        $second = $repo->accept(
            44,
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'provider-account',
            'create',
            ['sku_id' => 'V45']
        );

        $this->assertSame($first['operation_uuid'], $second['operation_uuid']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_operations']);
    }

    public function testSameCommandWithDifferentPayloadIsRejected(): void
    {
        $repo = new OperationRepository();
        $repo->accept(
            44,
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'provider-account',
            'create',
            ['sku_id' => 'V45']
        );

        $this->expectException(ContaboProvisioningException::class);
        $repo->accept(
            44,
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'provider-account',
            'create',
            ['sku_id' => 'V46']
        );
    }

    public function testOperationClaimHonorsLeaseThenRecoversExpiredWorker(): void
    {
        $repo = new OperationRepository();
        $operation = $repo->accept(
            44,
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'provider-account',
            'create',
            ['sku_id' => 'V45']
        );
        $first = $repo->claim((string) $operation['operation_uuid'], 'worker-one', 120);

        $this->assertNotNull($first);
        $this->assertGreaterThanOrEqual(
            time() + 590,
            strtotime((string) $first['lease_expires_at']),
            'the bounded provider-call budget requires a ten-minute minimum lease'
        );
        $this->assertNull(
            $repo->claim((string) $operation['operation_uuid'], 'worker-two', 120),
            'an unexpired lease must block a second claim even for the same operation'
        );
        $this->assertSame([], $repo->due(10), 'an active claim must not be selected as due');

        Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', (string) $operation['operation_uuid'])
            ->update(['lease_expires_at' => date('Y-m-d H:i:s', time() - 5)]);
        Capsule::table('mod_securiacevps_service_locks')
            ->where('service_id', 44)
            ->update(['lease_expires_at' => date('Y-m-d H:i:s', time() - 5)]);

        $due = $repo->due(10);
        $this->assertCount(1, $due, 'an expired claimed operation must become recoverable');

        $reclaimed = $repo->claim((string) $operation['operation_uuid'], 'worker-two', 120);
        $this->assertNotNull($reclaimed);
        $this->assertSame('worker-two', $reclaimed['lease_owner']);
        $this->assertGreaterThan(
            (int) $first['fencing_token'],
            (int) $reclaimed['fencing_token']
        );
        $this->assertFalse(
            $repo->transition(
                (string) $operation['operation_uuid'],
                (int) $first['fencing_token'],
                'succeeded'
            ),
            'the expired worker fencing token must no longer be able to write'
        );
    }

    public function testActiveWorkerCanRenewLeaseBeforeProviderMutation(): void
    {
        $repo = new OperationRepository();
        $operation = $repo->accept(
            44,
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'provider-account',
            'create',
            ['sku_id' => 'V45']
        );
        $claimed = $repo->claim(
            (string) $operation['operation_uuid'],
            'worker-one',
            120
        );
        $this->assertNotNull($claimed);

        $this->assertTrue($repo->renew(
            (string) $operation['operation_uuid'],
            44,
            (int) $claimed['fencing_token'],
            'worker-one',
            120
        ));
        $immediatelyRenewed = $repo->byUuid((string) $operation['operation_uuid']);
        $this->assertGreaterThan(
            strtotime((string) $claimed['lease_expires_at']),
            strtotime((string) $immediatelyRenewed['lease_expires_at']),
            'same-second renewal must still advance expiry and retain ownership'
        );

        $nearExpiry = date('Y-m-d H:i:s', time() + 5);
        Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', (string) $operation['operation_uuid'])
            ->update(['lease_expires_at' => $nearExpiry]);
        Capsule::table('mod_securiacevps_service_locks')
            ->where('service_id', 44)
            ->update(['lease_expires_at' => $nearExpiry]);

        $this->assertTrue($repo->renew(
            (string) $operation['operation_uuid'],
            44,
            (int) $claimed['fencing_token'],
            'worker-one',
            120
        ));
        $renewed = $repo->byUuid((string) $operation['operation_uuid']);
        $this->assertGreaterThanOrEqual(
            time() + 590,
            strtotime((string) $renewed['lease_expires_at'])
        );
        $this->assertSame([], $repo->due(10));
        $this->assertNull(
            $repo->claim((string) $operation['operation_uuid'], 'worker-two', 120)
        );
        $this->assertFalse($repo->renew(
            (string) $operation['operation_uuid'],
            44,
            (int) $claimed['fencing_token'],
            'worker-two',
            120
        ));
    }

    public function testNewLifecycleIntentSupersedesAndFencesClaimedOlderIntent(): void
    {
        $repo = new OperationRepository();
        $suspend = $repo->accept(
            44,
            null,
            'provider-account',
            'suspend',
            ['operation' => 'suspend', 'service_id' => 44]
        );
        $claimedSuspend = $repo->claim(
            (string) $suspend['operation_uuid'],
            'worker-suspend',
            120
        );
        $this->assertNotNull($claimedSuspend);

        $terminate = $repo->accept(
            44,
            null,
            'provider-account',
            'terminate',
            ['operation' => 'terminate', 'service_id' => 44]
        );

        $superseded = $repo->byUuid((string) $suspend['operation_uuid']);
        $this->assertSame('superseded', $superseded['state']);
        $this->assertGreaterThan(
            (int) $claimedSuspend['fencing_token'],
            (int) $superseded['fencing_token']
        );
        $this->assertFalse(
            $repo->transition(
                (string) $suspend['operation_uuid'],
                (int) $claimedSuspend['fencing_token'],
                'succeeded'
            )
        );
        $this->assertSame(
            $terminate['operation_uuid'],
            $repo->latestLifecycleIntent(44)['operation_uuid']
        );

        // The newer intent cannot overlap the older provider call. Once that
        // worker releases its service lease, the replacement can be claimed.
        $this->assertNull(
            $repo->claim((string) $terminate['operation_uuid'], 'worker-terminate', 120)
        );
        $repo->release(
            (string) $suspend['operation_uuid'],
            44,
            (int) $claimedSuspend['fencing_token']
        );
        $this->assertNotNull(
            $repo->claim((string) $terminate['operation_uuid'], 'worker-terminate', 120)
        );
    }

    public function testOneTimeSecretCannotBeReplayed(): void
    {
        $store = new OneTimeSecretStore();
        $stored = $store->store(44, 'root_password', 'Strong-Temporary-Password');

        $this->assertSame(
            'Strong-Temporary-Password',
            $store->reveal(44, $stored['reveal_token'])
        );

        $this->expectException(ContaboProvisioningException::class);
        $store->reveal(44, $stored['reveal_token']);
    }

    private function seedSuiteSchema(): void
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
            'mod_securiacevps_snapshot_inventory',
        ];
        foreach ($tables as $table) {
            Capsule::$columns[$table] = ['id'];
            Capsule::$tables[$table] = [];
        }
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'schema_version', 'value' => '5'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
            ['key' => 'provider_writes_enabled', 'value' => '0'],
        ];
    }
}
