<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\CanonicalJson;
use SecuriAceVps\ContaboProvisioningException;
use SecuriAceVps\OneTimeSecretStore;
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
        $this->assertSame(
            'provider_pending',
            Capsule::$tables['mod_securiacevps_billing_sagas'][0]['state']
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
        $this->assertSame('Active', Capsule::$tables['tblhosting'][0]['domainstatus']);
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['password']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_secrets']);
        $this->assertSame('completed', Capsule::$tables['mod_securiacevps_billing_sagas'][0]['state']);
        $this->assertSame('1999.00', Capsule::$tables['mod_securiacevps_billing_sagas'][0]['amount']);
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

    public function testSuspendWaitsForProviderStateBeforeCommercialProjection(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('stop');
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-1', 'stopped')],
        ]);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/actions/stop',
            200,
            ['data' => []]
        );

        $this->assertSame('success', securiacevps_SuspendAccount(Harness::params()));
        $this->assertSame('Suspended', Capsule::$tables['tblhosting'][0]['domainstatus']);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/actions/stop'
            )
        );
    }

    public function testUnsuspendWaitsForProviderStateBeforeCommercialProjection(): void
    {
        Capsule::$tables['tblhosting'][0]['domainstatus'] = 'Suspended';
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('start');
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-1', 'running')],
        ]);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/actions/start',
            200,
            ['data' => []]
        );

        $this->assertSame('success', securiacevps_UnsuspendAccount(Harness::params()));
        $this->assertSame('Active', Capsule::$tables['tblhosting'][0]['domainstatus']);
    }

    public function testTerminateWaitsUntilDeletionIsVerified(): void
    {
        Capsule::$tables['tblhosting'][0]['domainstatus'] = 'Active';
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('terminate');
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-1', 'running')],
        ]);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/cancel',
            201,
            ['data' => [['cancelDate' => '2026-08-01']]]
        );
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001',
            404,
            ['message' => 'not found']
        );
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);

        $this->assertSame('success', securiacevps_TerminateAccount(Harness::params()));
        $this->assertSame('Terminated', Capsule::$tables['tblhosting'][0]['domainstatus']);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/cancel'
            )
        );
    }

    public function testOwnershipMismatchBlocksLifecycleMutation(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('stop');
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [[
                'instanceId' => 9001,
                'displayName' => 'foreign-resource',
                'status' => 'running',
            ]],
        ]);

        $result = securiacevps_SuspendAccount(Harness::params());

        $this->assertStringContainsString('administrator review', $result);
        $this->assertCount(0, $this->harness->http->callsMatching('/actions/stop'));
        $this->assertSame('Pending', Capsule::$tables['tblhosting'][0]['domainstatus']);
    }

    public function testPasswordResetIsDurableAndUsesOneTimeDelivery(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('password_reset');
        $this->harness->stubTaggedInstance('9001');
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 701]]]);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/actions/resetPassword',
            201,
            ['data' => []]
        );

        $result = securiacevps_buttonResetPassword(Harness::params());

        $this->assertSame('success', $result);
        $this->assertSame('succeeded', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['password']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_secrets']);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/actions/resetPassword'
            )
        );

        $available = (new OneTimeSecretStore())->availableForService(300);
        $this->assertNotNull($available);
        $revealed = (new OneTimeSecretStore())->reveal(300, (string) $available['reveal_token']);
        $vaultCall = $this->harness->http->callsMatching('POST https://api.contabo.com/v1/secrets')[0];
        $vaulted = json_decode((string) $vaultCall['body'], true)['value'];
        $this->assertSame($vaulted, $revealed);
        $this->expectException(ContaboProvisioningException::class);
        (new OneTimeSecretStore())->reveal(300, (string) $available['reveal_token']);
    }

    public function testAmbiguousPasswordResetIsNeverRepeated(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('password_reset');
        $this->harness->stubTaggedInstance('9001');
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 702]]]);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/actions/resetPassword',
            0,
            [],
            28,
            'operation timed out'
        );

        $first = securiacevps_buttonResetPassword(Harness::params());
        $this->assertStringContainsString('being reconciled', $first);
        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);

        $second = securiacevps_buttonResetPassword(Harness::params());

        $this->assertStringContainsString('administrator review', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/actions/resetPassword'
            )
        );
        $this->assertSame('manual_review', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
    }

    public function testReinstallUsesSealedImageAndReconcilesBeforeSuccess(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('reinstall');
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-old', 'running')],
        ]);
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 710]]]);
        $this->harness->http->queue('PUT /v1/compute/instances/9001', 200, ['data' => []]);
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-1', 'running')],
        ]);

        $result = securiacevps_buttonReinstall(Harness::params([
            'configoption1' => 'mutable-image-must-be-ignored',
        ]));

        $this->assertSame('success', $result);
        $puts = $this->harness->http->callsMatching(
            'PUT https://api.contabo.com/v1/compute/instances/9001'
        );
        $this->assertCount(1, $puts);
        $body = json_decode((string) $puts[0]['body'], true);
        $this->assertSame('image-1', $body['imageId']);
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['password']);
    }

    public function testAmbiguousReinstallPollsInsteadOfRepeatingMutation(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('reinstall');
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-old', 'running')],
        ]);
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 711]]]);
        $this->harness->http->queue(
            'PUT /v1/compute/instances/9001',
            0,
            [],
            28,
            'operation timed out'
        );

        $first = securiacevps_buttonReinstall(Harness::params());
        $this->assertStringContainsString('being reconciled', $first);
        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);

        $second = securiacevps_buttonReinstall(Harness::params());

        $this->assertStringContainsString('being reconciled', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching('PUT https://api.contabo.com/v1/compute/instances/9001')
        );
    }

    /** @return array<string,mixed> */
    private function providerInstance(string $image, string $status): array
    {
        return [
            'instanceId' => 9001,
            'displayName' => 'whmcs-300 vps.example.com',
            'status' => $status,
            'region' => 'EU',
            'imageId' => $image,
            'createdDate' => '2026-07-30T00:00:00Z',
            'ipConfig' => ['v4' => [['ip' => '203.0.113.10']]],
        ];
    }

    private function seedVerifiedOwnership(string $resourceId): void
    {
        $account = hash('sha256', 'contabo|0|');
        Capsule::$tables['mod_securiacevps_resources'][] = [
            'id' => 1,
            'service_id' => 300,
            'installation_id' => 'test-installation',
            'provider_account_id' => $account,
            'provider_resource_id' => $resourceId,
            'provider_state' => 'running',
            'provisioning_state' => 'ready',
            'ownership_state' => 'verified',
            'resource_version' => 1,
        ];
        Capsule::$tables['mod_securiacevps_adoption'][] = [
            'id' => 1,
            'service_id' => 300,
            'provider_account_id' => $account,
            'provider_resource_id' => $resourceId,
            'state' => 'verified',
            'confidence' => '1.0000',
        ];
    }

    private function seedCapability(string $capability): void
    {
        Capsule::$tables['mod_securiacevps_capabilities'][] = [
            'id' => count(Capsule::$tables['mod_securiacevps_capabilities']) + 1,
            'provider_account_id' => hash('sha256', 'contabo|0|'),
            'capability' => $capability,
            'state' => 'requires_polling',
        ];
        Capsule::$tables['mod_securiacevps_schema'][] = [
            'key' => 'capability.' . $capability . '.enabled',
            'value' => '1',
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
            ['key' => 'schema_version', 'value' => '3'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
            ['key' => 'provider_writes_enabled', 'value' => '1'],
            ['key' => 'operation_lease_seconds', 'value' => '120'],
        ];
    }
}
