<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\CanonicalJson;
use SecuriAceVps\ContaboApiClient;
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

    public function testCreatePreflightFailureDoesNotStrandSubmissionMarker(): void
    {
        for ($attempt = 0; $attempt < 4; $attempt++) {
            $this->harness->http->queue(
                'GET /v1/compute/instances?',
                503,
                ['message' => 'catalog temporarily unavailable']
            );
        }

        $first = securiacevps_CreateAccount(Harness::params());

        $this->assertStringContainsString('still in progress', $first);
        $this->assertSame([], Capsule::$tables['mod_securiacevps_provider_requests']);
        $this->assertCount(
            0,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances'
            )
        );

        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);
        $this->harness->http->stub(
            'GET /v1/compute/instances?',
            200,
            ['data' => [], '_pagination' => ['totalElements' => 0]]
        );
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue(
            'POST /v1/secrets',
            201,
            ['data' => [['secretId' => 78]]]
        );
        $this->harness->http->queue('POST /v1/compute/instances', 201, [
            'data' => [['instanceId' => 9003]],
        ]);
        $this->harness->http->stub('GET /v1/compute/instances/9003', 200, [
            'data' => [[
                'instanceId' => 9003,
                'displayName' => 'whmcs-300 vps.example.com',
                'status' => 'running',
                'region' => 'EU',
                'imageId' => 'image-1',
                'createdDate' => '2026-07-30T00:00:00Z',
                'ipConfig' => ['v4' => [['ip' => '203.0.113.30']]],
            ]],
        ]);

        $second = securiacevps_CreateAccount(Harness::params());

        $this->assertSame('success', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances'
            )
        );
        $this->assertSame(
            'reconciled',
            Capsule::$tables['mod_securiacevps_provider_requests'][0]['state']
        );
    }

    public function testDelayedCreateCannotReactivateCancelledService(): void
    {
        Capsule::$tables['tblhosting'][0]['domainstatus'] = 'Cancelled';
        $this->harness->http->stub(
            'GET /v1/compute/instances?',
            200,
            ['data' => [], '_pagination' => ['totalElements' => 0]]
        );
        $this->harness->http->stub('GET /v1/secrets?', 200, ['data' => []]);
        $this->harness->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 79]]]);
        $this->harness->http->queue('POST /v1/compute/instances', 201, [
            'data' => [['instanceId' => 9002]],
        ]);
        $ready = [
            'data' => [[
                'instanceId' => 9002,
                'displayName' => 'whmcs-300 vps.example.com',
                'status' => 'running',
                'region' => 'EU',
                'imageId' => 'image-1',
                'createdDate' => '2026-07-30T00:00:00Z',
                'ipConfig' => ['v4' => [['ip' => '203.0.113.20']]],
            ]],
        ];
        $this->harness->http->stub('GET /v1/compute/instances/9002', 200, $ready);

        $result = securiacevps_CreateAccount(Harness::params());

        $this->assertStringContainsString('requires administrator review', $result);
        $this->assertSame('Cancelled', Capsule::$tables['tblhosting'][0]['domainstatus']);
        $this->assertSame('succeeded', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
        $this->assertSame(
            'service_status_projection_conflict',
            Capsule::$tables['mod_securiacevps_operations'][0]['safe_error_code']
        );
        $this->assertSame(
            'whmcs_service_projection',
            Capsule::$tables['mod_securiacevps_reconciliation'][0]['finding_type']
        );
        $this->assertCount(
            1,
            $this->harness->http->callsMatching('POST https://api.contabo.com/v1/compute/instances')
        );
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
        Capsule::$tables['tblhosting'][0]['domainstatus'] = 'Active';
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

    public function testPowerPreflightFailureRetriesWithoutFalseSubmissionMarker(): void
    {
        Capsule::$tables['tblhosting'][0]['domainstatus'] = 'Active';
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('stop');
        for ($attempt = 0; $attempt < 4; $attempt++) {
            $this->harness->http->queue(
                'GET /v1/compute/instances/9001',
                503,
                ['message' => 'provider temporarily unavailable']
            );
        }

        $first = securiacevps_SuspendAccount(Harness::params());

        $this->assertStringContainsString('still in progress', $first);
        $this->assertSame([], Capsule::$tables['mod_securiacevps_provider_requests']);
        $this->assertCount(
            0,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/actions/stop'
            )
        );

        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->providerInstance('image-1', 'stopped')],
        ]);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/actions/stop',
            200,
            ['data' => []]
        );

        $second = securiacevps_SuspendAccount(Harness::params());

        $this->assertSame('success', $second);
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

    public function testSnapshotCreateUsesDurableRequestAndProjectsInventory(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('snapshot_list');
        $this->seedCapability('snapshot_create');
        $instance = ['data' => [$this->providerInstance('image-1', 'running')]];
        $snapshot = $this->providerSnapshot('snap-100', 'Before release');
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/snapshots',
            201,
            ['data' => [$snapshot]]
        );
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001/snapshots?',
            200,
            ['data' => [$snapshot], '_pagination' => ['totalPages' => 1]]
        );

        $result = Runtime::lifecycle()->createSnapshot(
            Harness::params(),
            'Before release',
            'Known-good application state'
        );

        $this->assertSame('success', $result);
        $this->assertSame('succeeded', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
        $this->assertSame('snap-100', Capsule::$tables['mod_securiacevps_operations'][0]['provider_resource_id']);
        $this->assertSame('snap-100', Capsule::$tables['mod_securiacevps_snapshot_inventory'][0]['snapshot_id']);
        $calls = $this->harness->http->callsMatching(
            'POST https://api.contabo.com/v1/compute/instances/9001/snapshots'
        );
        $this->assertCount(1, $calls);
        $requestHeaders = implode("\n", $calls[0]['headers']);
        $this->assertMatchesRegularExpression(
            '/x-request-id: [0-9a-f-]{36}/',
            $requestHeaders
        );
    }

    public function testSnapshotDeleteVerifiesAbsenceAndNeverBlindlyRepeats(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('snapshot_list');
        $this->seedCapability('snapshot_delete');
        $row = $this->seedSnapshotInventory('snap-200', 'Disposable');
        $instance = ['data' => [$this->providerInstance('image-1', 'running')]];
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'DELETE /v1/compute/instances/9001/snapshots/snap-200',
            204,
            ''
        );
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001/snapshots?',
            200,
            ['data' => [], '_pagination' => ['totalPages' => 1]]
        );

        $result = Runtime::lifecycle()->deleteSnapshot(
            Harness::params(),
            'snap-200',
            (string) $row['payload_hash']
        );

        $this->assertSame('success', $result);
        $this->assertSame([], Capsule::$tables['mod_securiacevps_snapshot_inventory']);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'DELETE https://api.contabo.com/v1/compute/instances/9001/snapshots/snap-200'
            )
        );
    }

    public function testSnapshotRollbackRequiresExactAuditAndRefreshesInventory(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('snapshot_list');
        $this->seedCapability('snapshot_rollback');
        $row = $this->seedSnapshotInventory('snap-300', 'Known good');
        $requestId = $this->snapshotRequestId('snapshot_rollback');
        $instance = ['data' => [$this->providerInstance('image-1', 'running')]];
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/snapshots/snap-300/rollback',
            200,
            ['_links' => ['self' => '/v1/compute/instances/9001/snapshots/snap-300']]
        );
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue('GET /v1/compute/snapshots/audits?', 200, [
            'data' => [[
                'action' => 'ROLLED_BACK',
                'requestId' => $requestId,
                'instanceId' => 9001,
                'snapshotId' => 'snap-300',
            ]],
        ]);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001/snapshots?',
            200,
            [
                'data' => [$this->providerSnapshot('snap-300', 'Known good')],
                '_pagination' => ['totalPages' => 1],
            ]
        );

        $result = Runtime::lifecycle()->rollbackSnapshot(
            Harness::params(),
            'snap-300',
            (string) $row['payload_hash']
        );

        $this->assertSame('success', $result);
        $this->assertSame('succeeded', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
        $this->assertSame(
            $requestId,
            Capsule::$tables['mod_securiacevps_provider_requests'][0]['provider_request_id']
        );
    }

    public function testAmbiguousSnapshotCreateReconcilesAuditWithoutSecondPost(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('snapshot_list');
        $this->seedCapability('snapshot_create');
        $requestId = $this->snapshotRequestId('snapshot_create');
        $instance = ['data' => [$this->providerInstance('image-1', 'running')]];
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/snapshots',
            0,
            '',
            28,
            'timeout after provider acceptance'
        );

        $first = Runtime::lifecycle()->createSnapshot(Harness::params(), 'Ambiguous');
        $this->assertStringContainsString('being reconciled', $first);

        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue('GET /v1/compute/snapshots/audits?', 200, [
            'data' => [[
                'action' => 'CREATED',
                'requestId' => $requestId,
                'instanceId' => 9001,
                'snapshotId' => 'snap-400',
            ]],
        ]);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001/snapshots?',
            200,
            [
                'data' => [$this->providerSnapshot('snap-400', 'Ambiguous')],
                '_pagination' => ['totalPages' => 1],
            ]
        );

        $second = Runtime::lifecycle()->createSnapshot(Harness::params(), 'Ambiguous');

        $this->assertSame('success', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/snapshots'
            )
        );
    }

    public function testAmbiguousSnapshotDeleteVerifiesAbsenceWithoutSecondDelete(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('snapshot_list');
        $this->seedCapability('snapshot_delete');
        $row = $this->seedSnapshotInventory('snap-500', 'Disposable');
        $instance = ['data' => [$this->providerInstance('image-1', 'running')]];
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'DELETE /v1/compute/instances/9001/snapshots/snap-500',
            0,
            '',
            28,
            'timeout after provider acceptance'
        );

        $first = Runtime::lifecycle()->deleteSnapshot(
            Harness::params(),
            'snap-500',
            (string) $row['payload_hash']
        );
        $this->assertStringContainsString('being reconciled', $first);

        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001/snapshots?',
            200,
            ['data' => [], '_pagination' => ['totalPages' => 1]]
        );

        $second = Runtime::lifecycle()->deleteSnapshot(
            Harness::params(),
            'snap-500',
            (string) $row['payload_hash']
        );

        $this->assertSame('success', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'DELETE https://api.contabo.com/v1/compute/instances/9001/snapshots/snap-500'
            )
        );
    }

    public function testSnapshotRollbackWaitsForAuditWithoutSecondPost(): void
    {
        $this->harness->linkService('9001');
        $this->seedVerifiedOwnership('9001');
        $this->seedCapability('snapshot_list');
        $this->seedCapability('snapshot_rollback');
        $row = $this->seedSnapshotInventory('snap-600', 'Known good');
        $requestId = $this->snapshotRequestId('snapshot_rollback');
        $instance = ['data' => [$this->providerInstance('image-1', 'running')]];
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'POST /v1/compute/instances/9001/snapshots/snap-600/rollback',
            200,
            ['_links' => ['self' => '/v1/compute/instances/9001/snapshots/snap-600']]
        );
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/snapshots/audits?',
            200,
            ['data' => []]
        );

        $first = Runtime::lifecycle()->rollbackSnapshot(
            Harness::params(),
            'snap-600',
            (string) $row['payload_hash']
        );
        $this->assertStringContainsString('still in progress', $first);

        Capsule::$tables['mod_securiacevps_operations'][0]['next_attempt_at'] =
            date('Y-m-d H:i:s', time() - 1);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue('GET /v1/compute/snapshots/audits?', 200, [
            'data' => [[
                'action' => 'ROLLED_BACK',
                'requestId' => $requestId,
                'instanceId' => 9001,
                'snapshotId' => 'snap-600',
            ]],
        ]);
        $this->harness->http->queue('GET /v1/compute/instances/9001', 200, $instance);
        $this->harness->http->queue(
            'GET /v1/compute/instances/9001/snapshots?',
            200,
            [
                'data' => [$this->providerSnapshot('snap-600', 'Known good')],
                '_pagination' => ['totalPages' => 1],
            ]
        );

        $second = Runtime::lifecycle()->rollbackSnapshot(
            Harness::params(),
            'snap-600',
            (string) $row['payload_hash']
        );

        $this->assertSame('success', $second);
        $this->assertCount(
            1,
            $this->harness->http->callsMatching(
                'POST https://api.contabo.com/v1/compute/instances/9001/snapshots/snap-600/rollback'
            )
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

    /** @return array<string,mixed> */
    private function providerSnapshot(string $snapshotId, string $name): array
    {
        return [
            'snapshotId' => $snapshotId,
            'name' => $name,
            'description' => 'Test snapshot',
            'instanceId' => 9001,
            'createdDate' => '2026-07-30T02:00:00Z',
            'autoDeleteDate' => '2026-08-29T02:00:00Z',
            'imageId' => 'image-1',
            'imageName' => 'Ubuntu',
        ];
    }

    /** @return array<string,mixed> */
    private function seedSnapshotInventory(string $snapshotId, string $name): array
    {
        $safe = [
            'snapshot_id' => $snapshotId,
            'name' => $name,
            'description' => 'Test snapshot',
            'image_id' => 'image-1',
            'image_name' => 'Ubuntu',
            'provider_created_at' => '2026-07-30 02:00:00',
            'provider_auto_delete_at' => '2026-08-29 02:00:00',
        ];
        $row = array_merge($safe, [
            'id' => count(Capsule::$tables['mod_securiacevps_snapshot_inventory']) + 1,
            'service_id' => 300,
            'provider_account_id' => hash('sha256', 'contabo|0|'),
            'provider_resource_id' => '9001',
            'payload_hash' => hash('sha256', CanonicalJson::encode($safe)),
            'observed_at' => '2026-07-30 02:00:00',
        ]);
        Capsule::$tables['mod_securiacevps_snapshot_inventory'][] = $row;
        return $row;
    }

    private function snapshotRequestId(string $type): string
    {
        $command = hash(
            'sha256',
            implode('|', ['test-installation', '300', '', $type, '1'])
        );
        return ContaboApiClient::requestIdForIdentity(hash('sha256', 'provider|' . $command));
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
            'mod_securiacevps_snapshot_inventory',
        ];
        foreach ($tables as $table) {
            Capsule::$columns[$table] = ['id'];
            Capsule::$tables[$table] = [];
        }
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'schema_version', 'value' => '5'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
            ['key' => 'provider_writes_enabled', 'value' => '1'],
            ['key' => 'operation_lease_seconds', 'value' => '120'],
        ];
    }
}
