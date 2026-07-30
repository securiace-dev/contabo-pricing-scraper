<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\AdoptionService;
use SecuriAceVps\ContaboApiClient;
use SecuriAceVps\ContaboAuth;
use WHMCS\Database\Capsule;

final class AdoptionServiceTest extends TestCase
{
    /** @var Harness */
    private $harness;
    /** @var AdoptionService */
    private $adoption;

    protected function setUp(): void
    {
        Harness::reset();
        $this->harness = new Harness();
        Harness::seedWhmcs();
        foreach ([
            'mod_securiacevps_schema',
            'mod_securiacevps_order_snapshots',
            'mod_securiacevps_resources',
            'mod_securiacevps_operations',
            'mod_securiacevps_operation_attempts',
            'mod_securiacevps_provider_requests',
            'mod_securiacevps_service_locks',
            'mod_securiacevps_capabilities',
            'mod_securiacevps_adoption',
            'mod_securiacevps_reconciliation',
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
            ['key' => 'schema_version', 'value' => '4'],
            ['key' => 'installation_id', 'value' => 'test-installation'],
        ];
        $auth = new ContaboAuth('cid', 'cs', 'u@example.com', 'pw', $this->harness->http);
        $client = new ContaboApiClient(
            $auth,
            $this->harness->http,
            static function (int $seconds): void {
            }
        );
        $this->adoption = new AdoptionService($client);
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testExplicitTwoAnchorOwnershipIsVerifiedWithoutProviderWrite(): void
    {
        $this->harness->linkService('9001');
        $this->harness->stubTaggedInstance('9001');

        $result = $this->adoption->assess(Harness::params());

        $this->assertSame('verified', $result['state']);
        $this->assertSame('verified', Capsule::$tables['mod_securiacevps_resources'][0]['ownership_state']);
        $this->assertSame([], $this->providerMutationCalls());
    }

    public function testTagOnlyCandidateRequiresExplicitAdministratorApproval(): void
    {
        $this->harness->http->stub('GET /v1/compute/instances?', 200, [
            'data' => [$this->instance('9001', 'whmcs-300 vps.example.com')],
        ]);

        $candidate = $this->adoption->assess(Harness::params());

        $this->assertSame('probable', $candidate['state']);
        $this->assertSame([], Capsule::$tables['tblcustomfieldsvalues']);
        $this->harness->stubTaggedInstance('9001');

        $verified = $this->adoption->approveCandidate(
            Harness::params(),
            '9001',
            (string) $candidate['evidence_hash'],
            42
        );

        $this->assertSame('verified', $verified['state']);
        $this->assertSame('9001', Capsule::$tables['tblcustomfieldsvalues'][0]['value']);
        $this->assertSame([], $this->providerMutationCalls());
    }

    public function testForeignTagProducesConflictAndNeverRewritesIt(): void
    {
        $this->harness->linkService('9001');
        $this->harness->http->stub('GET /v1/compute/instances/9001', 200, [
            'data' => [$this->instance('9001', 'whmcs-999 another-service')],
        ]);

        $result = $this->adoption->assess(Harness::params());

        $this->assertSame('conflict', $result['state']);
        $this->assertSame('conflict', Capsule::$tables['mod_securiacevps_resources'][0]['ownership_state']);
        $this->assertSame([], $this->providerMutationCalls());
    }

    public function testProviderInventoryFindsOnlyTaggedUnmappedResources(): void
    {
        $this->harness->http->stub('GET /v1/compute/instances?', 200, [
            'data' => [
                $this->instance('9001', 'whmcs-300 vps.example.com'),
                $this->instance('9002', 'manual-resource'),
            ],
        ]);

        $result = $this->adoption->inventoryProviderAccount(Harness::params());

        $this->assertSame(2, $result['observed']);
        $this->assertSame(1, $result['managed']);
        $this->assertSame(1, $result['orphans']);
        $this->assertFalse($result['truncated']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_reconciliation']);
        $finding = Capsule::$tables['mod_securiacevps_reconciliation'][0];
        $this->assertSame('orphan_upstream', $finding['finding_type']);
        $this->assertSame(300, $finding['service_id']);
        $this->assertSame('review_adoption', $finding['safe_next_action']);
        $this->assertSame([], $this->providerMutationCalls());
    }

    public function testMappedProviderInventoryResolvesPriorOrphanFinding(): void
    {
        $accountId = hash('sha256', 'contabo|0|');
        Capsule::$tables['mod_securiacevps_resources'][] = [
            'id' => 10,
            'service_id' => 300,
            'provider_account_id' => $accountId,
            'provider_resource_id' => '9001',
        ];
        Capsule::$tables['mod_securiacevps_reconciliation'][] = [
            'id' => 11,
            'finding_uuid' => 'finding-1',
            'service_id' => 300,
            'provider_account_id' => $accountId,
            'provider_resource_id' => '9001',
            'finding_type' => 'orphan_upstream',
            'state' => 'open',
        ];
        $this->harness->http->stub('GET /v1/compute/instances?', 200, [
            'data' => [$this->instance('9001', 'whmcs-300 vps.example.com')],
        ]);

        $result = $this->adoption->inventoryProviderAccount(Harness::params());

        $this->assertSame(0, $result['orphans']);
        $this->assertSame('resolved', Capsule::$tables['mod_securiacevps_reconciliation'][0]['state']);
        $this->assertSame([], $this->providerMutationCalls());
    }

    /** @return array<string,mixed> */
    private function instance(string $id, string $displayName): array
    {
        return [
            'instanceId' => (int) $id,
            'displayName' => $displayName,
            'status' => 'running',
            'region' => 'EU',
            'imageId' => 'image-1',
            'ipConfig' => ['v4' => [['ip' => '203.0.113.10']]],
        ];
    }

    /** @return list<array<string,mixed>> */
    private function providerMutationCalls(): array
    {
        return array_values(array_filter(
            $this->harness->http->calls,
            static function (array $call): bool {
                return in_array((string) ($call['method'] ?? ''), ['POST', 'PUT', 'PATCH', 'DELETE'], true)
                    && strpos((string) ($call['url'] ?? ''), 'auth.contabo.com') === false;
            }
        ));
    }
}
