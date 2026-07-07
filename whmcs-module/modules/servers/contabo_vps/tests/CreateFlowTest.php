<?php
declare(strict_types=1);

use ContaboVps\Tests\Harness;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Exercises the REAL contabo_vps_CreateAccount entry function end-to-end
 * (via Runtime::swap) — the idempotency and never-double-provision invariants.
 */
final class CreateFlowTest extends TestCase
{
    private Harness $h;

    protected function setUp(): void
    {
        Harness::reset();
        $this->h = new Harness();
        Harness::seedWhmcs();
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    private function stubEmptyTagSearch(): void
    {
        $this->h->http->stub('GET /v1/compute/instances?', 200, ['data' => []]);
    }

    private function stubSecretCreation(int $secretId = 700): void
    {
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->stub('POST /v1/secrets', 201, ['data' => [['secretId' => $secretId]]]);
    }

    public function testFreshCreateProvisionsStoresAndTags(): void
    {
        $this->stubEmptyTagSearch();
        $this->stubSecretCreation();
        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => [['instanceId' => 9001]]]);

        $result = contabo_vps_CreateAccount(Harness::params(['billingcycle' => 'Annually']));

        $this->assertSame('success', $result);

        $creates = $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances');
        $this->assertCount(1, $creates);
        $body = json_decode((string) $creates[0]['body'], true);
        $this->assertSame(12, $body['period'], 'annual order must buy a 12-month contract');
        $this->assertSame('whmcs-300 vps.example.com', $body['displayName']);
        $this->assertSame(700, $body['rootPassword'], 'WHMCS password must ride in as a vault secretId');
        $this->assertSame('V45', $body['productId']);

        // Instance id linked to the service.
        $this->assertSame('9001', Capsule::$tables['tblcustomfieldsvalues'][0]['value']);
        // WHMCS-supplied password is NOT overwritten.
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['password']);
    }

    public function testRetryAfterSuccessfulCreateIsANoOp(): void
    {
        $this->h->linkService('9001');
        $this->h->stubTaggedInstance('9001');

        $result = contabo_vps_CreateAccount(Harness::params());

        $this->assertSame('success', $result);
        $this->assertCount(0, $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances'), 'a retry must never re-provision');
    }

    public function testInterruptedCreateIsRecoveredByTagAdoption(): void
    {
        // Instance exists at Contabo (tagged) but the DB write never happened.
        $this->h->http->stub('GET /v1/compute/instances?', 200, ['data' => [
            ['instanceId' => 9001, 'displayName' => 'whmcs-300 vps.example.com'],
        ]]);

        $result = contabo_vps_CreateAccount(Harness::params());

        $this->assertSame('success', $result);
        $this->assertCount(0, $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances'));
        $this->assertSame('9001', Capsule::$tables['tblcustomfieldsvalues'][0]['value'], 'orphan must be adopted');
    }

    public function testAmbiguousOrphansBlockProvisioningInsteadOfGuessing(): void
    {
        $this->h->http->stub('GET /v1/compute/instances?', 200, ['data' => [
            ['instanceId' => 1, 'displayName' => 'whmcs-300 a'],
            ['instanceId' => 2, 'displayName' => 'whmcs-300 b'],
        ]]);

        $result = contabo_vps_CreateAccount(Harness::params());

        $this->assertStringContainsString('refusing to guess', $result);
        $this->assertCount(0, $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances'));
    }

    public function testMissingCustomFieldIsCreatedBeforeTheApiCall(): void
    {
        Capsule::$tables['tblcustomfields'] = []; // field does not exist
        $this->stubEmptyTagSearch();
        $this->stubSecretCreation();
        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => [['instanceId' => 9002]]]);

        $result = contabo_vps_CreateAccount(Harness::params());

        $this->assertSame('success', $result);
        $this->assertSame('contabo_instance_id|Contabo Instance ID', Capsule::$inserts[0]['values']['fieldname'] ?? '', 'field creation must be the FIRST write');
        // Value stored against the fresh field.
        $this->assertNotEmpty(Capsule::$tables['tblcustomfieldsvalues']);
    }

    public function testGeneratedPasswordIsPersistedEncrypted(): void
    {
        $this->stubEmptyTagSearch();
        $this->stubSecretCreation();
        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => [['instanceId' => 9001]]]);

        $result = contabo_vps_CreateAccount(Harness::params(['password' => '']));

        $this->assertSame('success', $result);
        $stored = (string) Capsule::$tables['tblhosting'][0]['password'];
        $this->assertNotSame('', $stored);
        $plain = decrypt($stored);
        $this->assertMatchesRegularExpression('/^[A-Za-z0-9]{20,}$/', $plain);

        // The same password went to the vault.
        $secretPosts = $this->h->http->callsMatching('POST https://api.contabo.com/v1/secrets');
        $secretBody = json_decode((string) $secretPosts[0]['body'], true);
        $this->assertSame($plain, $secretBody['value']);
    }

    public function testApiReturningNoInstanceIdIsAnError(): void
    {
        $this->stubEmptyTagSearch();
        $this->stubSecretCreation();
        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => []]);

        $result = contabo_vps_CreateAccount(Harness::params());

        $this->assertStringContainsString('no instanceId', $result);
        $this->assertSame([], Capsule::$tables['tblcustomfieldsvalues'], 'nothing must be linked');
    }

    public function testStoredIdPointingAtAForeignInstanceBlocksRetry(): void
    {
        $this->h->linkService('9001');
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [
            ['instanceId' => 9001, 'displayName' => 'someone elses box', 'status' => 'running'],
        ]]);

        $result = contabo_vps_CreateAccount(Harness::params());

        $this->assertStringContainsString('no longer carries the tag', $result);
        $this->assertCount(0, $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances'));
    }
}
