<?php
declare(strict_types=1);

use ContaboVps\Tests\Harness;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class ResetPasswordFlowTest extends TestCase
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

    public function testResetGeneratesVaultsAndPersistsThePassword(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 701]]]);
        $this->h->http->queue('POST /v1/compute/instances/9001/actions/resetPassword', 201, ['data' => []]);

        $result = contabo_vps_buttonResetPassword(Harness::params());

        $this->assertSame('success', $result);

        $actions = $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances/9001/actions/resetPassword');
        $this->assertCount(1, $actions);
        $this->assertSame(['rootPassword' => 701], json_decode((string) $actions[0]['body'], true));

        // The password WHMCS now shows matches what went to the vault.
        $stored = decrypt((string) Capsule::$tables['tblhosting'][0]['password']);
        $secretPosts = $this->h->http->callsMatching('POST https://api.contabo.com/v1/secrets');
        $vaulted = json_decode((string) $secretPosts[0]['body'], true)['value'];
        $this->assertSame($vaulted, $stored);
    }

    public function testClientButtonUsesTheSameFlow(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 702]]]);
        $this->h->http->queue('POST /v1/compute/instances/9001/actions/resetPassword', 201, ['data' => []]);

        $this->assertSame('success', contabo_vps_clientResetPassword(Harness::params()));
    }

    public function testFailedActionLeavesTheStoredPasswordUntouched(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 703]]]);
        $this->h->http->setDefault(400, ['message' => 'action rejected']);

        $result = contabo_vps_buttonResetPassword(Harness::params());

        $this->assertStringContainsString('HTTP 400', $result);
        $this->assertSame('', Capsule::$tables['tblhosting'][0]['password'], 'a failed reset must not change the WHMCS password');
    }

    public function testTagMismatchBlocksReset(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [
            ['instanceId' => 9001, 'displayName' => 'foreign box', 'status' => 'running'],
        ]]);

        $result = contabo_vps_buttonResetPassword(Harness::params());

        $this->assertStringContainsString('Refusing to reset', $result);
        $this->assertCount(0, $this->h->http->callsMatching('resetPassword'));
    }
}
