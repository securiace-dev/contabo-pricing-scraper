<?php
declare(strict_types=1);

use ContaboVps\Tests\Harness;
use PHPUnit\Framework\TestCase;

/**
 * No secret material may ever reach logModuleCall: not the vault secret value,
 * not the generated password, not server credentials.
 */
final class LogRedactionTest extends TestCase
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

    public function testCreateAndResetFlowsNeverLogSecrets(): void
    {
        // Full create with a generated password…
        $this->h->http->stub('GET /v1/compute/instances?', 200, ['data' => []]);
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 700]]]);
        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => [['instanceId' => 9001]]]);
        $this->assertSame('success', contabo_vps_CreateAccount(Harness::params(['password' => ''])));

        // …then a reset.
        $this->h->stubTaggedInstance('9001');
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 701]]]);
        $this->assertSame('success', contabo_vps_buttonResetPassword(Harness::params()));

        // Recover the real passwords that were sent to the vault.
        $vaulted = [];
        foreach ($this->h->http->callsMatching('POST https://api.contabo.com/v1/secrets') as $call) {
            $vaulted[] = (string) (json_decode((string) $call['body'], true)['value'] ?? '');
        }
        $this->assertNotEmpty($vaulted);

        $this->assertNotEmpty($GLOBALS['__module_log']);
        $logDump = json_encode($GLOBALS['__module_log']);
        foreach ($vaulted as $password) {
            $this->assertStringNotContainsString($password, $logDump, 'a raw password leaked into logModuleCall');
        }
    }

    public function testLoggerMasksSecretBearingRequests(): void
    {
        _contabo_vps_log('TestAction', ['name' => 'x', 'value' => 'TopSecret9'], ['ok' => true]);
        $logDump = json_encode($GLOBALS['__module_log']);
        $this->assertStringNotContainsString('TopSecret9', $logDump);
        $this->assertStringContainsString('REDACTED', $logDump);
    }

    public function testSanitizerMasksNestedSecretKeys(): void
    {
        $out = _contabo_vps_sanitize([
            'name'  => 'whmcs-svc-1-root',
            'value' => 'SuperSecret1',
            'nested' => ['password' => 'AlsoSecret2', 'ok' => 'visible'],
            'rootPassword' => 701,
        ]);
        $this->assertSame('***REDACTED***', $out['value']);
        $this->assertSame('***REDACTED***', $out['nested']['password']);
        $this->assertSame('visible', $out['nested']['ok']);
        $this->assertSame(701, $out['rootPassword'], 'numeric secretIds stay readable');
    }
}
