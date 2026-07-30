<?php
declare(strict_types=1);

use SecuriAceVps\Tests\Harness;
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

    public function testDurableCredentialMetadataNeverLogsSecretValues(): void
    {
        $secret = 'GeneratedRootSecret-DoNotLog';
        _securiacevps_log(
            'DurableCredentialAction',
            ['nested' => ['password' => $secret], 'value' => $secret],
            ['state' => 'accepted']
        );

        $this->assertNotEmpty($GLOBALS['__module_log']);
        $logDump = json_encode($GLOBALS['__module_log']);
        $this->assertStringNotContainsString($secret, $logDump, 'a raw password leaked into logModuleCall');
    }

    public function testLoggerMasksSecretBearingRequests(): void
    {
        _securiacevps_log('TestAction', ['name' => 'x', 'value' => 'TopSecret9'], ['ok' => true]);
        $logDump = json_encode($GLOBALS['__module_log']);
        $this->assertStringNotContainsString('TopSecret9', $logDump);
        $this->assertStringContainsString('REDACTED', $logDump);
    }

    public function testSanitizerMasksNestedSecretKeys(): void
    {
        $out = _securiacevps_sanitize([
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
