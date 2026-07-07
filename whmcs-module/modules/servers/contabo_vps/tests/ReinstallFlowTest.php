<?php
declare(strict_types=1);

use ContaboVps\Tests\Harness;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class ReinstallFlowTest extends TestCase
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

    public function testReinstallRebuildsWithConfiguredImageAndFreshPassword(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 710]]]);
        $this->h->http->queue('PUT /v1/compute/instances/9001', 200, ['data' => []]);

        $result = contabo_vps_buttonReinstall(Harness::params());

        $this->assertSame('success', $result);
        $puts = $this->h->http->callsMatching('PUT https://api.contabo.com/v1/compute/instances/9001');
        $this->assertCount(1, $puts);
        $body = json_decode((string) $puts[0]['body'], true);
        $this->assertSame('afecbb85-e2fc-46f0-9684-b46b1faf00bb', $body['imageId']);
        $this->assertSame(710, $body['rootPassword']);

        // The rebuild's password is what WHMCS now shows.
        $stored = decrypt((string) Capsule::$tables['tblhosting'][0]['password']);
        $vaulted = json_decode((string) $this->h->http->callsMatching('POST https://api.contabo.com/v1/secrets')[0]['body'], true)['value'];
        $this->assertSame($vaulted, $stored);
    }

    public function testReinstallCarriesSshAndCloudInit(): void
    {
        $this->h->stubTaggedInstance('9001');
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 711]]]);
        $this->h->http->queue('PUT /v1/compute/instances/9001', 200, ['data' => []]);

        contabo_vps_buttonReinstall(Harness::params([
            'configoption3' => '4242',
            'configoption5' => "#cloud-config\npackages: [nginx]",
        ]));

        $body = json_decode((string) $this->h->http->callsMatching('PUT https://api.contabo.com/v1/compute/instances/9001')[0]['body'], true);
        $this->assertSame([4242], $body['sshKeys']);
        $this->assertSame("#cloud-config\npackages: [nginx]", $body['userData']);
    }

    public function testReinstallTagMismatchBlocks(): void
    {
        $this->h->http->stub('GET /v1/compute/instances/9001', 200, ['data' => [
            ['instanceId' => 9001, 'displayName' => 'foreign box', 'status' => 'running'],
        ]]);

        $result = contabo_vps_buttonReinstall(Harness::params());

        $this->assertStringContainsString('Refusing to reinstall', $result);
        $this->assertCount(0, $this->h->http->callsMatching('PUT https://api.contabo.com/v1/compute/instances/9001'));
    }

    public function testReinstallWithNoImageFailsClosed(): void
    {
        $this->h->stubTaggedInstance('9001');
        $result = contabo_vps_buttonReinstall(Harness::params(['configoption1' => '']));

        $this->assertStringContainsString('no image resolved', $result);
        $this->assertCount(0, $this->h->http->callsMatching('PUT https://api.contabo.com/v1/compute/instances/9001'));
    }
}
