<?php
declare(strict_types=1);

use SecuriAceVps\Tests\Harness;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * End-to-end: customer selections on a configurable product override the
 * product-level fallbacks in the create payload (the Phase-C round-trip).
 */
final class SelectionProvisioningTest extends TestCase
{
    private Harness $h;

    protected function setUp(): void
    {
        Harness::reset();
        $this->h = new Harness();
        Harness::seedWhmcs();

        // Addon link tables: Image + Region curated & passed to provisioning.
        Capsule::$columns['mod_contabo_config_option_link'] = ['id'];
        Capsule::$columns['mod_contabo_config_option_value_link'] = ['id'];
        Capsule::$tables['mod_contabo_config_option_link'] = [
            ['id' => 10, 'dimension_key' => 'Image',  'pass_to_provisioning' => 1],
            ['id' => 11, 'dimension_key' => 'Region', 'pass_to_provisioning' => 1],
            ['id' => 12, 'dimension_key' => 'Data Protection', 'pass_to_provisioning' => 1],
        ];
        Capsule::$tables['mod_contabo_config_option_value_link'] = [
            ['id' => 100, 'option_link_id' => 10, 'whmcs_sub_id' => 501, 'contabo_value_key' => 'OS:Debian 12', 'contabo_label' => '[OS] Debian 12'],
            ['id' => 101, 'option_link_id' => 11, 'whmcs_sub_id' => 502, 'contabo_value_key' => 'Asia:Singapore', 'contabo_label' => 'Singapore'],
            ['id' => 102, 'option_link_id' => 12, 'whmcs_sub_id' => 503, 'contabo_value_key' => 'Data Protection:Auto Backup', 'contabo_label' => 'Auto Backup'],
        ];
        // The customer picked Debian 12 in Singapore with auto backup.
        Capsule::$tables['tblhostingconfigoptions'] = [
            ['relid' => 300, 'optionid' => 501, 'qty' => 1],
            ['relid' => 300, 'optionid' => 502, 'qty' => 1],
            ['relid' => 300, 'optionid' => 503, 'qty' => 1],
        ];

        $this->h->http->stub('GET /v1/compute/instances?', 200, ['data' => []]);
        $this->h->http->stub('GET /v1/secrets', 200, ['data' => []]);
        $this->h->http->stub('POST /v1/secrets', 201, ['data' => [['secretId' => 700]]]);
        $this->h->http->stub('GET /v1/compute/images', 200, ['data' => [
            ['imageId' => 'img-debian-12', 'name' => 'Debian 12'],
            ['imageId' => 'img-ubuntu-2404', 'name' => 'Ubuntu 24.04'],
        ]]);
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testSelectionsOverrideProductFallbacks(): void
    {
        $GLOBALS['__activity_log'] = [];
        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => [['instanceId' => 9100]]]);

        $result = securiacevps_CreateAccount(Harness::params());

        $this->assertSame('success', $result);
        $creates = $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances');
        $body = json_decode((string) $creates[0]['body'], true);

        $this->assertSame('img-debian-12', $body['imageId'], 'customer image selection must win over configoption1');
        $this->assertSame('SIN', $body['region'], 'customer region selection must win over configoption2');

        // The unmappable Data Protection selection is acknowledged, not dropped.
        $joined = implode(' | ', $GLOBALS['__activity_log']);
        $this->assertStringContainsString('Data Protection', $joined);
        $this->assertStringContainsString('no automated Contabo mapping', $joined);
    }

    public function testPrivateNetworkingSelectionBecomesAnAddOn(): void
    {
        Capsule::$tables['mod_contabo_config_option_link'][] =
            ['id' => 13, 'dimension_key' => 'Networking:Private Networking', 'pass_to_provisioning' => 1];
        Capsule::$tables['mod_contabo_config_option_value_link'][] =
            ['id' => 103, 'option_link_id' => 13, 'whmcs_sub_id' => 504, 'contabo_value_key' => 'Private Networking:Enabled', 'contabo_label' => 'Enabled'];
        Capsule::$tables['tblhostingconfigoptions'][] = ['relid' => 300, 'optionid' => 504, 'qty' => 1];

        $this->h->http->queue('POST /v1/compute/instances', 201, ['data' => [['instanceId' => 9101]]]);

        $this->assertSame('success', securiacevps_CreateAccount(Harness::params()));

        $creates = $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances');
        $body = json_decode((string) $creates[0]['body'], true);
        $this->assertArrayHasKey('privateNetworking', $body['addOns']);
    }

    public function testUnresolvableImageSelectionFailsClosed(): void
    {
        Capsule::$tables['mod_contabo_config_option_value_link'][0]['contabo_value_key'] = 'OS:TempleOS';

        $result = securiacevps_CreateAccount(Harness::params());

        $this->assertStringContainsString('Cannot resolve image selection', $result);
        $this->assertCount(0, $this->h->http->callsMatching('POST https://api.contabo.com/v1/compute/instances'), 'must not provision a guessed OS');
    }
}
