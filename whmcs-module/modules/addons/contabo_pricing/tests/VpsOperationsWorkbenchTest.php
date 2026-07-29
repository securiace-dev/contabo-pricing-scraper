<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CatalogImportService;
use ContaboPricing\VpsOperationsWorkbench;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class VpsOperationsWorkbenchTest extends TestCase
{
    private string $providerAccountId;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->providerAccountId = hash('sha256', 'contabo|5|client-id');
        Capsule::$tables['tblservers'] = [[
            'id' => 5,
            'name' => 'Contabo Production',
            'type' => 'securiacevps',
            'username' => 'Client-ID',
            'active' => 1,
        ]];
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'provider_writes_enabled', 'value' => '0'],
        ];
        Capsule::$tables['mod_securiacevps_operations'] = [[
            'id' => 1,
            'operation_uuid' => '11111111-1111-4111-8111-111111111111',
            'service_id' => 10,
            'state' => 'unknown_outcome',
            'created_at' => '2026-07-30 10:00:00',
        ]];
        Capsule::$tables['mod_securiacevps_operation_attempts'] = [];
        Capsule::$tables['mod_securiacevps_resources'] = [];
        Capsule::$tables['mod_securiacevps_reconciliation'] = [];
        Capsule::$tables['mod_securiacevps_adoption'] = [];
        Capsule::$tables['mod_securiacevps_capabilities'] = [];
        Capsule::$tables['mod_securiacevps_operator_commands'] = [];
        Capsule::$tables['mod_securiacevps_billing_sagas'] = [];
        Capsule::$tables['mod_securiacevps_communications'] = [];
        Capsule::$tables['tblhosting'] = [[
            'id' => 10,
            'userid' => 20,
            'packageid' => 30,
            'domain' => 'vps.example.test',
            'domainstatus' => 'Pending',
            'server' => 5,
        ]];
    }

    public function testRecoveryRequestIsAppendOnlyAndHashProtected(): void
    {
        $uuid = (new VpsOperationsWorkbench())->queueCommand(
            'reconcile_operation',
            null,
            '11111111-1111-4111-8111-111111111111',
            ['reason' => 'operator_review'],
            9
        );

        $this->assertNotSame('', $uuid);
        $this->assertSame('unknown_outcome', Capsule::$tables['mod_securiacevps_operations'][0]['state']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_operator_commands']);
        $command = Capsule::$tables['mod_securiacevps_operator_commands'][0];
        $this->assertSame('pending_validation', $command['state']);
        $this->assertSame(
            hash('sha256', CatalogImportService::canonicalJson(['reason' => 'operator_review'])),
            $command['payload_hash']
        );
    }

    public function testEnablingWritesRequiresExactConfirmation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        (new VpsOperationsWorkbench())->queueCommand(
            'set_global_write_state',
            null,
            null,
            ['enabled' => true, 'confirmation' => 'yes'],
            9
        );
    }

    public function testCapabilityCertificationAndWriteSwitchRemainSeparate(): void
    {
        $workbench = new VpsOperationsWorkbench();
        $workbench->certifyCapability(
            $this->providerAccountId,
            'create',
            'supported',
            'customer-api-2026-07',
            ['request_id' => 'redacted-evidence'],
            9,
            'CERTIFY CAPABILITY'
        );

        $this->assertCount(1, Capsule::$tables['mod_securiacevps_capabilities']);
        $this->assertSame('supported', Capsule::$tables['mod_securiacevps_capabilities'][0]['state']);
        $this->assertSame('0', Capsule::$tables['mod_securiacevps_schema'][0]['value']);
        $this->assertArrayNotHasKey('capability.create.enabled', array_column(
            Capsule::$tables['mod_securiacevps_schema'],
            'value',
            'key'
        ));
    }
}
