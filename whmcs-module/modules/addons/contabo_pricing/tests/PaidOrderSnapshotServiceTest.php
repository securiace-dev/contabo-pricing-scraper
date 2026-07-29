<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\PaidOrderSnapshotService;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class PaidOrderSnapshotServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        $this->seed();
    }

    public function testDraftUsesPublishedMappingAndExactWhmcsAmounts(): void
    {
        $uuid = (new PaidOrderSnapshotService())->createDraft(10, 20, 30);
        $row = (array) Capsule::$tables['mod_securiacevps_order_snapshots'][0];
        $payload = json_decode((string) $row['payload_json'], true);

        $this->assertNotSame('', $uuid);
        $this->assertSame('mapping-v1', $row['mapping_version']);
        $this->assertSame('V45', $payload['provider']['sku_id']);
        $this->assertSame('1999.00', $payload['pricing']['recurring']);
        $this->assertSame('Ubuntu 24.04', $payload['configuration']['options'][0]['label']);
        $this->assertSame('INR', $payload['pricing']['currency']);
        $this->assertSame('draft', $row['state']);
    }

    public function testOnlyPaidNonFraudOrderCanSeal(): void
    {
        $service = new PaidOrderSnapshotService();
        $service->createDraft(10, 20, 30);

        $this->assertSame(0, $service->sealOrder(10, 20));
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        $this->assertSame(1, $service->sealOrder(10, 20));
        $this->assertSame('sealed', Capsule::$tables['mod_securiacevps_order_snapshots'][0]['state']);
    }

    public function testSealedSnapshotCannotBeRewrittenByCatalogOrPriceChanges(): void
    {
        $service = new PaidOrderSnapshotService();
        $uuid = $service->createDraft(10, 20, 30);
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        $service->sealOrder(10, 20);
        $before = Capsule::$tables['mod_securiacevps_order_snapshots'][0]['payload_json'];

        Capsule::table('tblhosting')->where('id', 30)->update(['amount' => '9999.00']);
        Capsule::table('mod_contabo_mapping_publications')
            ->where('mapping_version', 'mapping-v1')
            ->update(['payload_json' => json_encode(['provider' => ['sku_id' => 'DIFFERENT']])]);

        $this->assertSame($uuid, $service->createDraft(10, 20, 30));
        $this->assertSame($before, Capsule::$tables['mod_securiacevps_order_snapshots'][0]['payload_json']);
        $this->assertCount(1, Capsule::$tables['mod_securiacevps_order_snapshots']);
    }

    private function seed(): void
    {
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'installation_id', 'value' => 'install-1'],
        ];
        Capsule::$tables['mod_securiacevps_order_snapshots'] = [];
        Capsule::$tables['tblhosting'] = [[
            'id' => 30,
            'userid' => 40,
            'packageid' => 50,
            'billingcycle' => 'Monthly',
            'domain' => 'vps.example.com',
            'firstpaymentamount' => '1999.00',
            'amount' => '1999.00',
        ]];
        Capsule::$tables['tblproducts'] = [[
            'id' => 50,
            'gid' => 60,
            'servertype' => 'securiacevps',
        ]];
        Capsule::$tables['tblorders'] = [[
            'id' => 10,
            'status' => 'Pending',
            'promocode' => 'WELCOME',
            'amount' => '1999.00',
        ]];
        Capsule::$tables['tblinvoices'] = [[
            'id' => 20,
            'status' => 'Unpaid',
            'subtotal' => '1999.00',
            'tax' => '0.00',
            'tax2' => '0.00',
            'credit' => '0.00',
            'total' => '1999.00',
        ]];
        Capsule::$tables['tblclients'] = [['id' => 40, 'currency' => 1]];
        Capsule::$tables['tblcurrencies'] = [['id' => 1, 'code' => 'INR']];
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id' => 70,
            'profile_id' => 80,
            'product_id' => 50,
            'active' => 1,
            'mapping_state' => 'published',
            'published_mapping_version' => 'mapping-v1',
            'rust_catalog_version' => 'catalog-v1',
        ]];
        Capsule::$tables['mod_contabo_mapping_publications'] = [[
            'id' => 90,
            'mapping_version' => 'mapping-v1',
            'state' => 'published',
            'payload_json' => json_encode([
                'provider' => [
                    'sku_id' => 'V45',
                    'region_id' => 'EU',
                    'image_id' => 'image-1',
                ],
                'pricing_profile_version' => 'profile-v3',
                'compatibility_version' => 'compat-v2',
                'management_code' => 'self_managed',
                'tax_policy' => 'whmcs',
                'renewal_amount' => '2199.00',
            ]),
        ]];
        Capsule::$tables['tblhostingconfigoptions'] = [[
            'relid' => 30,
            'configid' => 100,
            'optionid' => 101,
            'qty' => 1,
        ]];
        Capsule::$tables['mod_contabo_config_option_value_link'] = [[
            'option_link_id' => 102,
            'whmcs_sub_id' => 101,
            'contabo_value_key' => 'ubuntu-24-04',
            'contabo_label' => 'Ubuntu 24.04',
        ]];
        Capsule::$tables['mod_contabo_config_option_link'] = [[
            'id' => 102,
            'dimension_key' => 'Image',
        ]];
    }
}
