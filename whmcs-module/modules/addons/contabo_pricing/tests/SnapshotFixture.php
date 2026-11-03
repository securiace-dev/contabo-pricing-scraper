<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use WHMCS\Database\Capsule;

/** Shared WHMCS order/invoice/mapping fixture for the paid-order snapshot flow tests. */
trait SnapshotFixture
{
    /** Stamps every publication row with the sha256 of its payload_json (what CheckoutGuard stores). */
    private function hashPublications(): void
    {
        foreach (Capsule::$tables['mod_contabo_mapping_publications'] ?? [] as $i => $row) {
            $row = (array) $row;
            $row['payload_hash'] = hash('sha256', (string) ($row['payload_json'] ?? ''));
            Capsule::$tables['mod_contabo_mapping_publications'][$i] = $row;
        }
    }

    private function bootFixture(): void
    {
        Capsule::reset();
        $GLOBALS['contabo_pricing_test_activity_log'] = [];
        $GLOBALS['contabo_pricing_test_module_log'] = [];
        Capsule::$columns['mod_contabo_catalog_versions'] = [
            'catalog_version', 'state', 'payload_hash', 'source_observed_at',
        ];
        Capsule::$columns['mod_contabo_catalog_items'] = [
            'catalog_version_id', 'machine_id', 'provider_id', 'item_type',
            'availability_state', 'payload_hash', 'payload_json',
        ];
        Capsule::$columns['mod_contabo_mapping_publications'] = [
            'mapping_version', 'product_id', 'catalog_version_id', 'provider_sku_id',
            'state', 'payload_hash', 'payload_json',
        ];
        Capsule::$columns['mod_contabo_publication_approvals'] = [
            'publication_type', 'publication_version', 'decision', 'admin_id', 'preview_hash',
        ];
        $this->seedFixture();
    }

    private function seedFixture(): void
    {
        Capsule::$tables['mod_securiacevps_schema'] = [
            ['key' => 'installation_id', 'value' => 'install-1'],
        ];
        Capsule::$tables['mod_securiacevps_order_snapshots'] = [];
        Capsule::$tables['tblhosting'] = [[
            'id' => 30,
            'orderid' => 10,
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
            'invoiceid' => 20,
            'status' => 'Pending',
            'promocode' => 'WELCOME',
            'amount' => '1999.00',
        ]];
        Capsule::$tables['tblinvoices'] = [[
            'id' => 20,
            'userid' => 40,
            'status' => 'Unpaid',
            'subtotal' => '1999.00',
            'tax' => '0.00',
            'tax2' => '0.00',
            'credit' => '0.00',
            'total' => '1999.00',
        ]];
        Capsule::$tables['tblclients'] = [['id' => 40, 'currency' => 1]];
        // Admin 7 is a Full Administrator (role 1), 8 a Support Operator (role 3), 9 a disabled Full Administrator.
        Capsule::$tables['tbladmins'] = [
            ['id' => 7, 'roleid' => 1, 'disabled' => 0],
            ['id' => 8, 'roleid' => 3, 'disabled' => 0],
            ['id' => 9, 'roleid' => 1, 'disabled' => 1],
        ];
        $GLOBALS['cc_encryption_hash'] = 'test-encryption-hash-not-a-secret';
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
            'product_id' => 50,
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
        $this->hashPublications();
    }
}
