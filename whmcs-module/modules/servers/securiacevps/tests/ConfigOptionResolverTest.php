<?php
declare(strict_types=1);

use SecuriAceVps\ConfigOptionResolver;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class ConfigOptionResolverTest extends TestCase
{
    private ConfigOptionResolver $resolver;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->resolver = new ConfigOptionResolver();
    }

    private function seedLinkTables(): void
    {
        Capsule::$columns['mod_contabo_config_option_link'] = ['id'];
        Capsule::$columns['mod_contabo_config_option_value_link'] = ['id'];
        Capsule::$tables['mod_contabo_config_option_link'] = [
            ['id' => 10, 'dimension_key' => 'Image',  'pass_to_provisioning' => 1],
            ['id' => 11, 'dimension_key' => 'Region', 'pass_to_provisioning' => 1],
            ['id' => 12, 'dimension_key' => 'Data Protection', 'pass_to_provisioning' => 0],
        ];
        Capsule::$tables['mod_contabo_config_option_value_link'] = [
            ['id' => 100, 'option_link_id' => 10, 'whmcs_sub_id' => 501, 'contabo_value_key' => 'OS:Ubuntu 24.04', 'contabo_label' => '[OS] Ubuntu 24.04'],
            ['id' => 101, 'option_link_id' => 11, 'whmcs_sub_id' => 502, 'contabo_value_key' => 'Europe:European Union', 'contabo_label' => 'European Union'],
            ['id' => 102, 'option_link_id' => 12, 'whmcs_sub_id' => 503, 'contabo_value_key' => 'Data Protection:Auto Backup', 'contabo_label' => 'Auto Backup'],
        ];
    }

    public function testReturnsEmptyWhenLinkTablesAbsent(): void
    {
        Capsule::$tables['tblhostingconfigoptions'] = [
            ['relid' => 300, 'optionid' => 501, 'qty' => 1],
        ];
        $this->assertSame([], $this->resolver->selectionsForService(300));
    }

    public function testRoundTripsSelectionsThroughLinkTables(): void
    {
        $this->seedLinkTables();
        Capsule::$tables['tblhostingconfigoptions'] = [
            ['relid' => 300, 'optionid' => 501, 'qty' => 1],
            ['relid' => 300, 'optionid' => 502, 'qty' => 1],
            ['relid' => 999, 'optionid' => 501, 'qty' => 1], // other service
        ];

        $selections = $this->resolver->selectionsForService(300);

        $this->assertCount(2, $selections);
        $this->assertSame('Image', $selections[0]['dimension_key']);
        $this->assertSame('OS:Ubuntu 24.04', $selections[0]['value_key']);
        $this->assertSame('Region', $selections[1]['dimension_key']);
    }

    public function testPassToProvisioningZeroIsSkipped(): void
    {
        $this->seedLinkTables();
        Capsule::$tables['tblhostingconfigoptions'] = [
            ['relid' => 300, 'optionid' => 503, 'qty' => 1], // Data Protection, flag off
        ];
        $this->assertSame([], $this->resolver->selectionsForService(300));
    }

    public function testUncuratedSelectionsAreIgnored(): void
    {
        $this->seedLinkTables();
        Capsule::$tables['tblhostingconfigoptions'] = [
            ['relid' => 300, 'optionid' => 888, 'qty' => 1], // no value link
        ];
        $this->assertSame([], $this->resolver->selectionsForService(300));
    }
}
