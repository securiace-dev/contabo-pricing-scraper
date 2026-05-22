<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionLinkRepository;
use ContaboPricing\ConfigOptionPricingContext;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * A.6.3 — ConfigurableOptionsSyncer::apply() (real-write path) against
 * FakeCapsule. Proves it writes WHMCS config objects, records link rows, and is
 * idempotent on a second run.
 */
final class ConfigurableOptionsApplyTest extends TestCase
{
    /** @var array<int,array<string,mixed>> */
    private $audits = [];

    protected function setUp(): void
    {
        Capsule::reset();
        $this->audits = [];
    }

    private function auditLog(): OptionAuditLog
    {
        $sink = &$this->audits;
        return new class('apply-batch', $sink) extends OptionAuditLog {
            /** @var array<int,array<string,mixed>> */
            private $sink;
            public function __construct(string $b, array &$s)
            {
                parent::__construct($b);
                $this->sink = &$s;
            }
            protected function storeRow(array $row): int
            {
                $this->sink[] = $row;
                return count($this->sink);
            }
        };
    }

    /** @return list<array<string,mixed>> */
    private function specs(): array
    {
        return [
            [
                'dimension_key' => 'Image',
                'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN,
                'values'        => [
                    ['value_key' => 'os:ubuntu', 'label' => '[OS] Ubuntu 24.04', 'monthly_eur_delta' => 0.0, 'is_default' => true,  'sortorder' => 0],
                    ['value_key' => 'os:windows', 'label' => '[OS] Windows',     'monthly_eur_delta' => 10.0, 'is_default' => false, 'sortorder' => 1],
                ],
            ],
            [
                'dimension_key' => 'Networking:IPv4',
                'optiontype'    => OptionTypeMapper::TYPE_QUANTITY,
                'values'        => [
                    ['value_key' => 'ipv4', 'label' => 'Additional IPv4', 'monthly_eur_delta' => 1.0, 'is_default' => false, 'sortorder' => 0],
                ],
            ],
        ];
    }

    private function ctx(): ConfigOptionPricingContext
    {
        return new ConfigOptionPricingContext(1, 90.0, 'cost_plus_pct', 15.0, 'exact_2_decimals');
    }

    private function syncer(ConfigOptionLinkRepository $links, bool $dryRun = false): ConfigurableOptionsSyncer
    {
        return new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter($dryRun), $this->auditLog(), $links);
    }

    public function testApplyWritesObjectsAndRecordsLinks(): void
    {
        $links = new ConfigOptionLinkRepository();
        $r = $this->syncer($links)->apply(7, 501, 'contabo-cloud-vps-10', 'Contabo Cloud VPS 10', $this->specs(), $this->ctx());

        $this->assertSame(7, $r['profile_id']);
        $this->assertSame(501, $r['product_id']);
        $this->assertSame(2, $r['options']);
        $this->assertSame(3, $r['values']); // 2 image + 1 ipv4
        $this->assertGreaterThan(0, $r['summary']['created']);

        // Real WHMCS objects written.
        $this->assertNotEmpty(Capsule::$tables['tblproductconfiggroups'] ?? []);
        $this->assertCount(2, Capsule::$tables['tblproductconfigoptions'] ?? []);
        $this->assertCount(3, Capsule::$tables['tblproductconfigoptionssub'] ?? []);

        // Link rows recorded with the WHMCS ids.
        $gl = $links->findGroupLink(7, 501, 'contabo-cloud-vps-10');
        $this->assertNotNull($gl);
        $this->assertGreaterThan(0, (int) $gl['whmcs_group_id']);

        $ol = $links->findOptionLink(7, 'Networking:IPv4');
        $this->assertNotNull($ol);
        $this->assertSame(OptionTypeMapper::TYPE_QUANTITY, (int) $ol['optiontype']);
        $this->assertGreaterThan(0, (int) $ol['whmcs_option_id']);

        $vl = $links->findValueLink((int) $ol['id'], 'ipv4');
        $this->assertNotNull($vl);
        $this->assertGreaterThan(0, (int) $vl['whmcs_sub_id']);
    }

    public function testSecondApplyIsIdempotent(): void
    {
        $links = new ConfigOptionLinkRepository();
        $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());

        $groups1 = count(Capsule::$tables['tblproductconfigoptions'] ?? []);
        $links1  = count(Capsule::$tables['mod_contabo_config_option_link'] ?? []);

        // Second apply, same inputs → no new WHMCS options, no new link rows.
        $r2 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());

        $this->assertCount($groups1, Capsule::$tables['tblproductconfigoptions'] ?? [], 'no duplicate options');
        $this->assertCount($links1, Capsule::$tables['mod_contabo_config_option_link'] ?? [], 'no duplicate option links');
        $this->assertSame(0, $r2['summary']['created'], '2nd apply creates nothing');
    }

    public function testApplyRefusesDryRunAdapter(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->syncer(new ConfigOptionLinkRepository(), true) // dry-run
            ->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
    }

    public function testApplyRefusesWithoutLinkRepo(): void
    {
        $syncer = new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(false), $this->auditLog(), null);
        $this->expectException(\RuntimeException::class);
        $syncer->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
    }
}
