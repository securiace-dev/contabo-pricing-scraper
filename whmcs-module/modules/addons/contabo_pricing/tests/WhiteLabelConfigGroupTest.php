<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\ConfigOptionLinkRepository;
use ContaboPricing\ConfigOptionPricingContext;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\Settings;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * R3: no provider name in customer-visible WHMCS strings. Inline stand-in for
 * the sibling lane's LabelGuard: every name/description written to
 * tblproductconfiggroups / tblproducts must not contain "contabo".
 */
final class WhiteLabelConfigGroupTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
    }

    private function audit(): OptionAuditLog
    {
        return new class('wl') extends OptionAuditLog {
            protected function storeRow(array $row): int { return 1; }
        };
    }

    /** @return list<array<string,mixed>> */
    private function specs(): array
    {
        return [[
            'dimension_key' => 'Image',
            'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN,
            'values'        => [
                ['value_key' => 'os:ubuntu', 'label' => '[OS] Ubuntu 24.04', 'monthly_eur_delta' => 0.0, 'is_default' => true, 'sortorder' => 0],
            ],
        ]];
    }

    private function assertNoProviderNameWritten(): void
    {
        foreach (['tblproductconfiggroups', 'tblproducts'] as $table) {
            foreach (Capsule::$tables[$table] ?? [] as $row) {
                foreach (['name', 'description'] as $col) {
                    $this->assertStringNotContainsStringIgnoringCase(
                        'contabo',
                        (string) ($row[$col] ?? ''),
                        $table . '.' . $col . ' leaks the provider name'
                    );
                }
            }
        }
    }

    public function testApplyWritesNoProviderNameToConfigGroup(): void
    {
        $label = $this->controllerLabel('cloud-vps-10');
        $syncer = new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(false), $this->audit(), new ConfigOptionLinkRepository());
        $syncer->apply(7, 501, 'contabo-cloud-vps-10', $label, $this->specs(), new ConfigOptionPricingContext(1, 90.0, 'cost_plus_pct', 15.0, 'exact_2_decimals'));

        $this->assertNotEmpty(Capsule::$tables['tblproductconfiggroups']);
        $this->assertSame('Configurable options (profile #7)', Capsule::$tables['tblproductconfiggroups'][0]['description']);
        $this->assertNoProviderNameWritten();
    }

    public function testObservePayloadHasNoProviderName(): void
    {
        $syncer = new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(true), $this->audit());
        $report = $syncer->observe(7, $this->controllerLabel('cloud-vps-10'), $this->specs(), new ConfigOptionPricingContext(1, 90.0, 'cost_plus_pct', 15.0, 'exact_2_decimals'));
        $this->assertStringNotContainsStringIgnoringCase('contabo', (string) json_encode($report));
    }

    public function testGroupLabelIsBrandNeutral(): void
    {
        $this->assertSame('Cloud Vps 10', $this->controllerLabel('cloud-vps-10'));
        $this->assertStringNotContainsStringIgnoringCase('contabo', $this->controllerLabel('contabo-cloud-vps-10'));
        $this->assertNotSame('', $this->controllerLabel('contabo'));
    }

    private function controllerLabel(string $slug): string
    {
        $c = new AdminController(
            new Settings('http://x', '', 'manual', 'INR', true, 0.0, 365, ''),
            __DIR__ . '/../templates/admin'
        );
        $m = new \ReflectionMethod($c, 'planGroupLabel');
        $m->setAccessible(true);
        return (string) $m->invoke($c, $slug);
    }
}
