<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProvisioningGate;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/** PreModuleCreate: fail-closed for new orders, logged grandfathering for the upgrade window. */
final class ProvisioningGateTest extends TestCase
{
    private const CUTOFF = '2026-11-01 00:00:00';

    protected function setUp(): void
    {
        Capsule::reset();
        $GLOBALS['contabo_pricing_test_activity_log'] = [];
        $GLOBALS['contabo_pricing_test_module_log'] = [];
        $this->setting(ProvisioningGate::SETTING_CUTOFF, self::CUTOFF);
        Capsule::$tables['mod_securiacevps_order_snapshots'] = [];
        Capsule::$tables['tblhosting'] = [['id' => 10, 'orderid' => 100]];
        Capsule::$tables['tblorders'] = [['id' => 100, 'date' => '2026-10-20 10:00:00', 'invoiceid' => 500, 'status' => 'Active']];
        Capsule::$tables['tblinvoices'] = [['id' => 500, 'status' => 'Paid']];
    }

    private function setting(string $key, string $value): void
    {
        Capsule::table('mod_contabo_settings')->updateOrInsert(['key' => $key], ['value' => $value]);
    }

    /** @return array<string,mixed> */
    private function create(int $serviceId = 10, string $module = 'securiacevps'): array
    {
        return (new ProvisioningGate())->handle(['params' => ['moduletype' => $module, 'serviceid' => $serviceId]]);
    }

    private function lastLog(): string
    {
        $log = $GLOBALS['contabo_pricing_test_activity_log'];
        return $log === [] ? '' : (string) end($log);
    }

    public function testOtherModulesAreIgnoredAndNotLogged(): void
    {
        $this->assertSame([], $this->create(10, 'cpanel'));
        $this->assertSame([], $GLOBALS['contabo_pricing_test_activity_log']);
    }

    public function testSealedSnapshotAllowsSilently(): void
    {
        Capsule::$tables['mod_securiacevps_order_snapshots'] = [['id' => 1, 'service_id' => 10, 'state' => 'sealed']];
        $this->assertSame([], $this->create());
        $this->assertSame([], $GLOBALS['contabo_pricing_test_activity_log']);
    }

    public function testPaidPreCutoffOrderWithNoSnapshotIsGrandfatheredWithAWarning(): void
    {
        $this->assertSame([], $this->create());
        $this->assertStringStartsWith('WARNING:', $this->lastLog());
        $this->assertStringContainsString('service #10', $this->lastLog());
        $this->assertStringContainsString(ProvisioningGate::REASON_GRANDFATHERED, $this->lastLog());
        $this->assertSame('allow', $GLOBALS['contabo_pricing_test_module_log'][0][3]['outcome']);
    }

    public function testNewOrderAtOrAfterTheCutoffStaysFailClosedAndIsLogged(): void
    {
        Capsule::$tables['tblorders'][0]['date'] = self::CUTOFF;
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('ABORTED', $this->lastLog());
        $this->assertStringContainsString('service #10', $this->lastLog());
        $this->assertStringContainsString(ProvisioningGate::REASON_NO_SNAPSHOT, $this->lastLog());
        $this->assertStringContainsString('at or after the cutoff', $this->lastLog());
        $this->assertSame('abort', $GLOBALS['contabo_pricing_test_module_log'][0][3]['outcome']);
        $this->assertSame('PreModuleCreate', $GLOBALS['contabo_pricing_test_module_log'][0][1]);
    }

    /** @return array<string,array{0:string}> */
    public static function nonSealedStates(): array
    {
        return ['draft' => ['draft'], 'invalid' => ['invalid'], 'superseded' => ['superseded']];
    }

    #[DataProvider('nonSealedStates')]
    public function testAnyNonSealedSnapshotRowKeepsTheServiceClosedEvenIfPreCutoff(string $state): void
    {
        Capsule::$tables['mod_securiacevps_order_snapshots'] = [['id' => 1, 'service_id' => 10, 'state' => $state]];
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('non-sealed snapshot row', $this->lastLog());
    }

    public function testUnpaidFraudCancelledAndOrderlessServicesStayClosed(): void
    {
        Capsule::$tables['tblinvoices'][0]['status'] = 'Unpaid';
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('no paid invoice', $this->lastLog());

        Capsule::$tables['tblinvoices'][0]['status'] = 'Paid';
        foreach (['Fraud', 'Cancelled'] as $status) {
            Capsule::$tables['tblorders'][0]['status'] = $status;
            $this->assertSame(['abortcmd' => true], $this->create(), $status);
        }

        Capsule::$tables['tblorders'][0]['status'] = 'Active';
        Capsule::$tables['tblhosting'][0]['orderid'] = 0;
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('has no order', $this->lastLog());
    }

    public function testKillSwitchAndMissingCutoffFailClosed(): void
    {
        $this->setting(ProvisioningGate::SETTING_ENABLED, '0');
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('disabled', $this->lastLog());

        $this->setting(ProvisioningGate::SETTING_ENABLED, '1');
        $this->setting(ProvisioningGate::SETTING_CUTOFF, '');
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('no cutoff is recorded', $this->lastLog());
    }

    public function testMissingServiceIdAbortsWithAReason(): void
    {
        $this->assertSame(['abortcmd' => true], $this->create(0));
        $this->assertStringContainsString(ProvisioningGate::REASON_NO_SERVICE, $this->lastLog());
    }

    public function testHookIsRegisteredAndDelegates(): void
    {
        require_once __DIR__ . '/../hooks.php';
        $hooks = $GLOBALS['contabo_pricing_test_hooks']['PreModuleCreate'] ?? [];
        $this->assertNotEmpty($hooks, 'PreModuleCreate hook must be registered');
        Capsule::$tables['tblorders'][0]['date'] = '2026-11-02 00:00:00';
        $out = ($hooks[0]['callback'])(['params' => ['moduletype' => 'securiacevps', 'serviceid' => 10]]);
        $this->assertSame(['abortcmd' => true], $out);
        $this->assertStringContainsString('ABORTED', $this->lastLog());
    }
}
