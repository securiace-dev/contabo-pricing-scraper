<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use ContaboPricing\ProvisioningGate;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/** mod_securiacevps_schema is shared with the securiacevps module: stamps never go backwards. */
final class SchemaStampTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function stamp(string $key): ?string
    {
        $v = Capsule::table('mod_securiacevps_schema')->where('key', $key)->value('value');
        return $v === null ? null : (string) $v;
    }

    private function seed(array $rows): void
    {
        Capsule::$tables['mod_securiacevps_schema'] = [];
        foreach ($rows as $k => $v) {
            Capsule::table('mod_securiacevps_schema')->insert(['key' => $k, 'value' => $v, 'updated_at' => '2026-01-01 00:00:00']);
        }
    }

    public function testOlderMigrationStepsDoNotLowerAHigherSuiteVersion(): void
    {
        $this->seed(['schema_version' => '9', 'suite_schema_version' => '9', 'installed_by' => 'securiacevps']);
        $i = new Installer();
        $i->migrateTo10();
        $i->migrateTo11();
        $i->migrateTo13();
        $i->migrateTo14();
        $this->assertSame('9', $this->stamp('schema_version'));
        $this->assertSame('9', $this->stamp('suite_schema_version'));
    }

    public function testStampRaisesALowerOrNonNumericValueAndInsertsWhenMissing(): void
    {
        $this->seed(['schema_version' => '2', 'suite_schema_version' => 'garbage']);
        (new Installer())->migrateTo11();
        $this->assertSame('3', $this->stamp('schema_version'));
        $this->assertSame('3', $this->stamp('suite_schema_version'));

        $this->seed([]);
        (new Installer())->migrateTo14();
        $this->assertSame('5', $this->stamp('schema_version'));
    }

    public function testInstalledByKeepsItsFirstWriter(): void
    {
        $this->seed(['installed_by' => 'securiacevps']);
        $i = new Installer();
        $i->migrateTo9();
        $this->assertSame('securiacevps', $this->stamp('installed_by'));

        $this->seed([]);
        $i->migrateTo9();
        $this->assertSame('contabo_pricing', $this->stamp('installed_by'));
        (new Installer())->migrateTo9();
        $this->assertSame('contabo_pricing', $this->stamp('installed_by'));
        $this->assertSame(1, Capsule::table('mod_securiacevps_schema')->where('key', 'installed_by')->count());
    }

    public function testGrandfatherCutoffIsPinnedOnceByMigrationV16AndNeverMoved(): void
    {
        Capsule::$columns['mod_contabo_scrape_run_attempts'] = ['id', 'run_id'];
        $this->assertNull(Capsule::table('mod_contabo_settings')->where('key', ProvisioningGate::SETTING_CUTOFF)->value('value'));
        (new Installer())->migrateTo16();
        $first = (string) Capsule::table('mod_contabo_settings')->where('key', ProvisioningGate::SETTING_CUTOFF)->value('value');
        $this->assertNotFalse(strtotime($first));

        Capsule::table('mod_contabo_settings')->where('key', ProvisioningGate::SETTING_CUTOFF)->update(['value' => '2026-10-01 00:00:00']);
        (new Installer())->migrateTo16();
        $this->assertFalse((new Installer())->ensureGrandfatherCutoff());
        $this->assertSame('2026-10-01 00:00:00', (string) Capsule::table('mod_contabo_settings')->where('key', ProvisioningGate::SETTING_CUTOFF)->value('value'));
    }
}
