<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\EmailTemplateSeeder;
use ContaboPricing\Installer;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class EmailTemplateSeederTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$columns['tblemailtemplates'] = [
            'id',
            'type',
            'name',
            'subject',
            'message',
            'fromname',
            'fromemail',
            'disabled',
            'custom',
            'language',
        ];
        Capsule::$tables['tblemailtemplates'] = [];
    }

    public function testLifecycleTemplatesAreInstalledWithoutCredentials(): void
    {
        $result = (new EmailTemplateSeeder())->ensure();

        $this->assertSame(9, $result['created']);
        $this->assertSame(0, $result['skipped']);
        $names = array_column(Capsule::$tables['tblemailtemplates'], 'name');
        $this->assertContains('SecuriAce VPS Ready', $names);
        $this->assertContains('SecuriAce VPS Provisioning Delayed', $names);
        $this->assertContains('SecuriAce VPS Provisioning Review', $names);
        $this->assertContains('SecuriAce VPS Password Reset Complete', $names);
        $this->assertContains('SecuriAce VPS Reinstall Complete', $names);

        foreach (Capsule::$tables['tblemailtemplates'] as $template) {
            if (strpos((string) $template['name'], 'SecuriAce VPS') !== 0) {
                continue;
            }
            $body = strtolower((string) $template['message']);
            $this->assertStringNotContainsString('{$service_password}', $body);
            $this->assertStringNotContainsString('{$password}', $body);
            $this->assertStringContainsString('{$operation_reference}', (string) $template['message']);
        }
    }

    public function testSeedingAndMigrationAreIdempotent(): void
    {
        $seeder = new EmailTemplateSeeder();
        $seeder->ensure();
        $second = $seeder->ensure();
        (new Installer())->migrateTo12();

        $this->assertSame(0, $second['created']);
        $this->assertSame(9, $second['skipped']);
        $this->assertCount(9, Capsule::$tables['tblemailtemplates']);
    }

    public function testMigrationDoesNotOverwriteAdministratorCustomisation(): void
    {
        (new EmailTemplateSeeder())->ensure();
        foreach (Capsule::$tables['tblemailtemplates'] as $index => $template) {
            if ($template['name'] === 'SecuriAce VPS Ready') {
                Capsule::$tables['tblemailtemplates'][$index]['subject'] = 'My custom subject';
            }
        }

        (new Installer())->migrateTo12();

        $ready = Capsule::table('tblemailtemplates')
            ->where('name', 'SecuriAce VPS Ready')
            ->first();
        $readyRow = (array) $ready;
        $this->assertSame('My custom subject', (string) ($readyRow['subject'] ?? ''));
    }
}
