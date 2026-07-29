<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\ContaboProvisioningException;
use SecuriAceVps\Runtime;

final class DirectMutationBypassTest extends TestCase
{
    /** @var Harness */
    private $harness;

    protected function setUp(): void
    {
        Harness::reset();
        $this->harness = new Harness();
        Harness::seedWhmcs();
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testLegacyMutationMethodsCannotReachTheProvider(): void
    {
        $service = Runtime::instanceService(Harness::params());
        $calls = [
            static function () use ($service): void {
                $service->create(Harness::params());
            },
            static function () use ($service): void {
                $service->terminate(Harness::params());
            },
            static function () use ($service): void {
                $service->resetPassword(Harness::params());
            },
            static function () use ($service): void {
                $service->reinstall(Harness::params());
            },
            static function () use ($service): void {
                $service->powerAction(Harness::params(), 'start');
            },
        ];

        foreach ($calls as $call) {
            try {
                $call();
                $this->fail('A legacy mutation method did not fail closed.');
            } catch (ContaboProvisioningException $e) {
                $this->assertSame('direct_mutation_bypass_disabled', $e->safeCode());
            }
        }
        $this->assertSame([], $this->harness->http->calls);
    }
}
