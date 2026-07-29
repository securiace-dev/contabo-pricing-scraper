<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use SecuriAceVps\Runtime;
use SecuriAceVps\Tests\Harness;

final class ReinstallFlowTest extends TestCase
{
    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testAdminReinstallCallbackDelegatesToDurableLifecycle(): void
    {
        $lifecycle = new class {
            /** @var int */
            public $calls = 0;

            /** @param array<string,mixed> $params */
            public function reinstall(array $params): string
            {
                $this->calls++;
                return ((int) ($params['serviceid'] ?? 0)) === 300
                    ? 'success'
                    : 'unexpected service';
            }
        };
        Runtime::swapLifecycle(static function () use ($lifecycle) {
            return $lifecycle;
        });

        $this->assertSame('success', securiacevps_buttonReinstall(Harness::params()));
        $this->assertSame(1, $lifecycle->calls);
    }

    public function testUncertifiedReinstallIsNotAdvertisedAsAnAdminButton(): void
    {
        $this->assertArrayNotHasKey('Reinstall', securiacevps_AdminCustomButtonArray());
    }
}
