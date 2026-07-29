<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use SecuriAceVps\Runtime;
use SecuriAceVps\Tests\Harness;

/**
 * Customer mutations are rendered by the capability-driven module panel.
 * WHMCS's unconditional custom-button registry must not advertise actions
 * that have not been certified for the service's provider account.
 */
final class ResetPasswordFlowTest extends TestCase
{
    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testCustomerCustomButtonsNeverExposeUncertifiedReset(): void
    {
        $this->assertSame([], securiacevps_ClientAreaCustomButtonArray());
    }

    public function testAdminCustomButtonsKeepOnlyReadOnlySync(): void
    {
        $this->assertSame(
            ['Sync from Contabo' => 'buttonSync'],
            securiacevps_AdminCustomButtonArray()
        );
    }

    public function testResetCallbackDelegatesToDurableLifecycle(): void
    {
        $lifecycle = new class {
            /** @var int */
            public $calls = 0;

            /** @param array<string,mixed> $params */
            public function resetPassword(array $params): string
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

        $this->assertSame('success', securiacevps_buttonResetPassword(Harness::params()));
        $this->assertSame(1, $lifecycle->calls);
    }
}
