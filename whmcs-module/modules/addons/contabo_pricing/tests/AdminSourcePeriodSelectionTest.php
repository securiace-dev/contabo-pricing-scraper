<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;

final class AdminSourcePeriodSelectionTest extends TestCase
{
    /** @return AdminController */
    private function controller()
    {
        return new AdminController(
            new Settings(
                'http://localhost:8080/api/v1',
                '',
                'notify',
                'INR',
                false,
                3.5,
                365,
                'addonmodules.php?module=contabo_pricing'
            ),
            __DIR__ . '/../templates/admin'
        );
    }

    public function testSourcePeriodCoercionUsesPostedFieldFallback(): void
    {
        $ref = new \ReflectionClass(AdminController::class);
        $method = $ref->getMethod('coerceSourcePeriodMonths');
        if (PHP_VERSION_ID < 80100) {
            $method->setAccessible(true);
        }

        $controller = $this->controller();
        $this->assertSame(24, $method->invoke($controller, '24', 12));
        $this->assertSame(12, $method->invoke($controller, '', 12));
        $this->assertSame(24, $method->invoke($controller, 'invalid', 24));
    }

    public function testProfileCreateAndSaveNoLongerDeriveSourcePeriodFromPublishedMask(): void
    {
        $code = (string) file_get_contents(__DIR__ . '/../lib/AdminController.php');

        $createStart = strpos($code, 'private function profileCreate(array $req): void');
        $saveStart = strpos($code, 'private function profileSave(array $req): void');
        $coerceMaskStart = strpos($code, 'private function coercePublishedMask');

        $this->assertIsInt($createStart);
        $this->assertIsInt($saveStart);
        $this->assertIsInt($coerceMaskStart);

        $createBody = substr($code, (int) $createStart, (int) $saveStart - (int) $createStart);
        $saveBody = substr($code, (int) $saveStart, (int) $coerceMaskStart - (int) $saveStart);

        $this->assertStringContainsString(
            '$periodMonths  = $this->coerceSourcePeriodMonths($req[\'period_months\'] ?? null, 12);',
            $createBody
        );
        $this->assertStringNotContainsString('longestPublishedMonths', $createBody);
        $this->assertStringContainsString(
            '$publishedMask = $this->coercePublishedMask($req[\'published_cycles_mask\']);',
            $saveBody
        );
        $this->assertStringContainsString(
            '$periodMonths = $this->coerceSourcePeriodMonths(',
            $saveBody
        );
        $this->assertStringContainsString('$existing = $pm->find($id);', $saveBody);
        $this->assertStringNotContainsString('longestPublishedMonths', $saveBody);
    }
}
