<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

final class AdminPostOnlyContractTest extends TestCase
{
    public function testMutatingAdminHandlersRequirePostBeforeCsrfCheck(): void
    {
        $code = (string) file_get_contents(__DIR__ . '/../lib/AdminController.php');

        foreach ($this->mutatingHandlers() as $method) {
            $pattern = '/private function ' . preg_quote($method, '/') . '\s*\([^)]*\): void\s*\{\s*if \(!\$this->requirePost\(\)\) \{ return; \}\s*if \(!\$this->verifyToken\(\)\) \{ return; \}/s';
            $this->assertSame(
                1,
                preg_match($pattern, $code),
                $method . ' must enforce POST before CSRF validation'
            );
        }
    }

    /**
     * @return list<string>
     */
    private function mutatingHandlers(): array
    {
        return [
            'profileDelete',
            'profileRestore',
            'profilePurge',
            'profileCreate',
            'profileSave',
            'configApply',
            'configExposureSave',
            'capabilityEditorSave',
            'compatibilityEditorSave',
            'mappingSave',
            'refreshApi',
            'approvalApprove',
            'approvalReject',
            'maintenanceMigrate',
            'maintenancePurge',
            'taxSettingsSave',
        ];
    }
}
