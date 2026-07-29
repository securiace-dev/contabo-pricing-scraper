<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;

final class CompatibilityShimTest extends TestCase
{
    public function testLegacyEntrypointDelegatesToCanonicalModule(): void
    {
        require_once __DIR__ . '/../../contabo_vps/contabo_vps.php';

        $this->assertTrue(function_exists('contabo_vps_CreateAccount'));
        $this->assertSame(securiacevps_ConfigOptions(), contabo_vps_ConfigOptions());
        $this->assertStringContainsString(
            'legacy compatibility',
            contabo_vps_MetaData()['DisplayName']
        );
    }
}
