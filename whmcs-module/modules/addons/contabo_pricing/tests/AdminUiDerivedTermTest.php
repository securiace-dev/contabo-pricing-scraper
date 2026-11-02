<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;

final class AdminUiDerivedTermTest extends TestCase
{
    public function testProposalErrorRedactsAiKeyAndDoesNotReadRemovedApiToken(): void
    {
        $settings = new Settings(
            'manual',
            'EUR',
            false,
            0.0,
            365,
            '',
            false,
            'openai',
            'https://api.openai.com/v1',
            'sk-secret-token-xyz'
        );
        $this->assertFalse(property_exists($settings, 'apiToken'));
        $ctrl = new AdminController($settings, __DIR__);
        $method = new \ReflectionMethod(AdminController::class, 'proposalError');
        $method->setAccessible(true);
        $out = $method->invoke($ctrl, new \RuntimeException('upstream failed sk-secret-token-xyz while quoting'));
        $this->assertStringNotContainsString('sk-secret-token-xyz', $out);
        $this->assertStringContainsString('[redacted]', $out);
    }

    public function testProfilesTemplateShowsDerivedPillOnlyForFlaggedPeriod(): void
    {
        $html = $this->renderTemplate('profiles.tpl', [
            'profiles' => [
                [
                    'id' => 1,
                    'slug' => 'cloud-36',
                    'name' => 'Cloud 36',
                    'plan_slug' => 'cloud-vps-10',
                    'period_months' => 36,
                    'region' => '',
                    'os' => '',
                    'sync_strategy' => 'manual',
                    'active' => 1,
                    'updated_at' => '2026-10-16 00:00:00',
                    'derived_term' => [
                        'months' => 36,
                        'formula' => 'monthly(36-mo) = monthly(12-mo) x (1 - 0%)',
                    ],
                ],
                [
                    'id' => 2,
                    'slug' => 'cloud-12',
                    'name' => 'Cloud 12',
                    'plan_slug' => 'cloud-vps-10',
                    'period_months' => 12,
                    'region' => '',
                    'os' => '',
                    'sync_strategy' => 'manual',
                    'active' => 1,
                    'updated_at' => '2026-10-16 00:00:00',
                    'derived_term' => null,
                ],
            ],
            'available_plans' => [],
            'flash' => '',
            'trash_count' => 0,
        ]);
        $this->assertStringContainsString('>derived</span>', $html);
        $this->assertStringContainsString('monthly(36-mo) = monthly(12-mo)', $html);
        $this->assertSame(1, substr_count($html, 'cb-pill warn'));
    }

    public function testSyncHistoryAndResultTemplatesSurfaceDerivedAndWarnings(): void
    {
        $summary = [
            'status' => 'no-change',
            'term_policy' => 'nearest-term',
            'warnings' => ['profile x: no published term can price 48 mo for p; skipped'],
            'derived_terms' => [[
                'profile_slug' => 'cloud-36',
                'plan_slug' => 'cloud-vps-10',
                'terms' => [[
                    'months' => 36,
                    'formula' => 'monthly(36-mo) = monthly(12-mo) x (1 - 0%)',
                    'source_months' => 12,
                ]],
            ]],
        ];
        $history = $this->renderTemplate('sync_history.tpl', [
            'logs' => [[
                'id' => 9,
                'trigger' => 'manual',
                'status' => 'no-change',
                'started_at' => '2026-10-16 00:00:00',
                'finished_at' => '2026-10-16 00:00:01',
                'profiles_checked' => 1,
                'profiles_changed' => 0,
                'products_updated' => 0,
                'error_message' => '',
                'summary' => json_encode($summary),
            ]],
        ]);
        $this->assertStringContainsString('profile(s) with derived terms', $history);
        $this->assertStringContainsString('warning(s)', $history);

        $result = $this->renderTemplate('sync_run_result.tpl', ['summary' => $summary]);
        $this->assertStringContainsString('Derived terms and warnings', $result);
        $this->assertStringContainsString('cloud-36', $result);
        $this->assertStringContainsString('monthly(36-mo) = monthly(12-mo)', $result);
        $this->assertStringContainsString('no published term can price 48 mo', $result);
    }

    /**
     * @param array<string,mixed> $vars
     */
    private function renderTemplate(string $name, array $vars): string
    {
        $esc = static function ($v): string {
            return htmlspecialchars((string) $v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        };
        $module_link = 'addonmodules.php?module=contabo_pricing';
        $cb_addon_version = '1.1.1-test';
        extract($vars, EXTR_SKIP);
        ob_start();
        require __DIR__ . '/../templates/admin/' . $name;
        return (string) ob_get_clean();
    }
}
