<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

final class ProfileCreateTemplateTest extends TestCase
{
    public function testProfilesTemplateEmbedsCreateFormStateAndErrors(): void
    {
        $profiles = [];
        $available_plans = [
            ['product_slug' => 'cloud-vps-10', 'product_name' => 'Cloud VPS 10', 'family' => 'Cloud VPS'],
        ];
        $flash = '';
        $module_link = 'addonmodules.php?module=contabo_pricing';
        $esc = static function ($v): string {
            return htmlspecialchars((string) $v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        };
        $cb_addon_version = '1.0.0-test';
        $trash_count = 0;
        $cb_profile_form_state = [
            'profile_mode' => 'customer_configurable_product',
            'plan_slug' => 'cloud-vps-10',
            'name' => 'Cloud VPS 10 - configurable',
            'tags' => 'sales',
            'sync_strategy' => 'manual',
            'expose_configurable_options' => 0,
            'published_cycles_mask' => 9,
            'options' => [
                'Image:OS' => ['label' => 'Ubuntu 24.04'],
            ],
            'advanced_open' => true,
        ];
        $cb_profile_form_errors = [
            'Choose a Contabo plan first.',
            'Enter a display name for operators.',
        ];

        ob_start();
        require __DIR__ . '/../templates/admin/profiles.tpl';
        $html = (string) ob_get_clean();

        $this->assertStringContainsString('data-cb-profile-form-state', $html);
        $this->assertStringContainsString('Cloud VPS 10 - configurable', $html);
        $this->assertStringContainsString('customer_configurable_product', $html);
        $this->assertStringContainsString('&quot;advanced_open&quot;:true', $html);
        $this->assertStringContainsString('Review this profile before saving.', $html);
        $this->assertStringContainsString('Choose a Contabo plan first.', $html);
    }
}
