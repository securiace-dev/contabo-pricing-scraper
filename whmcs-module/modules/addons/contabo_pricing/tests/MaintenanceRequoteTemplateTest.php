<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

if (!function_exists('generate_token')) {
    function generate_token(): string
    {
        return '<input type="hidden" name="token" value="t">';
    }
}

/** The Re-quote panel: the diff is shown on every path and the confirm button is permission gated. */
final class MaintenanceRequoteTemplateTest extends TestCase
{
    /** @param array<string,mixed>|null $preview */
    private function render(?array $preview, array $stuck = []): string
    {
        $esc = static fn ($v): string => htmlspecialchars((string) $v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        $module_link = 'addonmodules.php?module=contabo_pricing';
        $cb_requote_preview = $preview;
        $cb_stuck_services = $stuck;
        $cb_schema_health = ['healthy' => true, 'missing' => [], 'schema_version' => 16];
        $cb_schema_target = 16;
        $cb_purge_phrase = 'PURGE CONTABO PRICING DATA';
        ob_start();
        require __DIR__ . '/../templates/admin/maintenance.tpl';
        return (string) ob_get_clean();
    }

    /** @return array<string,mixed> */
    private function plan(array $over = []): array
    {
        return array_merge([
            'service_id' => 30, 'code' => 'ready', 'message' => 'm', 'needs_override' => false,
            'override_kind' => 'recorded_mapping', 'mapping_version' => 'mapping-v3', 'diff_token' => 'TOK',
            'may_confirm' => true, 'deviations' => [],
            'diff' => [['field' => 'Recurring', 'old' => '19.99', 'new' => '19.99', 'live' => '24.99',
                'changed' => false, 'live_differs' => true]],
        ], $over);
    }

    public function testTheReadyPathShowsTheDiffAndTheWhmcsNowColumn(): void
    {
        $html = $this->render($this->plan());
        $this->assertStringContainsString('WHMCS now', $html);
        $this->assertStringContainsString('<strong>24.99</strong>', $html);
        $this->assertStringContainsString('Re-quote: seal exactly this quote', $html);
        $this->assertStringNotContainsString('value="TOK"', $html);
    }

    public function testADeviationShowsTheConfirmationWithTheTokenToAnAllowedAdmin(): void
    {
        $html = $this->render($this->plan([
            'code' => 'needs_override', 'needs_override' => true, 'override_kind' => 'recorded_deviation',
            'deviations' => ['Recurring recorded 19.99, WHMCS now 24.99'],
        ]));
        $this->assertStringContainsString('Recurring recorded 19.99, WHMCS now 24.99', $html);
        $this->assertStringContainsString('name="diff_token" value="TOK"', $html);
        $this->assertStringContainsString('although WHMCS differs', $html);
    }

    public function testAnAdminWithoutThePermissionSeesNoConfirmButton(): void
    {
        foreach (['recorded_deviation', 'recorded_snapshot', 'current_mapping'] as $kind) {
            $html = $this->render($this->plan([
                'code' => 'needs_override', 'needs_override' => true, 'override_kind' => $kind, 'may_confirm' => false,
            ]));
            $this->assertStringContainsString('Your admin role may not confirm this', $html, $kind);
            $this->assertStringNotContainsString('name="diff_token"', $html, $kind);
            $this->assertStringNotContainsString('name="override"', $html, $kind);
        }
    }

    public function testTheStuckListNeverSealsDirectlyItOnlyOpensTheReview(): void
    {
        $html = $this->render(null, [[
            'service_id' => 30, 'order_id' => 10, 'invoice_id' => 20, 'states' => 'draft', 'via' => 'order',
            'status' => 'ready', 'source' => 'recorded mapping', 'reason' => '', 'override_kind' => 'recorded_mapping',
        ]]);
        $this->assertStringContainsString('action=maintenance&amp;preview_service=30', $html);
        $this->assertStringNotContainsString('name="service_id" value="30"', $html, 'the list has no seal form for a service');
    }
}
