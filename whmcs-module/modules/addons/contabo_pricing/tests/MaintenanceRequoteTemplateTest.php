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
    private function render(?array $preview, array $stuck = [], ?array $keyStatus = null): string
    {
        $esc = static fn ($v): string => htmlspecialchars((string) $v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        $module_link = 'addonmodules.php?module=contabo_pricing';
        $cb_requote_preview = $preview;
        $cb_stuck_services = $stuck;
        if ($keyStatus !== null) {
            $cb_key_status = $keyStatus;
        }
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
        // 1.1.7: an exact match is confirmed like any other seal.
        $this->assertStringContainsString('Confirm: seal exactly this quote', $html);
        $this->assertStringContainsString('name="diff_token" value="TOK"', $html);
        $this->assertStringContainsString('name="override" value="1"', $html);
    }

    public function testTheKeyCheckSaysYesAndNamesTheSourceButNeverTheKey(): void
    {
        $html = $this->render(null, [], ['available' => true, 'source' => 'WHMCS application config (DI config, array access)', 'can_sign' => true]);
        $this->assertStringContainsString('data-cb-key-check="1"', $html);
        $this->assertStringContainsString('key resolves: <strong>yes</strong>', $html);
        $this->assertStringContainsString('WHMCS application config (DI config, array access)', $html);
    }

    public function testTheKeyCheckSaysNoWhenTheKeyDoesNotResolve(): void
    {
        $html = $this->render(null, [], ['available' => false, 'source' => 'none', 'can_sign' => false]);
        $this->assertStringContainsString('key resolves: <strong>NO</strong>', $html);
        $this->assertStringNotContainsString('key resolves: <strong>yes</strong>', $html);
    }

    public function testTheReadyPathOffersNoConfirmButtonToAnAdminWhoMayNotConfirm(): void
    {
        $html = $this->render($this->plan(['may_confirm' => false]));
        $this->assertStringContainsString('Your admin role may not confirm this', $html);
        $this->assertStringNotContainsString('name="diff_token"', $html);
        $this->assertStringNotContainsString('Confirm: seal exactly this quote', $html);
    }

    public function testTheManualBoxOnlyOpensTheDiff(): void
    {
        $html = $this->render($this->plan());
        $this->assertStringContainsString('Review re-quote', $html);
        $this->assertStringContainsString('nothing is sealed until you confirm it there', $html);
        $this->assertStringNotContainsString('>Re-quote service<', $html);
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
