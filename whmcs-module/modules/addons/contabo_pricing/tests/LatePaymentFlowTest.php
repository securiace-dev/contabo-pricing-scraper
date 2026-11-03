<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\PaidOrderSnapshotService;
use ContaboPricing\ProvisioningGate;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 1.1.4: a paid invoice must never strand provisioning because the checkout
 * quote window (formerly a hard 1800 s) ran out. Late and bank-transfer
 * payments seal at the quoted terms; tampered, cancelled and unpaid orders
 * stay closed.
 */
final class LatePaymentFlowTest extends TestCase
{
    use SnapshotFixture;

    protected function setUp(): void
    {
        $this->bootFixture();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** @return list<array<string,mixed>> */
    private function snapshots(): array
    {
        return array_map(
            static fn ($r): array => (array) $r,
            Capsule::$tables['mod_securiacevps_order_snapshots']
        );
    }

    private function draft(): string
    {
        return (new PaidOrderSnapshotService())->createDraft(10, 20, 30);
    }

    private function pastQuoteWindow(int $days = 3): void
    {
        Capsule::table('mod_securiacevps_order_snapshots')
            ->update(['quote_expires_at' => date('Y-m-d H:i:s', time() - $days * 86400)]);
    }

    private function pay(): void
    {
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        Capsule::table('tblorders')->where('id', 10)->update(['status' => 'Active']);
    }

    private function log(): string
    {
        return implode("\n", $GLOBALS['contabo_pricing_test_activity_log'] ?? []);
    }

    /** @return array<string,mixed> */
    private function create(): array
    {
        return (new ProvisioningGate())->handle(['params' => ['moduletype' => 'securiacevps', 'serviceid' => 30]]);
    }

    private function hook(string $point): callable
    {
        require_once __DIR__ . '/../hooks.php';
        $hooks = $GLOBALS['contabo_pricing_test_hooks'][$point] ?? [];
        $this->assertNotEmpty($hooks, $point . ' hook must be registered');
        return $hooks[0]['callback'];
    }

    private function setting(string $value): void
    {
        Capsule::table('mod_contabo_settings')->updateOrInsert(
            ['key' => PaidOrderSnapshotService::SETTING_QUOTE_TTL],
            ['value' => $value]
        );
    }

    private function expiresAt(): int
    {
        return (int) strtotime((string) $this->snapshots()[0]['quote_expires_at']);
    }

    // ── late payment (bank transfer, paid after the due date) ────────────────

    public function testLatePaymentAfterTheQuoteWindowStillSealsAtTheQuotedTerms(): void
    {
        $this->draft();
        $this->pastQuoteWindow();
        $this->pay();

        $this->assertSame(1, (new PaidOrderSnapshotService())->sealOrder(10, 20));
        $row = $this->snapshots()[0];
        $this->assertSame('sealed', $row['state']);
        $this->assertNotEmpty($row['paid_at']);
        $payload = json_decode((string) $row['payload_json'], true);
        $this->assertSame('1999.00', $payload['pricing']['recurring']);
        $this->assertStringContainsString('late payment sealed', $this->log());
        $this->assertStringContainsString('WARNING', $this->log());
    }

    public function testBankTransferPaidAfterTheDueDateProvisionsThroughTheRealHooks(): void
    {
        Capsule::table('tblinvoices')->where('id', 20)->update([
            'duedate' => date('Y-m-d', time() - 5 * 86400),
        ]);
        // Checkout: invoice unpaid, draft is made, nothing seals.
        ($this->hook('AfterShoppingCartCheckout'))(['OrderID' => 10, 'InvoiceID' => 20, 'ServiceIDs' => [30]]);
        $this->assertSame('draft', $this->snapshots()[0]['state']);
        $this->assertSame(['abortcmd' => true], $this->create(), 'unpaid stays closed');

        // Days later the bank transfer lands: OrderPaid fires before auto-setup creates the service.
        $this->pastQuoteWindow(6);
        $this->pay();
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 20]);

        $this->assertSame('sealed', $this->snapshots()[0]['state']);
        $this->assertSame([], $this->create(), 'a sealed late payment provisions silently');
    }

    public function testGateSealsAVerifiedDraftWhoseInvoiceIsPaidWhenOrderPaidDidNotSeal(): void
    {
        $this->draft();
        $this->pastQuoteWindow();
        $this->pay();

        $this->assertSame([], $this->create());
        $this->assertSame('sealed', $this->snapshots()[0]['state']);
        $this->assertStringContainsString(ProvisioningGate::REASON_SEALED_AT_GATE, $this->log());
        $this->assertStringContainsString('WARNING: Contabo provisioning gate allowed', $this->log());
    }

    public function testPriceDriftOnALatePaymentIsLoggedAndTheSealedPriceIsNeverRaised(): void
    {
        $this->draft();
        $this->pastQuoteWindow();
        Capsule::table('tblinvoices')->where('id', 20)->update(['total' => '2499.00']);
        Capsule::table('tblhosting')->where('id', 30)->update(['amount' => '2499.00']);
        $this->pay();

        $this->assertSame(1, (new PaidOrderSnapshotService())->sealOrder(10, 20));
        $payload = json_decode((string) $this->snapshots()[0]['payload_json'], true);
        $this->assertSame('1999.00', $payload['pricing']['recurring']);
        $this->assertSame('1999.00', $payload['whmcs']['total_due']);
        $this->assertStringContainsString('price drift', $this->log());
        $this->assertStringContainsString('invoice total quoted 1999.00 now 2499.00', $this->log());
        $this->assertStringContainsString('recurring quoted 1999.00 now 2499.00', $this->log());
    }

    // ── tampered drafts stay refused ─────────────────────────────────────────

    /** @return array<string,array{0:string}> */
    public static function tamperings(): array
    {
        return [
            'price edited in payload' => ['price'],
            'configuration hash replaced' => ['config_hash'],
            'cart total hash replaced' => ['cart_hash'],
            'payload not json' => ['garbage'],
            'bound to another invoice' => ['invoice'],
            'bound to another service' => ['service'],
            'provider region swapped' => ['provider_region'],
            'provider image swapped' => ['provider_image'],
            'provider sku removed' => ['provider_sku'],
        ];
    }

    #[DataProvider('tamperings')]
    public function testTamperedDraftIsNeverSealedEvenWhenPaidOnTimeOrLate(string $how): void
    {
        $this->draft();
        $row = $this->snapshots()[0];
        $payload = (string) $row['payload_json'];
        $edit = [];
        switch ($how) {
            case 'price':
                $edit = ['payload_json' => str_replace('"recurring":"1999.00"', '"recurring":"1.00"', $payload)];
                $this->assertNotSame($payload, $edit['payload_json']);
                break;
            case 'config_hash':
                $edit = ['configuration_hash' => str_repeat('a', 64)];
                break;
            case 'cart_hash':
                $edit = ['cart_total_hash' => str_repeat('b', 64)];
                break;
            case 'garbage':
                $edit = ['payload_json' => 'not json'];
                break;
            case 'invoice':
                $edit = ['payload_json' => str_replace('"invoice_id":20', '"invoice_id":99', $payload)];
                break;
            case 'service':
                $edit = ['payload_json' => str_replace('"service_id":30', '"service_id":31', $payload)];
                break;
            case 'provider_region':
                $edit = ['payload_json' => str_replace('"region_id":"EU"', '"region_id":"US-east"', $payload)];
                $this->assertNotSame($payload, $edit['payload_json']);
                break;
            case 'provider_image':
                // Only provider.image_id: the hashed configuration keeps image-1,
                // so this must be caught by the provider/configuration mirror check.
                $decoded = json_decode($payload, true);
                $this->assertSame('image-1', $decoded['provider']['image_id']);
                $this->assertSame('image-1', $decoded['configuration']['image_id']);
                $decoded['provider']['image_id'] = 'image-9';
                $edit = ['payload_json' => (string) json_encode($decoded)];
                break;
            case 'provider_sku':
                $edit = ['payload_json' => str_replace('"sku_id":"V45"', '"sku_id":""', $payload)];
                $this->assertNotSame($payload, $edit['payload_json']);
                break;
        }
        Capsule::table('mod_securiacevps_order_snapshots')->update($edit);
        $this->pastQuoteWindow();
        $this->pay();

        $this->assertSame(0, (new PaidOrderSnapshotService())->sealOrder(10, 20), $how);
        $this->assertSame('invalid', $this->snapshots()[0]['state'], $how);
        $this->assertStringContainsString('REFUSED to seal', $this->log());
        $this->assertSame(['abortcmd' => true], $this->create(), 'the gate also refuses a tampered draft');
        $this->assertStringContainsString('ABORTED', $this->log());
    }

    public function testTamperedDraftIsRefusedAtTheGateWithoutOrderPaidHavingRun(): void
    {
        $this->draft();
        Capsule::table('mod_securiacevps_order_snapshots')->update([
            'payload_json' => str_replace('"recurring":"1999.00"', '"recurring":"1.00"', (string) $this->snapshots()[0]['payload_json']),
        ]);
        $this->pay();

        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertSame('invalid', $this->snapshots()[0]['state']);
    }

    // ── cancelled and unpaid invoices ────────────────────────────────────────

    public function testCancelledInvoiceInvalidatesTheDraftAndAnUnpaidOrderStaysClosed(): void
    {
        $this->draft();
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 20]);

        $this->assertSame('invalid', $this->snapshots()[0]['state']);
        $this->assertStringContainsString('invoice #20 was cancelled', $this->log());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testPaymentAfterCancellationDoesNotSealAnInvalidatedDraft(): void
    {
        $this->draft();
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 20]);
        $this->pay();

        $this->assertSame(0, (new PaidOrderSnapshotService())->sealOrder(10, 20));
        $this->assertSame('invalid', $this->snapshots()[0]['state']);
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testCancelledInvoiceLeavesSealedSnapshotsAndOtherInvoicesAlone(): void
    {
        $this->draft();
        $this->pay();
        (new PaidOrderSnapshotService())->sealOrder(10, 20);
        $this->assertSame(0, (new PaidOrderSnapshotService())->invalidateForCancelledInvoice(20));
        $this->assertSame(0, (new PaidOrderSnapshotService())->invalidateForCancelledInvoice(999));
        $this->assertSame('sealed', $this->snapshots()[0]['state']);
    }

    public function testUnpaidFraudAndCancelledOrdersNeverSealAtTheGateOrByHook(): void
    {
        $this->draft();
        $this->pastQuoteWindow();
        $service = new PaidOrderSnapshotService();

        $this->assertSame(0, $service->sealOrder(10, 20), 'unpaid');
        $this->assertSame(0, $service->sealPaidService(30), 'unpaid at the gate');

        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        foreach (['Fraud', 'Cancelled'] as $status) {
            Capsule::table('tblorders')->where('id', 10)->update(['status' => $status]);
            $this->assertSame(0, $service->sealOrder(10, 20), $status);
            $this->assertSame(['abortcmd' => true], $this->create(), $status);
        }
        $this->assertSame('draft', $this->snapshots()[0]['state']);
    }

    // ── configurable quote window ────────────────────────────────────────────

    public function testQuoteWindowDefaultsToTheConfiguredTtlAndIsClamped(): void
    {
        $service = new PaidOrderSnapshotService();
        $this->assertSame(1800, $service->configuredQuoteTtl());
        $this->setting('7200');
        $this->assertSame(7200, $service->configuredQuoteTtl());
        $this->setting('5');
        $this->assertSame(60, $service->configuredQuoteTtl());
        $this->setting('999999999');
        $this->assertSame(PaidOrderSnapshotService::MAX_QUOTE_TTL, $service->configuredQuoteTtl());
        foreach (['abc', '-5', '12.5', ''] as $bad) {
            $this->setting($bad);
            $this->assertSame(1800, $service->configuredQuoteTtl(), 'value "' . $bad . '"');
        }
    }

    public function testDraftWindowUsesTheConfiguredTtlWhenThereIsNoLaterDueDate(): void
    {
        $this->setting('7200');
        $before = time();
        $this->draft();
        $this->assertGreaterThanOrEqual($before + 7200, $this->expiresAt());
        $this->assertLessThanOrEqual(time() + 7200, $this->expiresAt());
    }

    public function testDraftWindowExtendsToTheEndOfTheInvoiceDueDate(): void
    {
        $due = date('Y-m-d', time() + 10 * 86400);
        Capsule::table('tblinvoices')->where('id', 20)->update(['duedate' => $due]);
        $this->draft();
        $this->assertSame((int) strtotime($due . ' 23:59:59'), $this->expiresAt());
    }

    public function testZeroOrPastDueDatesDoNotShortenTheWindow(): void
    {
        foreach (['0000-00-00', date('Y-m-d', time() - 86400)] as $due) {
            Capsule::$tables['mod_securiacevps_order_snapshots'] = [];
            Capsule::table('tblinvoices')->where('id', 20)->update(['duedate' => $due]);
            $this->draft();
            $this->assertGreaterThanOrEqual(time() + 1790, $this->expiresAt(), $due);
        }
    }

    public function testAnExplicitTtlStillWinsOverTheDueDate(): void
    {
        Capsule::table('tblinvoices')->where('id', 20)->update(['duedate' => date('Y-m-d', time() + 10 * 86400)]);
        (new PaidOrderSnapshotService())->createDraft(10, 20, 30, 120);
        $this->assertLessThanOrEqual(time() + 120, $this->expiresAt());
    }

    // ── admin re-quote ───────────────────────────────────────────────────────

    public function testRequoteRecoversAPaidServiceWhoseDraftWasInvalidatedBy113(): void
    {
        $this->draft();
        Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid']);
        $this->pay();
        $service = new PaidOrderSnapshotService();

        $stuck = $service->stuckPaidServices();
        $this->assertCount(1, $stuck);
        // 1.1.6: the Ready path shows what will be sealed too (nothing differs).
        $diff = $stuck[0]['diff'];
        $this->assertNotSame([], $diff);
        $this->assertSame([], array_filter($diff, static fn (array $d): bool => !empty($d['changed']) || !empty($d['live_differs'])));
        unset($stuck[0]['diff']);
        $this->assertSame([[
            'service_id' => 30,
            'order_id' => 10,
            'invoice_id' => 20,
            'states' => 'invalid',
            'via' => 'order',
            'status' => 'ready',
            'source' => 'recorded_mapping',
            'reason' => '',
            'diff_token' => $stuck[0]['diff_token'],
            'override_kind' => 'recorded_mapping',
            'deviations' => [],
        ]], $stuck);
        $this->assertNotSame('', $stuck[0]['diff_token'], 'every seal, even an exact match, is confirmed against a keyed token');

        $r = $this->confirmedRequote($service, 30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('requoted', $r['code']);
        $states = array_column($this->snapshots(), 'state');
        $this->assertSame(['superseded', 'sealed'], $states);
        $this->assertSame([], $service->stuckPaidServices());
        $this->assertStringContainsString('re-quoted by admin #7', $this->log());
        $this->assertSame([], $this->create());

        $again = $service->requote(30, 7);
        $this->assertTrue($again['ok']);
        $this->assertSame('already_sealed', $again['code']);
        $this->assertCount(2, $this->snapshots());
    }

    public function testRequoteBillsTheAmountOnThePaidInvoiceNotAHigherCurrentPrice(): void
    {
        $this->draft();
        Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid']);
        $this->pay();
        $r = $this->confirmedRequote(new PaidOrderSnapshotService(), 30, 7);
        $this->assertTrue($r['ok']);
        $payload = json_decode((string) $this->snapshots()[1]['payload_json'], true);
        $this->assertSame('1999.00', $payload['pricing']['recurring']);
        $this->assertSame('1999.00', $payload['whmcs']['total_due']);
    }

    public function testRequoteRefusesAnUnpaidFraudOrUnknownOrNonVpsService(): void
    {
        $this->draft();
        $service = new PaidOrderSnapshotService();

        $this->assertSame('invoice_not_paid', $service->requote(30, 7)['code']);
        $this->assertSame('service_not_found', $service->requote(404, 7)['code']);

        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        Capsule::table('tblorders')->where('id', 10)->update(['status' => 'Fraud']);
        $this->assertSame('order_blocked', $service->requote(30, 7)['code']);

        Capsule::table('tblorders')->where('id', 10)->update(['status' => 'Active']);
        Capsule::table('tblproducts')->where('id', 50)->update(['servertype' => 'cpanel']);
        $this->assertSame('not_native_product', $service->requote(30, 7)['code']);
        $this->assertSame('draft', $this->snapshots()[0]['state']);
    }

    public function testRequoteUsesTheRecordedQuoteEvenWhenTheCurrentMappingIsGone(): void
    {
        $this->draft();
        Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid']);
        $this->pay();
        Capsule::$tables['mod_contabo_mapping'] = [];

        $r = $this->confirmedRequote(new PaidOrderSnapshotService(), 30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('requoted', $r['code']);
        $this->assertSame(['superseded', 'sealed'], array_column($this->snapshots(), 'state'));
    }

    public function testRequoteFailsClosedWhenThereIsNeitherARecordedQuoteNorAPublishedMapping(): void
    {
        Capsule::$tables['mod_contabo_mapping'] = [];
        $this->pay();

        $r = (new PaidOrderSnapshotService())->requote(30, 7);
        $this->assertFalse($r['ok']);
        $this->assertSame('draft_failed', $r['code']);
        $this->assertSame([], $this->snapshots());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testStuckListIgnoresUnpaidSealedAndCancelledOrders(): void
    {
        $this->draft();
        Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid']);
        $service = new PaidOrderSnapshotService();
        $this->assertSame([], $service->stuckPaidServices(), 'unpaid');
        $this->pay();
        Capsule::table('tblorders')->where('id', 10)->update(['status' => 'Cancelled']);
        $this->assertSame([], $service->stuckPaidServices(), 'cancelled');
    }
}
