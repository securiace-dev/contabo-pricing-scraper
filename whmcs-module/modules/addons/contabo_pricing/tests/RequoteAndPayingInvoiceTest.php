<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\PaidOrderSnapshotService;
use ContaboPricing\ProvisioningGate;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 1.1.5
 *  (a) re-quote rebuilds from the customer's OWN recorded quote, never silently
 *      from a republished mapping; the current mapping is reachable only through
 *      an explicit, logged override bound to the diff the admin was shown;
 *  (b) a payment on a Mass Pay, merged or re-issued invoice recovers the order
 *      through tblinvoiceitems links, with the tamper and binding checks intact.
 */
final class RequoteAndPayingInvoiceTest extends TestCase
{
    use SnapshotFixture;

    protected function setUp(): void
    {
        $this->bootFixture();
        Capsule::$tables['tblinvoiceitems'] = [];
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** @return list<array<string,mixed>> */
    private function snapshots(): array
    {
        return array_map(static fn ($r): array => (array) $r, Capsule::$tables['mod_securiacevps_order_snapshots']);
    }

    /** @return array<string,mixed> */
    private function payload(int $index): array
    {
        return json_decode((string) $this->snapshots()[$index]['payload_json'], true);
    }

    private function states(): array
    {
        return array_column($this->snapshots(), 'state');
    }

    private function draft(): string
    {
        return (new PaidOrderSnapshotService())->createDraft(10, 20, 30);
    }

    private function invalidate(): void
    {
        Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid']);
    }

    private function activeOrder(): void
    {
        Capsule::table('tblorders')->where('id', 10)->update(['status' => 'Active']);
    }

    private function payOwn(): void
    {
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        $this->activeOrder();
    }

    /** @param list<array{0:string,1:int}> $items [type, relid] */
    private function invoice(int $id, string $status, int $userId, array $items, string $total = '1999.00'): void
    {
        Capsule::$tables['tblinvoices'][] = [
            'id' => $id, 'userid' => $userId, 'status' => $status,
            'subtotal' => $total, 'tax' => '0.00', 'tax2' => '0.00', 'credit' => '0.00', 'total' => $total,
        ];
        foreach ($items as [$type, $relid]) {
            Capsule::$tables['tblinvoiceitems'][] = ['invoiceid' => $id, 'type' => $type, 'relid' => $relid];
        }
        $this->activeOrder();
    }

    private function deleteInvoice(int $id): void
    {
        Capsule::$tables['tblinvoices'] = array_values(array_filter(
            Capsule::$tables['tblinvoices'],
            static fn ($r): bool => (int) ((array) $r)['id'] !== $id
        ));
    }

    /** The mapping is republished: new SKU/region/image/renewal, old publication superseded. */
    private function republish(string $version = 'mapping-v2', string $sku = 'V99'): void
    {
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update(['state' => 'superseded']);
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 91,
            'mapping_version' => $version,
            'state' => 'published',
            'payload_json' => json_encode([
                'provider' => ['sku_id' => $sku, 'region_id' => 'US', 'image_id' => 'image-9'],
                'pricing_profile_version' => 'profile-v4',
                'compatibility_version' => 'compat-v3',
                'management_code' => 'self_managed',
                'tax_policy' => 'whmcs',
                'renewal_amount' => '2999.00',
            ]),
        ];
        Capsule::table('mod_contabo_mapping')->where('id', 70)->update(['published_mapping_version' => $version]);
    }

    private function mutatePayload(callable $change): void
    {
        $row = $this->snapshots()[0];
        $payload = json_decode((string) $row['payload_json'], true);
        $change($payload);
        Capsule::table('mod_securiacevps_order_snapshots')->where('id', $row['id'])
            ->update(['payload_json' => json_encode($payload)]);
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

    // ── (a) re-quote source ──────────────────────────────────────────────────

    public function testRequoteAfterARepublishKeepsTheCustomersOriginalQuote(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();

        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('ready', $plan['code']);
        $this->assertFalse($plan['needs_override']);
        $this->assertSame('recorded_mapping', $plan['source']);
        $this->assertSame('mapping-v1', $plan['mapping_version']);

        $r = (new PaidOrderSnapshotService())->requote(30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('requoted', $r['code']);
        $sealed = $this->payload(1);
        $this->assertSame('mapping-v1', $this->snapshots()[1]['mapping_version']);
        $this->assertSame('V45', $sealed['provider']['sku_id']);
        $this->assertSame('EU', $sealed['provider']['region_id']);
        $this->assertSame('image-1', $sealed['provider']['image_id']);
        $this->assertSame('2199.00', $sealed['pricing']['renewal']);
        $this->assertSame(['superseded', 'sealed'], $this->states());
        $this->assertStringContainsString('recorded_mapping', $this->log());
        $this->assertStringNotContainsString('OVERRIDE', $this->log());
        $this->assertSame([], $this->create());
    }

    public function testRequoteFallsBackToTheRecordedSnapshotWhenThePublicationIsGone(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();
        Capsule::$tables['mod_contabo_mapping_publications'] = array_values(array_filter(
            Capsule::$tables['mod_contabo_mapping_publications'],
            static fn ($r): bool => ((array) $r)['mapping_version'] !== 'mapping-v1'
        ));

        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('recorded_snapshot', $plan['source']);
        $r = (new PaidOrderSnapshotService())->requote(30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $sealed = $this->payload(1);
        $this->assertSame('V45', $sealed['provider']['sku_id']);
        $this->assertSame('EU', $sealed['provider']['region_id']);
        $this->assertSame('image-1', $sealed['provider']['image_id']);
        $this->assertSame('2199.00', $sealed['pricing']['renewal']);
    }

    public function testARecordedPublicationThatDisagreesWithTheRowIsNotTrusted(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update([
            'payload_json' => json_encode(['provider' => ['sku_id' => 'EDITED', 'region_id' => 'EU', 'image_id' => 'image-1'], 'renewal_amount' => '2199.00']),
        ]);

        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('needs_override', $plan['code'], 'the SKU is not hashed, so a disagreeing publication cannot be reconciled silently');
        $this->assertContains('SKU', array_column($plan['diff'], 'field'));
        $r = (new PaidOrderSnapshotService())->requote(30, 7);
        $this->assertFalse($r['ok']);
        $this->assertSame('needs_override', $r['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testTamperedRowWithNothingRecordedNeedsAnExplicitOverride(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->mutatePayload(static function (array &$p): void {
            $p['pricing']['recurring'] = '1.00';
        });
        $this->republish();
        $service = new PaidOrderSnapshotService();

        $plan = $service->requotePlan(30);
        $this->assertSame('needs_override', $plan['code']);
        $this->assertTrue($plan['needs_override']);
        $this->assertSame('mapping-v2', $plan['mapping_version']);
        $byField = array_column($plan['diff'], null, 'field');
        $this->assertSame('V45', $byField['SKU']['old']);
        $this->assertSame('V99', $byField['SKU']['new']);
        $this->assertTrue($byField['SKU']['changed']);
        $this->assertSame('EU', $byField['Region']['old']);
        $this->assertSame('US', $byField['Region']['new']);
        $this->assertSame('image-1', $byField['Image']['old']);
        $this->assertSame('image-9', $byField['Image']['new']);
        $this->assertSame('2999.00', $byField['Renewal']['new']);
        $this->assertNotSame('', $plan['diff_token']);

        $r = $service->requote(30, 7);
        $this->assertFalse($r['ok']);
        $this->assertSame('needs_override', $r['code']);
        $this->assertStringContainsString('V45', $r['message']);
        $this->assertStringContainsString('V99', $r['message']);
        $this->assertSame(['invalid'], $this->states(), 'nothing was changed');
        $this->assertSame(['abortcmd' => true], $this->create());

        $stuck = $service->stuckPaidServices();
        $this->assertSame('override', $stuck[0]['status']);
        $this->assertSame($plan['diff_token'], $stuck[0]['diff_token']);
    }

    public function testOverrideWithTheShownDiffTokenSealsOnTheCurrentMappingAndIsLogged(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->mutatePayload(static function (array &$p): void {
            $p['pricing']['recurring'] = '1.00';
        });
        $this->republish();
        $service = new PaidOrderSnapshotService();
        $token = $service->requotePlan(30)['diff_token'];

        $r = $service->requote(30, 7, true, $token);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('requoted', $r['code']);
        $this->assertSame(['superseded', 'sealed'], $this->states());
        $this->assertSame('V99', $this->payload(1)['provider']['sku_id']);
        $this->assertSame('mapping-v2', $this->snapshots()[1]['mapping_version']);
        $this->assertStringContainsString('re-quote OVERRIDE by admin #7', $this->log());
        $this->assertStringContainsString('V45', $this->log());
        $this->assertStringContainsString('V99', $this->log());
        $this->assertSame([], $this->create());
    }

    public function testOverrideWithAMissingWrongOrStaleTokenIsRefused(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->mutatePayload(static function (array &$p): void {
            $p['pricing']['recurring'] = '1.00';
        });
        $this->republish();
        $service = new PaidOrderSnapshotService();
        $token = $service->requotePlan(30)['diff_token'];

        $this->assertSame('diff_changed', $service->requote(30, 7, true, '')['code']);
        $this->assertSame('diff_changed', $service->requote(30, 7, true, 'nonsense')['code']);
        $this->assertSame('needs_override', $service->requote(30, 7, false, $token)['code']);

        $this->republish('mapping-v3', 'V77');
        $stale = $service->requote(30, 7, true, $token);
        $this->assertFalse($stale['ok']);
        $this->assertSame('diff_changed', $stale['code']);
        $this->assertSame(['invalid'], $this->states());
        $this->assertStringNotContainsString('OVERRIDE', $this->log());
        $fresh = $service->requotePlan(30)['diff_token'];
        $this->assertNotSame($token, $fresh);
        $this->assertTrue($service->requote(30, 7, true, $fresh)['ok']);
        $this->assertSame('V77', $this->payload(1)['provider']['sku_id']);
    }

    public function testNoEarlierRowAtAllAlsoNeedsAnOverrideAndShowsNotRecorded(): void
    {
        $this->payOwn();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('needs_override', $plan['code']);
        $byField = array_column($plan['diff'], null, 'field');
        $this->assertSame('(not recorded)', $byField['SKU']['old']);
        $r = (new PaidOrderSnapshotService())->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('V45', $this->payload(0)['provider']['sku_id']);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testAnUnverifiedOverrideStillNeedsAPaidInvoice(): void
    {
        $this->draft();
        $this->invalidate();
        $service = new PaidOrderSnapshotService();
        $this->assertSame('invoice_not_paid', $service->requote(30, 7, true, 'x')['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    // ── (b) paying invoice ───────────────────────────────────────────────────

    public function testMassPayInvoiceSealsFromTheInvoicePaidHook(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);

        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);

        $this->assertSame(['sealed'], $this->states());
        $this->assertStringContainsString('sealed via a linked invoice', $this->log());
        $this->assertStringContainsString('mass_pay', $this->log());
        $this->assertSame([], $this->create());
    }

    public function testMassPayInvoiceSealsWhenTheOrderPaidHookReportsTheMassPayInvoice(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 21]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testMassPayAlsoSealsEveryOrderItCovers(): void
    {
        $this->draft();
        Capsule::$tables['tblhosting'][] = [
            'id' => 31, 'orderid' => 11, 'userid' => 40, 'packageid' => 50, 'billingcycle' => 'Monthly',
            'domain' => 'two.example.com', 'firstpaymentamount' => '1999.00', 'amount' => '1999.00',
        ];
        Capsule::$tables['tblorders'][] = ['id' => 11, 'invoiceid' => 22, 'status' => 'Active', 'amount' => '1999.00'];
        Capsule::$tables['tblinvoices'][] = [
            'id' => 22, 'userid' => 40, 'status' => 'Unpaid', 'subtotal' => '1999.00', 'tax' => '0.00',
            'tax2' => '0.00', 'credit' => '0.00', 'total' => '1999.00',
        ];
        Capsule::$tables['tblhostingconfigoptions'][] = ['relid' => 31, 'configid' => 100, 'optionid' => 101, 'qty' => 1];
        (new PaidOrderSnapshotService())->createDraft(11, 22, 31);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20], ['Invoice', 22]], '3998.00');

        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);

        $this->assertSame(['sealed', 'sealed'], $this->states());
    }

    public function testMergedInvoiceWhoseSourceInvoiceWasDeletedSealsAtTheGate(): void
    {
        $this->draft();
        $this->deleteInvoice(20);
        $this->invoice(22, 'Paid', 40, [['Hosting', 30]]);

        $this->assertSame([], $this->create(), 'gate self-heals through the merged invoice');
        $this->assertSame(['sealed'], $this->states());
        $this->assertStringContainsString('merged', $this->log());
        $this->assertStringContainsString('sealed via a linked invoice', $this->log());
    }

    public function testMergedInvoiceWhenTheOrderWasRepointedToTheSurvivingInvoice(): void
    {
        $this->draft();
        $this->deleteInvoice(20);
        Capsule::table('tblorders')->where('id', 10)->update(['invoiceid' => 22]);
        $this->invoice(22, 'Paid', 40, [['Hosting', 30]]);

        ($this->hook('InvoicePaid'))(['invoiceid' => 22]);

        $this->assertSame(['sealed'], $this->states());
        $this->assertSame([], $this->create());
    }

    public function testReInvoiceForTheSameHostingSealsAndKeepsTheQuotedTerms(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30]]);

        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);

        $this->assertSame(['sealed'], $this->states());
        $this->assertSame('V45', $this->payload(0)['provider']['sku_id']);
        $this->assertStringContainsString('reinvoice', $this->log());
        $this->assertStringContainsString('1999.00', $this->log());
    }

    public function testCancellingTheOriginalInvoiceKeepsTheDraftWhenAReplacementInvoiceIsLive(): void
    {
        $this->draft();
        $this->invoice(23, 'Unpaid', 40, [['Hosting', 30]]);
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);

        ($this->hook('InvoiceCancelled'))(['invoiceid' => 20]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('replacement', $this->log());

        Capsule::table('tblinvoices')->where('id', 23)->update(['status' => 'Paid']);
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testCancellingWithNoReplacementInvoiceStillInvalidatesTheDraft(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 20]);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testACancelledReplacementDoesNotKeepTheDraftAlive(): void
    {
        $this->draft();
        $this->invoice(23, 'Cancelled', 40, [['Hosting', 30]]);
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 20]);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testAnUnpaidLinkedInvoiceDoesNotSeal(): void
    {
        $this->draft();
        $this->invoice(21, 'Unpaid', 40, [['Invoice', 20]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testPaidInvoiceOfAnotherClientIsRefusedAndListedWithAReason(): void
    {
        $this->draft();
        $this->invoice(24, 'Paid', 41, [['Hosting', 30]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 24]);

        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('belongs to client #41', $this->log());
        $this->assertSame(['abortcmd' => true], $this->create());

        $service = new PaidOrderSnapshotService();
        $this->assertSame('paying_invoice_refused', $service->requotePlan(30)['code']);
        $r = $service->requote(30, 7, true, 'anything');
        $this->assertFalse($r['ok']);
        $this->assertSame('paying_invoice_refused', $r['code'], 'a refusal is not overridable');
        $stuck = $service->stuckPaidServices();
        $this->assertCount(1, $stuck);
        $this->assertSame('refused', $stuck[0]['status']);
        $this->assertStringContainsString('client #41', $stuck[0]['reason']);
        $this->assertSame(['draft'], $this->states());
    }

    public function testMassPayInvoiceOfAnotherClientIsRefused(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 41, [['Invoice', 20]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testPaidInvoiceWithNoItemLinkNeverSeals(): void
    {
        $this->draft();
        $this->invoice(25, 'Paid', 40, []);
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 25]);
        $this->assertSame(['draft'], $this->states());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testPaidInvoiceLinkedToADifferentServiceNeverSeals(): void
    {
        $this->draft();
        $this->invoice(26, 'Paid', 40, [['Hosting', 31], ['Invoice', 99]]);
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 26]);
        $this->assertSame(['draft'], $this->states());
    }

    public function testAnUnlinkedPaidHintDoesNotInvalidateAnotherwiseValidDraft(): void
    {
        $this->draft();
        $this->invoice(25, 'Paid', 40, []);
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 25]);
        $this->assertSame(['draft'], $this->states());
        $this->payOwn();
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 20]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testDraftBoundToAnotherInvoiceDoesNotSealOnTheOrdersPaidInvoice(): void
    {
        (new PaidOrderSnapshotService())->createDraft(10, 99, 30);
        $this->payOwn();
        (new PaidOrderSnapshotService())->sealOrder(10, 20);
        $this->assertSame(['draft'], $this->states(), 'bound to #99, which is neither paid nor linked');
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testTamperedHashedFieldIsStillRefusedOnALinkedInvoice(): void
    {
        $this->draft();
        $this->mutatePayload(static function (array &$p): void {
            $p['pricing']['recurring'] = '1.00';
        });
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['invalid'], $this->states());
        $this->assertStringContainsString('REFUSED to seal', $this->log());
    }

    public function testTamperedProviderCopyIsStillRefusedOnALinkedInvoice(): void
    {
        $this->draft();
        $this->mutatePayload(static function (array &$p): void {
            $p['provider']['image_id'] = 'evil-image';
        });
        $this->invoice(22, 'Paid', 40, [['Hosting', 30]]);
        $this->deleteInvoice(20);
        ($this->hook('InvoicePaid'))(['invoiceid' => 22]);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testEditedInvoiceBindingIsRefusedNotFollowedToAnotherClientsInvoice(): void
    {
        $this->draft();
        $this->mutatePayload(static function (array &$p): void {
            $p['whmcs']['invoice_id'] = 24;
        });
        $this->invoice(24, 'Paid', 41, [['Hosting', 30]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 24]);
        $this->assertNotContains('sealed', $this->states());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testRequoteBindsToTheQuotedInvoiceAndPaysViaTheMassPayInvoice(): void
    {
        $this->draft();
        $this->invalidate();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        $service = new PaidOrderSnapshotService();

        $stuck = $service->stuckPaidServices();
        $this->assertSame(21, $stuck[0]['invoice_id']);
        $this->assertSame('mass_pay', $stuck[0]['via']);
        $this->assertSame('ready', $stuck[0]['status']);

        $r = $service->requote(30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame(20, $this->payload(1)['whmcs']['invoice_id'], 'quote stays bound to the invoice it was quoted on');
        $this->assertStringContainsString('paid on invoice #21 (mass_pay)', $this->log());
        $this->assertSame([], $this->create());
    }

    public function testRequoteViaAMergedAndAReInvoicedPayment(): void
    {
        $this->draft();
        $this->invalidate();
        $this->deleteInvoice(20);
        $this->invoice(22, 'Paid', 40, [['Hosting', 30]]);
        $service = new PaidOrderSnapshotService();
        $this->assertSame('merged', $service->requotePlan(30)['via']);
        $this->assertTrue($service->requote(30, 7)['ok']);
        $this->assertSame(['superseded', 'sealed'], $this->states());
    }

    public function testRequoteViaAReInvoiceKeepsTheOriginalQuoteBinding(): void
    {
        $this->draft();
        $this->invalidate();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30]]);
        $service = new PaidOrderSnapshotService();
        $plan = $service->requotePlan(30);
        $this->assertSame('reinvoice', $plan['via']);
        $this->assertSame(23, $plan['invoice_id']);
        $this->assertSame(20, $plan['bound_invoice_id']);
        $this->assertTrue($service->requote(30, 7)['ok']);
    }

    public function testTheOrdersOwnPaidInvoiceStillWinsOverALinkedOne(): void
    {
        $this->draft();
        $this->payOwn();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        $this->assertSame('order', (new PaidOrderSnapshotService())->payingInvoice(10, 30)['via']);
    }

    public function testGrandfatheredOrderPaidViaAMergedInvoiceIsStillAllowed(): void
    {
        Capsule::table('mod_contabo_settings')->updateOrInsert(
            ['key' => ProvisioningGate::SETTING_CUTOFF],
            ['value' => '2026-11-01 00:00:00']
        );
        Capsule::table('tblorders')->where('id', 10)->update(['date' => '2026-10-20 10:00:00']);
        $this->deleteInvoice(20);
        $this->invoice(22, 'Paid', 40, [['Hosting', 30]]);

        $this->assertSame([], $this->create());
        $this->assertStringContainsString(ProvisioningGate::REASON_GRANDFATHERED, $this->log());
    }

    public function testGateNamesTheLinkProblemWhenAPaidInvoiceIsRefused(): void
    {
        $this->draft();
        $this->invoice(24, 'Paid', 41, [['Hosting', 30]]);
        $this->create();
        $this->assertStringContainsString('client #41', $this->log());
    }

    public function testInvoicePaidHookIsRegisteredAtTheSnapshotPriority(): void
    {
        $this->hook('InvoicePaid');
        $this->assertSame(40, $GLOBALS['contabo_pricing_test_hooks']['InvoicePaid'][0]['priority'] ?? 40);
    }

    public function testInvoicePaidHookIgnoresUnrelatedInvoices(): void
    {
        $this->draft();
        $this->invoice(27, 'Paid', 40, [['Hosting', 999]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 27]);
        ($this->hook('InvoicePaid'))([]);
        $this->assertSame(['draft'], $this->states());
    }
}
