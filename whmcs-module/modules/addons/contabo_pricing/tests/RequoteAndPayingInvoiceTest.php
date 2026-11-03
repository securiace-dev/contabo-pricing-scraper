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

    /** @param list<array{0:string,1:int}|array{0:string,1:int,2:string}> $items [type, relid, amount (default the quoted 1999.00)] */
    private function invoice(int $id, string $status, int $userId, array $items, string $total = '1999.00'): void
    {
        Capsule::$tables['tblinvoices'][] = [
            'id' => $id, 'userid' => $userId, 'status' => $status,
            'subtotal' => $total, 'tax' => '0.00', 'tax2' => '0.00', 'credit' => '0.00', 'total' => $total,
        ];
        foreach ($items as $item) {
            Capsule::$tables['tblinvoiceitems'][] = [
                'invoiceid' => $id, 'type' => $item[0], 'relid' => $item[1], 'amount' => $item[2] ?? '1999.00',
            ];
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
            'product_id' => 50,
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
        $this->hashPublications();
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

        $r = $this->confirmedRequote(new PaidOrderSnapshotService(), 30, 7);
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

    private function dropPublication(string $version): void
    {
        Capsule::$tables['mod_contabo_mapping_publications'] = array_values(array_filter(
            Capsule::$tables['mod_contabo_mapping_publications'],
            static fn ($r): bool => ((array) $r)['mapping_version'] !== $version
        ));
    }

    public function testARecordedSnapshotWhoseSkuNothingCorroboratesNeedsAnExplicitConfirmation(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();
        $this->dropPublication('mapping-v1');
        $service = new PaidOrderSnapshotService();

        $plan = $service->requotePlan(30);
        $this->assertSame('needs_override', $plan['code']);
        $this->assertSame('recorded_snapshot', $plan['override_kind']);
        $this->assertSame('recorded_snapshot', $plan['source']);
        $this->assertStringContainsString('SKU V45', $plan['message']);
        $this->assertStringContainsString('cannot be checked', $plan['message']);

        $r = $service->requote(30, 7);
        $this->assertFalse($r['ok']);
        $this->assertSame('needs_override', $r['code']);
        $this->assertSame(['invalid'], $this->states(), 'an unconfirmed snapshot is never sealed');

        $r = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);
        $sealed = $this->payload(1);
        $this->assertSame('V45', $sealed['provider']['sku_id']);
        $this->assertSame('EU', $sealed['provider']['region_id']);
        $this->assertSame('image-1', $sealed['provider']['image_id']);
        $this->assertSame('2199.00', $sealed['pricing']['renewal']);
        $this->assertStringContainsString('CONFIRMED by admin #7', $this->log());
        $this->assertStringContainsString('SKU V45', $this->log());
    }

    public function testTheSnapshotConfirmationIsBoundToTheValuesTheAdminSaw(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->dropPublication('mapping-v1');
        $service = new PaidOrderSnapshotService();
        $token = $service->requotePlan(30)['diff_token'];
        $this->mutatePayload(static function (array &$p): void {
            $p['provider']['sku_id'] = 'V99';
        });
        $r = $service->requote(30, 7, true, $token);
        $this->assertFalse($r['ok']);
        $this->assertSame('diff_changed', $r['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testTheOverrideFlagWithAWrongTokenNeverSeals(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->dropPublication('mapping-v1');
        $r = (new PaidOrderSnapshotService())->requote(30, 7, true, 'nope');
        $this->assertFalse($r['ok']);
        $this->assertSame('diff_changed', $r['code']);
    }

    public function testARecordedPublicationThatFailsItsHashIsNotTrusted(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();
        // Edited after publication, hash left stale: the tamper signal must not
        // fall back to the row's own (unhashed) SKU.
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update([
            'payload_json' => json_encode(['provider' => ['sku_id' => 'EDITED', 'region_id' => 'EU', 'image_id' => 'image-1'], 'renewal_amount' => '2199.00']),
        ]);

        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('needs_override', $plan['code']);
        $this->assertSame('current_mapping', $plan['override_kind']);
        $this->assertStringContainsString('failed verification', $plan['reason']);
        $r = (new PaidOrderSnapshotService())->requote(30, 7);
        $this->assertFalse($r['ok']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testARecordedPublicationWithAHashButNoHashColumnValueIsNotTrusted(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update(['payload_hash' => '']);
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('current_mapping', $plan['override_kind']);
        $this->assertStringContainsString('no usable payload hash', $plan['reason']);
    }

    public function testARecordedPublicationThatDisagreesWithTheRowEvenWithAValidHashIsNotTrusted(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update([
            'payload_json' => json_encode(['provider' => ['sku_id' => 'EDITED', 'region_id' => 'EU', 'image_id' => 'image-1'], 'renewal_amount' => '2199.00']),
        ]);
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('current_mapping', $plan['override_kind']);
        $this->assertStringContainsString('disagrees with the draft row on provider.sku_id', $plan['reason']);
        $this->assertContains('SKU', array_column($plan['diff'], 'field'));
    }

    /** A hash-correct publication of ANOTHER product must not be treated as the recorded mapping. */
    private function pointRowAtPublication(string $version, string $sku): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $row = $this->snapshots()[0];
        $payload = json_decode((string) $row['payload_json'], true);
        $payload['provider']['sku_id'] = $sku;
        Capsule::table('mod_securiacevps_order_snapshots')->where('id', $row['id'])
            ->update(['payload_json' => json_encode($payload), 'mapping_version' => $version]);
    }

    public function testAPublicationOfAnotherProductIsNotTheRecordedMapping(): void
    {
        $this->pointRowAtPublication('map-p77-other', 'V999');
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 92, 'mapping_version' => 'map-p77-other', 'product_id' => 77, 'state' => 'published',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V999', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'self_managed']),
        ];
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertNotSame('recorded_mapping', $plan['source']);
        $this->assertTrue($plan['needs_override']);
        $this->assertStringContainsString('belongs to another product', $plan['reason']);
        $r = (new PaidOrderSnapshotService())->requote(30, 7);
        $this->assertFalse($r['ok']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testASameProductPublicationWithADifferentManagementCodeIsNotTrusted(): void
    {
        $this->pointRowAtPublication('map-managed', 'V999');
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 92, 'mapping_version' => 'map-managed', 'product_id' => 50, 'state' => 'superseded',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V999', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'managed']),
        ];
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertTrue($plan['needs_override']);
        $this->assertStringContainsString('different management code', $plan['reason']);
    }

    public function testAPublicationThatTookEffectAfterTheDraftWasMadeIsNotTrusted(): void
    {
        $this->pointRowAtPublication('map-later', 'V999');
        Capsule::table('mod_securiacevps_order_snapshots')->update(['created_at' => '2026-01-01 00:00:00']);
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 92, 'mapping_version' => 'map-later', 'product_id' => 50, 'state' => 'superseded', 'effective_at' => '2026-06-01 00:00:00',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V999', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'self_managed']),
        ];
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertTrue($plan['needs_override']);
        $this->assertStringContainsString('only took effect after the draft was created', $plan['reason']);
    }

    public function testAPublicationAlreadySupersededWhenTheDraftWasMadeIsNotTrusted(): void
    {
        $this->pointRowAtPublication('map-old', 'V999');
        Capsule::table('mod_securiacevps_order_snapshots')->update(['created_at' => '2026-06-01 00:00:00']);
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 92, 'mapping_version' => 'map-old', 'product_id' => 50, 'state' => 'superseded', 'effective_at' => '2026-01-01 00:00:00',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V999', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'self_managed']),
        ];
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 93, 'mapping_version' => 'map-newer', 'product_id' => 50, 'state' => 'published', 'effective_at' => '2026-03-01 00:00:00',
            'supersedes_mapping_version' => 'map-old',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V1', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'self_managed']),
        ];
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertTrue($plan['needs_override']);
        $this->assertStringContainsString('already been superseded', $plan['reason']);
    }

    public function testAnAbandonedPreviewDoesNotHideTheApprovedSuccessor(): void
    {
        $this->pointRowAtPublication('map-old', 'V999');
        Capsule::table('mod_securiacevps_order_snapshots')->update(['created_at' => '2026-06-01 00:00:00']);
        $body = json_encode(['provider' => ['sku_id' => 'V1', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'self_managed']);
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 92, 'mapping_version' => 'map-old', 'product_id' => 50, 'state' => 'superseded', 'effective_at' => '2026-01-01 00:00:00',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V999', 'region_id' => 'EU', 'image_id' => 'image-1'], 'management_code' => 'self_managed']),
        ];
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 93, 'mapping_version' => 'map-abandoned', 'product_id' => 50, 'state' => 'draft', 'effective_at' => null,
            'supersedes_mapping_version' => 'map-old', 'payload_json' => $body,
        ];
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 94, 'mapping_version' => 'map-approved', 'product_id' => 50, 'state' => 'published', 'effective_at' => '2026-03-01 00:00:00',
            'supersedes_mapping_version' => 'map-old', 'payload_json' => $body,
        ];
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertTrue($plan['needs_override']);
        $this->assertStringContainsString('already been superseded', $plan['reason']);
    }

    public function testAPublicationThatWasInEffectForTheDraftIsStillTrusted(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        Capsule::table('mod_securiacevps_order_snapshots')->update(['created_at' => '2026-02-01 00:00:00']);
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')
            ->update(['state' => 'superseded', 'effective_at' => '2026-01-01 00:00:00']);
        Capsule::$tables['mod_contabo_mapping_publications'][] = [
            'id' => 93, 'mapping_version' => 'mapping-v2', 'product_id' => 50, 'state' => 'published', 'effective_at' => '2026-03-01 00:00:00',
            'supersedes_mapping_version' => 'mapping-v1',
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V99', 'region_id' => 'US', 'image_id' => 'image-9'], 'management_code' => 'self_managed']),
        ];
        Capsule::table('mod_contabo_mapping')->where('id', 70)->update(['published_mapping_version' => 'mapping-v2']);
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('recorded_mapping', $plan['source']);
        $this->assertFalse($plan['needs_override']);
    }

    public function testAServiceMovedToAnotherClientStillSealsOnTheOrdersOwnInvoice(): void
    {
        $this->draft();
        Capsule::table('tblorders')->where('id', 10)->update(['userid' => 40]);
        Capsule::table('tblhosting')->where('id', 30)->update(['userid' => 41]);
        $this->payOwn();
        $this->activeOrder();
        $plan = (new PaidOrderSnapshotService())->payingInvoice(10, 30);
        $this->assertSame(20, $plan['invoice_id']);
        $this->assertSame('order', $plan['via']);
        $this->assertSame([], $this->create());
    }

    public function testARecordedPublicationWhoseRenewalDisagreesWithTheHashedPricingIsNotTrusted(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->republish();
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update([
            'payload_json' => json_encode(['provider' => ['sku_id' => 'V45', 'region_id' => 'EU', 'image_id' => 'image-1'], 'renewal_amount' => '1.00']),
        ]);
        $this->hashPublications();
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('current_mapping', $plan['override_kind']);
        $this->assertStringContainsString('renewal amount', $plan['reason']);
    }

    public function testATamperedCurrentPublicationCannotBeUsedForAnOverrideEither(): void
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        $this->mutatePayload(static function (array &$p): void {
            $p['pricing']['recurring'] = '1.00';
        });
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update(['payload_hash' => str_repeat('a', 64)]);
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('draft_failed', $plan['code']);
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

    // ── 1.1.6 B1: a paid linked invoice must really settle THIS quote ───────

    public function testB1APaidHostingInvoiceDoesNotSettleAnOrderInvoiceThatIsStillUnpaid(): void
    {
        $this->draft();
        // Same client, a Hosting line for the service, fully paid, while the
        // order's own invoice #20 is still Unpaid: this used to seal.
        $this->invoice(23, 'Paid', 40, [['Hosting', 30]]);

        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);

        $this->assertSame(['draft'], $this->states(), 'the order invoice is unpaid, so the other payment settles nothing');
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('still Unpaid', $this->log());
        $this->assertStringNotContainsString('sealed via a linked invoice', $this->log());
    }

    public function testB1ATinyPaidInvoiceWithAHostingLineCannotUnlockTheService(): void
    {
        $this->draft();
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1.00']], '1.00');
        ($this->hook('OrderPaid'))(['orderId' => 10, 'invoiceId' => 23]);
        $this->assertSame(['draft'], $this->states());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testB1TheGrandfatherPathDoesNotAcceptAnUnrelatedPaidHostingInvoiceEither(): void
    {
        Capsule::table('mod_contabo_settings')->updateOrInsert(
            ['key' => ProvisioningGate::SETTING_CUTOFF],
            ['value' => '2026-11-01 00:00:00']
        );
        Capsule::table('tblorders')->where('id', 10)->update(['date' => '2026-10-20 10:00:00']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1.00']], '1.00');

        $this->assertSame(['abortcmd' => true], $this->create(), 'order invoice #20 is Unpaid');
        $this->assertStringNotContainsString(ProvisioningGate::REASON_GRANDFATHERED, $this->log());
    }

    public function testB1RequoteRefusesTheSameBypassAndItIsNotOverridable(): void
    {
        $this->draft();
        $this->invalidate();
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1.00']], '1.00');
        $service = new PaidOrderSnapshotService();

        $plan = $service->requotePlan(30);
        $this->assertSame('paying_invoice_refused', $plan['code']);
        $r = $service->requote(30, 7, true, 'anything');
        $this->assertSame('paying_invoice_refused', $r['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testB1MassPayThatDoesNotCoverTheQuotedTotalIsRefused(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20, '10.00']], '10.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('shortfall', $this->log());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testB1MassPayCoveringTheQuotedTotalAcrossTwoItemsOfTheSameInvoiceStillSeals(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20, '1000.00'], ['Invoice', 20, '999.00']], '1999.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testB1MassPayAHostingLineAloneDoesNotMakeItAMassPay(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 99], ['Hosting', 30]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states(), 'the Hosting line is a re-issue and the original is still Unpaid');
    }

    public function testB1AReissuedInvoiceMustCoverTheQuotedFirstPayment(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '999.00']], '999.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('shortfall', $this->log());
    }

    public function testB1AReissuedInvoiceSumsItsHostingSetupAndPromoLines(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1899.00'], ['Setup', 30, '200.00'], ['PromoHosting', 30, '-100.00']], '1999.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testB1AMergedInvoiceMustCoverTheQuotedFirstPayment(): void
    {
        $this->draft();
        $this->deleteInvoice(20);
        $this->invoice(22, 'Paid', 40, [['Hosting', 30, '5.00']], '5.00');
        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertSame(['draft'], $this->states());
    }

    public function testB1AReissueWhileTheOriginalIsInAnyOtherStateIsRefused(): void
    {
        foreach (['Unpaid', 'Payment Pending', 'Collections', 'Refunded', 'Draft'] as $status) {
            $this->setUp();
            $this->draft();
            Capsule::table('tblinvoices')->where('id', 20)->update(['status' => $status]);
            $this->invoice(23, 'Paid', 40, [['Hosting', 30]]);
            (new PaidOrderSnapshotService())->sealOrder(10, 23);
            $this->assertSame(['draft'], $this->states(), 'original invoice ' . $status);
        }
    }

    public function testB1TheOriginalOwnersMassPayOfAMovedServiceIsAccepted(): void
    {
        Capsule::table('tblorders')->where('id', 10)->update(['userid' => 40]);
        Capsule::table('tblhosting')->where('id', 30)->update(['userid' => 41]);
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['sealed'], $this->states(), 'invoice #21 belongs to the ordering client #40');
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
        $this->assertStringContainsString(
            'invoice #20 was cancelled but service #30 has a replacement invoice; the draft stays open',
            $this->log()
        );

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

    public function testCancellingTheReplacementInvoiceInvalidatesTheDraftOnceNothingLiveRemains(): void
    {
        $this->draft();
        $this->invoice(23, 'Unpaid', 40, [['Hosting', 30]]);
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 20]);
        $this->assertSame(['draft'], $this->states());

        Capsule::table('tblinvoices')->where('id', 23)->update(['status' => 'Cancelled']);
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 23]);
        $this->assertSame(['invalid'], $this->states(), 'the replacement was the last live invoice');
    }

    public function testCancellingAReplacementKeepsTheDraftWhileTheBoundInvoiceIsStillLive(): void
    {
        $this->draft();
        $this->invoice(23, 'Cancelled', 40, [['Hosting', 30]]);
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 23]);
        $this->assertSame(['draft'], $this->states(), 'the order invoice #20 is still Unpaid');
    }

    public function testCancellingAMassPayInvoiceDoesNotInvalidateTheLiveOriginal(): void
    {
        $this->draft();
        $this->invoice(21, 'Cancelled', 40, [['Invoice', 20]]);
        ($this->hook('InvoiceCancelled'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states());
    }

    public function testMassPayThatAlsoMarksTheOriginalPaidResolvesThroughTheOrdersOwnInvoice(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);

        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['sealed'], $this->states());
        $this->assertStringNotContainsString('mass_pay', $this->log(), 'the order link wins over the item link');
        $this->assertSame([], $this->create());
    }

    public function testMassPayInvoiceCarryingAHostingItemForTheServiceSeals(): void
    {
        $this->draft();
        $this->invoice(21, 'Paid', 40, [['Invoice', 20], ['Hosting', 30]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testThePaidOrderInvoiceOfAnotherClientIsRefused(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid', 'userid' => 41]);
        $this->activeOrder();
        $this->assertSame(['abortcmd' => true], $this->create());
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('paying_invoice_refused', $plan['code']);
        $this->assertStringContainsString('belongs to client #41', $plan['message']);
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

        $r = $this->confirmedRequote($service, 30, 7);
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
        $this->assertTrue($this->confirmedRequote($service, 30, 7)['ok']);
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
        $this->assertTrue($this->confirmedRequote($service, 30, 7)['ok']);
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

    // ── 1.1.6 B2: a re-quote replays the verified draft ─────────────────────

    /** Draft, invalidated, order invoice paid: the Ready situation. */
    private function readyToRequote(): PaidOrderSnapshotService
    {
        $this->draft();
        $this->invalidate();
        $this->payOwn();
        return new PaidOrderSnapshotService();
    }

    /** WHMCS edited after checkout: cheaper first payment and renewal, doubled quantity. */
    private function editLiveService(): void
    {
        Capsule::table('tblhosting')->where('id', 30)->update(['firstpaymentamount' => '1.00', 'amount' => '1.00']);
        Capsule::table('tblhostingconfigoptions')->where('relid', 30)->update(['qty' => 2]);
    }

    /** @return array<string,array<string,mixed>> */
    private function rows(array $plan): array
    {
        return array_column($plan['diff'], null, 'field');
    }

    public function testB2ReadyPathShowsTheDiffAndSealsExactlyTheRecordedQuote(): void
    {
        $service = $this->readyToRequote();
        $recorded = $this->payload(0);

        $plan = $service->requotePlan(30);
        $this->assertSame('ready', $plan['code']);
        $this->assertFalse($plan['needs_override']);
        $rows = $this->rows($plan);
        foreach (['Options', 'First payment', 'Recurring', 'Tax 1', 'Tax 2', 'Total due', 'Currency', 'Domain', 'Billing cycle', 'Product'] as $field) {
            $this->assertArrayHasKey($field, $rows, $field);
            $this->assertFalse($rows[$field]['live_differs'], $field);
        }
        $this->assertSame('1999.00', $rows['First payment']['old']);
        $this->assertSame('1999.00', $rows['First payment']['new']);
        $this->assertStringContainsString('Image=ubuntu-24-04 x1', $rows['Options']['old']);
        $this->assertStringContainsString('1999.00', $plan['message']);

        $r = $this->confirmedRequote($service, 30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $sealed = $this->payload(1);
        $this->assertSame($recorded['configuration'], $sealed['configuration']);
        $this->assertSame($recorded['pricing'], $sealed['pricing']);
        $this->assertSame('Pending', $recorded['whmcs']['order_status']);
        $this->assertSame('Active', $sealed['whmcs']['order_status'], 'only the order status is refreshed');
        unset($recorded['whmcs']['order_status'], $sealed['whmcs']['order_status']);
        $this->assertSame($recorded['whmcs'], $sealed['whmcs']);
    }

    public function testB2LiveServiceEditsAfterCheckoutAreNotSealedAndNeedAConfirmation(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();

        $plan = $service->requotePlan(30);
        $this->assertSame('needs_override', $plan['code']);
        $this->assertSame('recorded_deviation', $plan['override_kind']);
        $this->assertSame('recorded_mapping', $plan['source']);
        $rows = $this->rows($plan);
        $this->assertTrue($rows['First payment']['live_differs']);
        $this->assertSame('1999.00', $rows['First payment']['new'], 'what gets sealed is the recorded value');
        $this->assertSame('1.00', $rows['First payment']['live']);
        $this->assertTrue($rows['Recurring']['live_differs']);
        $this->assertTrue($rows['Options']['live_differs']);
        $this->assertStringContainsString('x2', $rows['Options']['live']);
        $this->assertFalse($rows['Domain']['live_differs']);
        $this->assertNotSame('', $plan['diff_token']);
        $this->assertStringContainsString('First payment recorded 1999.00, WHMCS now 1.00', $plan['message']);

        $refused = $service->requote(30, 7);
        $this->assertSame('needs_override', $refused['code']);
        $this->assertSame(['invalid'], $this->states(), 'nothing is sealed without the confirmation');

        $r = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);
        $sealed = $this->payload(1);
        $this->assertSame('1999.00', $sealed['pricing']['setup']);
        $this->assertSame('1999.00', $sealed['pricing']['recurring']);
        $this->assertSame(1, $sealed['configuration']['options'][0]['quantity']);
        $this->assertStringContainsString('CONFIRMED by admin #7', $this->log());
        $this->assertStringContainsString('WHMCS now 1.00', $this->log());
        $this->assertStringContainsString('taken from the verified recorded quote', $this->log());
    }

    public function testB2ALiveEditMadeAfterThePlanWasShownInvalidatesTheToken(): void
    {
        $service = $this->readyToRequote();
        Capsule::table('tblhosting')->where('id', 30)->update(['firstpaymentamount' => '1.00']);
        $token = $service->requotePlan(30)['diff_token'];
        Capsule::table('tblhosting')->where('id', 30)->update(['firstpaymentamount' => '2.00']);
        $this->assertSame('diff_changed', $service->requote(30, 7, true, $token)['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testB2AnInvoiceEditedAfterCheckoutIsShownAndNeverSealedAsTheTotal(): void
    {
        $service = $this->readyToRequote();
        Capsule::table('tblinvoices')->where('id', 20)->update(['subtotal' => '1.00', 'total' => '1.00', 'tax' => '0.50']);
        $plan = $service->requotePlan(30);
        $rows = $this->rows($plan);
        $this->assertSame('recorded_deviation', $plan['override_kind']);
        $this->assertTrue($rows['Total due']['live_differs']);
        $this->assertSame('1999.00', $rows['Total due']['new']);
        $this->assertTrue($rows['Tax 1']['live_differs']);
        $r = $service->requote(30, 7, true, $plan['diff_token']);
        if ($r['ok']) {
            $this->assertSame('1999.00', $this->payload(1)['whmcs']['total_due']);
            $this->assertSame('0.00', $this->payload(1)['pricing']['tax_1']);
        } else {
            $this->assertNotContains('sealed', $this->states(), 'a drifted invoice total still cannot seal');
        }
    }

    public function testB2AMergedAwayInvoiceShowsThePayingInvoiceNotAGoneOriginal(): void
    {
        $service = $this->readyToRequote();
        $this->deleteInvoice(20);
        $this->invoice(24, 'Paid', 40, [['Hosting', 30]]);
        $plan = $service->requotePlan(30);
        $rows = $this->rows($plan);
        $this->assertSame('merged', $plan['via']);
        $this->assertSame('1999.00', $rows['Total due']['live'], '"WHMCS now" is the invoice that paid');
        $this->assertFalse($rows['Total due']['live_differs']);
    }

    public function testTheWhmcsNowColumnReadsThePayingInvoiceOnAReInvoice(): void
    {
        $service = $this->readyToRequote();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled', 'total' => '0.00', 'subtotal' => '0.00']);
        $this->invoice(24, 'Paid', 40, [['Hosting', 30, '1999.00']], '2500.00');
        $plan = $service->requotePlan(30);
        $this->assertSame('reinvoice', $plan['via']);
        $rows = $this->rows($plan);
        $this->assertSame('2500.00', $rows['Total due']['live'], 'not the cancelled original (0.00)');
        $this->assertTrue($rows['Total due']['live_differs']);
        $this->assertSame('recorded_deviation', $plan['override_kind']);
    }

    public function testTheWhmcsNowColumnReadsTheMassPayLineNotTheOriginal(): void
    {
        $service = $this->readyToRequote();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled', 'total' => '0.00', 'subtotal' => '0.00']);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20, '1999.00']], '1999.00');
        $plan = $service->requotePlan(30);
        $this->assertSame('mass_pay', $plan['via']);
        $rows = $this->rows($plan);
        $this->assertSame('1999.00', $rows['Total due']['live']);
        $this->assertFalse($rows['Total due']['live_differs']);
    }

    public function testB2UnreadableLiveOptionsAreADeviationButTheRecordedOptionsStillSeal(): void
    {
        $service = $this->readyToRequote();
        Capsule::$tables['mod_contabo_config_option_value_link'] = [];
        Capsule::$tables['mod_contabo_config_option_link'] = [];
        $plan = $service->requotePlan(30);
        $rows = $this->rows($plan);
        $this->assertTrue($rows['Options']['live_differs']);
        $this->assertStringStartsWith('(unreadable:', $rows['Options']['live']);
        $r = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('ubuntu-24-04', $this->payload(1)['configuration']['options'][0]['machine_code']);
    }

    public function testB2TheRecordedSnapshotKindAlsoReplaysTheRecordedQuoteAndShowsLiveDeviations(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();
        Capsule::$tables['mod_contabo_mapping_publications'] = [];
        $plan = $service->requotePlan(30);
        $this->assertSame('recorded_snapshot', $plan['override_kind']);
        $this->assertTrue($this->rows($plan)['First payment']['live_differs']);
        $this->assertStringContainsString('First payment recorded 1999.00, WHMCS now 1.00', $plan['message']);
        $r = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame('1999.00', $this->payload(1)['pricing']['setup']);
    }

    public function testB2TheCurrentMappingOverrideShowsAndBindsTheLiveQuoteFields(): void
    {
        $this->payOwn();
        $service = new PaidOrderSnapshotService();
        $plan = $service->requotePlan(30);
        $this->assertSame('current_mapping', $plan['override_kind']);
        $rows = $this->rows($plan);
        $this->assertSame('(not recorded)', $rows['First payment']['old']);
        $this->assertSame('1999.00', $rows['First payment']['new']);
        $this->assertTrue($rows['First payment']['changed']);
        Capsule::table('tblhosting')->where('id', 30)->update(['firstpaymentamount' => '5.00']);
        $this->assertSame('diff_changed', $service->requote(30, 7, true, $plan['diff_token'])['code']);
        $this->assertSame([], $this->snapshots());
    }

    // ── 1.1.6: keyed confirmation token ──────────────────────────────────────

    public function testTheConfirmationTokenIsAnHmacKeyedFromTheWhmcsEncryptionKey(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();
        $plan = $service->requotePlan(30);
        $token = $plan['diff_token'];
        $this->assertMatchesRegularExpression('/^[0-9a-f]{64}$/', $token);
        $this->assertSame($token, $service->requotePlan(30)['diff_token'], 'deterministic for the same state');
        // not the unkeyed sha256 of the shown content
        $this->assertNotSame(hash('sha256', json_encode($plan['diff'])), $token);

        $GLOBALS['cc_encryption_hash'] = 'a-different-install-key';
        $other = $service->requotePlan(30)['diff_token'];
        $this->assertNotSame($token, $other);
        $this->assertSame('diff_changed', $service->requote(30, 7, true, $token)['code'], 'a token minted without the key is worthless');
        $this->assertSame(['invalid'], $this->states());
    }

    public function testWithoutTheEncryptionKeyNoConfirmationIsIssuedAndNothingSeals(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();
        unset($GLOBALS['cc_encryption_hash']);
        $plan = $service->requotePlan(30);
        $this->assertSame('token_unavailable', $plan['code']);
        $this->assertFalse($plan['ready']);
        $this->assertSame('', $plan['diff_token']);
        $r = $service->requote(30, 7, true, '');
        $this->assertFalse($r['ok']);
        $this->assertSame('token_unavailable', $r['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testTheReadyPathNeedsTheKeyToo(): void
    {
        $service = $this->readyToRequote();
        $this->assertSame('ready', $service->requotePlan(30)['code']);
        unset($GLOBALS['cc_encryption_hash']);
        $plan = $service->requotePlan(30);
        $this->assertSame('token_unavailable', $plan['code']);
        $r = $service->requote(30, 7, true, '');
        $this->assertFalse($r['ok']);
        $this->assertSame('token_unavailable', $r['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testTheTokenCoversTheRecordedRowItReplays(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();
        $token = $service->requotePlan(30)['diff_token'];
        $this->mutatePayload(static function (array &$p): void {
            $p['labels'] = ['changed' => 'x'];
        });
        $after = $service->requotePlan(30);
        $this->assertNotSame($token, $after['diff_token'] ?? '');
    }

    // ── 1.1.6: only a permitted admin may confirm ───────────────────────────

    public function testAnAdminWithoutTheRoleCannotConfirmAnOverrideOrAConfirmation(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();
        $token = $service->requotePlan(30)['diff_token'];
        foreach ([8, 9, 99, 0] as $adminId) {
            $r = $service->requote(30, $adminId, true, $token);
            $this->assertFalse($r['ok'], 'admin ' . $adminId);
            $this->assertSame('not_permitted', $r['code'], 'admin ' . $adminId);
        }
        $this->assertSame(['invalid'], $this->states());
        $this->assertStringContainsString('confirmation REFUSED for service #30: admin #8', $this->log());
        $this->assertTrue($service->requote(30, 7, true, $token)['ok']);
    }

    public function testTheOverrideToTheCurrentMappingIsAlsoRoleGated(): void
    {
        $this->payOwn();
        $service = new PaidOrderSnapshotService();
        $token = $service->requotePlan(30)['diff_token'];
        $this->assertSame('not_permitted', $service->requote(30, 8, true, $token)['code']);
        $this->assertSame([], $this->snapshots());
        $this->assertTrue($service->requote(30, 7, true, $token)['ok']);
    }

    public function testTheRoleListIsConfigurableAndReplacesTheDefault(): void
    {
        $service = $this->readyToRequote();
        $this->editLiveService();
        $token = $service->requotePlan(30)['diff_token'];
        Capsule::$tables['mod_contabo_settings'] = [['key' => 'snapshot.requote_role_ids', 'value' => '3, 4']];
        $this->assertFalse($service->adminMayConfirm(7));
        $this->assertTrue($service->adminMayConfirm(8));
        $this->assertFalse($service->adminMayConfirm(9), 'a disabled admin never may');
        $this->assertSame('not_permitted', $service->requote(30, 7, true, $token)['code']);
        $this->assertTrue($service->requote(30, 8, true, $token)['ok']);
    }

    public function testTheRoleGateAppliesToTheReadyPathToo(): void
    {
        $service = $this->readyToRequote();
        $token = $service->requotePlan(30)['diff_token'];
        $this->assertNotSame('', $token);
        $this->assertSame('not_permitted', $service->requote(30, 8, true, $token)['code']);
        $this->assertSame(['invalid'], $this->states());
        $this->assertTrue($service->requote(30, 7, true, $token)['ok']);
    }

    /** @return array<string,array{0:string}> */
    public static function malformedRoleSettings(): array
    {
        return [
            'letters' => ['abc'],
            'mixed list' => ['3, x, 0'],
            'zero only' => ['0'],
            'negative' => ['-1'],
            'only commas' => [',,'],
            'decimal' => ['1.5'],
        ];
    }

    /** @dataProvider malformedRoleSettings */
    public function testAMalformedRoleSettingFallsBackToFullAdministratorAndWarns(string $value): void
    {
        Capsule::$tables['mod_contabo_settings'] = [['key' => 'snapshot.requote_role_ids', 'value' => $value]];
        $service = new PaidOrderSnapshotService();
        $this->assertTrue($service->adminMayConfirm(7), 'role 1 must never be locked out');
        $this->assertFalse($service->adminMayConfirm(8));
        $this->assertFalse($service->adminMayConfirm(9));
        $this->assertStringContainsString('WARNING: Contabo setting snapshot.requote_role_ids is malformed', $this->log());
    }

    public function testAWellFormedRoleSettingDoesNotWarn(): void
    {
        Capsule::$tables['mod_contabo_settings'] = [['key' => 'snapshot.requote_role_ids', 'value' => ' 1 , 3 ']];
        $service = new PaidOrderSnapshotService();
        $this->assertTrue($service->adminMayConfirm(7));
        $this->assertTrue($service->adminMayConfirm(8));
        $this->assertStringNotContainsString('malformed', $this->log());
    }

    // ── 1.1.6: edge cases from the 1.1.5 review ─────────────────────────────

    public function testALaterRetryDraftDoesNotMoveTheCheckpointTheRecordedMappingIsJudgedAgainst(): void
    {
        $this->draft();
        // The checkout draft was made on 2026-01-01, while mapping-v1 was current.
        Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid', 'created_at' => '2026-01-01 00:00:00']);
        $this->republish();
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update(['effective_at' => '2025-12-01 00:00:00']);
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v2')->update([
            'effective_at' => '2026-02-01 00:00:00', 'supersedes_mapping_version' => 'mapping-v1',
        ]);
        $this->hashPublications();
        // A failed re-quote left a newer row that still records mapping-v1.
        $retry = $this->snapshots()[0];
        $retry['id'] = 2;
        $retry['snapshot_uuid'] = 'retry-uuid';
        $retry['created_at'] = '2026-03-01 00:00:00';
        Capsule::$tables['mod_securiacevps_order_snapshots'][] = $retry;
        $this->payOwn();

        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('recorded_mapping', $plan['source'], $plan['message']);
        $this->assertFalse($plan['needs_override']);
    }

    // ── credit is already netted out of tblinvoices.total ────────────────────

    public function testB1MassPayMustCoverTheQuotedTotalDueEvenWhenCreditWasAppliedAtCheckout(): void
    {
        Capsule::table('tblinvoices')->where('id', 20)->update(['credit' => '500.00']);
        $this->draft();
        $this->assertSame('500.00', $this->payload(0)['whmcs']['credit']);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20, '1499.00']], '1499.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states(), 'the quoted total is already net of the credit; it is not subtracted twice');
        $this->assertStringContainsString('shortfall', $this->log());
        $this->invoice(22, 'Paid', 40, [['Invoice', 20, '1999.00']], '1999.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 22]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testB2CreditAppliedAfterCheckoutIsNotADeviationButAChangedGrossIs(): void
    {
        $service = $this->readyToRequote();
        // Mass Pay / "apply credit": the money moves from total to credit.
        Capsule::table('tblinvoices')->where('id', 20)->update(['total' => '0.00', 'credit' => '1999.00']);
        $plan = $service->requotePlan(30);
        $this->assertSame('ready', $plan['code']);
        $this->assertSame([], (array) ($plan['deviations'] ?? []));
        $this->assertTrue($this->confirmedRequote($service, 30, 7)['ok']);
    }

    public function testB2ATotalThatReallyChangedStillDeviates(): void
    {
        $service = $this->readyToRequote();
        Capsule::table('tblinvoices')->where('id', 20)->update(['total' => '1500.00', 'credit' => '0.00']);
        $plan = $service->requotePlan(30);
        $this->assertNotSame([], $plan['deviations']);
        $this->assertStringContainsString('Total due', implode('|', $plan['deviations']));
    }

    public function testTheGrandfatherPathComparesNetAgainstNet(): void
    {
        Capsule::table('mod_contabo_settings')->updateOrInsert(
            ['key' => ProvisioningGate::SETTING_CUTOFF],
            ['value' => '2026-11-01 00:00:00']
        );
        Capsule::table('tblorders')->where('id', 10)->update(['date' => '2026-10-20 10:00:00']);
        // 500.00 of credit was applied: the invoice's total is the 1499.00 balance, and a
        // Mass Pay line equals that balance, so a legitimate Mass Pay must be accepted.
        Capsule::table('tblinvoices')->where('id', 20)->update(['total' => '1499.00', 'credit' => '500.00']);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20, '1000.00']], '1000.00');
        $this->assertSame(['abortcmd' => true], $this->create(), 'a Mass Pay line below the 1499.00 balance is a shortfall');
        $this->invoice(22, 'Paid', 40, [['Invoice', 20, '1499.00']], '1499.00');
        $this->assertNotSame(['abortcmd' => true], $this->create(), 'a Mass Pay line equal to the net balance settles it');
    }

    // ── 1.1.7 ────────────────────────────────────────────────────────────────

    private function moveServiceToAnotherNativeProduct(): void
    {
        Capsule::$tables['tblproducts'][] = ['id' => 51, 'gid' => 60, 'servertype' => 'securiacevps'];
        Capsule::table('tblhosting')->where('id', 30)->update(['packageid' => 51]);
    }

    public function testTheRecordedSnapshotPathRefusesAServiceMovedToAnotherProduct(): void
    {
        $service = $this->readyToRequote();
        $this->dropPublication('mapping-v1');
        $this->assertSame('recorded_snapshot', $service->requotePlan(30)['override_kind'], 'precondition: the publication is gone');
        $this->moveServiceToAnotherNativeProduct();

        $plan = $service->requotePlan(30);
        $this->assertSame('product_changed', $plan['code']);
        $this->assertStringContainsString('product #50', $plan['message']);
        $this->assertStringContainsString('product #51', $plan['message']);
        foreach ([[false, ''], [true, ''], [true, (string) ($plan['diff_token'] ?? '')]] as [$override, $token]) {
            $r = $service->requote(30, 7, $override, $token);
            $this->assertFalse($r['ok']);
            $this->assertSame('product_changed', $r['code']);
        }
        $this->assertSame(['invalid'], $this->states(), 'nothing was sealed under the live product id');
        $this->assertSame(50, (int) $this->snapshots()[0]['product_id']);
    }

    public function testTheRecordedMappingPathRefusesAServiceMovedToAnotherProduct(): void
    {
        $service = $this->readyToRequote();
        $this->moveServiceToAnotherNativeProduct();
        $plan = $service->requotePlan(30);
        $this->assertSame('product_changed', $plan['code']);
        $r = $service->requote(30, 7, true, (string) ($plan['diff_token'] ?? ''));
        $this->assertSame('product_changed', $r['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testMovingTheServiceBackMakesItRecoverableAgain(): void
    {
        $service = $this->readyToRequote();
        $this->moveServiceToAnotherNativeProduct();
        $this->assertSame('product_changed', $service->requotePlan(30)['code']);
        Capsule::table('tblhosting')->where('id', 30)->update(['packageid' => 50]);
        $r = $this->confirmedRequote($service, 30, 7);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame(['superseded', 'sealed'], $this->states());
    }

    public function testCreateDraftNeverSealsARecordedQuoteUnderADifferentLiveProduct(): void
    {
        $this->draft();
        $this->invalidate();
        $row = $this->snapshots()[0];
        $method = new \ReflectionMethod(PaidOrderSnapshotService::class, 'recordedSource');
        $method->setAccessible(true);
        $why = '';
        $service = new PaidOrderSnapshotService();
        $recorded = $method->invokeArgs($service, [$row, &$why]);
        $this->assertIsArray($recorded);
        $this->moveServiceToAnotherNativeProduct();
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('recorded quote is for product #50');
        $service->createDraft(10, 20, 30, null, $recorded);
    }

    // PromoHosting pin: a promotion line reduces what a re-issued invoice bills for the service.

    public function testB1APromoLineThatTakesTheReissuedInvoiceBelowTheQuoteIsAShortfall(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        // Hosting alone covers 1999.00, but the promotion takes 500.00 off: the invoice bills 1499.00.
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00'], ['PromoHosting', 30, '-500.00']], '1499.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['draft'], $this->states(), 'PromoHosting must be counted: 1999.00 - 500.00 < the quoted 1999.00');
        $this->assertStringContainsString('shortfall 500.00', $this->log());
    }

    public function testB1APromoLineAloneNeverMakesAReissue(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['PromoHosting', 30, '1999.00']], '1999.00');
        // Called directly: with no Hosting/Setup/Invoice line the hook cannot even find the order.
        $this->assertSame(0, (new PaidOrderSnapshotService())->sealOrder(10, 23));
        $this->assertSame(['draft'], $this->states());
    }

    public function testB1APromoLineCountsForTheAdminPlanToo(): void
    {
        $service = $this->readyToRequote();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00'], ['PromoHosting', 30, '-500.00']], '1499.00');
        $plan = $service->requotePlan(30);
        $this->assertNotSame('reinvoice', $plan['via']);
        $this->assertSame('paying_invoice_refused', $plan['code']);
    }

    // Manual "Re-quote service" box: nothing is sealed without the shown diff.

    public function testTheManualBoxNeverSealsAReadyPlanWithoutTheDiffConfirmation(): void
    {
        $service = $this->readyToRequote();
        $plan = $service->requotePlan(30);
        $this->assertSame('ready', $plan['code']);
        $this->assertNotSame('', $plan['diff_token']);
        $this->assertSame('recorded_mapping', $plan['override_kind']);

        $bare = $service->requote(30, 7);
        $this->assertFalse($bare['ok']);
        $this->assertSame('needs_override', $bare['code']);
        $this->assertSame(['invalid'], $this->states());

        $this->assertSame('diff_changed', $service->requote(30, 7, true, '')['code']);
        $this->assertSame('diff_changed', $service->requote(30, 7, true, 'forged')['code']);
        $this->assertSame(['invalid'], $this->states());

        $ok = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($ok['ok'], $ok['message']);
        $this->assertSame(['superseded', 'sealed'], $this->states());
        $this->assertStringContainsString('re-quote CONFIRMED by admin #7', $this->log());
    }

    public function testAReadyConfirmationGoesStaleWhenWhmcsChangesUnderIt(): void
    {
        $service = $this->readyToRequote();
        $token = $service->requotePlan(30)['diff_token'];
        $this->editLiveService();
        $this->assertSame('diff_changed', $service->requote(30, 7, true, $token)['code']);
        $this->assertSame(['invalid'], $this->states());
    }

    public function testTheControllerSendsAnUnconfirmedManualRequoteToThePreview(): void
    {
        $code = (string) file_get_contents(__DIR__ . '/../lib/AdminController.php');
        $start = strpos($code, 'private function maintenanceRequote');
        $this->assertNotFalse($start);
        $body = substr($code, (int) $start, 1400);
        $this->assertStringContainsString("['needs_override', 'diff_changed', 'not_permitted']", $body);
        $this->assertStringContainsString("'preview_service'", $body);
    }

    // Currency and tax-exempt re-issues.

    public function testAReissueInAnotherCurrencyIsRefused(): void
    {
        $this->draft();
        Capsule::$tables['tblcurrencies'][] = ['id' => 2, 'code' => 'USD'];
        Capsule::table('tblclients')->where('id', 40)->update(['currency' => 2]);
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00']], '1999.00');
        (new PaidOrderSnapshotService())->sealOrder(10, 23);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('billed in USD but the quote was made in INR', $this->log());
    }

    public function testAMassPayInAnotherCurrencyIsRefused(): void
    {
        $this->draft();
        Capsule::$tables['tblcurrencies'][] = ['id' => 2, 'code' => 'USD'];
        Capsule::table('tblclients')->where('id', 40)->update(['currency' => 2]);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('billed in USD but the quote was made in INR', $this->log());
    }

    public function testAnUnknownInvoiceCurrencyIsRefusedNotAssumedEqual(): void
    {
        $this->draft();
        Capsule::table('tblclients')->where('id', 40)->update(['currency' => 99]);
        $this->invoice(21, 'Paid', 40, [['Invoice', 20]]);
        ($this->hook('InvoicePaid'))(['invoiceid' => 21]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('(an unknown currency)', $this->log());
    }

    public function testTheCurrencyCheckDoesNotBlockTheOrdersOwnInvoice(): void
    {
        $this->draft();
        Capsule::$tables['tblcurrencies'][] = ['id' => 2, 'code' => 'USD'];
        Capsule::table('tblclients')->where('id', 40)->update(['currency' => 2]);
        $this->payOwn();
        ($this->hook('InvoicePaid'))(['invoiceid' => 20]);
        $this->assertSame(['sealed'], $this->states(), 'the quoted invoice itself is the quote, whatever the account currency is now');
    }

    /** The order's invoice carried 180.00 tax (total 2179.00); the re-issue is tax-exempt (1999.00). */
    private function taxedQuoteThenTaxExemptReissue(): PaidOrderSnapshotService
    {
        Capsule::table('tblinvoices')->where('id', 20)->update(['subtotal' => '1999.00', 'tax' => '180.00', 'total' => '2179.00']);
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00']], '1999.00');
        return new PaidOrderSnapshotService();
    }

    public function testATaxExemptReissueNeverSettlesATaxedQuoteAutomatically(): void
    {
        $service = $this->taxedQuoteThenTaxExemptReissue();
        $this->assertSame(0, $service->sealOrder(10, 23));
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('totals 1999.00 but the quote was 2179.00', $this->log());
        $this->assertSame(['abortcmd' => true], $this->create());
    }

    public function testATaxExemptReissueShowsAsADeviationAndNeedsAnAdminConfirmation(): void
    {
        $service = $this->taxedQuoteThenTaxExemptReissue();
        $plan = $service->requotePlan(30);
        $this->assertSame('reinvoice', $plan['via']);
        $this->assertSame('needs_override', $plan['code']);
        $this->assertSame('recorded_deviation', $plan['override_kind']);
        $rows = $this->rows($plan);
        $this->assertSame('2179.00', $rows['Total due']['old']);
        $this->assertSame('1999.00', $rows['Total due']['live']);
        $this->assertTrue($rows['Total due']['live_differs']);
        $this->assertStringContainsString('Total due', implode('|', $plan['deviations']));

        $this->assertSame('needs_override', $service->requote(30, 7)['code']);
        $this->assertSame(['draft'], $this->states());
        $r = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);
        $this->assertSame(['superseded', 'sealed'], $this->states());
        $this->assertSame('2179.00', $this->payload(1)['whmcs']['total_due'], 'the sealed contract is still what the customer was quoted');
    }

    public function testAHigherReissueTotalIsNotAShortfall(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00'], ['Hosting', 31, '500.00']], '2499.00');
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['sealed'], $this->states());
    }

    public function testAReissueWithoutTheCreditThatTheQuoteHadIsAShortfall(): void
    {
        // The quoted invoice was 1499.00 net after 500.00 credit (1999.00 gross).
        Capsule::table('tblinvoices')->where('id', 20)->update(['total' => '1499.00', 'credit' => '500.00']);
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00']], '1499.00');
        $this->assertSame(0, (new PaidOrderSnapshotService())->sealOrder(10, 23));
        $this->assertStringContainsString('totals 1499.00 but the quote was 1999.00', $this->log());
    }

    public function testACreditAppliedToTheReissueStillCountsTowardsTheGrossTotal(): void
    {
        $this->draft();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00']], '1499.00');
        Capsule::table('tblinvoices')->where('id', 23)->update(['credit' => '500.00']);
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['sealed'], $this->states(), 'gross = total + credit = 1999.00');
    }

    private function secondServiceOnTheSameOrder(): void
    {
        Capsule::$tables['tblhosting'][] = [
            'id' => 31, 'orderid' => 10, 'userid' => 40, 'packageid' => 50, 'billingcycle' => 'Monthly',
            'domain' => 'two.example.com', 'firstpaymentamount' => '1999.00', 'amount' => '1999.00',
        ];
        Capsule::$tables['tblhostingconfigoptions'][] = ['relid' => 31, 'configid' => 100, 'optionid' => 101, 'qty' => 1];
    }

    public function testConfirmingOneServiceNeverSealsAnotherServicesGrossShortfallOnTheSameOrder(): void
    {
        $this->secondServiceOnTheSameOrder();
        Capsule::table('tblinvoices')->where('id', 20)->update(['subtotal' => '3998.00', 'tax' => '360.00', 'total' => '4358.00']);
        $service = new PaidOrderSnapshotService();
        $service->createDraft(10, 20, 30);
        $service->createDraft(10, 20, 31);
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00'], ['Hosting', 31, '1999.00']], '3998.00');

        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['draft', 'draft'], $this->states(), 'precondition: the automatic path refuses both');

        $plan = $service->requotePlan(30);
        $this->assertSame('recorded_deviation', $plan['override_kind']);
        $r = $service->requote(30, 7, true, $plan['diff_token']);
        $this->assertTrue($r['ok'], $r['message']);

        $byService = [];
        foreach ($this->snapshots() as $row) {
            $byService[(int) $row['service_id']][] = $row['state'];
        }
        $this->assertContains('sealed', $byService[30]);
        $this->assertSame(['draft'], $byService[31], 'service #31 was not confirmed; its shortfall must stay refused');
    }

    public function testWithoutAVerifiedQuoteTheGrossRefusalDoesNotPointAtARecoveryThatRefuses(): void
    {
        Capsule::table('mod_contabo_settings')->updateOrInsert(
            ['key' => ProvisioningGate::SETTING_CUTOFF],
            ['value' => '2026-11-01 00:00:00']
        );
        Capsule::table('tblorders')->where('id', 10)->update(['date' => '2026-10-20 10:00:00']);
        Capsule::table('tblinvoices')->where('id', 20)->update(['subtotal' => '1999.00', 'tax' => '180.00', 'total' => '2179.00', 'status' => 'Cancelled']);
        $this->invoice(23, 'Paid', 40, [['Hosting', 30, '1999.00']], '1999.00');

        $this->assertSame(['abortcmd' => true], $this->create());
        $this->assertStringContainsString('totals 1999.00 but the quote was 2179.00', $this->log());
        $this->assertStringContainsString('no verified recorded quote', $this->log());
        $this->assertStringNotContainsString('Re-quote service #30 from Maintenance', $this->log());
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertFalse($plan['ready'], 'the re-quote really does refuse here, as the message says');
    }

    public function testACombinedInvoiceDoesNotShowFalseTaxOrSubtotalDeviations(): void
    {
        Capsule::table('tblinvoices')->where('id', 20)->update(['subtotal' => '1999.00', 'tax' => '180.00', 'total' => '2179.00']);
        $this->draft();
        $this->invalidate();
        Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Cancelled']);
        // Mass Pay of this order's invoice plus another one: tax and subtotal are the combined invoice's.
        $this->invoice(21, 'Paid', 40, [['Invoice', 20, '2179.00'], ['Invoice', 99, '500.00']], '2679.00');
        $plan = (new PaidOrderSnapshotService())->requotePlan(30);
        $this->assertSame('mass_pay', $plan['via']);
        $rows = $this->rows($plan);
        foreach (['Tax 1', 'Tax 2', 'Subtotal'] as $field) {
            $this->assertFalse($rows[$field]['live_differs'], $field);
            $this->assertStringContainsString('not itemised', $rows[$field]['live'], $field);
        }
        $this->assertSame([], $plan['deviations'], 'a legitimate Mass Pay of a taxed quote is not a deviation');
        $this->assertSame('ready', $plan['code']);
    }

    public function testAVerifiedDraftWhoseRecordedMappingIsRejectedStaysStrictOnTheGrossCheck(): void
    {
        $service = $this->taxedQuoteThenTaxExemptReissue();
        // The publication the draft recorded now belongs to another product, so no recorded
        // source can be replayed and the re-quote can only be the current-mapping override.
        Capsule::table('mod_contabo_mapping_publications')->where('mapping_version', 'mapping-v1')->update(['product_id' => 51]);
        $this->hashPublications();
        ($this->hook('InvoicePaid'))(['invoiceid' => 23]);
        $this->assertSame(['draft'], $this->states());
        $this->assertStringContainsString('totals 1999.00 but the quote was 2179.00', $this->log());
        $this->assertStringContainsString('no verified recorded quote', $this->log(), 'no recovery is promised that refuses');
        $this->assertStringNotContainsString('Re-quote service #30 from Maintenance', $this->log());

        $note = $service->unsealedNote(30);
        $this->assertStringContainsString('no verified recorded quote', $note, 'the gate note promises no recovery that refuses');
        $this->assertStringNotContainsString('Re-quote service #30 from Maintenance', $note);

        $plan = $service->requotePlan(30);
        $this->assertFalse($plan['ready'], 'the current-mapping override cannot settle a smaller invoice');
        $this->assertSame('paying_invoice_refused', $plan['code']);
        $this->assertFalse($service->requote(30, 7, true, (string) ($plan['diff_token'] ?? 'x'))['ok']);
        $this->assertNotContains('sealed', $this->states());
    }
}
