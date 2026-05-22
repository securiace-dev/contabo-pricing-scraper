<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Notifier;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Verifies Notifier::upsertNotice() idempotency semantics.
 *
 * Plan reference:
 *   - Deliverable 8 (Notification workflow — durable + idempotent)
 *   - Deliverable 10, acceptance criterion 7:
 *     "Notice idempotency: calling upsertNotice() 10 times with the same args
 *      creates ONE row."
 *
 * The idempotency_key formula is documented in Notifier::idempotencyKey():
 *   sha1( serviceId . '|' . number_format(targetPrice,4,'.','') . '|'
 *         . effectiveAt(c) . '|' . noticeType )
 */
final class NoticeIdempotencyTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testUpsertSameInputsCreatesOneRow(): void
    {
        $n = new Notifier(false); // Phase A — gating does not affect upsertNotice
        $effective = new \DateTimeImmutable('2026-07-01T00:00:00+00:00');

        $results = [];
        for ($i = 0; $i < 10; $i++) {
            $results[] = $n->upsertNotice(
                42,
                1234.5678,
                'INR',
                $effective,
                30,
                'pre_change',
                'Contabo Pricing Change Notice',
                ['service_name' => 'VPS S']
            );
        }

        $noticeInserts = array_values(array_filter(
            Capsule::$inserts,
            static fn ($call) => $call['table'] === 'mod_contabo_price_notice'
        ));
        $this->assertCount(1, $noticeInserts, 'exactly one INSERT into mod_contabo_price_notice');

        // All 10 returned rows reference the same idempotency_key.
        $key0 = $results[0]['idempotency_key'];
        foreach ($results as $row) {
            $this->assertSame($key0, $row['idempotency_key']);
        }
    }

    public function testUpsertDifferentInputsCreatesNewRow(): void
    {
        $n = new Notifier(false);
        $effective = new \DateTimeImmutable('2026-07-01T00:00:00+00:00');

        $a = $n->upsertNotice(42, 1000.00, 'INR', $effective, 30, 'pre_change', 'Notice', []);
        $b = $n->upsertNotice(42, 1500.00, 'INR', $effective, 30, 'pre_change', 'Notice', []);

        $noticeInserts = array_values(array_filter(
            Capsule::$inserts,
            static fn ($call) => $call['table'] === 'mod_contabo_price_notice'
        ));
        $this->assertCount(2, $noticeInserts, 'two distinct target_price values → two rows');

        $this->assertNotSame($a['idempotency_key'], $b['idempotency_key']);
    }

    public function testIdempotencyKeyFormat(): void
    {
        $effective = new \DateTimeImmutable('2026-07-01T00:00:00+00:00');
        $key = Notifier::idempotencyKey(42, 1234.5678, $effective, 'pre_change');

        $expected = sha1(implode('|', [
            '42',
            '1234.5678',
            $effective->format('c'),
            'pre_change',
        ]));

        $this->assertSame($expected, $key);
        $this->assertSame(40, strlen($key), 'sha1 hex is 40 chars');
    }

    public function testIdempotencyKeyNormalisesFloatRepresentations(): void
    {
        $effective = new \DateTimeImmutable('2026-07-01T00:00:00+00:00');
        $a = Notifier::idempotencyKey(7, 1000.0, $effective, 'pre_change');
        $b = Notifier::idempotencyKey(7, 1000.00000001, $effective, 'pre_change');

        // Both collapse to "1000.0000" under number_format($v, 4, '.', '').
        $this->assertSame($a, $b);
    }
}
