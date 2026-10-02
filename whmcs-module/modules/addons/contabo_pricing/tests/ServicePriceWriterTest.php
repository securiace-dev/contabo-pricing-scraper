<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ServicePriceWriter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

require_once __DIR__ . '/_localapi_stub.php';

/**
 * ServicePriceWriter: LocalAPI-only, fail-closed (H2). The UpdateClientProduct
 * payload uses the `recurringamount` API field; any non-success result throws
 * and NEVER falls back to a raw tblhosting write.
 */
final class ServicePriceWriterTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        $GLOBALS['__cp_localapi_calls'] = [];
        unset($GLOBALS['__cp_localapi_response']);
    }

    protected function tearDown(): void
    {
        $GLOBALS['__cp_localapi_calls'] = [];
        unset($GLOBALS['__cp_localapi_response']);
    }

    public function testDisabledInPhaseAWritesNothing(): void
    {
        $r = (new ServicePriceWriter(false))->updateRecurringAmount(7, 99.0, 'policy', 1);
        $this->assertFalse($r['applied']);
        $this->assertSame('writer_disabled_phase_a', $r['via']);
        $this->assertSame([], $GLOBALS['__cp_localapi_calls']);
    }

    public function testLocalApiPayloadUsesRecurringamountParam(): void
    {
        $GLOBALS['__cp_localapi_response'] = ['result' => 'success'];

        $via = (new ServicePriceWriter(true))->writeViaLocalApiOrFallback(7, 123.45);

        $this->assertSame('localapi_updateclientproduct', $via['via']);
        $this->assertCount(1, $GLOBALS['__cp_localapi_calls']);
        $call = $GLOBALS['__cp_localapi_calls'][0];
        $this->assertSame('UpdateClientProduct', $call['command']);
        $this->assertArrayHasKey('recurringamount', $call['values']); // correct API field
        $this->assertSame('123.4500', $call['values']['recurringamount']);

        // Success path → NO raw tblhosting update happened.
        $tblhosting = array_filter(Capsule::$calls, static function ($c) {
            return ($c['table'] ?? '') === 'tblhosting' && isset($c['update']);
        });
        $this->assertSame([], array_values($tblhosting));
    }

    private function tblhostingUpdates(): array
    {
        return array_values(array_filter(Capsule::$calls, static function ($c) {
            return ($c['table'] ?? '') === 'tblhosting' && isset($c['update']);
        }));
    }

    public function testNonSuccessFailsClosedWithoutRawWrite(): void
    {
        $GLOBALS['__cp_localapi_response'] = ['result' => 'error', 'message' => 'boom'];
        try {
            (new ServicePriceWriter(true))->writeViaLocalApiOrFallback(7, 50.0);
            $this->fail('expected RuntimeException');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('boom', $e->getMessage());
        }
        $this->assertSame([], $this->tblhostingUpdates());
    }

    public function testMissingResultKeyIsFailure(): void
    {
        $GLOBALS['__cp_localapi_response'] = ['message' => 'weird'];
        $this->expectException(\RuntimeException::class);
        try {
            (new ServicePriceWriter(true))->writeViaLocalApiOrFallback(7, 50.0);
        } finally {
            $this->assertSame([], $this->tblhostingUpdates());
        }
    }

    public function testNonArrayLocalApiResponseFailsClosed(): void
    {
        $GLOBALS['__cp_localapi_response'] = 'not-an-array';
        $this->expectException(\RuntimeException::class);
        try {
            (new ServicePriceWriter(true))->writeViaLocalApiOrFallback(7, 50.0);
        } finally {
            $this->assertSame([], $this->tblhostingUpdates());
        }
    }

    public function testFailureInsideUpdateWritesNoActionLedgerRow(): void
    {
        $GLOBALS['__cp_localapi_response'] = ['result' => 'error', 'message' => 'denied'];
        try {
            (new ServicePriceWriter(true))->updateRecurringAmount(7, 50.0, 'policy', 1);
            $this->fail('expected RuntimeException');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('denied', $e->getMessage());
        }
        $this->assertSame([], Capsule::$inserts);
        $this->assertSame([], $this->tblhostingUpdates());
    }

    public function testExactWireStringOfWrittenPrice(): void
    {
        // Mutation survivor: pin number_format(..., 4, '.', '') — rounding,
        // 4 decimals, '.' decimal point, no thousands separator.
        $GLOBALS['__cp_localapi_response'] = ['result' => 'success'];
        $w = new ServicePriceWriter(true);
        $w->writeViaLocalApiOrFallback(7, 12345.678949);
        $w->writeViaLocalApiOrFallback(7, 0.5);
        $this->assertSame('12345.6789', $GLOBALS['__cp_localapi_calls'][0]['values']['recurringamount']);
        $this->assertSame('0.5000', $GLOBALS['__cp_localapi_calls'][1]['values']['recurringamount']);
        $this->assertTrue($GLOBALS['__cp_localapi_calls'][1]['values']['noemail']);
    }
}
