<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ServicePriceWriter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

require_once __DIR__ . '/_localapi_stub.php';

/**
 * 0.5.1 parity — ServicePriceWriter LocalAPI-vs-raw-column distinction.
 *
 * The LocalAPI UpdateClientProduct payload MUST use `recurringamount` (the API
 * field name — correct). The raw Capsule fallback MUST write `amount` (the real
 * column). Conflating the two is the exact trap a sed-style edit would fall into.
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

    public function testRawFallbackWritesAmountColumn(): void
    {
        // LocalAPI returns non-success → fall back to the raw update, which must
        // write the REAL `amount` column, NOT `recurringamount`.
        $GLOBALS['__cp_localapi_response'] = ['result' => 'error', 'message' => 'boom'];

        $via = (new ServicePriceWriter(true))->writeViaLocalApiOrFallback(7, 50.0);

        $this->assertStringContainsString('raw_fallback', $via['via']);
        // The LocalAPI attempt still used the recurringamount API field.
        $this->assertArrayHasKey('recurringamount', $GLOBALS['__cp_localapi_calls'][0]['values']);

        // The raw fallback wrote `amount` (and never recurringamount).
        $updates = array_values(array_filter(Capsule::$calls, static function ($c) {
            return ($c['table'] ?? '') === 'tblhosting' && isset($c['update']);
        }));
        $this->assertCount(1, $updates);
        $this->assertArrayHasKey('amount', $updates[0]['update']);
        $this->assertArrayNotHasKey('recurringamount', $updates[0]['update']);
        $this->assertSame(50.0, $updates[0]['update']['amount']);
    }
}
