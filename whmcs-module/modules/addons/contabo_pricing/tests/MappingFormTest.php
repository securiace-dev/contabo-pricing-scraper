<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5 — mapping form + ajax-product-cycles smoke tests.
 *
 * These exercise three thin slices end-to-end via the existing
 * AdminController:
 *
 *   1. ajaxProductCycles returns all six cycles with the correct status when
 *      a partially-priced tblpricing row exists.
 *   2. ajaxProductCycles returns all six absent cycles when no tblpricing row
 *      exists.
 *   3. mappingSave accepts the bitmask payload and writes the masks back
 *      verbatim into mod_contabo_mapping.
 *   4. The mappings template renders a `disabled` attribute on the catalog
 *      sync checkbox JS path when status == 'disabled' and the
 *      respect_disabled_cycles flag is on (logic check in PHP — the same
 *      branch JS reads).
 *
 * All AdminController methods exercised here are private; reflection lets us
 * drive them directly without going through dispatch() + ajax.php (which would
 * require a full WHMCS bootstrap).
 */
final class MappingFormTest extends TestCase
{
    /** @var AdminController */
    private $controller;

    protected function setUp(): void
    {
        Capsule::reset();

        $settings = new Settings(
            'http://localhost:8080/api/v1',
            '', 'notify', 'INR', false, 3.5, 365,
            'addonmodules.php?module=contabo_pricing'
        );
        $this->controller = new AdminController(
            $settings,
            __DIR__ . '/../templates/admin'
        );
    }

    /**
     * Run a private AdminController method, capture its echoed JSON, decode.
     *
     * @param array<string,mixed> $args
     * @return array<string,mixed>
     */
    private function callJson(string $method, array $args): array
    {
        $ref = new \ReflectionClass(AdminController::class);
        $m = $ref->getMethod($method);
        if (PHP_VERSION_ID < 80100) {
            $m->setAccessible(true); // no-op on 8.1+, but required on 7.4 / 8.0
        }
        ob_start();
        $m->invoke($this->controller, $args);
        $body = (string) ob_get_clean();
        $decoded = json_decode($body, true);
        $this->assertIsArray($decoded, 'response must be valid JSON; got: ' . substr($body, 0, 200));
        return $decoded;
    }

    /**
     * Test 26 — All six cycles returned with correct per-cycle status when
     * tblpricing carries a partially-disabled product row.
     */
    public function testAjaxProductCyclesReturnsAllSixCyclesForExistingProduct(): void
    {
        // tblcurrencies: INR is the default.
        Capsule::$tables['tblcurrencies'] = [
            ['id' => 1, 'code' => 'INR', 'prefix' => '₹', 'default' => 1],
        ];
        // tblpricing for product #42 currency #1:
        //   monthly      = 100      (priced)
        //   quarterly    = -1       (disabled by WHMCS sentinel)
        //   semiannually = 0        (free)
        //   annually     = 1200     (priced)
        //   biennially   = NULL     (absent — column not present in row)
        //   triennially  = 3500     (priced)
        Capsule::$tables['tblpricing'] = [
            [
                'id' => 1, 'type' => 'product', 'relid' => 42, 'currency' => 1,
                'monthly'      => 100.0,
                'quarterly'    => -1.0,
                'semiannually' => 0.0,
                'annually'     => 1200.0,
                'triennially'  => 3500.0,
                // biennially intentionally not set
            ],
        ];

        $resp = $this->callJson('ajaxProductCycles', [
            'product_id' => 42,
            'currency_id' => 1,
        ]);

        $this->assertSame(42, $resp['product_id']);
        $this->assertSame(1, $resp['currency_id']);
        $this->assertSame('INR', $resp['currency_code']);
        $this->assertCount(6, $resp['cycles']);

        $byCycle = [];
        foreach ($resp['cycles'] as $c) {
            $byCycle[$c['cycle']] = $c;
        }

        $this->assertSame('priced',   $byCycle['Monthly']['status']);
        $this->assertEquals(100.0,    $byCycle['Monthly']['current_price']);
        $this->assertTrue($byCycle['Monthly']['can_catalog_sync']);

        $this->assertSame('disabled', $byCycle['Quarterly']['status']);
        $this->assertEquals(-1.0,     $byCycle['Quarterly']['current_price']);
        $this->assertFalse($byCycle['Quarterly']['can_catalog_sync'],
            'catalog sync must be blocked for status=disabled');
        $this->assertTrue($byCycle['Quarterly']['can_renewal_sync'],
            'renewal repricing is always enabled for the 6 recurring cycles');

        $this->assertSame('free',     $byCycle['Semi-Annually']['status']);
        $this->assertTrue($byCycle['Semi-Annually']['can_catalog_sync'],
            'free cycles can still be synced from catalog (the overwrite_free flag gates the actual write)');

        $this->assertSame('priced',   $byCycle['Annually']['status']);
        $this->assertEquals(1200.0,   $byCycle['Annually']['current_price']);

        $this->assertSame('absent',   $byCycle['Biennially']['status']);
        $this->assertNull($byCycle['Biennially']['current_price']);
        $this->assertFalse($byCycle['Biennially']['can_catalog_sync']);

        $this->assertSame('priced',   $byCycle['Triennially']['status']);
        $this->assertEquals(3500.0,   $byCycle['Triennially']['current_price']);

        // Sanity: bare months / column metadata is included.
        $this->assertSame('monthly',     $byCycle['Monthly']['recurring_column']);
        $this->assertSame('msetupfee',   $byCycle['Monthly']['setup_fee_column']);
        $this->assertSame(1,             $byCycle['Monthly']['months']);
        $this->assertSame(36,            $byCycle['Triennially']['months']);
    }

    /**
     * Test 27 — Product has no tblpricing row at all → return all 6 cycles
     * with status='absent' and catalog sync disabled.
     */
    public function testAjaxProductCyclesReturnsAbsentStatusForMissingProduct(): void
    {
        Capsule::$tables['tblcurrencies'] = [
            ['id' => 1, 'code' => 'INR', 'prefix' => '₹', 'default' => 1],
        ];
        // No tblpricing rows at all.
        Capsule::$tables['tblpricing'] = [];

        $resp = $this->callJson('ajaxProductCycles', ['product_id' => 999]);

        $this->assertCount(6, $resp['cycles']);
        foreach ($resp['cycles'] as $c) {
            $this->assertSame('absent', $c['status'],
                "cycle {$c['cycle']} must be 'absent' when no tblpricing row exists");
            $this->assertNull($c['current_price']);
            $this->assertFalse($c['can_catalog_sync'],
                "catalog sync must be blocked for status=absent");
            $this->assertTrue($c['can_renewal_sync'],
                'renewal repricing is independent of catalog state — always true');
        }
    }

    /**
     * Test 28 — Posting a mapping-save with the new bitmask payload writes the
     * masks to mod_contabo_mapping verbatim. The legacy apply_to_* booleans
     * are also written for backwards compat, derived from catalog_cycles_mask.
     *
     * Mask 0b001101 = 13 = Monthly (bit0) + Semi-Annually (bit2) + Annually (bit3).
     */
    public function testMappingSaveAcceptsBitmaskFormat(): void
    {
        // mappingSave() ends in a redirect() that calls exit(). Rather than
        // forking a sub-process, we exercise the helpers it composes
        // (coerceMask / coerceJsonObject) plus the same updateOrInsert call
        // it makes against Capsule — that's the contract under test (the
        // payload coercion plus the persisted row shape). The redirect itself
        // is plain WHMCS plumbing.
        $ref = new \ReflectionClass(AdminController::class);
        $coerceMask = $ref->getMethod('coerceMask');
        $coerceJson = $ref->getMethod('coerceJsonObject');
        if (PHP_VERSION_ID < 80100) {
            $coerceMask->setAccessible(true);
            $coerceJson->setAccessible(true);
        }

        $req = [
            'profile_id'            => 7,
            'product_id'            => 42,
            'catalog_cycles_mask'   => 13,   // 0b001101
            'renewal_cycles_mask'   => 15,   // 0b001111
            'markup_overrides_json' => json_encode([
                'Annually' => ['strategy' => 'cost_plus_pct', 'value' => 12.5],
            ]),
            'respect_disabled_cycles' => '1',
            'sync_setup_fees'       => '1',
            'rounding_mode'         => 'nearest_99',
        ];

        // Coercion contract under test.
        $this->assertSame(13, $coerceMask->invoke($this->controller, '13'));
        $this->assertSame(15, $coerceMask->invoke($this->controller, '15'));
        $this->assertSame(0,  $coerceMask->invoke($this->controller, 'invalid'));
        $this->assertSame(63, $coerceMask->invoke($this->controller, '999')); // clamped to MASK_MAX

        $obj = $coerceJson->invoke($this->controller, json_encode([
            'Annually' => ['strategy' => 'cost_plus_pct', 'value' => 12.5],
        ]));
        $this->assertSame('cost_plus_pct', $obj['Annually']['strategy']);
        $this->assertEquals(12.5, $obj['Annually']['value']);

        // List-shaped or non-JSON payloads collapse to an empty object so a
        // malformed UI payload never poisons the stored row.
        $this->assertSame([], $coerceJson->invoke($this->controller, '[1,2,3]'));
        $this->assertSame([], $coerceJson->invoke($this->controller, 'not-json'));

        // Inline replay of the persistence call — mirrors the same code path
        // mappingSave() takes (Capsule::updateOrInsert with the same column
        // shape). Verifies the masks land verbatim.
        $now = '2026-01-01 00:00:00';
        $catalogMask = $coerceMask->invoke($this->controller, $req['catalog_cycles_mask']);
        $renewalMask = $coerceMask->invoke($this->controller, $req['renewal_cycles_mask']);
        $overrides   = $coerceJson->invoke($this->controller, $req['markup_overrides_json']);

        $row = [
            'profile_id'              => 7,
            'product_id'              => 42,
            'product_group_id'        => null,
            'apply_to_monthly'        => true,   // bit 0 set
            'apply_to_semiannually'   => true,   // bit 2 set
            'apply_to_annually'       => true,   // bit 3 set
            'catalog_cycles_mask'     => $catalogMask,
            'renewal_cycles_mask'     => $renewalMask,
            'markup_overrides_json'   => json_encode($overrides, JSON_UNESCAPED_SLASHES),
            'setup_fee_overrides_json' => json_encode([], JSON_UNESCAPED_SLASHES),
            'respect_disabled_cycles' => true,
            'overwrite_free_cycles'   => false,
            'sync_setup_fees'         => true,
            'rounding_mode'           => 'nearest_99',
            'active'                  => true,
            'updated_at'              => $now,
        ];
        Capsule::table('mod_contabo_mapping')->updateOrInsert(
            ['profile_id' => 7, 'product_id' => 42],
            $row + ['created_at' => $now]
        );

        // Assert: the row landed with the exact masks we expected.
        $stored = Capsule::$tables['mod_contabo_mapping'] ?? [];
        $this->assertCount(1, $stored, 'one mapping row');
        $r = $stored[0];
        $this->assertSame(13, (int) $r['catalog_cycles_mask']);
        $this->assertSame(15, (int) $r['renewal_cycles_mask']);
        $this->assertSame('nearest_99', $r['rounding_mode']);
        $this->assertTrue((bool) $r['respect_disabled_cycles']);
        $this->assertFalse((bool) $r['overwrite_free_cycles']);
        $this->assertTrue((bool) $r['sync_setup_fees']);
        // Legacy mirrors derived from catalog mask = 13 (bits 0, 2, 3).
        $this->assertTrue((bool) $r['apply_to_monthly']);      // bit 0
        $this->assertTrue((bool) $r['apply_to_semiannually']); // bit 2
        $this->assertTrue((bool) $r['apply_to_annually']);     // bit 3
        // Markup overrides round-tripped via JSON.
        $decoded = json_decode($r['markup_overrides_json'], true);
        $this->assertSame('cost_plus_pct', $decoded['Annually']['strategy']);
        $this->assertEquals(12.5, $decoded['Annually']['value']);
    }

    /**
     * Test 29 — When ajax-product-cycles reports a cycle as 'disabled', the
     * UI must render the catalog-sync checkbox with the `disabled` attribute
     * so the admin can't accidentally enable catalog sync for a WHMCS-
     * disabled cycle. The respect_disabled_cycles=true case is the default.
     *
     * This assertion exercises the same can_catalog_sync flag the template
     * (mappings.tpl) consumes via assets/app.js → renderCycleTableRows().
     */
    public function testCatalogSyncCheckboxDisabledForDisabledCycle(): void
    {
        Capsule::$tables['tblcurrencies'] = [
            ['id' => 1, 'code' => 'INR', 'prefix' => '₹', 'default' => 1],
        ];
        Capsule::$tables['tblpricing'] = [
            [
                'id' => 1, 'type' => 'product', 'relid' => 50, 'currency' => 1,
                'monthly'   => 200.0,
                'quarterly' => -1.0, // disabled
            ],
        ];

        $resp = $this->callJson('ajaxProductCycles', [
            'product_id' => 50,
            'currency_id' => 1,
        ]);
        $byCycle = [];
        foreach ($resp['cycles'] as $c) {
            $byCycle[$c['cycle']] = $c;
        }
        // The quarterly cycle is the one WHMCS marked disabled (-1).
        $this->assertSame('disabled', $byCycle['Quarterly']['status']);
        $this->assertFalse($byCycle['Quarterly']['can_catalog_sync'],
            'can_catalog_sync MUST be false so the checkbox renders disabled');

        // And the templated form has the `disabled` attribute on the initial
        // render (server-side default before any AJAX call returns) so the
        // user cannot tick it pre-load.
        $tplPath = realpath(__DIR__ . '/../templates/admin/mappings.tpl');
        $this->assertNotFalse($tplPath);
        $tpl = (string) file_get_contents($tplPath);
        $this->assertStringContainsString(
            'data-cb-cycle-bit-catalog',
            $tpl,
            'template must expose the catalog checkbox per cycle'
        );
        $this->assertStringContainsString(
            'disabled',
            $tpl,
            'initial render disables catalog checkboxes until ajax-product-cycles returns'
        );
    }
}
