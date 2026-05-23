<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CurrencySupportReport;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 0.5.1 — CurrencySupportReport. Mirrors the production currency audit's logic:
 * Active/Suspended/Pending = meaningful; Cancelled/Terminated/Fraud excluded.
 * Verdict escalates only when a non-INR service is on a contabo_pricing-mapped
 * product. Reads tblhosting.`amount` (the real column), tblclients.currency.
 */
final class CurrencySupportReportTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function seedCurrencies(): void
    {
        Capsule::table('tblcurrencies')->insert(['id' => 1, 'code' => 'INR', 'prefix' => '₹', 'suffix' => ' INR', 'rate' => 1.0]);
        Capsule::table('tblcurrencies')->insert(['id' => 2, 'code' => 'USD', 'prefix' => '$', 'suffix' => ' USD', 'rate' => 0.01045]);
    }

    public function testAllInrServicesCleanVerdict(): void
    {
        $this->seedCurrencies();
        Capsule::table('tblclients')->insert(['id' => 1, 'currency' => 1]);
        Capsule::table('tblhosting')->insert(['id' => 10, 'userid' => 1, 'packageid' => 501, 'domainstatus' => 'Active', 'billingcycle' => 'Monthly', 'amount' => 500.0]);
        Capsule::table('tblhosting')->insert(['id' => 11, 'userid' => 1, 'packageid' => 7, 'domainstatus' => 'Suspended', 'billingcycle' => 'Monthly', 'amount' => 300.0]);
        Capsule::table('mod_contabo_mapping')->insert(['id' => 1, 'profile_id' => 1, 'product_id' => 501, 'active' => 1]);

        $r = (new CurrencySupportReport())->build();
        $this->assertSame('no_non_inr', $r['verdict']);
        $this->assertSame(0, $r['non_inr_meaningful_total']);
        $this->assertSame(1, $r['base_currency_id']);
        $this->assertSame('INR', $r['base_currency_code']);
        $this->assertSame([], $r['non_inr_mapped']);
    }

    public function testOneCancelledUsdServiceOnlyStaysClean(): void
    {
        // Mirrors prod exactly: all meaningful services INR, the only non-INR
        // service is a single Cancelled USD one → excluded, verdict stays clean.
        $this->seedCurrencies();
        Capsule::table('tblclients')->insert(['id' => 1, 'currency' => 1]); // INR
        Capsule::table('tblclients')->insert(['id' => 102, 'currency' => 2]); // USD
        Capsule::table('tblhosting')->insert(['id' => 10, 'userid' => 1, 'packageid' => 7, 'domainstatus' => 'Active', 'billingcycle' => 'Monthly', 'amount' => 500.0]);
        Capsule::table('tblhosting')->insert(['id' => 173, 'userid' => 102, 'packageid' => 10, 'domainstatus' => 'Cancelled', 'billingcycle' => 'Annually', 'amount' => 75.0]);

        $r = (new CurrencySupportReport())->build();
        $this->assertSame('no_non_inr', $r['verdict']);
        $this->assertSame(0, $r['non_inr_meaningful_total']);
        $this->assertSame(1, $r['excluded_non_inr_total']); // the Cancelled USD one
    }

    public function testNonInrActiveUnmappedIsLatent(): void
    {
        $this->seedCurrencies();
        Capsule::table('tblclients')->insert(['id' => 102, 'currency' => 2]); // USD
        // Active USD service on product 10, which is NOT a mapped product.
        Capsule::table('tblhosting')->insert(['id' => 200, 'userid' => 102, 'packageid' => 10, 'domainstatus' => 'Active', 'billingcycle' => 'Monthly', 'amount' => 90.0]);
        Capsule::table('mod_contabo_mapping')->insert(['id' => 1, 'profile_id' => 1, 'product_id' => 501, 'active' => 1]);

        $r = (new CurrencySupportReport())->build();
        $this->assertSame('non_inr_unmapped', $r['verdict']);
        $this->assertSame(1, $r['non_inr_meaningful_total']);
        $this->assertSame([], $r['non_inr_mapped']);
        $this->assertSame(0, $r['mapped_live_services_total']);
    }

    public function testNonInrActiveMappedIsActiveRisk(): void
    {
        $this->seedCurrencies();
        Capsule::table('tblclients')->insert(['id' => 102, 'currency' => 2]); // USD
        // Active USD service ON the mapped product 501 → escalates to active risk.
        Capsule::table('tblhosting')->insert(['id' => 300, 'userid' => 102, 'packageid' => 501, 'domainstatus' => 'Active', 'billingcycle' => 'Monthly', 'amount' => 120.5]);
        Capsule::table('mod_contabo_mapping')->insert(['id' => 1, 'profile_id' => 1, 'product_id' => 501, 'active' => 1]);

        $r = (new CurrencySupportReport())->build();
        $this->assertSame('non_inr_mapped_active_risk', $r['verdict']);
        $this->assertSame(1, $r['mapped_live_services_total']);
        $this->assertCount(1, $r['non_inr_mapped']);
        $row = $r['non_inr_mapped'][0];
        $this->assertSame(300, $row['service_id']);
        $this->assertSame(501, $row['packageid']);
        $this->assertSame('USD', $row['currency_code']);
        $this->assertSame(120.5, $row['amount']); // from tblhosting.amount
    }

    public function testClassifyPureLogic(): void
    {
        $r = new CurrencySupportReport();
        $this->assertSame('no_non_inr', $r->classify(0, 0));
        $this->assertSame('non_inr_unmapped', $r->classify(5, 0));
        $this->assertSame('non_inr_mapped_active_risk', $r->classify(5, 2));
        $this->assertSame('non_inr_mapped_active_risk', $r->classify(0, 1)); // mapped beats everything
    }

    public function testBaseCurrencyFlaggedById(): void
    {
        $this->seedCurrencies();
        $r = (new CurrencySupportReport())->build();
        $byId = [];
        foreach ($r['currencies'] as $c) {
            $byId[$c['id']] = $c;
        }
        $this->assertTrue($byId[1]['is_base']);   // INR (id == INR_CURRENCY_ID)
        $this->assertFalse($byId[2]['is_base']);  // USD
    }
}
