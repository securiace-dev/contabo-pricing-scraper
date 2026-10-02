<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class SettingsTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testFromVarsAppliesDefaultsWhenNoVarsGiven(): void
    {
        $s = Settings::fromVars([]);

        $this->assertSame('notify', $s->defaultSyncStrategy);
        $this->assertSame('INR', $s->currencyIso);
        $this->assertTrue($s->applyGst18);
        $this->assertSame(3.5, $s->fxMarkupPct);
        $this->assertSame(365, $s->logRetentionDays);
        $this->assertSame('addonmodules.php?module=contabo_pricing', $s->moduleLink);
    }

    public function testFromVarsParsesYesAsTrue(): void
    {
        $s = Settings::fromVars(['apply_gst_18' => 'yes']);
        $this->assertTrue($s->applyGst18);
    }

    public function testFromVarsParsesNoAsFalse(): void
    {
        $s = Settings::fromVars(['apply_gst_18' => 'no']);
        $this->assertFalse($s->applyGst18);
    }

    public function testFromVarsParsesEmptyAsFalse(): void
    {
        $s = Settings::fromVars(['apply_gst_18' => '']);
        $this->assertFalse($s->applyGst18);
    }

    public function testFromVarsCastsFxMarkupPctToFloat(): void
    {
        $s = Settings::fromVars(['fx_markup_pct' => '4.25']);
        $this->assertSame(4.25, $s->fxMarkupPct);
    }

    public function testFromVarsCastsFxMarkupPctZero(): void
    {
        $s = Settings::fromVars(['fx_markup_pct' => '0']);
        $this->assertSame(0.0, $s->fxMarkupPct);
    }

    public function testFromVarsCastsLogRetentionToInt(): void
    {
        $s = Settings::fromVars(['log_retention_days' => '90']);
        $this->assertSame(90, $s->logRetentionDays);
    }

    public function testFromVarsUppercasesCurrencyIso(): void
    {
        $s = Settings::fromVars(['currency_iso' => 'eur']);
        $this->assertSame('EUR', $s->currencyIso);
    }

    public function testFromVarsAcceptsModuleLinkOverride(): void
    {
        $s = Settings::fromVars(['modulelink' => 'addonmodules.php?module=contabo_pricing&action=custom']);
        $this->assertSame('addonmodules.php?module=contabo_pricing&action=custom', $s->moduleLink);
    }

}
