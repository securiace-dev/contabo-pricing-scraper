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

        $this->assertSame('http://localhost:8080/api/v1', $s->apiBaseUrl);
        $this->assertSame('', $s->apiToken);
        $this->assertSame('notify', $s->defaultSyncStrategy);
        $this->assertSame('INR', $s->currencyIso);
        $this->assertTrue($s->applyGst18);
        $this->assertSame(3.5, $s->fxMarkupPct);
        $this->assertSame(365, $s->logRetentionDays);
        $this->assertSame('addonmodules.php?module=contabo_pricing', $s->moduleLink);
    }

    public function testFromVarsTrimsTrailingSlashFromUrl(): void
    {
        $s = Settings::fromVars(['api_base_url' => 'https://api.example.com/v1/']);
        $this->assertSame('https://api.example.com/v1', $s->apiBaseUrl);
    }

    public function testFromVarsTrimsMultipleTrailingSlashes(): void
    {
        $s = Settings::fromVars(['api_base_url' => 'https://api.example.com///']);
        $this->assertSame('https://api.example.com', $s->apiBaseUrl);
    }

    public function testFromVarsLeavesUrlAloneWhenNoTrailingSlash(): void
    {
        $s = Settings::fromVars(['api_base_url' => 'https://api.example.com/v1']);
        $this->assertSame('https://api.example.com/v1', $s->apiBaseUrl);
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

    public function testFromVarsPreservesApiTokenForThisRequest(): void
    {
        // Plaintext on disk → in-memory value is still the plaintext (so the
        // request that just saw the Save click can immediately use the token).
        $s = Settings::fromVars(['api_token' => 'secret-xyz']);
        $this->assertSame('secret-xyz', $s->apiToken);
    }

    public function testFromVarsAcceptsModuleLinkOverride(): void
    {
        $s = Settings::fromVars(['modulelink' => 'addonmodules.php?module=contabo_pricing&action=custom']);
        $this->assertSame('addonmodules.php?module=contabo_pricing&action=custom', $s->moduleLink);
    }

    public function testResolveTokenReturnsEmptyStringForEmptyInput(): void
    {
        $this->assertSame('', Settings::resolveToken(''));
        $this->assertSame([], Capsule::$calls, 'empty token must not trigger a DB write');
    }

    public function testResolveTokenDecryptsEncryptedPrefixedValue(): void
    {
        $cipher = 'ENC:' . encrypt('super-secret');
        $this->assertSame('super-secret', Settings::resolveToken($cipher));
        $this->assertSame([], Capsule::$calls, 'already-encrypted value must not trigger migration');
    }

    public function testResolveTokenMigratesPlaintextToEncryptedAtRest(): void
    {
        $plaintext = Settings::resolveToken('plaintext-token');
        $this->assertSame('plaintext-token', $plaintext, 'plaintext is still returned for in-memory use');

        $this->assertCount(1, Capsule::$calls, 'plaintext input must trigger exactly one update');
        $call = Capsule::$calls[0];
        $this->assertSame('tbladdonmodules', $call['table']);
        $this->assertSame(['module' => 'contabo_pricing', 'setting' => 'api_token'], $call['where']);
        $this->assertArrayHasKey('value', $call['update']);
        $this->assertStringStartsWith('ENC:', (string) $call['update']['value']);

        $cipher = substr((string) $call['update']['value'], 4);
        $this->assertSame('plaintext-token', decrypt($cipher), 'round-trip must recover original');
    }

    public function testFromVarsTriggersMigrationWhenTokenIsPlaintext(): void
    {
        Settings::fromVars(['api_token' => 'fresh-from-form']);

        $this->assertCount(1, Capsule::$calls);
        $this->assertSame('tbladdonmodules', Capsule::$calls[0]['table']);
        $this->assertStringStartsWith('ENC:', (string) Capsule::$calls[0]['update']['value']);
    }

    public function testFromVarsDoesNotReMigrateAlreadyEncryptedToken(): void
    {
        $cipher = 'ENC:' . encrypt('already-rotated');

        $s = Settings::fromVars(['api_token' => $cipher]);

        $this->assertSame('already-rotated', $s->apiToken);
        $this->assertSame([], Capsule::$calls, 'second-and-subsequent reads must be idempotent');
    }
}
