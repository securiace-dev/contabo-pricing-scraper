<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\JsonPayloadCap;
use ContaboPricing\JsonPayloadTooLarge;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Oversize admin JSON must fail the save before a profile or mapping write.
 * null from decodeOptionsPayload means "do not touch options". [] from
 * coerceJsonObject is what a bad-but-small payload becomes, and that [] is
 * written — an oversize payload must not take either path.
 */
final class JsonPayloadSaveTest extends TestCase
{
    /** @var AdminController */
    private $controller;

    protected function setUp(): void
    {
        Capsule::reset();
        $settings = new Settings(
            'notify', 'INR', false, 3.5, 365,
            'addonmodules.php?module=contabo_pricing'
        );
        $this->controller = new AdminController($settings, __DIR__ . '/../templates/admin');
        Capsule::$tables['mod_contabo_profile'] = [[
            'id' => 7,
            'name' => 'keep',
            'options' => '{"Image:OS":{"label":"Ubuntu 24.04"}}',
        ]];
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id' => 3,
            'profile_id' => 7,
            'product_id' => 9,
            'markup_overrides_json' => '{"Annually":{"strategy":"cost_plus_pct","value":12.5}}',
            'setup_fee_overrides_json' => '{}',
            'source_overrides_json' => '{}',
        ]];
    }

    public function testCeilingIsAtLeastFourTimesLargestTestOptionsPayload(): void
    {
        // Largest options object posted by the suite (ProfileIdentityTest).
        $measured = strlen((string) json_encode([
            'Image:OS' => ['label' => 'Ubuntu 24.04', 'monthly' => 0.0],
            'Region' => ['label' => 'EU', 'monthly' => 0.0],
            'Storage' => ['label' => '200 GB NVMe', 'monthly' => 2.0],
            'Backup' => ['label' => 'Auto Backup', 'monthly' => 1.0],
            'Networking' => ['label' => '1 IPv4', 'monthly' => 0.0],
        ]));
        $this->assertSame(220, $measured);
        $this->assertGreaterThanOrEqual(4 * $measured, JsonPayloadCap::MAX_BYTES);
        $this->assertLessThanOrEqual(1024 * 1024, JsonPayloadCap::MAX_BYTES);
        $this->assertFalse(JsonPayloadCap::exceeds(json_encode([
            'Image:OS' => ['label' => 'Ubuntu 24.04', 'monthly' => 0.0],
            'Region' => ['label' => 'EU', 'monthly' => 0.0],
            'Storage' => ['label' => '200 GB NVMe', 'monthly' => 2.0],
            'Backup' => ['label' => 'Auto Backup', 'monthly' => 1.0],
            'Networking' => ['label' => '1 IPv4', 'monthly' => 0.0],
        ])));
    }

    public function testMissingOptionsStayNullAndSmallJsonStillDecodes(): void
    {
        $decode = $this->method('decodeOptionsPayload');
        $this->assertNull($decode->invoke($this->controller, null));
        $this->assertNull($decode->invoke($this->controller, ''));
        $opts = $decode->invoke($this->controller, '{"Image:OS":{"label":"Ubuntu 24.04"}}');
        $this->assertSame('Ubuntu 24.04', $opts['Image:OS']['label']);

        $coerce = $this->method('coerceJsonObject');
        $small = $coerce->invoke($this->controller, '{"Annually":{"strategy":"cost_plus_pct","value":12.5}}');
        $this->assertSame('cost_plus_pct', $small['Annually']['strategy']);
        $this->assertSame([], $coerce->invoke($this->controller, 'not-json'));
    }

    public function testOversizeOptionsStringDoesNotWrite(): void
    {
        $raw = '{"Image:OS":{"label":"' . str_repeat('x', JsonPayloadCap::MAX_BYTES) . '"}}';
        $this->assertGreaterThan(JsonPayloadCap::MAX_BYTES, strlen($raw));
        $html = $this->invokeSave('profileSave', [
            'id' => 7,
            'name' => 'changed',
            'options' => $raw,
        ]);
        $this->assertStringContainsString('exceeds the size limit', $html);
        $this->assertStringContainsString('errorbox', $html);
        $this->assertSame(
            '{"Image:OS":{"label":"Ubuntu 24.04"}}',
            Capsule::$tables['mod_contabo_profile'][0]['options']
        );
        $this->assertSame('keep', Capsule::$tables['mod_contabo_profile'][0]['name']);
        $this->assertSame([], Capsule::$calls);
        $this->assertSame([], Capsule::$inserts);
    }

    public function testOversizeOptionsArrayDoesNotWrite(): void
    {
        $hostile = array_fill(0, intdiv(JsonPayloadCap::MAX_BYTES, 2) + 1, 0);
        $this->assertTrue(JsonPayloadCap::exceeds($hostile));
        $html = $this->invokeSave('profileSave', [
            'id' => 7,
            'options' => $hostile,
        ]);
        $this->assertStringContainsString('exceeds the size limit', $html);
        $this->assertSame(
            '{"Image:OS":{"label":"Ubuntu 24.04"}}',
            Capsule::$tables['mod_contabo_profile'][0]['options']
        );
        $this->assertSame([], Capsule::$calls);
        $this->assertSame([], Capsule::$inserts);
    }

    public function testOversizeOverrideStringDoesNotWriteEmptyObject(): void
    {
        $raw = '{"Annually":{"strategy":"' . str_repeat('y', JsonPayloadCap::MAX_BYTES) . '"}}';
        $html = $this->invokeSave('mappingSave', [
            'profile_id' => 7,
            'product_id' => 9,
            'markup_overrides_json' => $raw,
            'setup_fee_overrides_json' => '{}',
            'source_overrides_json' => '{}',
        ]);
        $this->assertStringContainsString('exceeds the size limit', $html);
        $this->assertSame(
            '{"Annually":{"strategy":"cost_plus_pct","value":12.5}}',
            Capsule::$tables['mod_contabo_mapping'][0]['markup_overrides_json']
        );
        $this->assertNotSame('[]', Capsule::$tables['mod_contabo_mapping'][0]['markup_overrides_json']);
        $this->assertNotSame([], Capsule::$tables['mod_contabo_mapping'][0]['markup_overrides_json']);
        $this->assertSame([], Capsule::$calls);
        $this->assertSame([], Capsule::$inserts);
    }

    public function testOversizeOverrideArrayDoesNotWriteEmptyObject(): void
    {
        $hostile = [];
        for ($i = 0; $i < intdiv(JsonPayloadCap::MAX_BYTES, 2) + 1; $i++) {
            $hostile['k' . $i] = 0;
        }
        $this->assertTrue(JsonPayloadCap::exceeds($hostile));
        $html = $this->invokeSave('mappingSave', [
            'profile_id' => 7,
            'product_id' => 9,
            'markup_overrides_json' => '{}',
            'setup_fee_overrides_json' => $hostile,
        ]);
        $this->assertStringContainsString('exceeds the size limit', $html);
        $this->assertSame('{}', Capsule::$tables['mod_contabo_mapping'][0]['setup_fee_overrides_json']);
        $this->assertSame(
            '{"Annually":{"strategy":"cost_plus_pct","value":12.5}}',
            Capsule::$tables['mod_contabo_mapping'][0]['markup_overrides_json']
        );
        $this->assertSame([], Capsule::$calls);
        $this->assertSame([], Capsule::$inserts);
    }

    public function testDecodeThrowsBeforeJsonDecodeOfOversizeString(): void
    {
        $decode = $this->method('decodeOptionsPayload');
        try {
            $decode->invoke($this->controller, str_repeat('z', JsonPayloadCap::MAX_BYTES + 1));
            $this->fail('expected JsonPayloadTooLarge');
        } catch (JsonPayloadTooLarge $e) {
            $this->assertStringContainsString('exceeds the size limit', $e->getMessage());
        }
    }

    /**
     * @param array<string,mixed> $req
     */
    private function invokeSave(string $method, array $req): string
    {
        $m = $this->method($method);
        ob_start();
        try {
            $m->invoke($this->controller, $req);
        } finally {
            $html = (string) ob_get_clean();
        }
        return $html;
    }

    private function method(string $name): \ReflectionMethod
    {
        $m = new \ReflectionMethod(AdminController::class, $name);
        if (PHP_VERSION_ID < 80100) {
            $m->setAccessible(true);
        }
        return $m;
    }
}
