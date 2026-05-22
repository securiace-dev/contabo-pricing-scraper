<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * One-shot tax-rule installer for India 18% GST. Idempotent — running it twice
 * does not duplicate rules. Called from the settings page on demand.
 *
 * Profiles can choose to bake GST into the final price (Settings::applyGst18 = true,
 * the default) OR delegate to WHMCS tax rules. If you delegate, run this once and
 * disable applyGst18; otherwise leave the WHMCS-side rule absent.
 */
class TaxRuleManager
{
    public const TAX_NAME = 'Contabo Pricing — India GST 18%';
    public const TAX_RATE = 18.0;
    public const COUNTRY  = 'IN';

    public function ensure(): array
    {
        $existing = Capsule::table('tbltax')
            ->where('name', self::TAX_NAME)
            ->where('country', self::COUNTRY)
            ->first();

        if ($existing) {
            return ['status' => 'exists', 'id' => (int) $existing->id];
        }

        $id = Capsule::table('tbltax')->insertGetId([
            'level'   => 1,
            'name'    => self::TAX_NAME,
            'state'   => '',
            'country' => self::COUNTRY,
            'taxrate' => self::TAX_RATE,
        ]);
        return ['status' => 'created', 'id' => (int) $id];
    }

    public function remove(): int
    {
        return (int) Capsule::table('tbltax')
            ->where('name', self::TAX_NAME)
            ->where('country', self::COUNTRY)
            ->delete();
    }

    /** @return array<string, mixed>|null */
    public function find(): ?array
    {
        $row = Capsule::table('tbltax')
            ->where('name', self::TAX_NAME)
            ->where('country', self::COUNTRY)
            ->first();
        return $row ? (array) $row : null;
    }
}
