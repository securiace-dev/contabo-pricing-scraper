<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Resolves the active pricing policy for a service by joining the per-service
 * row in `mod_contabo_service_policy` with the profile defaults.
 *
 * Contract: NEVER returns null. If no service_policy row exists, returns a
 * synthetic row backed by the profile's `default_policy` plus the profile's
 * default thresholds. This means RenewalEngine can always read
 * `$resolved['policy']` without a null check.
 *
 * Behaviour around manual override expiry is OWNED by RenewalEngine — this
 * class returns the raw row as stored. The engine compares
 * `manual_override_expires_at` to "today" and decides whether to honour or
 * fall through.
 *
 * PHP 7.4 polyglot: typed properties, no constructor promotion, no readonly.
 */
class PolicyResolver
{
    /**
     * Resolves the effective policy row for a service.
     *
     * @param int                  $serviceId WHMCS `tblhosting.id`.
     * @param array<string, mixed> $profile   Profile row (must include
     *                                        `default_policy`, `margin_floor_pct`,
     *                                        `notice_days_default`,
     *                                        `allow_auto_decrease`,
     *                                        `large_increase_threshold_pct`,
     *                                        `max_increase_pct`).
     * @return array<string, mixed> Resolved policy row. Keys guaranteed:
     *     - policy                       string
     *     - locked_price                 float|null
     *     - locked_currency              string|null
     *     - manual_override_price        float|null
     *     - manual_override_reason       string|null
     *     - manual_override_expires_at   string|null  (Y-m-d H:i:s)
     *     - margin_floor_pct             float        (falls back to profile)
     *     - frozen_until                 string|null  (Y-m-d)
     *     - allow_auto_decrease          bool         (falls back to profile)
     *     - min_sell_price               float|null
     *     - source                       'service'|'profile_default' (debug hint)
     */
    public function resolveForService(int $serviceId, array $profile): array
    {
        $row = self::fetchServicePolicy($serviceId);

        $marginFloorPct      = self::asFloat($profile['margin_floor_pct'] ?? null, 15.00);
        $allowAutoDecrease   = self::asBool($profile['allow_auto_decrease'] ?? false);
        $defaultPolicy       = (string) ($profile['default_policy'] ?? 'current_term');

        if ($row === null) {
            return [
                'policy'                     => $defaultPolicy,
                'locked_price'               => null,
                'locked_currency'            => null,
                'manual_override_price'      => null,
                'manual_override_reason'     => null,
                'manual_override_expires_at' => null,
                'margin_floor_pct'           => $marginFloorPct,
                'frozen_until'               => null,
                'allow_auto_decrease'        => $allowAutoDecrease,
                'min_sell_price'             => null,
                'source'                     => 'profile_default',
            ];
        }

        $perServiceFloor = self::asFloat($row['margin_floor_pct'] ?? null, null);
        $perServiceAuto  = array_key_exists('allow_auto_decrease', $row) && $row['allow_auto_decrease'] !== null
            ? self::asBool($row['allow_auto_decrease'])
            : null;

        return [
            'policy'                     => (string) ($row['policy'] ?? $defaultPolicy),
            'locked_price'               => self::asFloat($row['locked_price'] ?? null, null),
            'locked_currency'            => isset($row['locked_currency']) ? (string) $row['locked_currency'] : null,
            'manual_override_price'      => self::asFloat($row['manual_override_price'] ?? null, null),
            'manual_override_reason'     => isset($row['manual_override_reason']) ? (string) $row['manual_override_reason'] : null,
            'manual_override_expires_at' => isset($row['manual_override_expires_at']) && $row['manual_override_expires_at'] !== null
                                              ? (string) $row['manual_override_expires_at']
                                              : null,
            'margin_floor_pct'           => $perServiceFloor !== null ? $perServiceFloor : $marginFloorPct,
            'frozen_until'               => isset($row['frozen_until']) && $row['frozen_until'] !== null
                                              ? (string) $row['frozen_until']
                                              : null,
            'allow_auto_decrease'        => $perServiceAuto !== null ? $perServiceAuto : $allowAutoDecrease,
            'min_sell_price'             => self::asFloat($row['min_sell_price'] ?? null, null),
            'source'                     => 'service',
        ];
    }

    /**
     * Fetch the per-service policy row, or null if none exists. Wrapped so
     * tests can subclass and swap in fixture data without a DB.
     *
     * @return array<string, mixed>|null
     */
    protected function fetchServicePolicy(int $serviceId): ?array
    {
        try {
            $row = Capsule::table('mod_contabo_service_policy')
                ->where('service_id', $serviceId)
                ->first();
        } catch (\Throwable $e) {
            return null;
        }

        if ($row === null) {
            return null;
        }

        return (array) $row;
    }

    /**
     * @param mixed       $value
     * @param float|null  $default
     */
    private static function asFloat($value, $default): ?float
    {
        if ($value === null || $value === '') {
            return $default;
        }
        return (float) $value;
    }

    /**
     * @param mixed $value
     */
    private static function asBool($value): bool
    {
        if (is_bool($value)) return $value;
        if (is_numeric($value)) return ((int) $value) !== 0;
        if (is_string($value)) {
            $v = strtolower(trim($value));
            return $v === '1' || $v === 'true' || $v === 'yes' || $v === 'on';
        }
        return (bool) $value;
    }
}
