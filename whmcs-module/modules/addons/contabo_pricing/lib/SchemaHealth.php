<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Runtime schema guard for the Contabo Pricing addon.
 *
 * Two responsibilities:
 *
 *   1. assertOrMigrate() — a cheap, never-throwing pre-flight to be called at
 *      the top of mutating admin actions (dashboard load, profile create /
 *      edit, mapping save, sync trigger, addon config save). It reads the
 *      recorded `mod_contabo_settings.schema_version`, compares it to
 *      Installer::SCHEMA_VERSION, and runs Installer::upgrade() when stale.
 *      Failures are caught and surfaced as a structured array so the caller
 *      can render a friendly UI error instead of leaking a stack trace.
 *
 *   2. requiredColumnsPresent() — verifies the v4 runtime column set actually
 *      exists on `mod_contabo_mapping` + `mod_contabo_profile`, returning the
 *      list of missing "table.column" strings (powers the maintenance-page
 *      green/red health panel).
 *
 * Also hosts the purge-confirmation phrase validator (isPurgeConfirmed) so the
 * maintenance-purge handler + the maintenance template share one source of
 * truth for the typed phrase, and PURGE_CONFIRMATION_PHRASE for both sides.
 */
final class SchemaHealth
{
    /**
     * The exact phrase an admin must type to confirm the destructive purge.
     * The maintenance template renders this in its instructions; the
     * maintenance-purge handler validates the posted value against it via
     * isPurgeConfirmed().
     */
    public const PURGE_CONFIRMATION_PHRASE = 'PURGE CONTABO PRICING DATA';

    /**
     * The v4 required runtime column set, keyed by table. These are the columns
     * the live addon (controllers, repositories, sync engine, templates) reads
     * and writes — deliberately NOT an exhaustive schema inventory. Legacy
     * columns (apply_to_*) are intentionally absent: they must not be required
     * at runtime.
     *
     * @var array<string, list<string>>
     */
    private const REQUIRED_COLUMNS = [
        'mod_contabo_mapping' => [
            'catalog_cycles_mask',
            'renewal_cycles_mask',
            'markup_overrides_json',
            'setup_fee_overrides_json',
            'respect_disabled_cycles',
            'overwrite_free_cycles',
            'sync_setup_fees',
            'rounding_mode',
        ],
        'mod_contabo_profile' => [
            'profile_mode',
            'profile_fingerprint_hash',
            'profile_identity_json',
            // schema v5 (A.6.1) — configurable-product identity scope
            'product_scope_key',
            'commercial_variant',
            'audience_segment',
        ],
    ];

    /**
     * Reads the recorded schema_version; if lower than Installer::SCHEMA_VERSION
     * runs Installer::upgrade() to catch up. Never throws: any failure is caught
     * and returned in the 'error' field so callers can block the action with a
     * clear UI message rather than a stack trace.
     *
     * @return array{ok:bool, from:int, to:int, error:?string}
     */
    public static function assertOrMigrate(): array
    {
        $from = 0;
        try {
            $from = self::currentSchemaVersion();
            $target = Installer::SCHEMA_VERSION;

            if ($from >= $target) {
                return ['ok' => true, 'from' => $from, 'to' => $from, 'error' => null];
            }

            // upgrade() loops migrateTo{from+1..target} and bumps the recorded
            // schema_version after each step.
            (new Installer())->upgrade((string) $from);

            $to = self::currentSchemaVersion();
            $ok = $to >= $target;

            return [
                'ok'    => $ok,
                'from'  => $from,
                'to'    => $to,
                'error' => $ok
                    ? null
                    : sprintf('Migration ran but schema_version is still %d (expected %d).', $to, $target),
            ];
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing SchemaHealth::assertOrMigrate failed: ' . $e->getMessage());
            }
            return [
                'ok'    => false,
                'from'  => $from,
                'to'    => $from,
                'error' => $e->getMessage(),
            ];
        }
    }

    /**
     * Verifies the v4 required runtime column set exists. Returns the list of
     * missing "table.column" strings (empty list = healthy). Never throws.
     *
     * @return array{healthy:bool, missing:list<string>, schema_version:int}
     */
    public static function requiredColumnsPresent(): array
    {
        $missing = [];
        try {
            $schema = Capsule::schema();
            foreach (self::REQUIRED_COLUMNS as $table => $columns) {
                if (!$schema->hasTable($table)) {
                    foreach ($columns as $column) {
                        $missing[] = $table . '.' . $column;
                    }
                    continue;
                }
                foreach ($columns as $column) {
                    if (!$schema->hasColumn($table, $column)) {
                        $missing[] = $table . '.' . $column;
                    }
                }
            }
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing SchemaHealth::requiredColumnsPresent failed: ' . $e->getMessage());
            }
            // On inspection failure, report unhealthy with the whole required
            // set as "missing" so the UI flags red rather than falsely green.
            $missing = [];
            foreach (self::REQUIRED_COLUMNS as $table => $columns) {
                foreach ($columns as $column) {
                    $missing[] = $table . '.' . $column;
                }
            }
            return [
                'healthy'        => false,
                'missing'        => $missing,
                'schema_version' => 0,
            ];
        }

        return [
            'healthy'        => $missing === [],
            'missing'        => $missing,
            'schema_version' => self::currentSchemaVersion(),
        ];
    }

    /**
     * The flat list of v4 required "table.column" strings. Exposed so tests +
     * the maintenance page can reason about the required set without poking at
     * the live schema.
     *
     * @return list<string>
     */
    public static function requiredColumnList(): array
    {
        $out = [];
        foreach (self::REQUIRED_COLUMNS as $table => $columns) {
            foreach ($columns as $column) {
                $out[] = $table . '.' . $column;
            }
        }
        return $out;
    }

    /**
     * Validates the typed purge-confirmation phrase. The match is exact
     * (case-sensitive, surrounding whitespace trimmed). Used by both the
     * maintenance template (documentation) and the maintenance-purge handler.
     */
    public static function isPurgeConfirmed(string $phrase): bool
    {
        return trim($phrase) === self::PURGE_CONFIRMATION_PHRASE;
    }

    /**
     * Reads the recorded schema_version from mod_contabo_settings, defaulting
     * to 0 when unreadable / unset.
     */
    private static function currentSchemaVersion(): int
    {
        try {
            return (int) (Capsule::table('mod_contabo_settings')
                ->where('key', 'schema_version')
                ->value('value') ?? 0);
        } catch (\Throwable $e) {
            return 0;
        }
    }
}
