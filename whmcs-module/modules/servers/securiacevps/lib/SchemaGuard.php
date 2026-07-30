<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class SchemaGuard
{
    public const SUITE_SCHEMA_VERSION = 4;

    /** @var list<string> */
    private const REQUIRED_TABLES = [
        'mod_securiacevps_schema',
        'mod_securiacevps_order_snapshots',
        'mod_securiacevps_resources',
        'mod_securiacevps_operations',
        'mod_securiacevps_operation_attempts',
        'mod_securiacevps_provider_requests',
        'mod_securiacevps_service_locks',
        'mod_securiacevps_capabilities',
        'mod_securiacevps_reconciliation',
        'mod_securiacevps_adoption',
        'mod_securiacevps_billing_sagas',
        'mod_securiacevps_audit_events',
        'mod_securiacevps_operator_commands',
        'mod_securiacevps_secrets',
        'mod_securiacevps_communications',
        'mod_securiacevps_snapshot_inventory',
    ];

    public static function assertReady(): void
    {
        $schema = Capsule::schema();
        foreach (self::REQUIRED_TABLES as $table) {
            if (!$schema->hasTable($table)) {
                throw new ContaboProvisioningException(
                    'VPS module database schema is not ready; activate or upgrade the Contabo Pricing addon'
                );
            }
        }
        if ((int) self::setting('schema_version', '0') < self::SUITE_SCHEMA_VERSION) {
            throw new ContaboProvisioningException(
                'VPS module database schema is outdated; upgrade the Contabo Pricing addon before running provider actions'
            );
        }
    }

    public static function assertProviderWriteEnabled(string $capability): void
    {
        self::assertReady();
        if (self::setting('provider_writes_enabled', '0') !== '1') {
            throw new ContaboProvisioningException(
                'Provider writes are paused by the SecuriAce VPS safety switch'
            );
        }
        if (self::setting('capability.' . $capability . '.enabled', '0') !== '1') {
            throw new ContaboProvisioningException(
                'This provider action is paused by its capability safety switch'
            );
        }
    }

    public static function installationId(): string
    {
        self::assertReady();
        $id = trim(self::setting('installation_id', ''));
        if ($id === '') {
            throw new ContaboProvisioningException('WHMCS installation identity is not configured');
        }
        return $id;
    }

    public static function setting(string $key, string $default = ''): string
    {
        try {
            $value = Capsule::table('mod_securiacevps_schema')->where('key', $key)->value('value');
            return $value === null ? $default : (string) $value;
        } catch (\Throwable $e) {
            return $default;
        }
    }
}
