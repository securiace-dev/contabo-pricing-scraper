<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use ContaboPricing\SchemaHealth;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Contract for the schema auto-health checker (lib/SchemaHealth.php).
 *
 *   - assertOrMigrate() runs Installer::upgrade() when the recorded
 *     schema_version is behind Installer::SCHEMA_VERSION, returning to=4.
 *   - requiredColumnsPresent() lists the v4 column set as required (incl. the
 *     three new profile identity columns) and does NOT require legacy
 *     apply_to_* columns at runtime.
 */
final class SchemaHealthTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /**
     * Seed a v3-shape profile table (no v4 identity columns yet) + a stale
     * schema_version of 3. assertOrMigrate() must trigger Installer::upgrade
     * (running migrateTo4), bump the recorded version, and report to=4.
     */
    public function testAutoMigrationRunsBeforeCreateProfile(): void
    {
        // Settings table records a stale schema version.
        Capsule::$columns['mod_contabo_settings'] = ['key', 'value', 'updated_at'];
        Capsule::$tables['mod_contabo_settings'] = [
            ['key' => 'schema_version', 'value' => '3', 'updated_at' => '2026-01-01 00:00:00'],
        ];

        // Profile table at v3 shape — the three v4 identity columns are absent.
        Capsule::$columns['mod_contabo_profile'] = [
            'id', 'slug', 'name', 'plan_slug', 'period_months', 'region', 'os',
            'options', 'tags', 'sync_strategy', 'active', 'latest_version_id',
            'created_at', 'updated_at',
        ];
        Capsule::$tables['mod_contabo_profile'] = [];

        // Mapping table already at v3 (so requiredColumnsPresent stays focused
        // on the profile-side gap that migrateTo4 fills).
        Capsule::$columns['mod_contabo_mapping'] = [
            'id', 'profile_id', 'product_id', 'product_group_id', 'active',
            'catalog_cycles_mask', 'renewal_cycles_mask', 'markup_overrides_json',
            'setup_fee_overrides_json', 'respect_disabled_cycles',
            'overwrite_free_cycles', 'sync_setup_fees', 'rounding_mode',
            'created_at', 'updated_at',
        ];
        Capsule::$tables['mod_contabo_mapping'] = [];

        // Pre-condition: v4 profile columns genuinely missing.
        $before = SchemaHealth::requiredColumnsPresent();
        $this->assertFalse($before['healthy']);
        $this->assertContains('mod_contabo_profile.profile_mode', $before['missing']);

        $result = SchemaHealth::assertOrMigrate();

        $this->assertTrue($result['ok'], 'assertOrMigrate must succeed');
        $this->assertSame(3, $result['from']);
        $this->assertSame(Installer::SCHEMA_VERSION, $result['to']);
        $this->assertSame(16, $result["to"]);
        $this->assertNull($result['error']);

        // The migration must have added the v4 identity columns.
        $this->assertContains('profile_mode', Capsule::$columns['mod_contabo_profile']);
        $this->assertContains('profile_fingerprint_hash', Capsule::$columns['mod_contabo_profile']);
        $this->assertContains('profile_identity_json', Capsule::$columns['mod_contabo_profile']);

        // And the recorded schema_version is now current.
        $recorded = (int) Capsule::table('mod_contabo_settings')
            ->where('key', 'schema_version')->value('value');
        $this->assertSame(16, $recorded);

        // Post-condition: schema is now healthy.
        $after = SchemaHealth::requiredColumnsPresent();
        $this->assertTrue($after['healthy'], 'schema must be healthy after migration: ' . implode(',', $after['missing']));
    }

    /**
     * assertOrMigrate() on an already-current schema is a no-op that reports
     * from == to == SCHEMA_VERSION.
     */
    public function testAssertOrMigrateNoOpWhenCurrent(): void
    {
        Capsule::$columns['mod_contabo_settings'] = ['key', 'value', 'updated_at'];
        Capsule::$tables['mod_contabo_settings'] = [
            ['key' => 'schema_version', 'value' => (string) Installer::SCHEMA_VERSION, 'updated_at' => '2026-05-22 00:00:00'],
        ];

        $statementsBefore = count(Capsule::$statements);
        $result = SchemaHealth::assertOrMigrate();

        $this->assertTrue($result['ok']);
        $this->assertSame(Installer::SCHEMA_VERSION, $result['from']);
        $this->assertSame(Installer::SCHEMA_VERSION, $result['to']);
        $this->assertNull($result['error']);
        $this->assertSame($statementsBefore, count(Capsule::$statements), 'no migration work when current');
    }

    /**
     * The v4 required column set includes the three new profile identity
     * columns (Deliverable 3) plus the v3 mapping mask columns.
     */
    public function testV3RequiredColumnsExist(): void
    {
        $required = SchemaHealth::requiredColumnList();

        // v3 mapping mask columns.
        $this->assertContains('mod_contabo_mapping.catalog_cycles_mask', $required);
        $this->assertContains('mod_contabo_mapping.renewal_cycles_mask', $required);
        $this->assertContains('mod_contabo_mapping.markup_overrides_json', $required);
        $this->assertContains('mod_contabo_mapping.setup_fee_overrides_json', $required);
        $this->assertContains('mod_contabo_mapping.respect_disabled_cycles', $required);
        $this->assertContains('mod_contabo_mapping.overwrite_free_cycles', $required);
        $this->assertContains('mod_contabo_mapping.sync_setup_fees', $required);
        $this->assertContains('mod_contabo_mapping.rounding_mode', $required);

        // v4 profile identity columns.
        $this->assertContains('mod_contabo_profile.profile_mode', $required);
        $this->assertContains('mod_contabo_profile.profile_fingerprint_hash', $required);
        $this->assertContains('mod_contabo_profile.profile_identity_json', $required);
    }

    /**
     * Legacy apply_to_* columns must NOT appear in the runtime required set —
     * they were dropped in migrateTo3 and are forbidden at runtime.
     */
    public function testLegacyColumnsNotRequiredAtRuntime(): void
    {
        $required = SchemaHealth::requiredColumnList();

        $this->assertNotContains('mod_contabo_mapping.apply_to_monthly', $required);
        $this->assertNotContains('mod_contabo_mapping.apply_to_semiannually', $required);
        $this->assertNotContains('mod_contabo_mapping.apply_to_annually', $required);

        foreach ($required as $col) {
            $this->assertStringNotContainsString('apply_to_', $col, 'no apply_to_* column may be required at runtime');
        }
    }

    /**
     * requiredColumnsPresent() flags missing columns as table.column strings.
     */
    public function testRequiredColumnsPresentReportsMissing(): void
    {
        // Only the mapping table exists, fully populated; profile table absent.
        Capsule::$columns['mod_contabo_mapping'] = [
            'catalog_cycles_mask', 'renewal_cycles_mask', 'markup_overrides_json',
            'setup_fee_overrides_json', 'respect_disabled_cycles',
            'overwrite_free_cycles', 'sync_setup_fees', 'rounding_mode',
        ];

        $result = SchemaHealth::requiredColumnsPresent();

        $this->assertFalse($result['healthy']);
        $this->assertContains('mod_contabo_profile.profile_mode', $result['missing']);
        $this->assertContains('mod_contabo_profile.profile_fingerprint_hash', $result['missing']);
        $this->assertContains('mod_contabo_profile.profile_identity_json', $result['missing']);
        // Mapping columns all present → none of them listed missing.
        $this->assertNotContains('mod_contabo_mapping.catalog_cycles_mask', $result['missing']);
    }

    public function testPublicationTablesAreOptionalInCompatibilityMode(): void
    {
        foreach (['mod_contabo_catalog_versions', 'mod_contabo_catalog_items', 'mod_contabo_mapping_publications', 'mod_contabo_publication_approvals'] as $t) {
            unset(Capsule::$columns[$t]);
        }
        Capsule::$columns['mod_contabo_settings'] = ['key', 'value', 'updated_at'];
        Capsule::$tables['mod_contabo_settings'] = [
            ['key' => 'schema_version', 'value' => '14', 'updated_at' => '2026-09-06 00:00:00'],
        ];
        Capsule::$columns['mod_contabo_mapping'] = [
            'catalog_cycles_mask', 'renewal_cycles_mask', 'markup_overrides_json',
            'setup_fee_overrides_json', 'respect_disabled_cycles',
            'overwrite_free_cycles', 'sync_setup_fees', 'rounding_mode',
            'published_mapping_version', 'provider_sku_id', 'rust_catalog_version',
            'mapping_state', 'mapping_payload_hash', 'mapping_effective_at',
        ];
        Capsule::$columns['mod_contabo_profile'] = [
            'profile_mode', 'profile_fingerprint_hash', 'profile_identity_json',
            'product_scope_key', 'commercial_variant', 'audience_segment',
            'published_cycles_mask', 'deleted_at',
        ];
        Capsule::$columns['mod_securiacevps_schema'] = ['key', 'value'];
        Capsule::$columns['mod_securiacevps_order_snapshots'] = [
            'snapshot_uuid', 'service_id', 'state', 'payload_json',
            'configuration_hash', 'price_hash', 'sealed_at',
        ];
        Capsule::$columns['mod_securiacevps_resources'] = [
            'service_id', 'provider_account_id', 'provider_resource_id', 'provider_state',
            'provisioning_state', 'ownership_state', 'resource_version',
        ];
        Capsule::$columns['mod_securiacevps_operations'] = [
            'operation_uuid', 'service_id', 'operation_type', 'operation_generation',
            'state', 'command_id', 'request_fingerprint', 'idempotency_key',
            'fencing_token', 'unknown_outcome', 'correlation_id', 'payload_json',
        ];
        Capsule::$columns['mod_securiacevps_operation_attempts'] = [
            'operation_uuid', 'attempt_number', 'fencing_token', 'state',
        ];
        Capsule::$columns['mod_securiacevps_provider_requests'] = [
            'operation_uuid', 'request_fingerprint', 'idempotency_key', 'state', 'unknown_outcome',
        ];
        Capsule::$columns['mod_securiacevps_service_locks'] = [
            'service_id', 'operation_uuid', 'lease_owner', 'lease_expires_at', 'fencing_token',
        ];
        Capsule::$columns['mod_securiacevps_capabilities'] = [
            'provider_account_id', 'capability', 'state',
        ];
        Capsule::$columns['mod_securiacevps_reconciliation'] = [
            'finding_uuid', 'finding_type', 'severity', 'state', 'evidence_hash',
        ];
        Capsule::$columns['mod_securiacevps_adoption'] = [
            'service_id', 'provider_account_id', 'provider_resource_id', 'state', 'confidence',
        ];
        Capsule::$columns['mod_securiacevps_billing_sagas'] = [
            'saga_uuid', 'service_id', 'saga_type', 'state',
        ];
        Capsule::$columns['mod_securiacevps_audit_events'] = [
            'event_uuid', 'event_type', 'outcome', 'event_hash',
        ];
        Capsule::$columns['mod_securiacevps_operator_commands'] = [
            'command_uuid', 'command_type', 'requested_by_admin_id', 'state',
            'payload_hash', 'claim_token', 'claim_expires_at',
        ];
        Capsule::$columns['mod_securiacevps_secrets'] = [
            'secret_uuid', 'service_id', 'operation_uuid', 'secret_type',
            'encrypted_value', 'reveal_token_hash', 'reveal_token_ciphertext', 'expires_at',
        ];
        Capsule::$columns['mod_securiacevps_communications'] = [
            'communication_uuid', 'service_id', 'message_type', 'state',
            'payload_hash', 'claim_token', 'claim_expires_at',
        ];
        Capsule::$columns['mod_securiacevps_snapshot_inventory'] = [
            'service_id', 'provider_account_id', 'provider_resource_id', 'snapshot_id',
            'name', 'payload_hash', 'observed_at',
        ];

        // Schema v15/v16 scrape tables are not what this test is about: seed them
        // so only the optional publication/catalog tables are absent.
        foreach (SchemaHealth::requiredColumnList() as $qualified) {
            list($table, $column) = explode('.', $qualified, 2);
            if (!isset(Capsule::$columns[$table])) {
                Capsule::$columns[$table] = [];
            }
            if (!in_array($column, Capsule::$columns[$table], true)) {
                Capsule::$columns[$table][] = $column;
            }
        }

        $result = SchemaHealth::requiredColumnsPresent();

        $this->assertTrue($result['healthy']);
        $this->assertTrue($result['compatibility_mode']);
        $this->assertFalse($result['publication_catalog_supported']);
        $this->assertSame([], $result['missing']);
        $this->assertContains('mod_contabo_catalog_versions.catalog_version', $result['optional_missing']);
        $this->assertContains('mod_contabo_mapping_publications.mapping_version', $result['optional_missing']);
    }
}
