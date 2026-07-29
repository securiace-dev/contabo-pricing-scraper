<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;
use Illuminate\Database\Schema\Blueprint;

/**
 * Creates the mod_contabo_* tables on activation and runs schema migrations
 * between addon versions.
 */
class Installer
{
    public const SCHEMA_VERSION = 12;

    /** Tables created on activation. Order matters for FK references. */
    public function install(): void
    {
        $schema = Capsule::schema();

        if (!$schema->hasTable('mod_contabo_profile')) {
            $schema->create('mod_contabo_profile', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('slug', 120)->unique();
                $t->string('name', 200);
                $t->string('plan_slug', 80);
                $t->unsignedInteger('period_months');
                $t->string('region', 40)->nullable();
                $t->string('os', 80)->nullable();
                $t->json('options')->nullable();
                $t->string('tags', 200)->nullable();
                $t->enum('sync_strategy', ['manual', 'notify', 'auto-apply'])->default('notify');
                $t->boolean('active')->default(true);
                $t->unsignedBigInteger('latest_version_id')->nullable();
                $t->timestamps();
                $t->index(['plan_slug', 'period_months']);
                $t->index('active');
            });
        }

        if (!$schema->hasTable('mod_contabo_profile_version')) {
            $schema->create('mod_contabo_profile_version', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedBigInteger('profile_id');
                $t->unsignedInteger('version');
                $t->decimal('base_monthly_eur', 10, 4);
                $t->decimal('configured_monthly_eur', 10, 4);
                $t->decimal('setup_fee_eur', 10, 4)->default(0);
                $t->json('options_snapshot')->nullable();
                $t->json('specs_snapshot')->nullable();
                $t->decimal('fx_rate', 12, 6)->nullable();
                $t->string('fx_source', 80)->nullable();
                $t->decimal('fx_markup_pct', 5, 3)->default(0);
                $t->decimal('gst_pct', 5, 3)->default(0);
                $t->string('currency_iso', 6);
                $t->decimal('final_monthly', 12, 4);
                $t->decimal('final_setup', 12, 4)->default(0);
                $t->string('snapshot_generated_at', 40);
                $t->timestamps();
                $t->unique(['profile_id', 'version']);
                $t->index('profile_id');
            });
        }

        if (!$schema->hasTable('mod_contabo_mapping')) {
            $schema->create('mod_contabo_mapping', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedBigInteger('profile_id');
                $t->unsignedInteger('product_id');       // tblproducts.id
                $t->unsignedInteger('product_group_id')->nullable();
                $t->boolean('apply_to_monthly')->default(true);
                $t->boolean('apply_to_annually')->default(true);
                $t->boolean('apply_to_semiannually')->default(false);
                $t->boolean('active')->default(true);
                $t->timestamps();
                $t->index('profile_id');
                $t->unique(['profile_id', 'product_id']);
            });
        }

        if (!$schema->hasTable('mod_contabo_sync_log')) {
            $schema->create('mod_contabo_sync_log', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('trigger', 32);   // 'cron', 'manual', 'webhook'
                $t->enum('status', ['running', 'succeeded', 'failed', 'no-change'])->default('running');
                $t->timestamp('started_at')->useCurrent();
                $t->timestamp('finished_at')->nullable();
                $t->unsignedInteger('profiles_checked')->default(0);
                $t->unsignedInteger('profiles_changed')->default(0);
                $t->unsignedInteger('products_updated')->default(0);
                $t->text('error_message')->nullable();
                $t->json('summary')->nullable();
                $t->index('started_at');
                $t->index('status');
            });
        }

        if (!$schema->hasTable('mod_contabo_settings')) {
            $schema->create('mod_contabo_settings', static function (Blueprint $t): void {
                $t->string('key', 80)->primary();
                $t->text('value')->nullable();
                $t->timestamp('updated_at')->useCurrent()->useCurrentOnUpdate();
            });
        }

        // install() creates the tables at their ORIGINAL (v1) shape. Record the
        // schema as v1, then run the idempotent migration chain so a FRESH
        // activate ends up fully current (v2..vN columns + tables added).
        //
        // Without this, a clean install would stamp schema_version = SCHEMA_VERSION
        // while only the v1 columns exist — assertOrMigrate would then see
        // "already current" and never add the newer columns, leaving the schema
        // broken (catalog_cycles_mask, profile_mode, config_* tables, etc.
        // all missing). Prod never hit this because it activated at v1 and
        // upgraded step-by-step; a brand-new install must be brought current here.
        Capsule::table('mod_contabo_settings')->updateOrInsert(
            ['key' => 'schema_version'],
            ['value' => '1', 'updated_at' => date('Y-m-d H:i:s')],
        );
        $this->upgrade('fresh-install');
    }

    /**
     * Forward-only migrations between addon versions. Currently a no-op; add
     * cases as we evolve the schema.
     */
    public function upgrade(string $fromVersion): void
    {
        $current = (int) (Capsule::table('mod_contabo_settings')->where('key', 'schema_version')->value('value') ?? 0);

        for ($v = $current + 1; $v <= self::SCHEMA_VERSION; $v++) {
            $method = 'migrateTo' . $v;
            if (method_exists($this, $method)) {
                $this->{$method}();
                Capsule::table('mod_contabo_settings')->updateOrInsert(
                    ['key' => 'schema_version'],
                    ['value' => (string) $v, 'updated_at' => date('Y-m-d H:i:s')],
                );
            }
        }
    }

    /**
     * Schema v2 — WHMCS Renewal Pricing Policy Engine.
     *
     * Adds 6 new tables (service_policy, price_decision, pricing_action,
     * price_change_schedule, price_notice, repricing_lock), additive columns
     * to profile + profile_version, and 12 new settings keys.
     *
     * Idempotent: every create guarded by hasTable; every addColumn by hasColumn.
     */
    public function migrateTo2(): void
    {
        $schema = Capsule::schema();
        $now = date('Y-m-d H:i:s');

        // ── mod_contabo_service_policy ─────────────────────────────────────
        if (!$schema->hasTable('mod_contabo_service_policy')) {
            $schema->create('mod_contabo_service_policy', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedInteger('service_id')->unique();
                $t->enum('policy', [
                    'manual',
                    'lifetime',
                    'frozen_until',
                    'current_term',
                    'margin_floor',
                    'reprice_renewal',
                ])->default('current_term');
                $t->decimal('locked_price', 12, 4)->nullable();
                $t->char('locked_currency', 3)->nullable();
                $t->decimal('manual_override_price', 12, 4)->nullable();
                $t->string('manual_override_reason', 255)->nullable();
                $t->unsignedInteger('manual_override_created_by_admin_id')->nullable();
                $t->timestamp('manual_override_created_at')->nullable();
                $t->timestamp('manual_override_expires_at')->nullable();
                $t->decimal('margin_floor_pct', 5, 2)->nullable();
                $t->date('frozen_until')->nullable();
                $t->boolean('allow_auto_decrease')->default(false);
                $t->decimal('min_sell_price', 12, 4)->nullable();
                $t->text('notes')->nullable();
                $t->timestamps();
                $t->index('policy');
            });
        }

        // ── mod_contabo_price_decision ─────────────────────────────────────
        if (!$schema->hasTable('mod_contabo_price_decision')) {
            $schema->create('mod_contabo_price_decision', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedInteger('service_id');
                $t->unsignedBigInteger('profile_id')->nullable();
                $t->unsignedBigInteger('profile_version_id')->nullable();
                $t->char('cron_run_id', 36);
                $t->timestamp('decided_at')->useCurrent();
                $t->timestamp('effective_at')->nullable();
                $t->string('billing_cycle', 20);
                $t->unsignedTinyInteger('cycle_months');
                $t->char('currency', 3);
                $t->decimal('old_price', 12, 4);
                $t->decimal('proposed_new_price', 12, 4);
                $t->decimal('vendor_cost_eur_monthly', 12, 4)->nullable();
                $t->decimal('vendor_cost_local_monthly', 12, 4)->nullable();
                $t->decimal('vendor_cost_local_for_cycle', 12, 4)->nullable();
                $t->decimal('fx_rate', 12, 6)->nullable();
                $t->decimal('fx_buffer_pct', 5, 2)->nullable();
                $t->string('tax_mode_snapshot', 40)->nullable();
                $t->decimal('vendor_tax_rate_pct', 5, 2)->nullable();
                $t->decimal('vendor_tax_amount', 12, 4)->nullable();
                $t->boolean('vendor_tax_recoverable')->default(false);
                $t->decimal('output_tax_rate_pct', 5, 2)->nullable();
                $t->decimal('output_tax_amount', 12, 4)->nullable();
                $t->boolean('prices_include_output_tax')->default(false);
                $t->decimal('sell_price_gross_for_cycle', 12, 4)->nullable();
                $t->decimal('sell_price_net_revenue_for_cycle', 12, 4)->nullable();
                $t->decimal('margin_amount_for_cycle', 12, 4)->nullable();
                $t->decimal('margin_pct', 6, 3)->nullable();
                $t->string('policy_used', 40);
                $t->boolean('applied')->default(false);
                $t->string('applied_via', 40)->nullable();
                $t->string('skip_reason', 60)->nullable();
                $t->boolean('requires_notice')->default(false);
                $t->boolean('requires_admin_approval')->default(false);
                $t->unsignedBigInteger('notice_id')->nullable();
                $t->unsignedBigInteger('parent_decision_id')->nullable();
                $t->timestamps();
                $t->index(['service_id', 'decided_at']);
                $t->index('cron_run_id');
                $t->index(['applied', 'requires_admin_approval']);
                $t->index('policy_used');
            });
        }

        // ── mod_contabo_pricing_action ─────────────────────────────────────
        // Immutable action ledger; separate from decisions for audit integrity.
        if (!$schema->hasTable('mod_contabo_pricing_action')) {
            $schema->create('mod_contabo_pricing_action', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->enum('action_type', [
                    'approve',
                    'reject',
                    'defer',
                    'force_approve',
                    'cancel_schedule',
                    'manual_override_set',
                    'manual_override_cleared',
                    'policy_changed',
                    'phase_changed',
                    'apply',
                ]);
                $t->unsignedInteger('service_id')->nullable();
                $t->unsignedBigInteger('decision_id')->nullable();
                $t->unsignedBigInteger('schedule_id')->nullable();
                $t->unsignedInteger('admin_id')->default(0);
                $t->text('reason')->nullable();
                $t->timestamp('created_at')->useCurrent();
                $t->index(['service_id', 'created_at']);
                $t->index(['admin_id', 'created_at']);
                $t->index('action_type');
            });
        }

        // ── mod_contabo_price_change_schedule ──────────────────────────────
        if (!$schema->hasTable('mod_contabo_price_change_schedule')) {
            $schema->create('mod_contabo_price_change_schedule', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->enum('scope', ['service', 'profile']);
                $t->unsignedInteger('service_id')->nullable();
                $t->unsignedBigInteger('profile_id')->nullable();
                $t->decimal('new_price', 12, 4);
                $t->char('currency', 3);
                $t->timestamp('effective_at');
                $t->unsignedInteger('notice_days')->default(30);
                $t->string('email_template_name', 120)->nullable();
                $t->text('customvars_json')->nullable();
                $t->enum('status', ['pending', 'notified', 'applied', 'cancelled', 'superseded'])->default('pending');
                $t->timestamp('applied_at')->nullable();
                $t->timestamp('cancelled_at')->nullable();
                $t->unsignedInteger('cancelled_by_admin_id')->nullable();
                $t->unsignedInteger('created_by_admin_id');
                $t->timestamps();
                $t->index(['effective_at', 'status']);
                $t->index(['scope', 'status']);
            });
        }

        // ── mod_contabo_price_notice ───────────────────────────────────────
        if (!$schema->hasTable('mod_contabo_price_notice')) {
            $schema->create('mod_contabo_price_notice', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('idempotency_key', 160)->unique();
                $t->unsignedInteger('service_id');
                $t->enum('notice_type', ['pre_change', 'reminder', 'confirmation', 'force_approve_alert']);
                $t->decimal('target_price', 12, 4);
                $t->char('currency', 3);
                $t->timestamp('effective_at')->nullable();
                $t->unsignedInteger('notice_days_before')->default(30);
                $t->timestamp('scheduled_send_at')->nullable();
                $t->enum('status', ['pending', 'sent', 'failed', 'cancelled', 'superseded'])->default('pending');
                $t->timestamp('sent_at')->nullable();
                $t->string('failure_reason', 255)->nullable();
                $t->string('email_template_name', 120);
                $t->string('email_custom_type', 40)->default('product');
                $t->unsignedInteger('related_id');
                $t->text('customvars_json')->nullable();
                $t->unsignedBigInteger('decision_id')->nullable();
                $t->timestamps();
                $t->index(['scheduled_send_at', 'status']);
                $t->index(['service_id', 'notice_type']);
            });
        }

        // ── mod_contabo_repricing_lock ─────────────────────────────────────
        if (!$schema->hasTable('mod_contabo_repricing_lock')) {
            $schema->create('mod_contabo_repricing_lock', static function (Blueprint $t): void {
                $t->string('name', 64)->primary();
                $t->timestamp('locked_until')->nullable();
                $t->string('holder', 64)->nullable();
            });
        }

        // ── Additive columns: mod_contabo_profile ──────────────────────────
        if ($schema->hasTable('mod_contabo_profile')) {
            $schema->table('mod_contabo_profile', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_profile', 'default_policy')) {
                    $t->enum('default_policy', [
                        'manual',
                        'lifetime',
                        'frozen_until',
                        'current_term',
                        'margin_floor',
                        'reprice_renewal',
                    ])->default('current_term');
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'margin_floor_pct')) {
                    $t->decimal('margin_floor_pct', 5, 2)->default(15.00);
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'fx_buffer_pct')) {
                    $t->decimal('fx_buffer_pct', 5, 2)->default(2.00);
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'large_increase_threshold_pct')) {
                    $t->decimal('large_increase_threshold_pct', 5, 2)->default(10.00);
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'max_increase_pct')) {
                    $t->decimal('max_increase_pct', 5, 2)->default(25.00);
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'notice_days_default')) {
                    $t->unsignedInteger('notice_days_default')->default(30);
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'allow_auto_decrease')) {
                    $t->boolean('allow_auto_decrease')->default(false);
                }
            });
        }

        // ── Additive columns: mod_contabo_profile_version ──────────────────
        if ($schema->hasTable('mod_contabo_profile_version')) {
            $schema->table('mod_contabo_profile_version', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_profile_version', 'sell_price_local_monthly')) {
                    $t->decimal('sell_price_local_monthly', 12, 4)->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_profile_version', 'margin_pct_snapshot')) {
                    $t->decimal('margin_pct_snapshot', 5, 2)->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_profile_version', 'markup_strategy')) {
                    $t->string('markup_strategy', 32)->default('cost_plus_pct');
                }
                if (!$schema->hasColumn('mod_contabo_profile_version', 'markup_value')) {
                    $t->decimal('markup_value', 10, 4)->default(0);
                }
                if (!$schema->hasColumn('mod_contabo_profile_version', 'vendor_tax_rate_pct_snapshot')) {
                    $t->decimal('vendor_tax_rate_pct_snapshot', 5, 2)->nullable();
                }
            });
        }

        // ── New settings keys (idempotent via updateOrInsert) ─────────────
        $defaults = [
            'repricing_phase'                    => 'observe',
            'tax_registration_mode'              => 'unregistered_no_output_tax',
            'vendor_tax_rate_pct'                => '18.00',
            'vendor_tax_recoverable'             => '0',
            'charge_output_tax_to_client'        => '0',
            'prices_include_output_tax'          => '0',
            'output_tax_rate_pct'                => '0.00',
            'payment_buffer_pct'                 => '2.00',
            'safety_window_days'                 => '7',
            'invoice_generation_lookahead_days'  => '14',
            'email_template_name_notice'         => 'Contabo Pricing Change Notice',
            'email_template_name_confirmation'   => 'Contabo Pricing Change Confirmation',
        ];
        foreach ($defaults as $key => $value) {
            Capsule::table('mod_contabo_settings')->updateOrInsert(
                ['key' => $key],
                ['value' => $value, 'updated_at' => $now],
            );
        }
    }

    /**
     * Schema v3 — Phase A.5 cycle-aware mapping.
     *
     * Replaces the three legacy boolean flags on `mod_contabo_mapping`
     * (`apply_to_monthly`, `apply_to_semiannually`, `apply_to_annually`) with
     * two integer bitmasks (`catalog_cycles_mask` / `renewal_cycles_mask`),
     * adds per-cycle markup/setup-fee override JSON columns plus rounding
     * configuration, and introduces a `mod_contabo_catalog_audit` table that
     * SyncEngine will populate (one row per catalog price write).
     *
     * Migration steps (executed in this order):
     *   A. Pre-flight check + row count + column detection.
     *   B. Backup table `mod_contabo_mapping_backup_v3_YmdHis`.
     *   C. Add the nine new columns (idempotent per `hasColumn`).
     *   D. Backfill bitmasks from legacy booleans via raw SQL bitwise OR.
     *   E. Validation gate — abort BEFORE drop if any row fails.
     *   F. Drop legacy boolean columns.
     *   G. Create `mod_contabo_catalog_audit` table.
     *   H. Bump `schema_version` in `mod_contabo_settings` to '3'.
     *
     * Every step is idempotent: re-running the migration is a no-op.
     *
     * No native JSON column type is used (FastPanel PHP 7.4 + MySQL/MariaDB
     * compatibility is unknown); `markup_overrides_json` and
     * `setup_fee_overrides_json` are LONGTEXT and validated in PHP.
     */
    public function migrateTo3(): void
    {
        $schema = Capsule::schema();
        $now = date('Y-m-d H:i:s');

        // ── A. Pre-flight ───────────────────────────────────────────────────
        // Fresh install with no v2 mapping table is fine: the schema-version
        // bump will record us at v3 and the new columns/table get created
        // here directly.
        $hasMappingTable = $schema->hasTable('mod_contabo_mapping');
        if ($hasMappingTable) {
            $rowCount = (int) Capsule::table('mod_contabo_mapping')->count();
            $hasLegacyMonthly      = $schema->hasColumn('mod_contabo_mapping', 'apply_to_monthly');
            $hasLegacySemi         = $schema->hasColumn('mod_contabo_mapping', 'apply_to_semiannually');
            $hasLegacyAnnual       = $schema->hasColumn('mod_contabo_mapping', 'apply_to_annually');
            $hasNewCatalogMask     = $schema->hasColumn('mod_contabo_mapping', 'catalog_cycles_mask');
            $hasNewRenewalMask     = $schema->hasColumn('mod_contabo_mapping', 'renewal_cycles_mask');

            logActivity(sprintf(
                'Contabo Pricing migrateTo3: pre-flight — rows=%d, legacy_columns=[m=%s,s=%s,a=%s], new_masks=[c=%s,r=%s]',
                $rowCount,
                $hasLegacyMonthly ? 'yes' : 'no',
                $hasLegacySemi    ? 'yes' : 'no',
                $hasLegacyAnnual  ? 'yes' : 'no',
                $hasNewCatalogMask ? 'yes' : 'no',
                $hasNewRenewalMask ? 'yes' : 'no'
            ));

            // ── B. Backup table (only if we have real data to preserve and
            //       legacy columns still exist — i.e. a real upgrade is about
            //       to happen, not an idempotent re-run). ──────────────────
            $hasAnyLegacy = $hasLegacyMonthly || $hasLegacySemi || $hasLegacyAnnual;
            if ($rowCount > 0 && $hasAnyLegacy) {
                $backupName = 'mod_contabo_mapping_backup_v3_' . date('YmdHis');
                // CREATE TABLE LIKE + INSERT SELECT preserves schema + data.
                Capsule::connection()->statement(
                    'CREATE TABLE IF NOT EXISTS ' . $backupName . ' LIKE mod_contabo_mapping'
                );
                Capsule::connection()->statement(
                    'INSERT INTO ' . $backupName . ' SELECT * FROM mod_contabo_mapping'
                );
                $copied = (int) Capsule::table($backupName)->count();
                logActivity(sprintf(
                    'Contabo Pricing migrateTo3: backup table %s created, %d rows copied',
                    $backupName,
                    $copied
                ));
                if ($copied !== $rowCount) {
                    throw new \RuntimeException(sprintf(
                        'migrateTo3 backup row-count mismatch: source=%d, backup=%d (table %s)',
                        $rowCount,
                        $copied,
                        $backupName
                    ));
                }
            } elseif ($rowCount === 0 && !$hasNewCatalogMask && !$hasAnyLegacy) {
                // Partial-state guard: empty table with neither legacy nor
                // new columns is suspicious. Log + bail rather than silently
                // proceeding into an undefined schema.
                throw new \RuntimeException(
                    'migrateTo3 aborted: mod_contabo_mapping has zero rows AND no legacy columns AND no new columns — partial state'
                );
            }

            // ── C. Add new columns (idempotent per hasColumn) ──────────────
            $schema->table('mod_contabo_mapping', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_mapping', 'catalog_cycles_mask')) {
                    $t->unsignedInteger('catalog_cycles_mask')->default(0);
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'renewal_cycles_mask')) {
                    $t->unsignedInteger('renewal_cycles_mask')->default(0);
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'markup_overrides_json')) {
                    // LONGTEXT — never JSON. PHP-side validated.
                    $t->longText('markup_overrides_json')->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'respect_disabled_cycles')) {
                    $t->boolean('respect_disabled_cycles')->default(true);
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'overwrite_free_cycles')) {
                    $t->boolean('overwrite_free_cycles')->default(false);
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'sync_setup_fees')) {
                    $t->boolean('sync_setup_fees')->default(false);
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'setup_fee_overrides_json')) {
                    // LONGTEXT — never JSON. PHP-side validated.
                    $t->longText('setup_fee_overrides_json')->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'rounding_mode')) {
                    $t->string('rounding_mode', 32)->default('exact_2_decimals');
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'cycle_pricing_notes')) {
                    $t->text('cycle_pricing_notes')->nullable();
                }
            });

            // Re-detect now that the columns exist.
            $hasLegacyMonthly = $schema->hasColumn('mod_contabo_mapping', 'apply_to_monthly');
            $hasLegacySemi    = $schema->hasColumn('mod_contabo_mapping', 'apply_to_semiannually');
            $hasLegacyAnnual  = $schema->hasColumn('mod_contabo_mapping', 'apply_to_annually');

            // ── D. Backfill bitmasks via raw SQL bitwise OR. ───────────────
            // Re-running is safe: OR'ing a bit that's already set leaves the
            // mask unchanged. Each bit only OR'd when its legacy column still
            // exists (the column gets dropped in step F).
            if ($hasLegacyMonthly) {
                Capsule::connection()->statement(
                    'UPDATE mod_contabo_mapping SET '
                    . 'catalog_cycles_mask = catalog_cycles_mask | ' . (1 << CycleSet::BIT_MONTHLY) . ', '
                    . 'renewal_cycles_mask = renewal_cycles_mask | ' . (1 << CycleSet::BIT_MONTHLY) . ' '
                    . 'WHERE apply_to_monthly = 1'
                );
            }
            if ($hasLegacySemi) {
                Capsule::connection()->statement(
                    'UPDATE mod_contabo_mapping SET '
                    . 'catalog_cycles_mask = catalog_cycles_mask | ' . (1 << CycleSet::BIT_SEMIANNUAL) . ', '
                    . 'renewal_cycles_mask = renewal_cycles_mask | ' . (1 << CycleSet::BIT_SEMIANNUAL) . ' '
                    . 'WHERE apply_to_semiannually = 1'
                );
            }
            if ($hasLegacyAnnual) {
                Capsule::connection()->statement(
                    'UPDATE mod_contabo_mapping SET '
                    . 'catalog_cycles_mask = catalog_cycles_mask | ' . (1 << CycleSet::BIT_ANNUALLY) . ', '
                    . 'renewal_cycles_mask = renewal_cycles_mask | ' . (1 << CycleSet::BIT_ANNUALLY) . ' '
                    . 'WHERE apply_to_annually = 1'
                );
            }
            logActivity('Contabo Pricing migrateTo3: bitmask backfill complete');

            // ── E. Validation gate ─────────────────────────────────────────
            // For every row: verify the new masks are consistent with the
            // legacy booleans (if they still exist) and that no bit > 5 is
            // set. Abort BEFORE the drop in F if any row fails.
            if ($hasLegacyMonthly || $hasLegacySemi || $hasLegacyAnnual) {
                $rows = Capsule::table('mod_contabo_mapping')->get();
                $failures = [];
                foreach ($rows as $row) {
                    $r = is_array($row) ? $row : (array) $row;
                    $catalog = (int) ($r['catalog_cycles_mask'] ?? 0);
                    $renewal = (int) ($r['renewal_cycles_mask'] ?? 0);
                    $id      = (int) ($r['id'] ?? 0);

                    // Range check: no bit above bit 5.
                    if ($catalog < 0 || $catalog > CycleSet::MASK_MAX
                        || $renewal < 0 || $renewal > CycleSet::MASK_MAX) {
                        $failures[] = sprintf(
                            'row id=%d out-of-range mask (catalog=%d, renewal=%d, max=%d)',
                            $id,
                            $catalog,
                            $renewal,
                            CycleSet::MASK_MAX
                        );
                        continue;
                    }

                    // Backfill consistency: every "1" in legacy must be a "1"
                    // in both new masks. Extra bits in the new masks are fine
                    // (admin may have already pre-populated them).
                    if ($hasLegacyMonthly && (int) ($r['apply_to_monthly'] ?? 0) === 1) {
                        $bit = 1 << CycleSet::BIT_MONTHLY;
                        if (!($catalog & $bit) || !($renewal & $bit)) {
                            $failures[] = sprintf('row id=%d: legacy monthly=1 but bit 0 not set in both masks', $id);
                        }
                    }
                    if ($hasLegacySemi && (int) ($r['apply_to_semiannually'] ?? 0) === 1) {
                        $bit = 1 << CycleSet::BIT_SEMIANNUAL;
                        if (!($catalog & $bit) || !($renewal & $bit)) {
                            $failures[] = sprintf('row id=%d: legacy semiannually=1 but bit 2 not set in both masks', $id);
                        }
                    }
                    if ($hasLegacyAnnual && (int) ($r['apply_to_annually'] ?? 0) === 1) {
                        $bit = 1 << CycleSet::BIT_ANNUALLY;
                        if (!($catalog & $bit) || !($renewal & $bit)) {
                            $failures[] = sprintf('row id=%d: legacy annually=1 but bit 3 not set in both masks', $id);
                        }
                    }
                }
                if ($failures !== []) {
                    $msg = 'migrateTo3 validation gate FAILED — legacy columns NOT dropped. Failures: '
                         . implode('; ', array_slice($failures, 0, 10))
                         . (count($failures) > 10 ? sprintf(' (+%d more)', count($failures) - 10) : '');
                    logActivity('Contabo Pricing ' . $msg);
                    throw new \RuntimeException($msg);
                }
                logActivity(sprintf(
                    'Contabo Pricing migrateTo3: validation gate passed for %d rows',
                    count($rows)
                ));
            }

            // ── F. Drop legacy boolean columns ─────────────────────────────
            $schema->table('mod_contabo_mapping', static function (Blueprint $t) use ($schema): void {
                if ($schema->hasColumn('mod_contabo_mapping', 'apply_to_monthly')) {
                    $t->dropColumn('apply_to_monthly');
                }
                if ($schema->hasColumn('mod_contabo_mapping', 'apply_to_semiannually')) {
                    $t->dropColumn('apply_to_semiannually');
                }
                if ($schema->hasColumn('mod_contabo_mapping', 'apply_to_annually')) {
                    $t->dropColumn('apply_to_annually');
                }
            });
        }

        // ── G. mod_contabo_catalog_audit ───────────────────────────────────
        // Always create (regardless of whether mod_contabo_mapping existed).
        // SyncEngine (Agent B) will INSERT one row per catalog price write
        // attempt — applied or skipped — keyed by sync_batch_id (a UUID per
        // sync run).
        if (!$schema->hasTable('mod_contabo_catalog_audit')) {
            $schema->create('mod_contabo_catalog_audit', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('sync_batch_id', 36);
                $t->unsignedInteger('product_id');
                $t->unsignedInteger('currency_id');
                $t->string('cycle', 20);              // WHMCS literal
                $t->unsignedTinyInteger('cycle_months');
                $t->string('recurring_column', 20);   // monthly / quarterly / ...
                $t->string('setup_fee_column', 20);   // msetupfee / qsetupfee / ...
                $t->decimal('old_price', 16, 4)->nullable();
                $t->decimal('new_price', 16, 4)->nullable();
                $t->decimal('old_setup_fee', 16, 4)->nullable();
                $t->decimal('new_setup_fee', 16, 4)->nullable();
                $t->enum('price_status_before', ['disabled', 'free', 'priced', 'absent']);
                $t->string('markup_strategy_used', 32)->nullable();
                $t->decimal('markup_value_used', 12, 4)->nullable();
                $t->decimal('pre_round_price', 16, 4)->nullable();
                $t->decimal('rounded_price', 16, 4)->nullable();
                $t->string('rounding_mode', 32)->nullable();
                $t->string('skipped_reason', 64)->nullable();
                $t->boolean('applied')->default(false);
                $t->timestamp('created_at')->useCurrent();
                $t->index('sync_batch_id');
                $t->index(['product_id', 'cycle']);
            });
        }

        // ── H. mod_contabo_price_decision — Phase A.5 metadata sidecar ─────
        // RenewalEngine (Agent C) emits a JSON-encoded per-cycle audit blob
        // (markup_strategy_used / markup_value_used / pre_round_price /
        // rounded_price / rounding_mode / cycle_recurring_column /
        // cycle_setup_fee_column / catalog_cycle_enabled /
        // renewal_cycle_enabled) on every decision row. These fields don't
        // warrant individual columns (low cardinality query patterns; they're
        // for replay + dashboard drill-down, not WHERE clauses). LONGTEXT so
        // we never truncate.
        if ($schema->hasTable('mod_contabo_price_decision')) {
            $schema->table('mod_contabo_price_decision', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_price_decision', 'metadata_json')) {
                    $t->longText('metadata_json')->nullable();
                }
            });
        }

        // ── I. mod_contabo_price_change_schedule — Phase A.5 cycle scoping ─
        // ScheduledChangeProcessor (Agent C) reads cycles_mask to determine
        // which cycles a scheduled change applies to, plus the two routing
        // flags applies_to_catalog / applies_to_renewals. Legacy rows written
        // before A.5 default cycles_mask=0 → treated as "all recurring cycles"
        // by the processor; applies_to_catalog defaults to 0 (renewals only).
        if ($schema->hasTable('mod_contabo_price_change_schedule')) {
            $schema->table('mod_contabo_price_change_schedule', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_price_change_schedule', 'cycles_mask')) {
                    $t->unsignedInteger('cycles_mask')->default(0);
                }
                if (!$schema->hasColumn('mod_contabo_price_change_schedule', 'applies_to_catalog')) {
                    $t->boolean('applies_to_catalog')->default(false);
                }
                if (!$schema->hasColumn('mod_contabo_price_change_schedule', 'applies_to_renewals')) {
                    $t->boolean('applies_to_renewals')->default(true);
                }
            });
        }

        // ── J. Schema-version bump is performed by upgrade() after this
        //       method returns; nothing to do here.
    }

    /**
     * Schema v4 — Phase A.5.1 mode-aware profile identity foundation.
     *
     * Adds three additive columns to `mod_contabo_profile` that underpin the
     * graceful duplicate-profile handling + the A.6 configurable-product mode:
     *
     *   - profile_mode             VARCHAR(40) NOT NULL DEFAULT 'fixed_admin_profile'
     *                              enum-by-convention: 'fixed_admin_profile' (current)
     *                              | 'customer_configurable_product' (reserved, A.6).
     *   - profile_fingerprint_hash CHAR(64) NULL  — sha-of canonical identity
     *                              projection, used to disambiguate slug clashes.
     *   - profile_identity_json    LONGTEXT NULL   — the canonical identity
     *                              projection used to compute the hash (audit /
     *                              re-fingerprint trail). LONGTEXT, never JSON.
     *
     * Idempotent: every column add is guarded by hasColumn, so re-running this
     * migration is a no-op. The schema-version bump to 4 is performed by
     * upgrade() after this method returns (mirrors migrateTo2/migrateTo3).
     */
    public function migrateTo4(): void
    {
        $schema = Capsule::schema();

        if (!$schema->hasTable('mod_contabo_profile')) {
            // Fresh schema without the profile table is nothing to migrate;
            // install() will have created it at the current version.
            logActivity('Contabo Pricing migrateTo4: mod_contabo_profile table absent — nothing to migrate');
            return;
        }

        $addedProfileMode        = false;
        $addedFingerprintHash    = false;
        $addedIdentityJson       = false;

        $schema->table('mod_contabo_profile', static function (Blueprint $t) use (
            $schema,
            &$addedProfileMode,
            &$addedFingerprintHash,
            &$addedIdentityJson
        ): void {
            if (!$schema->hasColumn('mod_contabo_profile', 'profile_mode')) {
                $t->string('profile_mode', 40)->default('fixed_admin_profile');
                $addedProfileMode = true;
            }
            if (!$schema->hasColumn('mod_contabo_profile', 'profile_fingerprint_hash')) {
                $t->char('profile_fingerprint_hash', 64)->nullable();
                $addedFingerprintHash = true;
            }
            if (!$schema->hasColumn('mod_contabo_profile', 'profile_identity_json')) {
                // LONGTEXT — never JSON. PHP-side validated.
                $t->longText('profile_identity_json')->nullable();
                $addedIdentityJson = true;
            }
        });

        logActivity(sprintf(
            'Contabo Pricing migrateTo4: profile identity columns — profile_mode=%s, profile_fingerprint_hash=%s, profile_identity_json=%s',
            $addedProfileMode ? 'added' : 'already-present',
            $addedFingerprintHash ? 'added' : 'already-present',
            $addedIdentityJson ? 'added' : 'already-present'
        ));

        // ── Schema-version bump is performed by upgrade() after this method
        //    returns; nothing to do here (mirrors migrateTo2/migrateTo3).
    }

    /**
     * Schema v5 — Phase A.6.1 configurable-options foundation.
     *
     * Adds the addon-owned link / capability / compatibility / snapshot tables
     * that A.6.2+ will populate, plus three additive identity columns on
     * `mod_contabo_profile` (product_scope_key / commercial_variant /
     * audience_segment — amendment 7 of the A.5.2 design report).
     *
     * Foundation only: NO apply mode, NO UI, NO WHMCS-table writes here. Every
     * table created is addon-owned (mod_contabo_ prefix); not a single
     * tblproductconfig* / tblpricing row is touched (those go through
     * WhmcsConfigOptionsAdapter in a later step — amendment 3).
     *
     * Idempotent: every create() is hasTable-guarded and every column add is
     * hasColumn-guarded, so re-running migrateTo5() is a no-op (no duplicate
     * tables, no duplicate columns). The schema-version bump to 5 is performed
     * by upgrade() after this method returns (mirrors migrateTo2/3/4).
     *
     * LONGTEXT is used for every structured-blob column — never the native JSON
     * column type (FastPanel PHP 7.4 + unknown MySQL/MariaDB JSON support).
     */
    public function migrateTo5(): void
    {
        $schema = Capsule::schema();

        // ── 1. mod_contabo_config_group_link ───────────────────────────────
        // Maps an addon profile → a WHMCS configurable-option group, per
        // product + group_key. UNIQUE(profile_id, whmcs_product_id, group_key)
        // (NOT unique on profile_id alone) so a profile can own multiple groups
        // across products / scopes (amendment 7 / design §8).
        if (!$schema->hasTable('mod_contabo_config_group_link')) {
            $schema->create('mod_contabo_config_group_link', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedBigInteger('profile_id');
                $t->unsignedInteger('whmcs_product_id');
                $t->string('group_key', 60);
                $t->unsignedInteger('whmcs_group_id')->nullable();
                $t->boolean('enabled')->default(true);
                $t->timestamps();
                $t->unique(['profile_id', 'whmcs_product_id', 'group_key'], 'uq_cfg_group_link');
                $t->index('profile_id');
                $t->index('whmcs_product_id');
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_config_group_link');
        }

        // ── 2. mod_contabo_config_option_link ──────────────────────────────
        // Maps (profile_id, dimension_key) → one WHMCS configurable option,
        // carrying the admin-curated exposure flags (design §3). All flags are
        // TINYINT(1); UNIQUE(profile_id, dimension_key).
        if (!$schema->hasTable('mod_contabo_config_option_link')) {
            $schema->create('mod_contabo_config_option_link', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedBigInteger('profile_id');
                $t->string('dimension_key', 60);
                $t->unsignedInteger('whmcs_option_id')->nullable();
                $t->unsignedTinyInteger('optiontype')->default(0);
                $t->boolean('enabled')->default(true);
                // Admin-curated exposure flags (preview-first; nothing exposed
                // until ticked — design §3 / amendment 8).
                $t->boolean('expose_to_customer')->default(false);
                $t->boolean('hidden')->default(false);
                $t->boolean('deprecated')->default(false);
                $t->boolean('allowed_for_new_orders')->default(false);
                $t->boolean('allowed_on_create')->default(false);
                $t->boolean('allowed_post_provision')->default(false);
                $t->boolean('allowed_on_reinstall')->default(false);
                $t->boolean('allowed_on_upgrade')->default(false);
                $t->boolean('allowed_on_downgrade')->default(false);
                $t->boolean('pass_to_provisioning')->default(false);
                $t->boolean('destructive_if_changed')->default(false);
                $t->boolean('requires_confirmation')->default(false);
                $t->boolean('requires_admin_approval')->default(false);
                $t->string('default_value', 160)->nullable();
                $t->timestamps();
                $t->unique(['profile_id', 'dimension_key'], 'uq_cfg_option_link');
                $t->index('profile_id');
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_config_option_link');
        }

        // ── 3. mod_contabo_config_option_value_link ────────────────────────
        // Maps an option-link → one WHMCS sub-option value. contabo_value_key /
        // contabo_label are the round-trip keys provisioning uses to map a
        // WHMCS sub-option back to a Contabo value (design §8, §17). The
        // per-value monthly EUR delta is the marginal cost MarginCalculator
        // prices in A.6.2. UNIQUE(option_link_id, contabo_value_key) and
        // UNIQUE(whmcs_sub_id); contabo_label INDEXed for round-trip lookups.
        if (!$schema->hasTable('mod_contabo_config_option_value_link')) {
            $schema->create('mod_contabo_config_option_value_link', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedBigInteger('option_link_id');
                $t->string('contabo_value_key', 160);
                $t->string('contabo_label', 190);
                $t->unsignedInteger('whmcs_sub_id')->nullable();
                $t->boolean('is_default')->default(false);
                $t->decimal('monthly_eur_delta', 12, 4)->default(0);
                $t->unsignedBigInteger('capability_id')->nullable();
                $t->unsignedBigInteger('compatibility_id')->nullable();
                $t->timestamps();
                $t->unique(['option_link_id', 'contabo_value_key'], 'uq_cfg_value_link');
                $t->unique('whmcs_sub_id', 'uq_cfg_value_sub');
                $t->index('option_link_id');
                $t->index('contabo_label');
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_config_option_value_link');
        }

        // ── 4. mod_contabo_config_option_audit ─────────────────────────────
        // Append-only sync audit (design §8). One row per observed/applied/
        // skipped action, keyed by sync_batch_id (UUID per sync run). action is
        // an enum-by-varchar; old/new value blobs are LONGTEXT (never JSON).
        if (!$schema->hasTable('mod_contabo_config_option_audit')) {
            $schema->create('mod_contabo_config_option_audit', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('sync_batch_id', 36);
                $t->unsignedBigInteger('profile_id');
                $t->string('dimension_key', 60)->nullable();
                $t->string('target_table', 40);
                $t->unsignedInteger('target_id')->nullable();
                // enum-by-varchar: insert|update|delete|skip_no_change|
                // skip_disabled|observed|error.
                $t->string('action', 40);
                $t->longText('old_value_json')->nullable();
                $t->longText('new_value_json')->nullable();
                $t->string('note', 255)->nullable();
                $t->timestamp('created_at')->useCurrent();
                $t->index('sync_batch_id');
                $t->index(['profile_id', 'created_at']);
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_config_option_audit');
        }

        // ── 5. mod_contabo_option_capability ───────────────────────────────
        // Per (plan, dimension, value) capability matrix (design §4). Drives
        // whether a change can auto-apply post-provision and how destructive it
        // is. capability_source defaults to 'manual_assumption'; only
        // api_verified may auto-apply destructive/in-place changes (amendment
        // 6). All capability/flag columns TINYINT(1).
        if (!$schema->hasTable('mod_contabo_option_capability')) {
            $schema->create('mod_contabo_option_capability', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('contabo_plan_slug', 80);
                $t->string('dimension_key', 60);
                $t->string('value_key', 160);
                $t->boolean('allowed_on_create')->default(false);
                $t->boolean('allowed_on_reinstall')->default(false);
                $t->boolean('allowed_on_post_provision')->default(false);
                $t->boolean('allowed_on_upgrade')->default(false);
                $t->boolean('allowed_on_downgrade')->default(false);
                $t->boolean('requires_reinstall')->default(false);
                $t->boolean('requires_recreate')->default(false);
                $t->boolean('destructive_change')->default(false);
                $t->boolean('data_loss_expected')->default(false);
                $t->boolean('requires_backup_warning')->default(false);
                $t->boolean('requires_admin_approval')->default(false);
                $t->boolean('billing_change_possible')->default(false);
                $t->string('provisioning_action', 40)->nullable();
                // api_verified|scrape_verified|manual_assumption|admin_override|
                // unknown (amendment 6). Conservative default.
                $t->string('capability_source', 20)->default('manual_assumption');
                $t->timestamp('last_verified_at')->nullable();
                $t->index(['contabo_plan_slug', 'dimension_key', 'value_key'], 'ix_cap_plan_dim_val');
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_option_capability');
        }

        // ── 6. mod_contabo_option_compatibility ────────────────────────────
        // Per (plan, dimension, value) compatibility matrix (design §5).
        // compatible/incompatible/required value sets are LONGTEXT JSON blobs
        // (never native JSON). Prevents unsupported combos at selection /
        // provisioning time.
        if (!$schema->hasTable('mod_contabo_option_compatibility')) {
            $schema->create('mod_contabo_option_compatibility', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('plan_slug', 80);
                $t->string('dimension_key', 60);
                $t->string('value_key', 160);
                $t->longText('compatible_with_json')->nullable();
                $t->longText('incompatible_with_json')->nullable();
                $t->longText('required_values_json')->nullable();
                $t->integer('min_value')->nullable();
                $t->integer('max_value')->nullable();
                $t->unsignedBigInteger('source_snapshot_id')->nullable();
                $t->timestamp('last_verified_at')->nullable();
                $t->index(['plan_slug', 'dimension_key', 'value_key'], 'ix_compat_plan_dim_val');
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_option_compatibility');
        }

        // ── 7. mod_contabo_service_config_snapshot ─────────────────────────
        // Captured at order/provision (design §12). Source of truth for renewal
        // margin, deprecation, disputes, reinstall, drift, audit. All structured
        // blobs LONGTEXT (never JSON); INDEX on service_id.
        if (!$schema->hasTable('mod_contabo_service_config_snapshot')) {
            $schema->create('mod_contabo_service_config_snapshot', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedInteger('service_id');
                $t->unsignedBigInteger('profile_id')->nullable();
                $t->string('profile_mode', 40);
                $t->string('plan_slug', 80);
                $t->unsignedInteger('whmcs_product_id');
                $t->string('selected_image', 190)->nullable();
                $t->string('selected_region', 120)->nullable();
                $t->longText('selected_options_json')->nullable();
                $t->longText('contabo_payload_json')->nullable();
                $t->decimal('base_price_snapshot', 12, 4)->nullable();
                $t->decimal('config_option_price_snapshot', 12, 4)->nullable();
                $t->decimal('landed_cost_snapshot', 12, 4)->nullable();
                $t->string('tax_mode_snapshot', 40)->nullable();
                $t->string('pricing_version_snapshot', 20)->nullable();
                $t->unsignedInteger('provisioning_metadata_version')->nullable();
                $t->timestamps();
                $t->index('service_id');
            });
            logActivity('Contabo Pricing migrateTo5: created mod_contabo_service_config_snapshot');
        }

        // ── Additive columns: mod_contabo_profile ──────────────────────────
        // Identity scope keys for multiple configurable products per plan
        // (amendment 7). Default scope 'default'; a second product on the same
        // plan is a conflict only when scope keys match.
        if ($schema->hasTable('mod_contabo_profile')) {
            $addedScope   = false;
            $addedVariant = false;
            $addedSegment = false;

            $schema->table('mod_contabo_profile', static function (Blueprint $t) use (
                $schema,
                &$addedScope,
                &$addedVariant,
                &$addedSegment
            ): void {
                if (!$schema->hasColumn('mod_contabo_profile', 'product_scope_key')) {
                    $t->string('product_scope_key', 60)->default('default');
                    $addedScope = true;
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'commercial_variant')) {
                    $t->string('commercial_variant', 60)->nullable();
                    $addedVariant = true;
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'audience_segment')) {
                    $t->string('audience_segment', 60)->nullable();
                    $addedSegment = true;
                }
            });

            logActivity(sprintf(
                'Contabo Pricing migrateTo5: profile scope columns — product_scope_key=%s, commercial_variant=%s, audience_segment=%s',
                $addedScope ? 'added' : 'already-present',
                $addedVariant ? 'added' : 'already-present',
                $addedSegment ? 'added' : 'already-present'
            ));
        } else {
            logActivity('Contabo Pricing migrateTo5: mod_contabo_profile table absent — scope columns skipped');
        }

        // ── Schema-version bump is performed by upgrade() after this method
        //    returns; nothing to do here (mirrors migrateTo2/migrateTo3/migrateTo4).
    }

    /**
     * Schema v6 — drift detection (design §14 / amendment 14). Adds an
     * `expected_hash` column to the three config link tables. apply() stores a
     * canonical DriftHasher hash of each WHMCS object's addon-controlled fields
     * when it writes; a later re-apply re-hashes the live object and, on a
     * mismatch (an admin hand-edited it out of band), FLAGS the drift and
     * refuses to overwrite — never silently clobbering the admin's change.
     *
     * Idempotent: each addColumn is hasColumn-guarded. The schema-version bump
     * is done by upgrade() after this returns (mirrors migrateTo2..5).
     */
    public function migrateTo6(): void
    {
        $schema = Capsule::schema();
        $tables = [
            'mod_contabo_config_group_link',
            'mod_contabo_config_option_link',
            'mod_contabo_config_option_value_link',
        ];
        foreach ($tables as $table) {
            if (!$schema->hasTable($table)) {
                logActivity('Contabo Pricing migrateTo6: ' . $table . ' absent — expected_hash skipped');
                continue;
            }
            if (!$schema->hasColumn($table, 'expected_hash')) {
                $schema->table($table, static function (Blueprint $t): void {
                    $t->string('expected_hash', 40)->nullable();
                });
                logActivity('Contabo Pricing migrateTo6: added expected_hash to ' . $table);
            }
        }
    }

    /**
     * Schema v7 — Phase C: profile-level `expose_configurable_options` master
     * switch. When 0, ConfigurableOptionsSyncer::apply() skips WHMCS config
     * option group creation for that profile (catalog price sync only).
     *
     * This MUST be its own migration (not folded into migrateTo2): installs
     * already at schema v6 never re-run earlier migrations, so the column would
     * otherwise never be added on an upgrade. Idempotent via hasColumn.
     */
    public function migrateTo7(): void
    {
        $schema = Capsule::schema();
        if (!$schema->hasTable('mod_contabo_profile')) {
            logActivity('Contabo Pricing migrateTo7: mod_contabo_profile absent — expose_configurable_options skipped');
            return;
        }
        if (!$schema->hasColumn('mod_contabo_profile', 'expose_configurable_options')) {
            $schema->table('mod_contabo_profile', static function (Blueprint $t): void {
                $t->tinyInteger('expose_configurable_options')->default(1);
            });
            logActivity('Contabo Pricing migrateTo7: added expose_configurable_options to mod_contabo_profile');
        }
    }

    /**
     * Schema v8 — two-layer pricing (profile = SOURCE, mapping = CUSTOMER) +
     * recoverable profile delete.
     *
     * The profile becomes the authority for WHICH cycles are published and the
     * per-cycle SOURCE (cost-basis) price; the mapping keeps the CUSTOMER pricing
     * (markup / rounding / guards) it already carries. Concretely this migration:
     *
     *   - mod_contabo_profile.published_cycles_mask  (uint, default 15 = 1/3/6/12)
     *       which cycles this profile sells. Back-filled from the union of the
     *       profile's existing mappings' catalog_cycles_mask so current behaviour
     *       is preserved; defaults to 15 when a profile has no mappings.
     *   - mod_contabo_profile.deleted_at             (nullable timestamp)
     *       soft-delete marker for the Trash / Undo flow. Default queries exclude
     *       rows where this is set.
     *   - mod_contabo_profile_version.period_prices_json (longText, never JSON)
     *       the per-period EUR SOURCE vector SyncEngine writes each pass
     *       ({1,3,6,12} scraped + {24,36} derived from the longest available
     *       period). computeCyclePrice reads this; NULL falls back to the legacy
     *       single-finalMonthly basis until the next sync repopulates it.
     *   - mod_contabo_mapping.source_overrides_json  (longText, never JSON)
     *       optional per-product per-cycle SOURCE basis override; falls back to
     *       the profile vector.
     *
     * Idempotent: every column add is hasColumn-guarded; the back-fill only sets
     * rows still at the default. The schema-version bump to 8 is performed by
     * upgrade() after this method returns (mirrors migrateTo2..7).
     */
    public function migrateTo8(): void
    {
        $schema = Capsule::schema();

        if ($schema->hasTable('mod_contabo_profile')) {
            $schema->table('mod_contabo_profile', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_profile', 'published_cycles_mask')) {
                    // 63 = all six bits. The profile SOURCES a price for every
                    // cycle it can derive (1/3/6/12 scraped + 24/36 projected), so
                    // the offered set defaults to all six. The customer-facing
                    // gate is the MAPPING's catalog_cycles_mask (unchanged), so an
                    // all-offered default leaves current behaviour untouched.
                    $t->unsignedInteger('published_cycles_mask')->default(63);
                }
                if (!$schema->hasColumn('mod_contabo_profile', 'deleted_at')) {
                    $t->timestamp('deleted_at')->nullable();
                }
            });
        } else {
            logActivity('Contabo Pricing migrateTo8: mod_contabo_profile absent — profile columns skipped');
        }

        if ($schema->hasTable('mod_contabo_profile_version')) {
            if (!$schema->hasColumn('mod_contabo_profile_version', 'period_prices_json')) {
                $schema->table('mod_contabo_profile_version', static function (Blueprint $t): void {
                    $t->longText('period_prices_json')->nullable();
                });
                logActivity('Contabo Pricing migrateTo8: added period_prices_json to mod_contabo_profile_version');
            }
        }

        if ($schema->hasTable('mod_contabo_mapping')) {
            if (!$schema->hasColumn('mod_contabo_mapping', 'source_overrides_json')) {
                $schema->table('mod_contabo_mapping', static function (Blueprint $t): void {
                    $t->longText('source_overrides_json')->nullable();
                });
                logActivity('Contabo Pricing migrateTo8: added source_overrides_json to mod_contabo_mapping');
            }
        }
    }

    /**
     * Schema v9 — WHMCS-native SecuriAce VPS suite foundation.
     *
     * The pricing addon owns installation and forward-only migration of the
     * shared suite schema.  The provisioning module owns every runtime write to
     * these tables and only checks the recorded suite schema version.  Keeping
     * DDL here ensures customer requests, lifecycle callbacks and cron workers
     * never attempt schema changes.
     *
     * All structured payloads use LONGTEXT rather than a database-native JSON
     * type so the package remains portable across WHMCS-supported MySQL and
     * MariaDB versions.
     */
    public function migrateTo9(): void
    {
        $schema = Capsule::schema();

        if (!$schema->hasTable('mod_securiacevps_schema')) {
            $schema->create('mod_securiacevps_schema', static function (Blueprint $t): void {
                $t->string('key', 80)->primary();
                $t->text('value')->nullable();
                $t->timestamp('updated_at')->useCurrent()->useCurrentOnUpdate();
            });
        }

        if (!$schema->hasTable('mod_securiacevps_order_snapshots')) {
            $schema->create('mod_securiacevps_order_snapshots', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('snapshot_uuid', 36)->unique('savps_snap_uuid_uq');
                $t->string('installation_id', 120);
                $t->unsignedInteger('order_id')->nullable();
                $t->unsignedInteger('service_id')->nullable();
                $t->unsignedInteger('product_id');
                $t->unsignedInteger('product_group_id')->nullable();
                $t->string('mapping_version', 120);
                $t->string('catalog_version', 120);
                $t->string('pricing_profile_version', 120)->nullable();
                $t->enum('state', ['draft', 'sealed', 'superseded', 'invalid'])->default('draft');
                $t->longText('payload_json');
                $t->char('configuration_hash', 64);
                $t->char('price_hash', 64);
                $t->char('cart_total_hash', 64);
                $t->timestamp('quote_expires_at')->nullable();
                $t->timestamp('paid_at')->nullable();
                $t->timestamp('sealed_at')->nullable();
                $t->char('supersedes_snapshot_uuid', 36)->nullable();
                $t->timestamps();
                $t->index(['service_id', 'state'], 'savps_snap_service_state_ix');
                $t->index(['order_id', 'state'], 'savps_snap_order_state_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_resources')) {
            $schema->create('mod_securiacevps_resources', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedInteger('service_id')->unique('savps_resource_service_uq');
                $t->string('installation_id', 120);
                $t->string('provider_account_id', 120);
                $t->string('provider_resource_id', 160)->nullable();
                $t->char('snapshot_uuid', 36)->nullable();
                $t->string('provider_state', 40)->default('unknown');
                $t->string('provisioning_state', 40)->default('not_requested');
                $t->string('ownership_state', 40)->default('unverified');
                $t->unsignedBigInteger('resource_version')->default(1);
                $t->char('observed_payload_hash', 64)->nullable();
                $t->timestamp('last_observed_at')->nullable();
                $t->timestamps();
                $t->unique(
                    ['provider_account_id', 'provider_resource_id'],
                    'savps_resource_provider_uq'
                );
                $t->index(
                    ['provisioning_state', 'updated_at'],
                    'savps_resource_prov_state_ix'
                );
            });
        }

        if (!$schema->hasTable('mod_securiacevps_operations')) {
            $schema->create('mod_securiacevps_operations', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('operation_uuid', 36)->unique('savps_op_uuid_uq');
                $t->unsignedInteger('service_id');
                $t->char('snapshot_uuid', 36)->nullable();
                $t->string('provider_account_id', 120);
                $t->string('operation_type', 40);
                $t->string('state', 40)->default('accepted');
                $t->string('command_id', 191)->unique('savps_op_command_uq');
                $t->char('request_fingerprint', 64);
                $t->char('idempotency_key', 64);
                $t->string('provider_resource_id', 160)->nullable();
                $t->unsignedInteger('attempt_count')->default(0);
                $t->timestamp('next_attempt_at')->nullable();
                $t->unsignedInteger('max_attempts')->default(8);
                $t->string('lease_owner', 120)->nullable();
                $t->timestamp('lease_expires_at')->nullable();
                $t->unsignedBigInteger('fencing_token')->default(0);
                $t->string('safe_error_code', 80)->nullable();
                $t->string('retry_classification', 40)->nullable();
                $t->boolean('unknown_outcome')->default(false);
                $t->char('correlation_id', 36);
                $t->longText('result_json')->nullable();
                $t->timestamp('submitted_at')->nullable();
                $t->timestamp('reconciled_at')->nullable();
                $t->timestamp('completed_at')->nullable();
                $t->timestamps();
                $t->index(['state', 'next_attempt_at'], 'savps_op_due_ix');
                $t->index(['service_id', 'created_at'], 'savps_op_service_created_ix');
                $t->index(['lease_expires_at', 'state'], 'savps_op_lease_state_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_operation_attempts')) {
            $schema->create('mod_securiacevps_operation_attempts', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('operation_uuid', 36);
                $t->unsignedInteger('attempt_number');
                $t->unsignedBigInteger('fencing_token');
                $t->string('state', 40);
                $t->string('provider_request_id', 160)->nullable();
                $t->string('safe_error_code', 80)->nullable();
                $t->string('retry_classification', 40)->nullable();
                $t->longText('request_metadata_json')->nullable();
                $t->longText('response_metadata_json')->nullable();
                $t->timestamp('started_at')->useCurrent();
                $t->timestamp('finished_at')->nullable();
                $t->unique(['operation_uuid', 'attempt_number'], 'savps_attempt_no_uq');
                $t->index(['operation_uuid', 'started_at'], 'savps_attempt_started_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_provider_requests')) {
            $schema->create('mod_securiacevps_provider_requests', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('operation_uuid', 36);
                $t->string('provider_request_id', 160)->nullable();
                $t->char('request_fingerprint', 64);
                $t->char('idempotency_key', 64);
                $t->string('state', 40);
                $t->string('provider_resource_id', 160)->nullable();
                $t->boolean('unknown_outcome')->default(false);
                $t->timestamp('submitted_at');
                $t->timestamp('last_checked_at')->nullable();
                $t->timestamps();
                $t->unique(
                    ['operation_uuid', 'request_fingerprint'],
                    'savps_request_fingerprint_uq'
                );
                $t->index('provider_request_id', 'savps_request_provider_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_service_locks')) {
            $schema->create('mod_securiacevps_service_locks', static function (Blueprint $t): void {
                $t->unsignedInteger('service_id')->primary();
                $t->char('operation_uuid', 36);
                $t->string('lease_owner', 120);
                $t->timestamp('lease_expires_at');
                $t->unsignedBigInteger('fencing_token')->default(1);
                $t->timestamp('updated_at')->useCurrent()->useCurrentOnUpdate();
                $t->index('lease_expires_at', 'savps_lock_expiry_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_capabilities')) {
            $schema->create('mod_securiacevps_capabilities', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('provider_account_id', 120);
                $t->string('capability', 80);
                $t->enum('state', [
                    'supported',
                    'unsupported',
                    'read_only',
                    'requires_polling',
                    'requires_manual_action',
                    'not_certified',
                ])->default('not_certified');
                $t->string('certification_version', 120)->nullable();
                $t->longText('evidence_json')->nullable();
                $t->unsignedInteger('certified_by_admin_id')->nullable();
                $t->timestamp('certified_at')->nullable();
                $t->timestamps();
                $t->unique(['provider_account_id', 'capability'], 'savps_cap_account_uq');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_reconciliation')) {
            $schema->create('mod_securiacevps_reconciliation', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('finding_uuid', 36)->unique('savps_recon_uuid_uq');
                $t->unsignedInteger('service_id')->nullable();
                $t->string('provider_account_id', 120);
                $t->string('provider_resource_id', 160)->nullable();
                $t->string('finding_type', 80);
                $t->string('severity', 20);
                $t->string('state', 40)->default('open');
                $t->char('evidence_hash', 64);
                $t->longText('evidence_json')->nullable();
                $t->string('safe_next_action', 120)->nullable();
                $t->timestamp('first_seen_at')->useCurrent();
                $t->timestamp('last_seen_at')->useCurrent();
                $t->timestamp('resolved_at')->nullable();
                $t->timestamps();
                $t->index(['state', 'severity', 'last_seen_at'], 'savps_recon_queue_ix');
                $t->index(['service_id', 'state'], 'savps_recon_service_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_adoption')) {
            $schema->create('mod_securiacevps_adoption', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedInteger('service_id')->unique('savps_adoption_service_uq');
                $t->string('provider_account_id', 120);
                $t->string('provider_resource_id', 160)->nullable();
                $t->enum('state', [
                    'verified',
                    'probable',
                    'ambiguous',
                    'missing_upstream',
                    'orphan_upstream',
                    'conflict',
                    'excluded',
                ]);
                $t->decimal('confidence', 5, 4)->default(0);
                $t->longText('evidence_json')->nullable();
                $t->unsignedInteger('reviewed_by_admin_id')->nullable();
                $t->timestamp('reviewed_at')->nullable();
                $t->timestamps();
                $t->index(['state', 'updated_at'], 'savps_adoption_state_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_billing_sagas')) {
            $schema->create('mod_securiacevps_billing_sagas', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('saga_uuid', 36)->unique('savps_saga_uuid_uq');
                $t->unsignedInteger('service_id');
                $t->char('operation_uuid', 36)->nullable();
                $t->string('saga_type', 40);
                $t->string('state', 40);
                $t->unsignedInteger('invoice_id')->nullable();
                $t->unsignedInteger('transaction_id')->nullable();
                $t->char('currency', 3)->nullable();
                $t->decimal('amount', 18, 6)->nullable();
                $t->string('compensation_state', 40)->nullable();
                $t->longText('evidence_json')->nullable();
                $t->timestamp('completed_at')->nullable();
                $t->timestamps();
                $t->index(['service_id', 'state'], 'savps_saga_service_ix');
                $t->index(['operation_uuid', 'state'], 'savps_saga_operation_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_audit_events')) {
            $schema->create('mod_securiacevps_audit_events', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('event_uuid', 36)->unique('savps_audit_uuid_uq');
                $t->char('correlation_id', 36)->nullable();
                $t->string('actor_type', 40);
                $t->unsignedInteger('actor_id')->nullable();
                $t->unsignedInteger('service_id')->nullable();
                $t->string('event_type', 80);
                $t->string('outcome', 40);
                $t->char('previous_event_hash', 64)->nullable();
                $t->char('event_hash', 64);
                $t->longText('metadata_json')->nullable();
                $t->timestamp('created_at')->useCurrent();
                $t->index(['service_id', 'created_at'], 'savps_audit_service_ix');
                $t->index(['event_type', 'created_at'], 'savps_audit_event_ix');
            });
        }

        if (!$schema->hasTable('mod_securiacevps_operator_commands')) {
            $schema->create('mod_securiacevps_operator_commands', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('command_uuid', 36)->unique('savps_command_uuid_uq');
                $t->string('command_type', 80);
                $t->unsignedInteger('service_id')->nullable();
                $t->char('operation_uuid', 36)->nullable();
                $t->unsignedInteger('requested_by_admin_id');
                $t->string('state', 40)->default('pending_validation');
                $t->char('payload_hash', 64);
                $t->longText('payload_json')->nullable();
                $t->string('safe_error_code', 80)->nullable();
                $t->timestamp('claimed_at')->nullable();
                $t->timestamp('completed_at')->nullable();
                $t->timestamps();
                $t->index(['state', 'created_at'], 'savps_command_queue_ix');
            });
        }

        // MySQL/MariaDB DDL is not transactional. If a previous migration was
        // interrupted after CREATE TABLE but before a later index ALTER, rerun
        // must repair the partial table instead of silently skipping it.
        $this->ensureIndex('mod_securiacevps_order_snapshots', 'savps_snap_uuid_uq', ['snapshot_uuid'], true);
        $this->ensureIndex(
            'mod_securiacevps_order_snapshots',
            'savps_snap_service_state_ix',
            ['service_id', 'state']
        );
        $this->ensureIndex(
            'mod_securiacevps_order_snapshots',
            'savps_snap_order_state_ix',
            ['order_id', 'state']
        );
        $this->ensureIndex('mod_securiacevps_resources', 'savps_resource_service_uq', ['service_id'], true);
        $this->ensureIndex(
            'mod_securiacevps_resources',
            'savps_resource_provider_uq',
            ['provider_account_id', 'provider_resource_id'],
            true
        );
        $this->ensureIndex(
            'mod_securiacevps_resources',
            'savps_resource_prov_state_ix',
            ['provisioning_state', 'updated_at']
        );
        $this->ensureIndex('mod_securiacevps_operations', 'savps_op_uuid_uq', ['operation_uuid'], true);
        $this->ensureIndex('mod_securiacevps_operations', 'savps_op_command_uq', ['command_id'], true);
        $this->ensureIndex(
            'mod_securiacevps_operations',
            'savps_op_due_ix',
            ['state', 'next_attempt_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_operations',
            'savps_op_service_created_ix',
            ['service_id', 'created_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_operations',
            'savps_op_lease_state_ix',
            ['lease_expires_at', 'state']
        );
        $this->ensureIndex(
            'mod_securiacevps_operation_attempts',
            'savps_attempt_no_uq',
            ['operation_uuid', 'attempt_number'],
            true
        );
        $this->ensureIndex(
            'mod_securiacevps_operation_attempts',
            'savps_attempt_started_ix',
            ['operation_uuid', 'started_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_provider_requests',
            'savps_request_fingerprint_uq',
            ['operation_uuid', 'request_fingerprint'],
            true
        );
        $this->ensureIndex(
            'mod_securiacevps_provider_requests',
            'savps_request_provider_ix',
            ['provider_request_id']
        );
        $this->ensureIndex('mod_securiacevps_service_locks', 'savps_lock_expiry_ix', ['lease_expires_at']);
        $this->ensureIndex(
            'mod_securiacevps_capabilities',
            'savps_cap_account_uq',
            ['provider_account_id', 'capability'],
            true
        );
        $this->ensureIndex('mod_securiacevps_reconciliation', 'savps_recon_uuid_uq', ['finding_uuid'], true);
        $this->ensureIndex(
            'mod_securiacevps_reconciliation',
            'savps_recon_queue_ix',
            ['state', 'severity', 'last_seen_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_reconciliation',
            'savps_recon_service_ix',
            ['service_id', 'state']
        );
        $this->ensureIndex('mod_securiacevps_adoption', 'savps_adoption_service_uq', ['service_id'], true);
        $this->ensureIndex(
            'mod_securiacevps_adoption',
            'savps_adoption_state_ix',
            ['state', 'updated_at']
        );
        $this->ensureIndex('mod_securiacevps_billing_sagas', 'savps_saga_uuid_uq', ['saga_uuid'], true);
        $this->ensureIndex(
            'mod_securiacevps_billing_sagas',
            'savps_saga_service_ix',
            ['service_id', 'state']
        );
        $this->ensureIndex(
            'mod_securiacevps_billing_sagas',
            'savps_saga_operation_ix',
            ['operation_uuid', 'state']
        );
        $this->ensureIndex('mod_securiacevps_audit_events', 'savps_audit_uuid_uq', ['event_uuid'], true);
        $this->ensureIndex(
            'mod_securiacevps_audit_events',
            'savps_audit_service_ix',
            ['service_id', 'created_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_audit_events',
            'savps_audit_event_ix',
            ['event_type', 'created_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_operator_commands',
            'savps_command_uuid_uq',
            ['command_uuid'],
            true
        );
        $this->ensureIndex(
            'mod_securiacevps_operator_commands',
            'savps_command_queue_ix',
            ['state', 'created_at']
        );

        Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'schema_version'],
            ['value' => '1', 'updated_at' => date('Y-m-d H:i:s')]
        );
        Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'installed_by'],
            ['value' => 'contabo_pricing', 'updated_at' => date('Y-m-d H:i:s')]
        );
    }

    /**
     * Schema v10 — versioned catalog publication, sealed-order support and
     * one-time secret delivery.
     */
    public function migrateTo10(): void
    {
        $schema = Capsule::schema();

        if (!$schema->hasTable('mod_contabo_catalog_versions')) {
            $schema->create('mod_contabo_catalog_versions', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('catalog_version', 120)->unique('contabo_catalog_version_uq');
                $t->string('source_version', 120)->nullable();
                $t->string('state', 40)->default('observed');
                $t->char('payload_hash', 64);
                $t->timestamp('source_observed_at');
                $t->timestamp('effective_at')->nullable();
                $t->timestamp('imported_at')->useCurrent();
                $t->unsignedInteger('imported_by_admin_id')->nullable();
                $t->longText('metadata_json')->nullable();
                $t->timestamps();
                $t->index(['state', 'effective_at'], 'contabo_catalog_state_ix');
            });
        }

        if (!$schema->hasTable('mod_contabo_catalog_items')) {
            $schema->create('mod_contabo_catalog_items', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->unsignedBigInteger('catalog_version_id');
                $t->string('machine_id', 191);
                $t->string('provider_id', 191)->nullable();
                $t->string('item_type', 60);
                $t->string('label', 255);
                $t->string('availability_state', 40);
                $t->boolean('deprecated')->default(false);
                $t->timestamp('effective_at')->nullable();
                $t->timestamp('source_observed_at');
                $t->char('payload_hash', 64);
                $t->longText('compatibility_json')->nullable();
                $t->longText('payload_json');
                $t->timestamps();
                $t->unique(
                    ['catalog_version_id', 'machine_id'],
                    'contabo_catalog_item_machine_uq'
                );
                $t->index(
                    ['item_type', 'availability_state'],
                    'contabo_catalog_item_availability_ix'
                );
            });
        }

        if (!$schema->hasTable('mod_contabo_mapping_publications')) {
            $schema->create('mod_contabo_mapping_publications', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('mapping_version', 120)->unique('contabo_publication_version_uq');
                $t->unsignedBigInteger('profile_id');
                $t->unsignedInteger('product_id');
                $t->unsignedBigInteger('catalog_version_id');
                $t->string('provider_sku_id', 191);
                $t->string('state', 40)->default('draft');
                $t->char('payload_hash', 64);
                $t->longText('payload_json');
                $t->unsignedInteger('approved_by_admin_id')->nullable();
                $t->timestamp('approved_at')->nullable();
                $t->timestamp('effective_at')->nullable();
                $t->string('supersedes_mapping_version', 120)->nullable();
                $t->timestamps();
                $t->index(
                    ['product_id', 'state', 'effective_at'],
                    'contabo_publication_product_ix'
                );
                $t->index(['profile_id', 'state'], 'contabo_publication_profile_ix');
            });
        }

        if (!$schema->hasTable('mod_contabo_publication_approvals')) {
            $schema->create('mod_contabo_publication_approvals', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->string('publication_type', 40);
                $t->string('publication_version', 120);
                $t->string('decision', 40);
                $t->unsignedInteger('admin_id');
                $t->text('reason')->nullable();
                $t->char('preview_hash', 64);
                $t->timestamp('created_at')->useCurrent();
                $t->index(
                    ['publication_type', 'publication_version'],
                    'contabo_approval_publication_ix'
                );
            });
        }

        if (!$schema->hasTable('mod_securiacevps_secrets')) {
            $schema->create('mod_securiacevps_secrets', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('secret_uuid', 36)->unique('savps_secret_uuid_uq');
                $t->unsignedInteger('service_id');
                $t->string('secret_type', 40);
                $t->text('encrypted_value');
                $t->char('reveal_token_hash', 64)->unique('savps_secret_reveal_hash_uq');
                $t->unsignedInteger('maximum_reveals')->default(1);
                $t->unsignedInteger('reveal_count')->default(0);
                $t->timestamp('expires_at');
                $t->timestamp('revealed_at')->nullable();
                $t->timestamp('destroyed_at')->nullable();
                $t->timestamps();
                $t->index(
                    ['service_id', 'secret_type', 'expires_at'],
                    'savps_secret_service_expiry_ix'
                );
            });
        }

        if (!$schema->hasTable('mod_securiacevps_communications')) {
            $schema->create('mod_securiacevps_communications', static function (Blueprint $t): void {
                $t->bigIncrements('id');
                $t->char('communication_uuid', 36)->unique('savps_comm_uuid_uq');
                $t->unsignedInteger('service_id');
                $t->char('operation_uuid', 36)->nullable();
                $t->string('message_type', 60);
                $t->string('state', 40)->default('pending');
                $t->string('template_name', 120);
                $t->char('payload_hash', 64);
                $t->unsignedInteger('attempt_count')->default(0);
                $t->timestamp('next_attempt_at')->nullable();
                $t->timestamp('sent_at')->nullable();
                $t->string('safe_error_code', 80)->nullable();
                $t->timestamps();
                $t->index(['state', 'next_attempt_at'], 'savps_comm_due_ix');
                $t->index(['service_id', 'created_at'], 'savps_comm_service_ix');
            });
        }

        $this->ensureIndex(
            'mod_contabo_catalog_versions',
            'contabo_catalog_version_uq',
            ['catalog_version'],
            true
        );
        $this->ensureIndex(
            'mod_contabo_catalog_versions',
            'contabo_catalog_state_ix',
            ['state', 'effective_at']
        );
        $this->ensureIndex(
            'mod_contabo_catalog_items',
            'contabo_catalog_item_machine_uq',
            ['catalog_version_id', 'machine_id'],
            true
        );
        $this->ensureIndex(
            'mod_contabo_catalog_items',
            'contabo_catalog_item_availability_ix',
            ['item_type', 'availability_state']
        );
        $this->ensureIndex(
            'mod_contabo_mapping_publications',
            'contabo_publication_version_uq',
            ['mapping_version'],
            true
        );
        $this->ensureIndex(
            'mod_contabo_mapping_publications',
            'contabo_publication_product_ix',
            ['product_id', 'state', 'effective_at']
        );
        $this->ensureIndex(
            'mod_contabo_mapping_publications',
            'contabo_publication_profile_ix',
            ['profile_id', 'state']
        );
        $this->ensureIndex(
            'mod_contabo_publication_approvals',
            'contabo_approval_publication_ix',
            ['publication_type', 'publication_version']
        );
        $this->ensureIndex('mod_securiacevps_secrets', 'savps_secret_uuid_uq', ['secret_uuid'], true);
        $this->ensureIndex(
            'mod_securiacevps_secrets',
            'savps_secret_reveal_hash_uq',
            ['reveal_token_hash'],
            true
        );
        $this->ensureIndex(
            'mod_securiacevps_secrets',
            'savps_secret_service_expiry_ix',
            ['service_id', 'secret_type', 'expires_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_communications',
            'savps_comm_uuid_uq',
            ['communication_uuid'],
            true
        );
        $this->ensureIndex(
            'mod_securiacevps_communications',
            'savps_comm_due_ix',
            ['state', 'next_attempt_at']
        );
        $this->ensureIndex(
            'mod_securiacevps_communications',
            'savps_comm_service_ix',
            ['service_id', 'created_at']
        );

        if ($schema->hasTable('mod_contabo_mapping')) {
            $schema->table('mod_contabo_mapping', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_contabo_mapping', 'published_mapping_version')) {
                    $t->string('published_mapping_version', 120)->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'provider_sku_id')) {
                    $t->string('provider_sku_id', 191)->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'rust_catalog_version')) {
                    $t->string('rust_catalog_version', 120)->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'mapping_state')) {
                    $t->string('mapping_state', 40)->default('draft');
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'mapping_payload_hash')) {
                    $t->char('mapping_payload_hash', 64)->nullable();
                }
                if (!$schema->hasColumn('mod_contabo_mapping', 'mapping_effective_at')) {
                    $t->timestamp('mapping_effective_at')->nullable();
                }
            });
        }

        if ($schema->hasTable('mod_securiacevps_operations')) {
            $schema->table('mod_securiacevps_operations', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_securiacevps_operations', 'operation_generation')) {
                    $t->unsignedInteger('operation_generation')->default(1);
                }
                if (!$schema->hasColumn('mod_securiacevps_operations', 'payload_json')) {
                    $t->longText('payload_json')->nullable();
                }
            });
        }

        $now = date('Y-m-d H:i:s');
        $installationId = Capsule::table('mod_securiacevps_schema')
            ->where('key', 'installation_id')
            ->value('value');
        if ($installationId === null || trim((string) $installationId) === '') {
            Capsule::table('mod_securiacevps_schema')->insert([
                'key' => 'installation_id',
                'value' => bin2hex(random_bytes(16)),
                'updated_at' => $now,
            ]);
        }

        $defaults = [
            'provider_writes_enabled' => '0',
            'operation_batch_size' => '25',
            'operation_lease_seconds' => '120',
            'suite_schema_version' => '2',
        ];
        foreach ($defaults as $key => $value) {
            $existing = Capsule::table('mod_securiacevps_schema')->where('key', $key)->value('value');
            if ($existing === null) {
                Capsule::table('mod_securiacevps_schema')->insert([
                    'key' => $key,
                    'value' => $value,
                    'updated_at' => $now,
                ]);
            }
        }

        Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'schema_version'],
            ['value' => '2', 'updated_at' => $now]
        );
    }

    /**
     * Schema v11 — bind one-time credentials to their durable operation and
     * retain only an encrypted copy of the opaque reveal token.
     */
    public function migrateTo11(): void
    {
        $schema = Capsule::schema();
        if ($schema->hasTable('mod_securiacevps_secrets')) {
            $schema->table('mod_securiacevps_secrets', static function (Blueprint $t) use ($schema): void {
                if (!$schema->hasColumn('mod_securiacevps_secrets', 'operation_uuid')) {
                    $t->char('operation_uuid', 36)->nullable();
                    $t->index('operation_uuid', 'savps_secret_operation_ix');
                }
                if (!$schema->hasColumn('mod_securiacevps_secrets', 'reveal_token_ciphertext')) {
                    $t->text('reveal_token_ciphertext')->nullable();
                }
            });
            $this->ensureIndex(
                'mod_securiacevps_secrets',
                'savps_secret_operation_ix',
                ['operation_uuid']
            );
        }

        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'suite_schema_version'],
            ['value' => '3', 'updated_at' => $now]
        );
        Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'schema_version'],
            ['value' => '3', 'updated_at' => $now]
        );
    }

    /**
     * Schema v12 — install the customer lifecycle email templates used by the
     * durable VPS communication queue. Existing administrator customisations
     * are never overwritten.
     */
    public function migrateTo12(): void
    {
        if (!Capsule::schema()->hasTable('tblemailtemplates')) {
            return;
        }

        (new EmailTemplateSeeder())->ensure();
    }

    /**
     * Ensure a named MySQL/MariaDB index exists, matching by column sequence so
     * migrations interrupted after CREATE TABLE remain safely resumable.
     *
     * FakeCapsule intentionally has no PDO; unit tests exercise the Blueprint
     * path while real-WHMCS integration exercises this repair path.
     *
     * @param list<string> $columns
     */
    private function ensureIndex(
        string $table,
        string $name,
        array $columns,
        bool $unique = false
    ): void {
        foreach (array_merge([$table, $name], $columns) as $identifier) {
            if (!preg_match('/^[A-Za-z0-9_]+$/', $identifier)) {
                throw new \InvalidArgumentException('Unsafe schema identifier');
            }
        }
        if (strlen($name) > 64 || $columns === []) {
            throw new \InvalidArgumentException('Invalid schema index definition');
        }

        $connection = Capsule::connection();
        if (!method_exists($connection, 'getPdo')) {
            return;
        }

        $statement = $connection->getPdo()->prepare(
            'SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique,'
            . ' SEQ_IN_INDEX AS sequence_number, COLUMN_NAME AS column_name'
            . ' FROM information_schema.STATISTICS'
            . ' WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?'
            . ' ORDER BY INDEX_NAME, SEQ_IN_INDEX'
        );
        $statement->execute([$table]);

        /** @var array<string,array{unique:bool,columns:list<string>}> $indexes */
        $indexes = [];
        foreach ($statement->fetchAll(\PDO::FETCH_ASSOC) as $row) {
            $indexName = (string) $row['index_name'];
            if (!isset($indexes[$indexName])) {
                $indexes[$indexName] = [
                    'unique' => (int) $row['non_unique'] === 0,
                    'columns' => [],
                ];
            }
            $indexes[$indexName]['columns'][] = (string) $row['column_name'];
        }

        foreach ($indexes as $index) {
            if ($index['columns'] === $columns && (!$unique || $index['unique'])) {
                return;
            }
        }

        $quotedColumns = implode(', ', array_map(
            static function (string $column): string {
                return '`' . $column . '`';
            },
            $columns
        ));
        $sql = 'ALTER TABLE `' . $table . '` ADD '
            . ($unique ? 'UNIQUE ' : '')
            . 'INDEX `' . $name . '` (' . $quotedColumns . ')';
        $connection->statement($sql);
    }
}
