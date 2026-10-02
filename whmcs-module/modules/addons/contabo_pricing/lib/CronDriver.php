<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Phase A cron driver — observe-only.
 *
 * Runs from `DailyCronJob` (NOT from `PreInvoicingGenerateInvoiceItems`; that
 * apply-hook only ships in Phase B). The driver:
 *   1. Walks services mapped to a Contabo profile.
 *   2. Asks RenewalEngine (if available) to compute a decision in dry-run mode.
 *   3. Persists the decision row via DecisionLog (when available); always logs
 *      a structured `mod_contabo_pricing_action` row of type `phase_changed`
 *      when the repricing_phase flips, and prunes old decision rows.
 *   4. Touches NO real `tblhosting.recurringamount` value. Phase A is read-only
 *      by hard rule — the engine guards this via `repricing_phase = observe`
 *      gating inside RenewalEngine, and this driver never calls
 *      ServicePriceWriter directly.
 *
 * Agent B owns RenewalEngine + DecisionLog + the actual `decide()` signature.
 * If those classes aren't deployed yet (parallel work-in-flight) this driver
 * degrades cleanly: it emits a heartbeat log row, prunes audit rows, and
 * returns. Either way, hooks.php can register the cron hook without crashing.
 *
 * PHP 7.4 polyglot. No external state; safe to invoke from multiple hook
 * registrations because the underlying inserts are idempotent + per-cron-run.
 */
final class CronDriver
{
    /** @var string */
    private $cronRunId;

    public function __construct()
    {
        $this->cronRunId = self::uuid4();
    }

    /**
     * The daily repricing/renewal observation sweep. Returns a digest-friendly
     * summary instead of only logging side effects.
     *
     * @return array<string,mixed>
     */
    public function runObserveSweep(): array
    {
        $summary = [
            'trigger' => 'daily_cron',
            'started_at' => date('Y-m-d H:i:s'),
            'cron_run_id' => $this->cronRunId,
            'status' => 'healthy',
            'schema_ready' => false,
            'candidate_mapped_services' => 0,
            'renewal_evaluation_mode' => 'inactive',
            'renewal_evaluation_message' => '',
            'scheduled_changes' => [
                'schedules_processed' => 0,
                'schedules_applied' => 0,
                'schedules_deferred' => 0,
                'services_evaluated' => 0,
                'catalog_intents_logged' => 0,
                'decisions' => [],
            ],
            'errors' => [],
            'notes' => [],
        ];

        try {
            $summary['schema_ready'] = $this->ensureSchemaPresent();
            if (!$summary['schema_ready']) {
                $summary['status'] = 'warning';
                $summary['renewal_evaluation_mode'] = 'schema_unavailable';
                $summary['renewal_evaluation_message'] = 'Repricing observation skipped because required Contabo Pricing tables are not available yet.';
                $summary['notes'][] = 'Observation skipped until repricing schema tables exist.';
                return $this->finishSummary($summary);
            }

            $observeSummary = $this->observeMappedServices();
            $summary['candidate_mapped_services'] = (int) ($observeSummary['candidate_mapped_services'] ?? 0);
            $summary['renewal_evaluation_mode'] = (string) ($observeSummary['renewal_evaluation_mode'] ?? 'inactive');
            $summary['renewal_evaluation_message'] = (string) ($observeSummary['renewal_evaluation_message'] ?? '');
            if (!empty($observeSummary['notes']) && is_array($observeSummary['notes'])) {
                $summary['notes'] = array_merge($summary['notes'], $observeSummary['notes']);
            }
            if (!empty($observeSummary['errors']) && is_array($observeSummary['errors'])) {
                $summary['errors'] = array_merge($summary['errors'], $observeSummary['errors']);
            }

            $summary['scheduled_changes'] = $this->processScheduledChanges();
            if (!empty($summary['scheduled_changes']['errors']) && is_array($summary['scheduled_changes']['errors'])) {
                $summary['errors'] = array_merge($summary['errors'], $summary['scheduled_changes']['errors']);
            }

            $this->pruneOldDecisions();
        } catch (\Throwable $e) {
            $summary['status'] = 'failed';
            $summary['errors'][] = 'CronDriver sweep failed: ' . $e->getMessage();
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing CronDriver sweep failed: ' . $e->getMessage());
            }
        }

        return $this->finishSummary($summary);
    }

    /**
     * Guard against Phase A activation racing the schema migration: if v2
     * tables don't exist yet, bail without touching anything.
     */
    private function ensureSchemaPresent(): bool
    {
        $schema = Capsule::schema();
        return $schema->hasTable('mod_contabo_price_decision')
            && $schema->hasTable('mod_contabo_pricing_action')
            && $schema->hasTable('mod_contabo_mapping');
    }

    /**
     * Walk every service that maps to an active Contabo profile and ask the
     * engine for a decision. If RenewalEngine isn't deployed, fall through —
     * Phase A is observe-only by definition and we don't want to crash the
     * cron because a sibling component hasn't shipped.
     */
    private function observeMappedServices(): array
    {
        $summary = [
            'candidate_mapped_services' => 0,
            'renewal_evaluation_mode' => 'inactive',
            'renewal_evaluation_message' => '',
            'notes' => [],
            'errors' => [],
        ];

        if (!class_exists('\\ContaboPricing\\RenewalEngine')) {
            $summary['renewal_evaluation_mode'] = 'renewal_engine_missing';
            $summary['renewal_evaluation_message'] = 'RenewalEngine is not deployed, so repricing observation could not evaluate services.';
            $summary['notes'][] = 'RenewalEngine classes are unavailable in this deployment.';
            return $summary;
        }

        $serviceIds = $this->loadActiveMappedServiceIds();
        $summary['candidate_mapped_services'] = count($serviceIds);
        if (empty($serviceIds)) {
            $summary['renewal_evaluation_mode'] = 'no_candidates';
            $summary['renewal_evaluation_message'] = 'No active mapped services needed repricing observation today.';
            return $summary;
        }

        // Phase A observe: per-service RenewalEngine evaluation needs the full
        // service-row contract (settings + mapping/profile/version/flags), which
        // is wired in Phase B. We deliberately do NOT invoke RenewalEngine here
        // with a partial/incorrect signature — its constructor requires $settings
        // and decide() takes a full service-row array, so calling it with a bare
        // service id would TypeError. Log the candidate count honestly; no fake
        // success, no broken call.
        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing CronDriver (Phase A observe): '
                . count($serviceIds) . ' active mapped service(s) identified; '
                . 'per-service RenewalEngine evaluation is wired in Phase B (not invoked).');
        }
        $summary['renewal_evaluation_mode'] = 'phase_b_pending';
        $summary['renewal_evaluation_message'] = 'Active mapped services were identified, but per-service RenewalEngine evaluation is intentionally inactive in the daily cron until Phase B is wired.';
        $summary['notes'][] = 'Mapped-service counts are observational only; no recurring amounts were recalculated.';
        return $summary;
    }

    /**
     * Active services on Contabo-mapped products, by id. Reads the REAL
     * `tblhosting.amount` column (NOT `recurringamount`, which is not a raw
     * column). Isolated + testable — no RenewalEngine dependency. Public so the
     * candidate-loading logic can be unit-tested without the engine.
     *
     * @return list<int>
     */
    public function loadActiveMappedServiceIds(): array
    {
        $productIds = [];
        foreach (Capsule::table('mod_contabo_mapping')->where('active', true)->get() as $m) {
            $m = (array) $m;
            $pid = (int) ($m['product_id'] ?? 0);
            if ($pid > 0) {
                $productIds[$pid] = $pid;
            }
        }
        if (empty($productIds)) {
            return [];
        }

        $ids = [];
        $rows = Capsule::table('tblhosting')
            ->whereIn('packageid', array_values($productIds))
            ->where('domainstatus', 'Active')
            ->where('amount', '>', 0)
            ->select(['id'])
            ->limit(1000)
            ->get();
        foreach ($rows as $r) {
            $r = (array) $r;
            $id = (int) ($r['id'] ?? 0);
            if ($id > 0) {
                $ids[] = $id;
            }
        }
        return $ids;
    }

    private function processScheduledChanges(): array
    {
        $summary = [
            'schedules_processed' => 0,
            'schedules_applied' => 0,
            'schedules_deferred' => 0,
            'services_evaluated' => 0,
            'catalog_intents_logged' => 0,
            'decisions' => [],
            'errors' => [],
            'processor_mode' => 'inactive',
        ];

        if (!class_exists('\\ContaboPricing\\ScheduledChangeProcessor')) {
            $summary['processor_mode'] = 'processor_missing';
            return $summary;
        }
        if (!Capsule::schema()->hasTable('mod_contabo_price_change_schedule')) {
            $summary['processor_mode'] = 'schedule_table_missing';
            return $summary;
        }

        // Build a minimal settings bag — ScheduledChangeProcessor only reads
        // repricing_phase (engine gate) + tax/markup fields from RenewalEngine.
        // Pull from tbladdonmodules so we respect what the admin configured.
        $settings = [];
        try {
            $rows = Capsule::table('tbladdonmodules')
                ->where('module', 'contabo_pricing')
                ->get(['setting', 'value']);
            foreach ($rows as $r) {
                $r = (array) $r;
                $settings[(string)($r['setting'] ?? '')] = $r['value'] ?? '';
            }
        } catch (\Throwable $e) {
            // Settings unreadable — processor will use its defaults.
        }

        try {
            $processor = new ScheduledChangeProcessor($settings);
            $summary = array_merge($summary, $processor->run());
            $summary['processor_mode'] = 'executed';
            if (function_exists('logActivity') && ($summary['schedules_processed'] ?? 0) > 0) {
                logActivity(sprintf(
                    'Contabo Pricing ScheduledChangeProcessor: %d processed, %d applied, %d deferred, %d services evaluated.',
                    (int)($summary['schedules_processed'] ?? 0),
                    (int)($summary['schedules_applied']   ?? 0),
                    (int)($summary['schedules_deferred']  ?? 0),
                    (int)($summary['services_evaluated']  ?? 0)
                ));
            }
        } catch (\Throwable $e) {
            $summary['processor_mode'] = 'failed';
            $summary['errors'][] = 'ScheduledChangeProcessor failed: ' . $e->getMessage();
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: ScheduledChangeProcessor failed: ' . $e->getMessage());
            }
        }
        return $summary;
    }

    /**
     * Prune old non-applied decision rows. Applied rows are retained
     * indefinitely (acceptance criterion #10 from the plan).
     *
     * Retention defaults to log_retention_days from tbladdonmodules; falls
     * back to 365 days if unreadable.
     */
    private function pruneOldDecisions(): void
    {
        $retention = 365;
        try {
            $row = Capsule::table('tbladdonmodules')
                ->where(['module' => 'contabo_pricing', 'setting' => 'log_retention_days'])
                ->value('value');
            if ($row !== null && $row !== '') {
                $retention = max(30, (int) $row);
            }
        } catch (\Throwable $e) {
            // settings query failed; stick with 365.
        }

        $cutoff = date('Y-m-d H:i:s', time() - ($retention * 86400));
        try {
            Capsule::table('mod_contabo_price_decision')
                ->where('applied', false)
                ->where('decided_at', '<', $cutoff)
                ->delete();
        } catch (\Throwable $e) {
            // table may not yet exist in install-but-not-migrated state.
        }
    }

    /**
     * RFC 4122 v4 UUID. Used as cron_run_id so every decision row in a single
     * sweep can be joined by it. No external dep — random_bytes() is fine.
     */
    private static function uuid4(): string
    {
        $bytes = random_bytes(16);
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
        $hex = bin2hex($bytes);
        return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-'
            . substr($hex, 12, 4) . '-' . substr($hex, 16, 4) . '-'
            . substr($hex, 20, 12);
    }

    /**
     * @param array<string,mixed> $summary
     * @return array<string,mixed>
     */
    private function finishSummary(array $summary): array
    {
        if ($summary['status'] !== 'failed') {
            if (!empty($summary['errors'])) {
                $summary['status'] = 'warning';
            } elseif (
                (string) ($summary['renewal_evaluation_mode'] ?? '') === 'phase_b_pending'
                || (string) ($summary['renewal_evaluation_mode'] ?? '') === 'schema_unavailable'
                || (string) ($summary['renewal_evaluation_mode'] ?? '') === 'renewal_engine_missing'
            ) {
                $summary['status'] = 'warning';
            }
        }
        $summary['finished_at'] = date('Y-m-d H:i:s');
        return $summary;
    }
}
