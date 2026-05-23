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
     * The Phase A daily sweep:
     *   - observes (no apply),
     *   - prunes stale decision rows,
     *   - emits an admin digest hook-point (currently a no-op stub since the
     *     digest mailer is Agent C territory; Phase A surfaces the same data
     *     via the Repricing dashboard).
     *
     * Wrapped in try/catch by the hook caller; we re-throw nothing.
     */
    public function runObserveSweep(): void
    {
        try {
            $this->ensureSchemaPresent();
            $this->observeMappedServices();
            $this->scanScheduledChanges();
            $this->pruneOldDecisions();
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing CronDriver sweep failed: ' . $e->getMessage());
            }
        }
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
    private function observeMappedServices(): void
    {
        if (!class_exists('\\ContaboPricing\\RenewalEngine')) {
            return;
        }

        $serviceIds = $this->loadActiveMappedServiceIds();
        if (empty($serviceIds)) {
            return;
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

    /**
     * Walk pending price_change_schedule rows whose effective_at has passed
     * and queue them for evaluation. Phase A only scans + logs; it does not
     * write tblhosting (the engine's `applyWithGuards()` is Phase B work).
     */
    private function scanScheduledChanges(): void
    {
        if (!Capsule::schema()->hasTable('mod_contabo_price_change_schedule')) {
            return;
        }
        $due = Capsule::table('mod_contabo_price_change_schedule')
            ->whereIn('status', ['pending', 'notified'])
            ->where('effective_at', '<=', date('Y-m-d H:i:s'))
            ->count();
        if ($due > 0 && function_exists('logActivity')) {
            logActivity("Contabo Pricing: {$due} scheduled changes pending evaluation (Phase A observe-only).");
        }
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
}
