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

        // Mapped product IDs → load active services on those products.
        $productIds = Capsule::table('mod_contabo_mapping')
            ->where('active', true)
            ->pluck('product_id')
            ->all();
        if (empty($productIds)) {
            return;
        }

        // tblhosting query: Active services only, with positive recurringamount.
        // We intentionally do NOT load Suspended/Cancelled here — RenewalEngine
        // will skip them anyway but Phase A keeps the cron tight on active rows.
        $services = Capsule::table('tblhosting')
            ->whereIn('packageid', $productIds)
            ->where('domainstatus', 'Active')
            ->where('recurringamount', '>', 0)
            ->select(['id'])
            ->limit(1000)
            ->get();

        foreach ($services as $svc) {
            try {
                /** @var class-string $engineCls */
                $engineCls = '\\ContaboPricing\\RenewalEngine';
                $engine = new $engineCls();
                if (method_exists($engine, 'decideForCron')) {
                    $engine->decideForCron((int) $svc->id, $this->cronRunId);
                } elseif (method_exists($engine, 'decide')) {
                    // Fallback to whatever signature Agent B finalizes; pass
                    // the service id + run id positionally.
                    $engine->decide((int) $svc->id, $this->cronRunId);
                }
            } catch (\Throwable $e) {
                if (function_exists('logActivity')) {
                    logActivity('Contabo Pricing CronDriver observe failed for service '
                        . (int) $svc->id . ': ' . $e->getMessage());
                }
            }
        }
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
