<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Append-only audit log for renewal decisions.
 *
 * Contract:
 *   - Decisions are immutable. There is NO updateApplied() or markApplied().
 *     If the apply-vs-skip status of a service changes later (e.g. admin
 *     approves a queued increase), a NEW decision row is inserted with
 *     `parent_decision_id` referencing the original. The original row is
 *     untouched, forever.
 *   - `decided_at` is forced to "now" when absent so callers don't need to
 *     remember.
 *   - Required keys are validated up-front and throw \InvalidArgumentException
 *     before the DB is touched.
 *   - findByCronRun() supports per-cron-pass idempotency so the engine never
 *     emits two decisions for the same service in the same cron run.
 *
 * Tests can subclass and override storeRow() / lookupRow() to drop DB.
 */
class DecisionLog
{
    /**
     * Keys every decision row MUST carry. Anything missing is a programming
     * error in RenewalEngine and we want to fail loudly during dev.
     *
     * @var list<string>
     */
    private const REQUIRED_KEYS = [
        'service_id',
        'cron_run_id',
        'policy_used',
        'applied',
    ];

    /**
     * Insert a decision row and return the inserted id.
     *
     * @param array<string, mixed> $row Decision payload — see plan deliverable 1
     *                                  for the full column list. Required keys
     *                                  are listed in self::REQUIRED_KEYS.
     * @return int Inserted decision id.
     * @throws \InvalidArgumentException when a required key is missing.
     */
    public function insert(array $row): int
    {
        self::validate($row);

        if (!isset($row['decided_at']) || $row['decided_at'] === null || $row['decided_at'] === '') {
            $row['decided_at'] = date('Y-m-d H:i:s');
        }

        // `applied` MUST be 0 or 1 — coerce.
        $row['applied'] = !empty($row['applied']) ? 1 : 0;

        // `requires_notice` / `requires_admin_approval` — coerce same way if present.
        if (array_key_exists('requires_notice', $row)) {
            $row['requires_notice'] = !empty($row['requires_notice']) ? 1 : 0;
        }
        if (array_key_exists('requires_admin_approval', $row)) {
            $row['requires_admin_approval'] = !empty($row['requires_admin_approval']) ? 1 : 0;
        }
        if (array_key_exists('vendor_tax_recoverable', $row)) {
            $row['vendor_tax_recoverable'] = !empty($row['vendor_tax_recoverable']) ? 1 : 0;
        }
        if (array_key_exists('prices_include_output_tax', $row)) {
            $row['prices_include_output_tax'] = !empty($row['prices_include_output_tax']) ? 1 : 0;
        }

        return $this->storeRow($row);
    }

    /**
     * Look up the existing decision (if any) for a (cron_run_id, service_id)
     * pair. Used by RenewalEngine for idempotency within a single cron pass.
     *
     * @return array<string, mixed>|null
     */
    public function findByCronRun(string $cronRunId, int $serviceId): ?array
    {
        return $this->lookupRow($cronRunId, $serviceId);
    }

    /**
     * Backed-by-Capsule INSERT. Subclasses (tests) override.
     *
     * @param array<string, mixed> $row
     */
    protected function storeRow(array $row): int
    {
        return (int) Capsule::table('mod_contabo_price_decision')->insertGetId($row);
    }

    /**
     * Backed-by-Capsule SELECT. Subclasses (tests) override.
     *
     * @return array<string, mixed>|null
     */
    protected function lookupRow(string $cronRunId, int $serviceId): ?array
    {
        try {
            $row = Capsule::table('mod_contabo_price_decision')
                ->where('cron_run_id', $cronRunId)
                ->where('service_id', $serviceId)
                ->orderByDesc('id')
                ->first();
        } catch (\Throwable $e) {
            return null;
        }
        return $row ? (array) $row : null;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function validate(array $row): void
    {
        $missing = [];
        foreach (self::REQUIRED_KEYS as $key) {
            if (!array_key_exists($key, $row)) {
                $missing[] = $key;
            }
        }
        if ($missing !== []) {
            throw new \InvalidArgumentException(
                'DecisionLog::insert missing required key(s): ' . implode(', ', $missing)
            );
        }

        if (!is_int($row['service_id']) || $row['service_id'] <= 0) {
            throw new \InvalidArgumentException(
                'DecisionLog::insert service_id must be a positive int'
            );
        }
        if (!is_string($row['cron_run_id']) || $row['cron_run_id'] === '') {
            throw new \InvalidArgumentException(
                'DecisionLog::insert cron_run_id must be a non-empty string'
            );
        }
        if (!is_string($row['policy_used']) || $row['policy_used'] === '') {
            throw new \InvalidArgumentException(
                'DecisionLog::insert policy_used must be a non-empty string'
            );
        }
        // `applied` is coerced, not validated for type — caller may pass bool/int/string.
        // When applied=false, plan requires skip_reason — but RenewalEngine fills it.
        if (empty($row['applied']) && empty($row['skip_reason'])) {
            throw new \InvalidArgumentException(
                'DecisionLog::insert applied=false requires non-empty skip_reason'
            );
        }
    }
}
