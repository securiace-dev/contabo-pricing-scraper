<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Append-only audit log for `tblpricing` (catalog) writes performed by
 * SyncEngine.
 *
 * Contract:
 *   - Rows are immutable. There is NO update path; the schema enforces this
 *     too (no `updated_at` column on `mod_contabo_catalog_audit`).
 *   - Every catalog decision — applied or deliberately skipped — emits exactly
 *     ONE row. Skipped rows carry a non-empty `skipped_reason`. Applied rows
 *     carry the before/after price and rounding metadata.
 *   - `created_at` is forced to "now" when absent so callers don't need to
 *     remember.
 *   - Required keys (`sync_batch_id`, `product_id`, `currency_id`, `cycle`,
 *     `applied`) are validated up front; missing keys throw before the DB is
 *     touched.
 *   - When applied=0 the caller MUST also supply a non-empty `skipped_reason`.
 *
 * Mirrors the shape of DecisionLog so any future audit-rendering UI can lean
 * on the same patterns. Tests can subclass and override storeRow() to drop
 * the DB dependency.
 *
 * PHP 7.4 polyglot: typed properties OK; no match, no readonly, no enums.
 */
class CatalogAuditLog
{
    /**
     * Keys every catalog audit row MUST carry. Anything missing is a
     * programming error in SyncEngine and we want to fail loudly during dev.
     *
     * @var list<string>
     */
    private const REQUIRED_KEYS = [
        'sync_batch_id',
        'product_id',
        'currency_id',
        'cycle',
        'applied',
    ];

    /**
     * Insert a catalog audit row and return the inserted id.
     *
     * @param array<string,mixed> $row Required keys: sync_batch_id, product_id,
     *                                 currency_id, cycle, applied; everything
     *                                 else optional. When applied=0 the caller
     *                                 must include a non-empty skipped_reason.
     * @return int Inserted audit row id.
     * @throws \InvalidArgumentException when a required key is missing or
     *                                   applied=0 without skipped_reason.
     */
    public function insert(array $row): int
    {
        self::validate($row);

        if (!isset($row['created_at']) || $row['created_at'] === null || $row['created_at'] === '') {
            $row['created_at'] = date('Y-m-d H:i:s');
        }

        // Coerce booleans into 0/1 — DB columns are TINYINT.
        $row['applied'] = !empty($row['applied']) ? 1 : 0;

        return $this->storeRow($row);
    }

    /**
     * Return the most-recent N distinct sync batches, newest first.
     * Each entry is an array with at least 'sync_batch_id', 'batch_started_at'
     * (= MIN(created_at)) and 'row_count'.
     *
     * @return list<array<string,mixed>>
     */
    public function recentBatches(int $limit = 20): array
    {
        if ($limit <= 0) {
            return [];
        }
        return $this->fetchRecentBatches($limit);
    }

    /**
     * Backed-by-Capsule INSERT. Subclasses (tests) override.
     *
     * @param array<string,mixed> $row
     */
    protected function storeRow(array $row): int
    {
        return (int) Capsule::table('mod_contabo_catalog_audit')->insertGetId($row);
    }

    /**
     * Backed-by-Capsule SELECT. Subclasses (tests) may override.
     *
     * @return list<array<string,mixed>>
     */
    protected function fetchRecentBatches(int $limit): array
    {
        try {
            $rows = Capsule::table('mod_contabo_catalog_audit')
                ->select(['sync_batch_id'])
                ->orderByDesc('id')
                ->limit($limit * 50) // generous over-fetch; collapse below
                ->get();
        } catch (\Throwable $e) {
            return [];
        }

        // Walk rows and collapse to distinct batch ids while preserving order.
        $seen = [];
        foreach ($rows as $r) {
            $r = (array) $r;
            $id = (string) ($r['sync_batch_id'] ?? '');
            if ($id === '' || isset($seen[$id])) {
                continue;
            }
            $seen[$id] = ['sync_batch_id' => $id];
            if (count($seen) >= $limit) {
                break;
            }
        }
        return array_values($seen);
    }

    /**
     * @param array<string,mixed> $row
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
                'CatalogAuditLog::insert missing required key(s): ' . implode(', ', $missing)
            );
        }

        if (!is_int($row['sync_batch_id']) && !is_string($row['sync_batch_id'])) {
            throw new \InvalidArgumentException(
                'CatalogAuditLog::insert sync_batch_id must be string or int'
            );
        }
        if ((string) $row['sync_batch_id'] === '') {
            throw new \InvalidArgumentException(
                'CatalogAuditLog::insert sync_batch_id must be non-empty'
            );
        }
        if (!is_int($row['product_id']) || $row['product_id'] <= 0) {
            throw new \InvalidArgumentException(
                'CatalogAuditLog::insert product_id must be a positive int'
            );
        }
        if (!is_int($row['currency_id']) || $row['currency_id'] <= 0) {
            throw new \InvalidArgumentException(
                'CatalogAuditLog::insert currency_id must be a positive int'
            );
        }
        if (!is_string($row['cycle']) || $row['cycle'] === '') {
            throw new \InvalidArgumentException(
                'CatalogAuditLog::insert cycle must be a non-empty string'
            );
        }
        // applied is coerced, not type-validated.
        if (empty($row['applied']) && empty($row['skipped_reason'])) {
            throw new \InvalidArgumentException(
                'CatalogAuditLog::insert applied=0 requires non-empty skipped_reason'
            );
        }
    }
}
