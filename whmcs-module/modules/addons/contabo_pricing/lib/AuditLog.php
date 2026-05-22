<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Light wrapper around mod_contabo_sync_log retention. Sync runs themselves
 * are written by SyncEngine; this class only handles pruning + read helpers.
 */
class AuditLog
{
    /** Delete sync_log rows older than $days. Returns count deleted. */
    public function prune(int $days): int
    {
        if ($days <= 0) return 0;
        $cutoff = date('Y-m-d H:i:s', time() - ($days * 86400));
        return (int) Capsule::table('mod_contabo_sync_log')
            ->where('started_at', '<', $cutoff)
            ->delete();
    }

    /** @return list<array<string, mixed>> */
    public function recent(int $limit = 50): array
    {
        return Capsule::table('mod_contabo_sync_log')
            ->orderByDesc('id')->limit($limit)
            ->get()->map(static fn ($r) => (array) $r)->all();
    }
}
