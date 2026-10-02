<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use WHMCS\Database\Capsule;

/** Writes mod_contabo_decisions rows: why a run was imported, held or rejected. */
final class DecisionRecorder
{
    public const BY_RULES = 'rules';
    public const BY_JEV = 'jev';
    public const BY_ADMIN = 'admin';

    /**
     * @param array<mixed>|string $rules  gate/bucket evidence (arrays are JSON-encoded)
     * @param array<mixed>|null   $jev    judge verdict, never contains the API key
     * @return int decision id
     */
    public function record(
        int $runId,
        string $outcome,
        $rules,
        ?array $jev,
        ?float $threshold,
        string $decidedBy,
        int $adminId = 0
    ): int {
        return (int) Capsule::table('mod_contabo_decisions')->insertGetId([
            'run_id' => $runId,
            'outcome' => substr($outcome, 0, 24),
            'rules_json' => is_string($rules) ? $rules : (string) json_encode($rules, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            'jev_json' => $jev === null ? null : (string) json_encode($jev, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            'threshold' => $threshold,
            'decided_by' => substr($decidedBy, 0, 16),
            'admin_id' => $adminId > 0 ? $adminId : null,
            'created_at' => date('Y-m-d H:i:s'),
        ]);
    }

    /** @return array<string,mixed>|null */
    public function find(int $id): ?array
    {
        $r = Capsule::table('mod_contabo_decisions')->where('id', $id)->first();
        if ($r === null) {
            return null;
        }
        $r = (array) $r;
        $r['rules'] = json_decode((string) ($r['rules_json'] ?? ''), true);
        $r['jev'] = json_decode((string) ($r['jev_json'] ?? ''), true);
        return $r;
    }
}
