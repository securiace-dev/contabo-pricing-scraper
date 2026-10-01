<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use WHMCS\Database\Capsule;

/** Persistence for mod_contabo_scrape_runs and mod_contabo_scrape_run_attempts. */
final class RunRepository
{
    public const RUNS = 'mod_contabo_scrape_runs';
    public const ATTEMPTS = 'mod_contabo_scrape_run_attempts';

    public const STATE_RUNNING = 'running';
    public const STATE_SUCCEEDED = 'succeeded';
    public const STATE_NEEDS_REVIEW = 'needs_review';
    public const STATE_REJECTED = 'rejected';
    public const STATE_FAILED = 'failed';
    public const STATE_DRY_RUN = 'dry_run';
    public const STATE_SKIPPED_LOCKED = 'skipped_locked';

    /** @var list<string> columns stored as JSON text */
    private const JSON_COLUMNS = ['families_json', 'gates_json', 'envelope_json'];

    public function create(string $trigger, int $adminId, bool $dryRun, string $state = self::STATE_RUNNING): int
    {
        return (int) Capsule::table(self::RUNS)->insertGetId([
            'trigger' => substr($trigger, 0, 16),
            'state' => $state,
            'started_at' => date('Y-m-d H:i:s'),
            'admin_id' => $adminId > 0 ? $adminId : null,
            'dry_run' => $dryRun ? 1 : 0,
            'plan_count' => 0,
            'total_cost_micro' => 0,
        ]);
    }

    /** @param array<string,mixed> $fields array values for *_json columns are encoded */
    public function update(int $runId, array $fields): void
    {
        foreach (self::JSON_COLUMNS as $c) {
            if (array_key_exists($c, $fields) && !is_string($fields[$c]) && $fields[$c] !== null) {
                $fields[$c] = (string) json_encode($fields[$c], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            }
        }
        if (isset($fields['error'])) {
            $fields['error'] = substr((string) $fields['error'], 0, 65000);
        }
        Capsule::table(self::RUNS)->where('id', $runId)->update($fields);
    }

    public function finish(int $runId, string $state, array $fields = []): void
    {
        $this->update($runId, array_merge($fields, ['state' => $state, 'finished_at' => date('Y-m-d H:i:s')]));
    }

    /** @param array<string,mixed> $a attempt record (see keys below) */
    public function addAttempt(int $runId, array $a): int
    {
        return (int) Capsule::table(self::ATTEMPTS)->insertGetId([
            'run_id' => $runId,
            'family' => substr((string) ($a['family'] ?? ''), 0, 20),
            'url' => substr((string) ($a['url'] ?? ''), 0, 255),
            'source_id' => substr((string) ($a['source_id'] ?? ''), 0, 40),
            'served_by' => isset($a['served_by']) ? substr((string) $a['served_by'], 0, 80) : null,
            'http_status' => isset($a['http_status']) ? (int) $a['http_status'] : null,
            'ok' => !empty($a['ok']) ? 1 : 0,
            'sapper_present' => !empty($a['sapper_present']) ? 1 : 0,
            'strategy' => isset($a['strategy']) ? substr((string) $a['strategy'], 0, 24) : null,
            'plan_count' => (int) ($a['plan_count'] ?? 0),
            'cost_micro' => (int) ($a['cost_micro'] ?? 0),
            'latency_ms' => (int) ($a['latency_ms'] ?? 0),
            'html_sha256' => $a['html_sha256'] ?? null,
            'html_bytes' => (int) ($a['html_bytes'] ?? 0),
            'error' => isset($a['error']) ? substr((string) $a['error'], 0, 500) : null,
            'final_url' => isset($a['final_url']) ? substr((string) $a['final_url'], 0, 255) : null,
            'created_at' => date('Y-m-d H:i:s'),
        ]);
    }

    /** @return array<string,mixed>|null decoded run row */
    public function find(int $runId): ?array
    {
        $r = Capsule::table(self::RUNS)->where('id', $runId)->first();
        return $r === null ? null : $this->decode((array) $r);
    }

    /** @return list<array<string,mixed>> attempts of a run in insertion order */
    public function attempts(int $runId): array
    {
        $out = [];
        foreach (Capsule::table(self::ATTEMPTS)->where('run_id', $runId)->orderBy('id')->get() as $r) {
            $out[] = (array) $r;
        }
        return $out;
    }

    /**
     * Newest run that actually shipped a catalog (state succeeded) and the
     * plans in its stored envelope: the baseline the validator diffs against.
     *
     * @return array{run:array<string,mixed>, plans:list<array<string,mixed>>}|null
     */
    public function latestSucceeded(): ?array
    {
        $r = Capsule::table(self::RUNS)->where('state', self::STATE_SUCCEEDED)->orderByDesc('id')->limit(1)->first();
        if ($r === null) {
            return null;
        }
        $run = $this->decode((array) $r);
        $plans = is_array($run['envelope_json'] ?? null) && is_array($run['envelope_json']['plans'] ?? null)
            ? array_values($run['envelope_json']['plans']) : [];
        return ['run' => $run, 'plans' => $plans];
    }

    /** @return list<array<string,mixed>> newest first, envelope omitted for weight */
    public function listRuns(int $limit = 50): array
    {
        $out = [];
        foreach (Capsule::table(self::RUNS)->orderByDesc('id')->limit($limit)->get() as $r) {
            $row = (array) $r;
            unset($row['envelope_json']);
            $out[] = $this->decode($row);
        }
        return $out;
    }

    /** @return array{run:array<string,mixed>, attempts:list<array<string,mixed>>, decision:?array<string,mixed>}|null */
    public function detail(int $runId): ?array
    {
        $run = $this->find($runId);
        if ($run === null) {
            return null;
        }
        $decision = null;
        $did = (int) ($run['decision_id'] ?? 0);
        if ($did > 0) {
            $decision = (new DecisionRecorder())->find($did);
        }
        return ['run' => $run, 'attempts' => $this->attempts($runId), 'decision' => $decision];
    }

    /**
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    private function decode(array $row): array
    {
        foreach (self::JSON_COLUMNS as $c) {
            if (isset($row[$c]) && is_string($row[$c]) && $row[$c] !== '') {
                $d = json_decode($row[$c], true);
                $row[$c] = $d === null ? $row[$c] : $d;
            }
        }
        return $row;
    }
}
