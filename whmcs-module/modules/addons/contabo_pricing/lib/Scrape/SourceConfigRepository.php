<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use ContaboPricing\SecretStore;
use WHMCS\Database\Capsule;

/**
 * CRUD over mod_contabo_scrape_sources. API keys are sealed at rest; the
 * plaintext only exists in SourceConfig value objects handed to adapters.
 */
final class SourceConfigRepository
{
    private const TABLE = 'mod_contabo_scrape_sources';

    /** @return list<array<string,mixed>> raw rows (api_key_enc stays sealed), ordered by priority then id */
    public function all(): array
    {
        $rows = [];
        foreach (Capsule::table(self::TABLE)->get() as $r) {
            $rows[] = (array) $r;
        }
        usort($rows, static function (array $a, array $b): int {
            $pa = ($a["priority"] ?? null) === null ? PHP_INT_MAX : (int) $a["priority"];
            $pb = ($b["priority"] ?? null) === null ? PHP_INT_MAX : (int) $b["priority"];
            if ($pa !== $pb) {
                return $pa <=> $pb;
            }
            return ((int) ($a['id'] ?? 0)) <=> ((int) ($b['id'] ?? 0));
        });
        return $rows;
    }

    /** @return list<array<string,mixed>> */
    public function enabled(): array
    {
        return array_values(array_filter($this->all(), static function (array $r): bool {
            return (int) ($r['enabled'] ?? 0) === 1;
        }));
    }

    /** @return array<string,mixed>|null */
    public function find(string $sourceId): ?array
    {
        $row = Capsule::table(self::TABLE)->where('source_id', $sourceId)->first();
        return $row === null ? null : (array) $row;
    }

    /** Builds the adapter config (plaintext key) for a stored row. */
    public function toConfig(array $row): SourceConfig
    {
        $opts = json_decode((string) ($row['options_json'] ?? ''), true);
        return new SourceConfig(
            (string) $row['source_id'],
            (string) ($row['base_url'] ?? ''),
            SecretStore::open((string) ($row['api_key_enc'] ?? '')),
            is_array($opts) ? $opts : []
        );
    }

    /**
     * Upserts the editable fields. A blank/absent 'api_key' keeps the existing
     * sealed key. Unknown source ids are rejected (rows are seeded by v15).
     *
     * @param array<string,mixed> $data keys: enabled, base_url, api_key, priority,
     *        monthly_budget_micro, per_run_cap_micro, options (array) / options_json
     */
    public function save(string $sourceId, array $data): void
    {
        $existing = $this->find($sourceId);
        if ($existing === null) {
            throw new \InvalidArgumentException('Unknown scrape source: ' . $sourceId);
        }

        $values = ['updated_at' => date('Y-m-d H:i:s')];
        if (array_key_exists('enabled', $data)) {
            $values['enabled'] = !empty($data['enabled']) ? 1 : 0;
        }
        if (array_key_exists('base_url', $data)) {
            $values['base_url'] = $data['base_url'] === '' || $data['base_url'] === null
                ? null : substr((string) $data['base_url'], 0, 255);
        }
        if (array_key_exists('priority', $data)) {
            $values['priority'] = $data['priority'] === '' || $data['priority'] === null
                ? null : (int) $data['priority'];
        }
        if (array_key_exists('monthly_budget_micro', $data)) {
            $values['monthly_budget_micro'] = max(0, (int) $data['monthly_budget_micro']);
        }
        if (array_key_exists('per_run_cap_micro', $data)) {
            $values['per_run_cap_micro'] = max(0, (int) $data['per_run_cap_micro']);
        }
        if (array_key_exists('options', $data) && is_array($data['options'])) {
            $values['options_json'] = (string) json_encode($data['options'], JSON_UNESCAPED_SLASHES);
        } elseif (array_key_exists('options_json', $data)) {
            $values['options_json'] = $data['options_json'] === '' ? null : (string) $data['options_json'];
        }
        $key = (string) ($data['api_key'] ?? '');
        if ($key !== '') {
            $values['api_key_enc'] = SecretStore::seal($key);
        }

        Capsule::table(self::TABLE)->where('source_id', $sourceId)->update($values);
    }

    /** Records a fetch outcome: success resets the failure streak. */
    public function recordAttemptOutcome(string $sourceId, bool $ok, ?string $error = null): void
    {
        $row = $this->find($sourceId);
        if ($row === null) {
            return;
        }
        $now = date('Y-m-d H:i:s');
        if ($ok) {
            $values = ['consecutive_failures' => 0, 'last_ok_at' => $now, 'updated_at' => $now];
        } else {
            $values = [
                'consecutive_failures' => ((int) ($row['consecutive_failures'] ?? 0)) + 1,
                'last_fail_at' => $now,
                'last_error' => $error === null ? null : substr($error, 0, 255),
                'updated_at' => $now,
            ];
        }
        Capsule::table(self::TABLE)->where('source_id', $sourceId)->update($values);
    }
}
