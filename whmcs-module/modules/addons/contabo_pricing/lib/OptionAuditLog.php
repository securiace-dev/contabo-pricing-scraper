<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Phase A.6.2 — append-only audit for configurable-option sync.
 *
 * One row per observed / applied / skipped action against the WHMCS config
 * tables, keyed by a per-run `sync_batch_id` (UUID). Writes to
 * `mod_contabo_config_option_audit` (schema v5, design §8). Old/new value
 * blobs are stored as JSON text (the column is LONGTEXT — never a JSON type).
 *
 * Tests subclass this and override {@see storeRow()} to drop the DB dependency,
 * mirroring {@see DecisionLog}.
 *
 * PHP 7.4 polyglot: no enums, no readonly, no constructor promotion.
 */
class OptionAuditLog
{
    public const ACTION_OBSERVED      = 'observed';
    public const ACTION_INSERT        = 'insert';
    public const ACTION_UPDATE        = 'update';
    public const ACTION_DELETE        = 'delete';
    public const ACTION_SKIP_NO_CHANGE = 'skip_no_change';
    public const ACTION_SKIP_DISABLED = 'skip_disabled';
    public const ACTION_ERROR         = 'error';

    private const TABLE = 'mod_contabo_config_option_audit';

    /** @var string */
    private $batchId;

    public function __construct(string $syncBatchId)
    {
        $this->batchId = $syncBatchId;
    }

    public function batchId(): string
    {
        return $this->batchId;
    }

    /**
     * Record one audit row. `$oldValue` / `$newValue` are encoded to JSON text
     * (null stays null). Returns the inserted row id (0 in a dry test stub).
     *
     * @param array<string,mixed>|null $oldValue
     * @param array<string,mixed>|null $newValue
     */
    public function record(
        int $profileId,
        ?string $dimensionKey,
        string $targetTable,
        ?int $targetId,
        string $action,
        ?array $oldValue,
        ?array $newValue,
        ?string $note = null
    ): int {
        $row = [
            'sync_batch_id'  => $this->batchId,
            'profile_id'     => $profileId,
            'dimension_key'  => $dimensionKey,
            'target_table'   => $targetTable,
            'target_id'      => $targetId,
            'action'         => $action,
            'old_value_json' => $oldValue === null ? null : self::encode($oldValue),
            'new_value_json' => $newValue === null ? null : self::encode($newValue),
            'note'           => $note !== null ? mb_substr($note, 0, 255) : null,
            'created_at'     => date('Y-m-d H:i:s'),
        ];

        return $this->storeRow($row);
    }

    /** Convenience for the observe (dry-run preview) path. */
    public function observe(
        int $profileId,
        ?string $dimensionKey,
        string $targetTable,
        array $newValue,
        ?string $note = null
    ): int {
        return $this->record(
            $profileId,
            $dimensionKey,
            $targetTable,
            null,
            self::ACTION_OBSERVED,
            null,
            $newValue,
            $note
        );
    }

    /**
     * @param array<string,mixed> $row
     */
    protected function storeRow(array $row): int
    {
        return (int) Capsule::table(self::TABLE)->insertGetId($row);
    }

    /**
     * @param array<string,mixed> $value
     */
    private static function encode(array $value): string
    {
        $json = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return $json === false ? '{}' : $json;
    }
}
