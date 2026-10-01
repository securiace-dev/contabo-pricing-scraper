<?php
declare(strict_types=1);

namespace ContaboPricing;

use DateTimeImmutable;
use InvalidArgumentException;
use RuntimeException;
use WHMCS\Database\Capsule;

/**
 * Validates and imports the immutable, versioned catalog envelope (exchange 1.0).
 *
 * Import is read-only with respect to WHMCS products and pricing. It writes
 * only addon-owned catalog tables; publication is a separate approval step.
 */
final class CatalogImportService
{
    private const VERSION_TABLE = 'mod_contabo_catalog_versions';
    private const ITEM_TABLE = 'mod_contabo_catalog_items';
    private const MAX_ITEMS = 50000;
    public const SUPPORTED_SCHEMA_VERSION = '1.0';

    /**
     * Guardrails (structured rejection, no version row written): an empty item
     * list, or a plan-item count that dropped from the previous version by more
     * than `scrape.max_drop_pct` percent (default 20) unless $opts['force'].
     *
     * @param array<string,mixed> $catalog
     * @param array{force?:bool} $opts
     * @return array{catalog_version:string,payload_hash:string,item_count:int,created:bool,error?:string,message?:string,previous_plan_count?:int,new_plan_count?:int}
     */
    public function import(array $catalog, int $adminId = 0, array $opts = []): array
    {
        $catalogVersion = trim((string) ($catalog['catalog_version'] ?? ''));
        $payloadHash = strtolower(trim((string) ($catalog['payload_hash'] ?? '')));
        $schemaVersion = trim((string) ($catalog['schema_version'] ?? ''));
        $observedAt = $this->mysqlTimestamp((string) ($catalog['source_observed_at'] ?? ''));
        $items = $catalog['items'] ?? null;

        if ($catalogVersion === '' || !preg_match('/^[A-Za-z0-9._:-]{1,120}$/', $catalogVersion)) {
            throw new InvalidArgumentException('The catalog version is missing or invalid.');
        }
        if ($schemaVersion !== self::SUPPORTED_SCHEMA_VERSION) {
            throw new RuntimeException('Unsupported catalog schema version: ' . $schemaVersion);
        }
        if (!is_array($items) || count($items) > self::MAX_ITEMS) {
            throw new RuntimeException('The catalog item list is invalid or exceeds the safe import limit.');
        }
        if ($items === []) {
            return $this->rejection(
                'empty_catalog',
                'The catalog contains no items; refusing to import an empty catalog.',
                $catalogVersion,
                $payloadHash,
                0
            );
        }
        if (!preg_match('/^[a-f0-9]{64}$/', $payloadHash)) {
            throw new RuntimeException('The catalog payload hash is invalid.');
        }

        $hashable = $catalog;
        unset($hashable['payload_hash']);
        $computedHash = hash('sha256', self::canonicalJson($hashable));
        if (!hash_equals($payloadHash, $computedHash)) {
            throw new RuntimeException('The catalog payload hash does not match its content.');
        }

        $normalizedItems = [];
        $seenMachineIds = [];
        foreach ($items as $item) {
            if (!is_array($item)) {
                throw new RuntimeException('The catalog contains a non-object item.');
            }
            $machineId = trim((string) ($item['machine_id'] ?? ''));
            $itemType = trim((string) ($item['item_type'] ?? ''));
            $label = trim((string) ($item['label'] ?? ''));
            $availability = trim((string) ($item['availability_state'] ?? ''));
            $itemHash = strtolower(trim((string) ($item['payload_hash'] ?? '')));
            $itemCatalogVersion = (string) ($item['catalog_version'] ?? '');
            $payload = $item['payload'] ?? null;

            if ($machineId === '' || strlen($machineId) > 191 || isset($seenMachineIds[$machineId])) {
                throw new RuntimeException('Catalog machine IDs must be present, bounded and unique.');
            }
            if ($itemCatalogVersion !== $catalogVersion) {
                throw new RuntimeException('A catalog item references a different catalog version.');
            }
            if ($itemType === '' || $label === '' || $availability === '') {
                throw new RuntimeException('A catalog item is missing its type, label or availability state.');
            }
            if (!is_array($payload) || !preg_match('/^[a-f0-9]{64}$/', $itemHash)) {
                throw new RuntimeException('A catalog item payload or hash is invalid.');
            }
            if (!hash_equals($itemHash, hash('sha256', self::canonicalJson($payload)))) {
                throw new RuntimeException('A catalog item payload hash does not match its content.');
            }

            $seenMachineIds[$machineId] = true;
            $normalizedItems[] = [
                'machine_id' => $machineId,
                'provider_id' => isset($item['provider_id']) && $item['provider_id'] !== null
                    ? (string) $item['provider_id']
                    : null,
                'item_type' => substr($itemType, 0, 60),
                'label' => substr($label, 0, 255),
                'availability_state' => substr($availability, 0, 40),
                'deprecated' => !empty($item['deprecated']) ? 1 : 0,
                'effective_at' => $this->mysqlTimestamp(
                    (string) ($item['effective_at'] ?? $catalog['effective_at'] ?? $observedAt)
                ),
                'source_observed_at' => $this->mysqlTimestamp(
                    (string) ($item['source_observed_at'] ?? $observedAt)
                ),
                'payload_hash' => $itemHash,
                'compatibility_json' => self::canonicalJson($item['compatibility'] ?? []),
                'payload_json' => self::canonicalJson($payload),
            ];
        }

        $existingObject = Capsule::table(self::VERSION_TABLE)
            ->where('catalog_version', $catalogVersion)
            ->first();
        if ($existingObject !== null) {
            $existing = (array) $existingObject;
            if (!hash_equals((string) ($existing['payload_hash'] ?? ''), $payloadHash)) {
                throw new RuntimeException('The catalog version already exists with different content.');
            }
            return [
                'catalog_version' => $catalogVersion,
                'payload_hash' => $payloadHash,
                'item_count' => count($normalizedItems),
                'created' => false,
            ];
        }

        if (empty($opts['force'])) {
            $drop = $this->planDropViolation($normalizedItems);
            if ($drop !== null) {
                return $this->rejection(
                    'plan_count_drop',
                    sprintf(
                        'Plan count dropped from %d to %d (more than %s%% allowed); re-import with force to override.',
                        $drop['previous'],
                        $drop['new'],
                        rtrim(rtrim(number_format($drop['max_pct'], 2, '.', ''), '0'), '.')
                    ),
                    $catalogVersion,
                    $payloadHash,
                    count($normalizedItems),
                    ['previous_plan_count' => $drop['previous'], 'new_plan_count' => $drop['new']]
                );
            }
        }

        Capsule::connection()->transaction(function () use (
            $catalog,
            $catalogVersion,
            $payloadHash,
            $observedAt,
            $adminId,
            $normalizedItems
        ): void {
            $now = date('Y-m-d H:i:s');
            $versionId = Capsule::table(self::VERSION_TABLE)->insertGetId([
                'catalog_version' => $catalogVersion,
                'source_version' => (string) ($catalog['source_version'] ?? ''),
                'state' => 'observed',
                'payload_hash' => $payloadHash,
                'source_observed_at' => $observedAt,
                'effective_at' => $this->mysqlTimestamp(
                    (string) ($catalog['effective_at'] ?? $observedAt)
                ),
                'imported_at' => $now,
                'imported_by_admin_id' => $adminId > 0 ? $adminId : null,
                'metadata_json' => self::canonicalJson([
                    'schema_version' => (string) ($catalog['schema_version'] ?? ''),
                    'profile_count' => is_array($catalog['profiles'] ?? null)
                        ? count($catalog['profiles'])
                        : 0,
                    'item_count' => count($normalizedItems),
                ]),
                'envelope_json' => self::canonicalJson($catalog),
                'created_at' => $now,
                'updated_at' => $now,
            ]);

            foreach ($normalizedItems as $item) {
                $item['catalog_version_id'] = (int) $versionId;
                $item['created_at'] = $now;
                $item['updated_at'] = $now;
                Capsule::table(self::ITEM_TABLE)->insert($item);
            }
        });

        return [
            'catalog_version' => $catalogVersion,
            'payload_hash' => $payloadHash,
            'item_count' => count($normalizedItems),
            'created' => true,
        ];
    }

    /**
     * @param list<array<string,mixed>> $normalizedItems
     * @return array{previous:int,new:int,max_pct:float}|null
     */
    private function planDropViolation(array $normalizedItems): ?array
    {
        $prev = Capsule::table(self::VERSION_TABLE)
            ->whereIn('state', ['observed', 'published'])
            ->orderByDesc('id')
            ->limit(1)
            ->get();
        $prevId = 0;
        foreach ($prev as $row) {
            $prevId = (int) (((array) $row)['id'] ?? 0);
            break;
        }
        if ($prevId <= 0) {
            return null;
        }
        $previous = (int) Capsule::table(self::ITEM_TABLE)
            ->where('catalog_version_id', $prevId)
            ->where('item_type', 'plan')
            ->count();
        if ($previous <= 0) {
            return null;
        }
        $new = 0;
        foreach ($normalizedItems as $item) {
            if ($item['item_type'] === 'plan') {
                $new++;
            }
        }
        $raw = Capsule::table('mod_contabo_settings')->where('key', 'scrape.max_drop_pct')->value('value');
        $maxPct = ($raw === null || !is_numeric($raw)) ? 20.0 : (float) $raw;
        $dropPct = (($previous - $new) / $previous) * 100.0;
        if ($new < $previous && $dropPct > $maxPct) {
            return ['previous' => $previous, 'new' => $new, 'max_pct' => $maxPct];
        }
        return null;
    }

    /**
     * @param array<string,int> $extra
     * @return array<string,mixed>
     */
    private function rejection(
        string $code,
        string $message,
        string $catalogVersion,
        string $payloadHash,
        int $itemCount,
        array $extra = []
    ): array {
        return array_merge([
            'catalog_version' => $catalogVersion,
            'payload_hash' => $payloadHash,
            'item_count' => $itemCount,
            'created' => false,
            'error' => $code,
            'message' => $message,
        ], $extra);
    }

    /**
     * @param mixed $value
     */
    public static function canonicalJson($value): string
    {
        $encoded = json_encode(
            self::normalize($value),
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
        );
        if (!is_string($encoded)) {
            throw new RuntimeException('Could not encode the catalog payload.');
        }
        return $encoded;
    }

    /**
     * @param mixed $value
     * @return mixed
     */
    private static function normalize($value)
    {
        if (!is_array($value)) {
            return $value;
        }
        if (self::isList($value)) {
            return array_map([self::class, 'normalize'], $value);
        }
        ksort($value, SORT_STRING);
        foreach ($value as $key => $item) {
            $value[$key] = self::normalize($item);
        }
        return $value;
    }

    /** @param array<mixed> $value */
    private static function isList(array $value): bool
    {
        $expected = 0;
        foreach ($value as $key => $_) {
            if ($key !== $expected++) {
                return false;
            }
        }
        return true;
    }

    private function mysqlTimestamp(string $value): string
    {
        try {
            return (new DateTimeImmutable($value))->format('Y-m-d H:i:s');
        } catch (\Throwable $e) {
            throw new InvalidArgumentException('The catalog contains an invalid observation timestamp.');
        }
    }
}
