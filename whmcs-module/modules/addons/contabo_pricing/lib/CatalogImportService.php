<?php
declare(strict_types=1);

namespace ContaboPricing;

use DateTimeImmutable;
use InvalidArgumentException;
use RuntimeException;
use WHMCS\Database\Capsule;

/**
 * Validates and imports the Rust service's immutable, versioned catalog.
 *
 * Import is read-only with respect to WHMCS products and pricing. It writes
 * only addon-owned catalog tables; publication is a separate approval step.
 */
final class CatalogImportService
{
    private const VERSION_TABLE = 'mod_contabo_catalog_versions';
    private const ITEM_TABLE = 'mod_contabo_catalog_items';
    private const MAX_ITEMS = 50000;

    /**
     * @param array<string,mixed> $catalog
     * @return array{catalog_version:string,payload_hash:string,item_count:int,created:bool}
     */
    public function import(array $catalog, int $adminId = 0): array
    {
        $catalogVersion = trim((string) ($catalog['catalog_version'] ?? ''));
        $payloadHash = strtolower(trim((string) ($catalog['payload_hash'] ?? '')));
        $schemaVersion = trim((string) ($catalog['schema_version'] ?? ''));
        $observedAt = $this->mysqlTimestamp((string) ($catalog['source_observed_at'] ?? ''));
        $items = $catalog['items'] ?? null;

        if ($catalogVersion === '' || !preg_match('/^[A-Za-z0-9._:-]{1,120}$/', $catalogVersion)) {
            throw new InvalidArgumentException('The Rust catalog version is missing or invalid.');
        }
        if ($schemaVersion !== '1.0') {
            throw new RuntimeException('Unsupported Rust catalog schema version: ' . $schemaVersion);
        }
        if (!is_array($items) || count($items) > self::MAX_ITEMS) {
            throw new RuntimeException('The Rust catalog item list is invalid or exceeds the safe import limit.');
        }
        if (!preg_match('/^[a-f0-9]{64}$/', $payloadHash)) {
            throw new RuntimeException('The Rust catalog payload hash is invalid.');
        }

        $hashable = $catalog;
        unset($hashable['payload_hash']);
        $computedHash = hash('sha256', self::canonicalJson($hashable));
        if (!hash_equals($payloadHash, $computedHash)) {
            throw new RuntimeException('The Rust catalog payload hash does not match its content.');
        }

        $normalizedItems = [];
        $seenMachineIds = [];
        foreach ($items as $item) {
            if (!is_array($item)) {
                throw new RuntimeException('The Rust catalog contains a non-object item.');
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
