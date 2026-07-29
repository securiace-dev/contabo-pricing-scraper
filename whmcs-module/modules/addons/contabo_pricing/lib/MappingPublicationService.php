<?php

declare(strict_types=1);

namespace ContaboPricing;

use InvalidArgumentException;
use RuntimeException;
use WHMCS\Database\Capsule;

/**
 * Preview-first publication boundary between mutable administrator mappings
 * and immutable provisioning inputs.
 *
 * A draft publication is harmless: it does not change the active mapping.
 * Approval verifies the exact preview hash under a named lock and atomically
 * advances the mapping pointer. Paid-order snapshots then copy the published
 * payload; they never read mutable mapping fields at provisioning time.
 */
final class MappingPublicationService
{
    private const TABLE = 'mod_contabo_mapping_publications';
    private const APPROVAL_TABLE = 'mod_contabo_publication_approvals';
    private const MAPPING_TABLE = 'mod_contabo_mapping';
    private const CATALOG_TABLE = 'mod_contabo_catalog_versions';

    /**
     * @param array<string,mixed> $selection
     * @return array<string,mixed>
     */
    public function preview(int $mappingId, array $selection): array
    {
        if ($mappingId <= 0) {
            throw new InvalidArgumentException('A valid mapping is required.');
        }

        $mappingObject = Capsule::table(self::MAPPING_TABLE)->where('id', $mappingId)->first();
        if ($mappingObject === null) {
            throw new RuntimeException('The selected mapping does not exist.');
        }
        $mapping = (array) $mappingObject;

        $catalogVersion = trim((string) ($selection['rust_catalog_version'] ?? ''));
        $providerSkuId = trim((string) ($selection['provider_sku_id'] ?? ''));
        $regionId = trim((string) ($selection['region_id'] ?? ''));
        $imageId = trim((string) ($selection['image_id'] ?? ''));
        $managementCode = trim((string) ($selection['management_code'] ?? 'self_managed'));

        if ($catalogVersion === '' || $providerSkuId === '' || $regionId === '' || $imageId === '') {
            throw new InvalidArgumentException(
                'Catalog version, provider SKU, region and image are required before publication.'
            );
        }
        if (!in_array($managementCode, ['self_managed', 'lite', 'pro', 'enterprise'], true)) {
            throw new InvalidArgumentException('The management code is not recognized.');
        }

        $catalogObject = Capsule::table(self::CATALOG_TABLE)
            ->where('catalog_version', $catalogVersion)
            ->first();
        if ($catalogObject === null) {
            throw new RuntimeException('The selected Rust catalog version has not been imported.');
        }
        $catalog = (array) $catalogObject;
        if (in_array((string) ($catalog['state'] ?? ''), ['invalid', 'retired'], true)) {
            throw new RuntimeException('The selected catalog version is not publishable.');
        }

        $payload = [
            'schema_version' => 1,
            'profile_id' => (int) ($mapping['profile_id'] ?? 0),
            'whmcs' => [
                'product_id' => (int) ($mapping['product_id'] ?? 0),
                'product_group_id' => isset($mapping['product_group_id'])
                    ? (int) $mapping['product_group_id']
                    : null,
            ],
            'catalog' => [
                'version' => $catalogVersion,
                'payload_hash' => (string) ($catalog['payload_hash'] ?? ''),
                'source_observed_at' => (string) ($catalog['source_observed_at'] ?? ''),
            ],
            'provider' => [
                'sku_id' => $providerSkuId,
                'region_id' => $regionId,
                'image_id' => $imageId,
            ],
            'management_code' => $managementCode,
            'pricing_policy' => [
                'catalog_cycles_mask' => (int) ($mapping['catalog_cycles_mask'] ?? 0),
                'renewal_cycles_mask' => (int) ($mapping['renewal_cycles_mask'] ?? 0),
                'rounding_mode' => (string) ($mapping['rounding_mode'] ?? 'exact_2_decimals'),
                'respect_disabled_cycles' => !empty($mapping['respect_disabled_cycles']),
                'overwrite_free_cycles' => !empty($mapping['overwrite_free_cycles']),
                'sync_setup_fees' => !empty($mapping['sync_setup_fees']),
                'markup_overrides' => $this->decodeObject($mapping['markup_overrides_json'] ?? null),
                'setup_fee_overrides' => $this->decodeObject($mapping['setup_fee_overrides_json'] ?? null),
                'source_overrides' => $this->decodeObject($mapping['source_overrides_json'] ?? null),
            ],
        ];

        $payloadJson = self::canonicalJson($payload);
        $payloadHash = hash('sha256', $payloadJson);
        $mappingVersion = sprintf(
            'map-p%d-%s',
            (int) ($mapping['product_id'] ?? 0),
            substr($payloadHash, 0, 24)
        );
        $now = date('Y-m-d H:i:s');

        $existing = Capsule::table(self::TABLE)
            ->where('mapping_version', $mappingVersion)
            ->first();
        if ($existing === null) {
            Capsule::table(self::TABLE)->insert([
                'mapping_version' => $mappingVersion,
                'profile_id' => (int) ($mapping['profile_id'] ?? 0),
                'product_id' => (int) ($mapping['product_id'] ?? 0),
                'catalog_version_id' => (int) ($catalog['id'] ?? 0),
                'provider_sku_id' => $providerSkuId,
                'state' => 'draft',
                'payload_hash' => $payloadHash,
                'payload_json' => $payloadJson,
                'approved_by_admin_id' => null,
                'approved_at' => null,
                'effective_at' => null,
                'supersedes_mapping_version' => $mapping['published_mapping_version'] ?? null,
                'created_at' => $now,
                'updated_at' => $now,
            ]);
        } else {
            $row = (array) $existing;
            if (!hash_equals((string) ($row['payload_hash'] ?? ''), $payloadHash)) {
                throw new RuntimeException('Publication identity collision detected.');
            }
        }

        return [
            'mapping_version' => $mappingVersion,
            'preview_hash' => $payloadHash,
            'state' => $existing === null ? 'draft' : (string) (((array) $existing)['state'] ?? 'draft'),
            'payload' => $payload,
        ];
    }

    /**
     * @return array<string,mixed>
     */
    public function approve(
        string $mappingVersion,
        string $previewHash,
        int $adminId,
        string $confirmation,
        string $reason = ''
    ): array {
        $mappingVersion = trim($mappingVersion);
        $previewHash = strtolower(trim($previewHash));
        if ($mappingVersion === '' || strlen($previewHash) !== 64) {
            throw new InvalidArgumentException('The publication version and preview hash are required.');
        }
        if ($adminId <= 0) {
            throw new InvalidArgumentException('An authenticated administrator is required.');
        }
        if (!hash_equals('PUBLISH MAPPING', trim($confirmation))) {
            throw new InvalidArgumentException('Type PUBLISH MAPPING to approve this publication.');
        }

        $lock = new Lock();
        $lockName = 'contabo_mapping_publish_' . substr(hash('sha256', $mappingVersion), 0, 32);
        $token = $lock->acquire($lockName, 120);
        if ($token === null) {
            throw new RuntimeException('This mapping is already being published. Try again shortly.');
        }

        try {
            /** @var array<string,mixed> $result */
            $result = Capsule::connection()->transaction(function () use (
                $mappingVersion,
                $previewHash,
                $adminId,
                $reason
            ): array {
                $publicationObject = Capsule::table(self::TABLE)
                    ->where('mapping_version', $mappingVersion)
                    ->first();
                if ($publicationObject === null) {
                    throw new RuntimeException('The publication preview no longer exists.');
                }
                $publication = (array) $publicationObject;
                $storedHash = strtolower((string) ($publication['payload_hash'] ?? ''));
                if (!hash_equals($storedHash, $previewHash)) {
                    throw new RuntimeException('The preview changed. Generate and review a new preview.');
                }

                if ((string) ($publication['state'] ?? '') === 'published') {
                    return $publication;
                }
                if ((string) ($publication['state'] ?? '') !== 'draft') {
                    throw new RuntimeException('Only a draft mapping publication can be approved.');
                }

                $payload = json_decode((string) ($publication['payload_json'] ?? ''), true);
                if (!is_array($payload)) {
                    throw new RuntimeException('The publication payload is invalid.');
                }

                $productId = (int) ($publication['product_id'] ?? 0);
                $profileId = (int) ($publication['profile_id'] ?? 0);
                $mappingObject = Capsule::table(self::MAPPING_TABLE)
                    ->where('product_id', $productId)
                    ->where('profile_id', $profileId)
                    ->first();
                if ($mappingObject === null) {
                    throw new RuntimeException('The source mapping no longer exists.');
                }
                $mapping = (array) $mappingObject;
                $now = date('Y-m-d H:i:s');

                Capsule::table(self::TABLE)
                    ->where('product_id', $productId)
                    ->where('state', 'published')
                    ->update([
                        'state' => 'superseded',
                        'updated_at' => $now,
                    ]);

                Capsule::table(self::TABLE)
                    ->where('mapping_version', $mappingVersion)
                    ->where('state', 'draft')
                    ->update([
                        'state' => 'published',
                        'approved_by_admin_id' => $adminId,
                        'approved_at' => $now,
                        'effective_at' => $now,
                        'supersedes_mapping_version' => $mapping['published_mapping_version'] ?? null,
                        'updated_at' => $now,
                    ]);

                Capsule::table(self::MAPPING_TABLE)
                    ->where('id', (int) ($mapping['id'] ?? 0))
                    ->update([
                        'published_mapping_version' => $mappingVersion,
                        'provider_sku_id' => (string) ($payload['provider']['sku_id'] ?? ''),
                        'rust_catalog_version' => (string) ($payload['catalog']['version'] ?? ''),
                        'mapping_state' => 'published',
                        'mapping_payload_hash' => $storedHash,
                        'mapping_effective_at' => $now,
                        'updated_at' => $now,
                    ]);

                Capsule::table(self::APPROVAL_TABLE)->insert([
                    'publication_type' => 'mapping',
                    'publication_version' => $mappingVersion,
                    'decision' => 'approved',
                    'admin_id' => $adminId,
                    'reason' => trim($reason) !== '' ? trim($reason) : null,
                    'preview_hash' => $storedHash,
                    'created_at' => $now,
                ]);

                $publication['state'] = 'published';
                $publication['approved_by_admin_id'] = $adminId;
                $publication['approved_at'] = $now;
                $publication['effective_at'] = $now;
                return $publication;
            });
        } finally {
            $lock->release($lockName, $token);
        }

        return $result;
    }

    /**
     * @return array<string,mixed>
     */
    private function decodeObject($raw): array
    {
        if (is_array($raw)) {
            return $raw;
        }
        if (!is_string($raw) || trim($raw) === '') {
            return [];
        }
        $decoded = json_decode($raw, true);
        return is_array($decoded) ? $decoded : [];
    }

    /**
     * @param mixed $value
     */
    private static function canonicalJson($value): string
    {
        $normalized = self::normalize($value);
        $encoded = json_encode(
            $normalized,
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
        );
        if (!is_string($encoded)) {
            throw new RuntimeException('Could not encode the publication payload.');
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

    /**
     * PHP 7.4-compatible array_is_list().
     *
     * @param array<mixed> $value
     */
    private static function isList(array $value): bool
    {
        $index = 0;
        foreach ($value as $key => $_) {
            if ($key !== $index) {
                return false;
            }
            $index++;
        }
        return true;
    }
}
