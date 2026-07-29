<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class OneTimeSecretStore
{
    /**
     * @return array{secret_uuid:string,reveal_token:string,expires_at:string}
     */
    public function store(
        int $serviceId,
        string $type,
        string $plaintext,
        int $ttlSeconds = 1800,
        ?string $operationUuid = null
    ): array
    {
        if (!function_exists('encrypt')) {
            throw new ContaboProvisioningException('WHMCS encryption service is unavailable');
        }
        $uuid = Uuid::v4();
        $token = bin2hex(random_bytes(32));
        $expiresAt = date('Y-m-d H:i:s', time() + max(60, $ttlSeconds));
        Capsule::table('mod_securiacevps_secrets')->insert([
            'secret_uuid' => $uuid,
            'service_id' => $serviceId,
            'operation_uuid' => $operationUuid,
            'secret_type' => $type,
            'encrypted_value' => encrypt($plaintext),
            'reveal_token_hash' => hash('sha256', $token),
            'reveal_token_ciphertext' => encrypt($token),
            'maximum_reveals' => 1,
            'reveal_count' => 0,
            'expires_at' => $expiresAt,
            'revealed_at' => null,
            'destroyed_at' => null,
            'created_at' => date('Y-m-d H:i:s'),
            'updated_at' => date('Y-m-d H:i:s'),
        ]);
        return ['secret_uuid' => $uuid, 'reveal_token' => $token, 'expires_at' => $expiresAt];
    }

    /**
     * Prepare one stable credential per durable operation. Retrying an
     * ambiguous provider request reuses the same secret instead of rotating it
     * underneath the already-submitted action.
     *
     * @return array{secret_uuid:string,reveal_token:string,expires_at:string,plaintext:string}
     */
    public function prepareForOperation(
        int $serviceId,
        string $type,
        string $operationUuid,
        int $ttlSeconds = 1800
    ): array {
        if (!function_exists('decrypt')) {
            throw new ContaboProvisioningException('WHMCS encryption service is unavailable');
        }
        $existing = Capsule::table('mod_securiacevps_secrets')
            ->where('service_id', $serviceId)
            ->where('operation_uuid', $operationUuid)
            ->where('secret_type', $type)
            ->first();
        if ($existing !== null) {
            $row = (array) $existing;
            if (empty($row['destroyed_at'])) {
                $plaintext = decrypt((string) ($row['encrypted_value'] ?? ''));
                $token = decrypt((string) ($row['reveal_token_ciphertext'] ?? ''));
                if ($plaintext !== '' && $token !== '') {
                    return [
                        'secret_uuid' => (string) ($row['secret_uuid'] ?? ''),
                        'reveal_token' => $token,
                        'expires_at' => (string) ($row['expires_at'] ?? ''),
                        'plaintext' => $plaintext,
                    ];
                }
            }
            throw new ContaboProvisioningException(
                'The credential prepared for this operation is no longer available',
                'operation_credential_unavailable',
                'manual_review'
            );
        }

        $plaintext = SecretManager::generatePassword();
        $stored = $this->store($serviceId, $type, $plaintext, $ttlSeconds, $operationUuid);
        $stored['plaintext'] = $plaintext;
        return $stored;
    }

    public function reveal(int $serviceId, string $token): string
    {
        if (!function_exists('decrypt')) {
            throw new ContaboProvisioningException('WHMCS encryption service is unavailable');
        }
        $hash = hash('sha256', $token);
        return Capsule::connection()->transaction(function () use ($serviceId, $hash): string {
            $query = Capsule::table('mod_securiacevps_secrets')
                ->where('service_id', $serviceId)
                ->where('reveal_token_hash', $hash);
            if (method_exists($query, 'lockForUpdate')) {
                $query->lockForUpdate();
            }
            $rowObject = $query->first();
            if ($rowObject === null) {
                throw new ContaboProvisioningException('This credential link is invalid or has expired');
            }
            $row = (array) $rowObject;
            if (!empty($row['destroyed_at'])
                || (int) ($row['reveal_count'] ?? 0) >= (int) ($row['maximum_reveals'] ?? 1)
                || strtotime((string) ($row['expires_at'] ?? '1970-01-01')) < time()
            ) {
                throw new ContaboProvisioningException('This credential link is invalid or has expired');
            }
            $plaintext = decrypt((string) ($row['encrypted_value'] ?? ''));
            if ($plaintext === '') {
                throw new ContaboProvisioningException('This credential is no longer available');
            }
            $now = date('Y-m-d H:i:s');
            $updated = Capsule::table('mod_securiacevps_secrets')
                ->where('id', (int) $row['id'])
                ->whereNull('destroyed_at')
                ->where('reveal_count', '<', (int) ($row['maximum_reveals'] ?? 1))
                ->update([
                    'encrypted_value' => '',
                    'reveal_token_ciphertext' => '',
                    'reveal_count' => ((int) ($row['reveal_count'] ?? 0)) + 1,
                    'revealed_at' => $now,
                    'destroyed_at' => $now,
                    'updated_at' => $now,
                ]);
            if ($updated !== 1) {
                throw new ContaboProvisioningException('This credential link is invalid or has expired');
            }
            return $plaintext;
        });
    }

    /**
     * Return only the opaque reveal token and expiry for the newest usable
     * credential. The plaintext is decrypted exclusively by reveal().
     *
     * @return array{secret_uuid:string,reveal_token:string,secret_type:string,expires_at:string}|null
     */
    public function availableForService(int $serviceId): ?array
    {
        if (!function_exists('decrypt')) {
            return null;
        }
        $rows = Capsule::table('mod_securiacevps_secrets')
            ->where('service_id', $serviceId)
            ->orderByDesc('id')
            ->limit(20)
            ->get();
        foreach ($rows as $item) {
            $row = (array) $item;
            if (!empty($row['destroyed_at'])
                || (int) ($row['reveal_count'] ?? 0) >= (int) ($row['maximum_reveals'] ?? 1)
                || strtotime((string) ($row['expires_at'] ?? '1970-01-01')) < time()
            ) {
                continue;
            }
            $token = decrypt((string) ($row['reveal_token_ciphertext'] ?? ''));
            if ($token === '') {
                continue;
            }
            return [
                'secret_uuid' => (string) ($row['secret_uuid'] ?? ''),
                'reveal_token' => $token,
                'secret_type' => (string) ($row['secret_type'] ?? ''),
                'expires_at' => (string) ($row['expires_at'] ?? ''),
            ];
        }
        return null;
    }

    public function destroyExpired(): int
    {
        $rows = Capsule::table('mod_securiacevps_secrets')->get();
        $destroyed = 0;
        $now = date('Y-m-d H:i:s');
        foreach ($rows as $item) {
            $row = (array) $item;
            if (empty($row['destroyed_at']) && strtotime((string) ($row['expires_at'] ?? '1970-01-01')) < time()) {
                Capsule::table('mod_securiacevps_secrets')
                    ->where('id', (int) ($row['id'] ?? 0))
                    ->update([
                        'encrypted_value' => '',
                        'reveal_token_ciphertext' => '',
                        'destroyed_at' => $now,
                        'updated_at' => $now,
                    ]);
                $destroyed++;
            }
        }
        return $destroyed;
    }
}
