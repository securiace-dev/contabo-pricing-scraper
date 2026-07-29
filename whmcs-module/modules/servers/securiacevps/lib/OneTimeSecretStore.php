<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class OneTimeSecretStore
{
    /**
     * @return array{secret_uuid:string,reveal_token:string,expires_at:string}
     */
    public function store(int $serviceId, string $type, string $plaintext, int $ttlSeconds = 1800): array
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
            'secret_type' => $type,
            'encrypted_value' => encrypt($plaintext),
            'reveal_token_hash' => hash('sha256', $token),
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

    public function reveal(int $serviceId, string $token): string
    {
        if (!function_exists('decrypt')) {
            throw new ContaboProvisioningException('WHMCS encryption service is unavailable');
        }
        $hash = hash('sha256', $token);
        $row = Capsule::table('mod_securiacevps_secrets')
            ->where('service_id', $serviceId)
            ->where('reveal_token_hash', $hash)
            ->first();
        if ($row === null) {
            throw new ContaboProvisioningException('This credential link is invalid or has expired');
        }
        $row = (array) $row;
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
        Capsule::table('mod_securiacevps_secrets')
            ->where('id', (int) $row['id'])
            ->update([
                'encrypted_value' => '',
                'reveal_count' => ((int) ($row['reveal_count'] ?? 0)) + 1,
                'revealed_at' => $now,
                'destroyed_at' => $now,
                'updated_at' => $now,
            ]);
        return $plaintext;
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
                    ->update(['encrypted_value' => '', 'destroyed_at' => $now, 'updated_at' => $now]);
                $destroyed++;
            }
        }
        return $destroyed;
    }
}
