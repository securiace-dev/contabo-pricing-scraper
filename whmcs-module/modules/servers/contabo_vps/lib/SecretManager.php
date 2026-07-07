<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Lifecycle of the per-service root-password secret in Contabo's Secret
 * Management vault. Contabo's create-instance / resetPassword endpoints only
 * accept a `secretId` for rootPassword — never a raw string — so the WHMCS
 * service password must be pushed to the vault first.
 *
 * Convention: ONE reusable password secret per service, named
 * "whmcs-svc-{serviceid}-root". Reset flows PATCH the same secret's value so
 * the vault never accumulates stale credentials; terminate deletes it
 * (best-effort — termination never fails because vault cleanup failed).
 *
 * Secret VALUES never pass through logs: callers log only secret ids, and the
 * module-level log sanitizer masks password-bearing keys defensively.
 */
final class SecretManager
{
    /** @var ContaboApiClient */
    private $client;

    public function __construct(ContaboApiClient $client)
    {
        $this->client = $client;
    }

    public static function secretName(int $serviceId): string
    {
        return 'whmcs-svc-' . $serviceId . '-root';
    }

    /**
     * Create (or update in place) the service's root-password secret and
     * return its secretId.
     */
    public function ensureRootPasswordSecret(int $serviceId, string $password): int
    {
        if ($password === '') {
            throw new ContaboProvisioningException('Refusing to store an empty root password secret');
        }
        $name = self::secretName($serviceId);

        $existingId = $this->findSecretIdByName($name);
        if ($existingId !== null) {
            $this->client->patch('/v1/secrets/' . $existingId, ['value' => $password]);
            return $existingId;
        }

        $resp = $this->client->post('/v1/secrets', [
            'name'  => $name,
            'value' => $password,
            'type'  => 'password',
        ]);
        $secretId = (int) ($resp['data'][0]['secretId'] ?? 0);
        if ($secretId <= 0) {
            throw new ContaboProvisioningException('Contabo secret vault returned no secretId');
        }
        return $secretId;
    }

    /**
     * Best-effort removal of the service's vault secret on termination.
     * Failures are logged and swallowed — the instance cancellation must never
     * be reported as failed because of vault housekeeping.
     */
    public function cleanupServiceSecrets(int $serviceId): void
    {
        try {
            $id = $this->findSecretIdByName(self::secretName($serviceId));
            if ($id !== null) {
                $this->client->delete('/v1/secrets/' . $id);
            }
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo VPS: could not clean up vault secret for service #' . $serviceId . ' — ' . $e->getMessage());
            }
        }
    }

    /**
     * Generate a Contabo-compatible root password: length ≥ the panel's rules,
     * guaranteed upper + lower + digit, no shell/quoting hazards.
     */
    public static function generatePassword(int $length = 20): string
    {
        $upper  = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
        $lower  = 'abcdefghijkmnpqrstuvwxyz';
        $digits = '23456789';
        $all    = $upper . $lower . $digits;

        $length = max(12, $length);
        $chars = [
            $upper[random_int(0, strlen($upper) - 1)],
            $lower[random_int(0, strlen($lower) - 1)],
            $digits[random_int(0, strlen($digits) - 1)],
        ];
        for ($i = count($chars); $i < $length; $i++) {
            $chars[] = $all[random_int(0, strlen($all) - 1)];
        }
        // Fisher–Yates so the guaranteed classes aren't always at the front.
        for ($i = count($chars) - 1; $i > 0; $i--) {
            $j = random_int(0, $i);
            $tmp = $chars[$i];
            $chars[$i] = $chars[$j];
            $chars[$j] = $tmp;
        }
        return implode('', $chars);
    }

    private function findSecretIdByName(string $name): ?int
    {
        $resp = $this->client->get('/v1/secrets?name=' . rawurlencode($name) . '&type=password');
        $rows = isset($resp['data']) && is_array($resp['data']) ? $resp['data'] : [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            // The name param is a filter, not an exact match — compare strictly.
            if ((string) ($row['name'] ?? '') === $name && (int) ($row['secretId'] ?? 0) > 0) {
                return (int) $row['secretId'];
            }
        }
        return null;
    }
}
