<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class OperationRepository
{
    /** @var list<string> */
    private const TERMINAL_STATES = [
        'succeeded',
        'failed_terminal',
        'manual_review',
        'cancelled',
        'superseded',
    ];

    /** @var list<string> */
    private const COMMERCIAL_LIFECYCLE_TYPES = [
        'create',
        'suspend',
        'unsuspend',
        'terminate',
    ];

    /**
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function accept(
        int $serviceId,
        ?string $snapshotUuid,
        string $providerAccountId,
        string $type,
        array $payload,
        ?int $generation = null
    ): array {
        SchemaGuard::assertReady();
        if (in_array($type, self::COMMERCIAL_LIFECYCLE_TYPES, true)) {
            return Capsule::connection()->transaction(function () use (
                $serviceId,
                $snapshotUuid,
                $providerAccountId,
                $type,
                $payload,
                $generation
            ): array {
                // Serialize intent creation with commercial-state projection.
                // tblhosting always exists for a valid WHMCS service and gives
                // both paths one stable row to lock.
                $serviceQuery = Capsule::table('tblhosting')->where('id', $serviceId);
                if (method_exists($serviceQuery, 'lockForUpdate')) {
                    $serviceQuery->lockForUpdate()->first();
                }
                return $this->acceptUnlocked(
                    $serviceId,
                    $snapshotUuid,
                    $providerAccountId,
                    $type,
                    $payload,
                    $generation
                );
            });
        }
        return $this->acceptUnlocked(
            $serviceId,
            $snapshotUuid,
            $providerAccountId,
            $type,
            $payload,
            $generation
        );
    }

    /**
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    private function acceptUnlocked(
        int $serviceId,
        ?string $snapshotUuid,
        string $providerAccountId,
        string $type,
        array $payload,
        ?int $generation
    ): array {
        $payloadJson = CanonicalJson::encode($payload);
        $fingerprint = hash('sha256', $payloadJson);
        if ($type === 'create') {
            $generation = 1;
        } elseif ($generation === null) {
            $latest = $this->latestForServiceType($serviceId, $type);
            $generation = $latest === null || !in_array((string) ($latest['state'] ?? ''), self::TERMINAL_STATES, true)
                ? (int) ($latest['operation_generation'] ?? 1)
                : ((int) ($latest['operation_generation'] ?? 0)) + 1;
        }
        $generation = max(1, (int) $generation);
        $commandMaterial = implode('|', [
            SchemaGuard::installationId(),
            (string) $serviceId,
            (string) $snapshotUuid,
            $type,
            (string) $generation,
        ]);
        $commandId = hash('sha256', $commandMaterial);
        $existing = Capsule::table('mod_securiacevps_operations')
            ->where('command_id', $commandId)
            ->first();
        if ($existing !== null) {
            $existing = (array) $existing;
            if (!hash_equals((string) ($existing['request_fingerprint'] ?? ''), $fingerprint)) {
                throw new ContaboProvisioningException(
                    'The same operation identity was submitted with a different payload'
                );
            }
            return $existing;
        }

        $uuid = Uuid::v4();
        $correlationId = Uuid::v4();
        $now = date('Y-m-d H:i:s');
        try {
            Capsule::table('mod_securiacevps_operations')->insert([
                'operation_uuid' => $uuid,
                'service_id' => $serviceId,
                'snapshot_uuid' => $snapshotUuid,
                'provider_account_id' => $providerAccountId,
                'operation_type' => $type,
                'operation_generation' => $generation,
                'state' => 'accepted',
                'command_id' => $commandId,
                'request_fingerprint' => $fingerprint,
                'idempotency_key' => hash('sha256', 'provider|' . $commandId),
                'provider_resource_id' => null,
                'attempt_count' => 0,
                'next_attempt_at' => $now,
                'max_attempts' => 8,
                'lease_owner' => null,
                'lease_expires_at' => null,
                'fencing_token' => 0,
                'safe_error_code' => null,
                'retry_classification' => null,
                'unknown_outcome' => 0,
                'correlation_id' => $correlationId,
                'payload_json' => $payloadJson,
                'created_at' => $now,
                'updated_at' => $now,
            ]);
        } catch (\Throwable $e) {
            // A concurrent callback may win the UNIQUE(command_id) insert
            // between our read and write. Resolve that race to the winner.
            $winner = Capsule::table('mod_securiacevps_operations')
                ->where('command_id', $commandId)
                ->first();
            if ($winner === null) {
                throw $e;
            }
            $winner = (array) $winner;
            if (!hash_equals((string) ($winner['request_fingerprint'] ?? ''), $fingerprint)) {
                throw new ContaboProvisioningException(
                    'The same operation identity was submitted with a different payload'
                );
            }
            return $winner;
        }
        $created = $this->byUuid($uuid);
        if (in_array($type, self::COMMERCIAL_LIFECYCLE_TYPES, true)) {
            $this->supersedeOlderLifecycleIntents($created);
        }
        return $created;
    }

    /** @return array<string,mixed> */
    public function byUuid(string $uuid): array
    {
        $row = Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $uuid)
            ->first();
        if ($row === null) {
            throw new ContaboProvisioningException('VPS operation was not found');
        }
        return (array) $row;
    }

    /** @return array<string,mixed>|null */
    public function latestForServiceType(int $serviceId, string $type): ?array
    {
        $row = Capsule::table('mod_securiacevps_operations')
            ->where('service_id', $serviceId)
            ->where('operation_type', $type)
            ->orderByDesc('id')
            ->first();
        return $row !== null ? (array) $row : null;
    }

    /** @return array<string,mixed>|null */
    public function latestLifecycleIntent(int $serviceId): ?array
    {
        $row = Capsule::table('mod_securiacevps_operations')
            ->where('service_id', $serviceId)
            ->whereIn('operation_type', self::COMMERCIAL_LIFECYCLE_TYPES)
            ->orderByDesc('id')
            ->first();
        return $row !== null ? (array) $row : null;
    }

    /** @param array<string,mixed> $newIntent */
    private function supersedeOlderLifecycleIntents(array $newIntent): void
    {
        $rows = Capsule::table('mod_securiacevps_operations')
            ->where('service_id', (int) ($newIntent['service_id'] ?? 0))
            ->whereIn('operation_type', self::COMMERCIAL_LIFECYCLE_TYPES)
            ->where('id', '<', (int) ($newIntent['id'] ?? 0))
            ->whereNotIn('state', self::TERMINAL_STATES)
            ->get();
        foreach ($rows as $item) {
            $older = (array) $item;
            Capsule::table('mod_securiacevps_operations')
                ->where('operation_uuid', (string) ($older['operation_uuid'] ?? ''))
                ->where('state', (string) ($older['state'] ?? ''))
                ->where('fencing_token', (int) ($older['fencing_token'] ?? 0))
                ->update([
                    'state' => 'superseded',
                    // Invalidate an in-flight worker. Its provider effect may
                    // still complete, but it cannot project stale state; the
                    // newer intent waits for the service lease and reconciles.
                    'fencing_token' => ((int) ($older['fencing_token'] ?? 0)) + 1,
                    'next_attempt_at' => null,
                    'completed_at' => date('Y-m-d H:i:s'),
                    'updated_at' => date('Y-m-d H:i:s'),
                ]);
        }
    }

    /**
     * Claim one operation using a service-scoped lease and monotonically
     * increasing fencing token.
     *
     * @return array<string,mixed>|null
     */
    public function claim(string $uuid, string $worker, int $leaseSeconds = 120): ?array
    {
        $op = $this->byUuid($uuid);
        $lockName = 'securiacevps:' . (int) $op['service_id'];
        $connection = Capsule::connection();
        $namedLockAcquired = true;
        if (method_exists($connection, 'select')) {
            $rows = $connection->select('SELECT GET_LOCK(?, 2) AS acquired', [$lockName]);
            $first = isset($rows[0]) ? (array) $rows[0] : [];
            $namedLockAcquired = (int) ($first['acquired'] ?? 0) === 1;
        }
        if (!$namedLockAcquired) {
            return null;
        }
        try {
            return $connection->transaction(function () use ($uuid, $worker, $leaseSeconds) {
            $op = $this->byUuid($uuid);
            if (in_array((string) ($op['state'] ?? ''), self::TERMINAL_STATES, true)) {
                return null;
            }
            $operationLeaseExpires = trim((string) ($op['lease_expires_at'] ?? ''));
            if ($operationLeaseExpires !== ''
                && strtotime($operationLeaseExpires) >= time()
            ) {
                return null;
            }
            $serviceId = (int) $op['service_id'];
            $lock = Capsule::table('mod_securiacevps_service_locks')
                ->where('service_id', $serviceId)
                ->first();
            $lock = $lock !== null ? (array) $lock : null;
            if ($lock !== null
                && strtotime((string) ($lock['lease_expires_at'] ?? '1970-01-01')) >= time()
            ) {
                return null;
            }
            $token = $lock === null ? 1 : ((int) ($lock['fencing_token'] ?? 0)) + 1;
            $expires = date('Y-m-d H:i:s', time() + max(30, $leaseSeconds));
            Capsule::table('mod_securiacevps_service_locks')->updateOrInsert(
                ['service_id' => $serviceId],
                [
                    'operation_uuid' => $uuid,
                    'lease_owner' => $worker,
                    'lease_expires_at' => $expires,
                    'fencing_token' => $token,
                    'updated_at' => date('Y-m-d H:i:s'),
                ]
            );
            Capsule::table('mod_securiacevps_operations')
                ->where('operation_uuid', $uuid)
                ->update([
                    'state' => 'claimed',
                    'lease_owner' => $worker,
                    'lease_expires_at' => $expires,
                    'fencing_token' => $token,
                    'updated_at' => date('Y-m-d H:i:s'),
                ]);
            return $this->byUuid($uuid);
            });
        } finally {
            if (method_exists($connection, 'select')) {
                $connection->select('SELECT RELEASE_LOCK(?) AS released', [$lockName]);
            }
        }
    }

    /**
     * @param array<string,mixed> $changes
     */
    public function transition(string $uuid, int $fencingToken, string $state, array $changes = []): bool
    {
        $changes['state'] = $state;
        $changes['updated_at'] = date('Y-m-d H:i:s');
        $updated = Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $uuid)
            ->where('fencing_token', $fencingToken)
            ->update($changes);
        return (int) $updated > 0;
    }

    /**
     * @param array<string,mixed> $metadata
     */
    public function attempt(
        string $uuid,
        int $number,
        int $fencingToken,
        string $state,
        array $metadata = []
    ): void {
        Capsule::table('mod_securiacevps_operation_attempts')->insert([
            'operation_uuid' => $uuid,
            'attempt_number' => $number,
            'fencing_token' => $fencingToken,
            'state' => $state,
            'provider_request_id' => $metadata['provider_request_id'] ?? null,
            'safe_error_code' => $metadata['safe_error_code'] ?? null,
            'retry_classification' => $metadata['retry_classification'] ?? null,
            'request_metadata_json' => isset($metadata['request'])
                ? CanonicalJson::encode($metadata['request'])
                : null,
            'response_metadata_json' => isset($metadata['response'])
                ? CanonicalJson::encode($metadata['response'])
                : null,
            'started_at' => $metadata['started_at'] ?? date('Y-m-d H:i:s'),
            'finished_at' => $metadata['finished_at'] ?? date('Y-m-d H:i:s'),
        ]);
    }

    /** @return list<array<string,mixed>> */
    public function due(int $limit): array
    {
        $rows = Capsule::table('mod_securiacevps_operations')
            ->whereIn('state', [
                'accepted',
                'claimed',
                'retry_scheduled',
                'failed_retryable',
                'provider_pending',
                'reconciling',
                'unknown_outcome',
            ])
            ->orderBy('id')
            ->limit(max(1, $limit * 3))
            ->get();
        $out = [];
        foreach ($rows as $item) {
            $row = (array) $item;
            if ((string) ($row['state'] ?? '') === 'claimed') {
                $leaseExpires = trim((string) ($row['lease_expires_at'] ?? ''));
                if ($leaseExpires !== '' && strtotime($leaseExpires) >= time()) {
                    continue;
                }
            }
            $next = trim((string) ($row['next_attempt_at'] ?? ''));
            if ($next !== '' && strtotime($next) > time()) {
                continue;
            }
            $out[] = $row;
            if (count($out) >= $limit) {
                break;
            }
        }
        return $out;
    }

    public function release(string $uuid, int $serviceId, int $fencingToken): void
    {
        Capsule::table('mod_securiacevps_service_locks')
            ->where('service_id', $serviceId)
            ->where('operation_uuid', $uuid)
            ->where('fencing_token', $fencingToken)
            ->update([
                'lease_expires_at' => date('Y-m-d H:i:s', time() - 1),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
        Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $uuid)
            ->where('fencing_token', $fencingToken)
            ->update(['lease_owner' => null, 'lease_expires_at' => null, 'updated_at' => date('Y-m-d H:i:s')]);
    }
}
