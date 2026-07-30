<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class LifecycleOrchestrator
{
    /** @var OperationRepository */
    private $operations;
    /** @var OrderSnapshotRepository */
    private $snapshots;
    /** @var OperationProcessor */
    private $processor;

    public function __construct(
        ?OperationRepository $operations = null,
        ?OrderSnapshotRepository $snapshots = null,
        ?OperationProcessor $processor = null
    ) {
        $this->operations = $operations !== null ? $operations : new OperationRepository();
        $this->snapshots = $snapshots !== null ? $snapshots : new OrderSnapshotRepository();
        $this->processor = $processor !== null ? $processor : new OperationProcessor($this->operations);
    }

    /** @param array<string,mixed> $params */
    public function create(array $params): string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $snapshot = $this->snapshots->sealedForService($serviceId);
        $operation = $this->operations->accept(
            $serviceId,
            (string) $snapshot['row']['snapshot_uuid'],
            ProviderAccount::id($params),
            'create',
            $snapshot['payload'],
            1
        );
        return $this->progress($operation, $params);
    }

    /** @param array<string,mixed> $params */
    public function suspend(array $params): string
    {
        return $this->acceptAndProgress($params, 'suspend');
    }

    /** @param array<string,mixed> $params */
    public function unsuspend(array $params): string
    {
        return $this->acceptAndProgress($params, 'unsuspend');
    }

    /** @param array<string,mixed> $params */
    public function terminate(array $params): string
    {
        return $this->acceptAndProgress($params, 'terminate');
    }

    /** @param array<string,mixed> $params */
    public function power(array $params, string $action): string
    {
        return $this->acceptAndProgress($params, $action);
    }

    /** @param array<string,mixed> $params */
    public function resetPassword(array $params): string
    {
        return $this->acceptAndProgress($params, 'reset_password');
    }

    /** @param array<string,mixed> $params */
    public function reinstall(array $params): string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        OwnershipGuard::assertVerified($serviceId);
        $snapshot = $this->snapshots->sealedForService($serviceId);
        $operation = $this->operations->accept(
            $serviceId,
            (string) $snapshot['row']['snapshot_uuid'],
            ProviderAccount::id($params),
            'reinstall',
            $snapshot['payload']
        );
        return $this->progress($operation, $params);
    }

    /** @param array<string,mixed> $params */
    public function createSnapshot(array $params, string $name, string $description = ''): string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        OwnershipGuard::assertVerified($serviceId);
        $this->assertSnapshotReadCertified($params);
        $name = trim($name);
        $description = trim($description);
        if (strlen($name) < 1
            || strlen($name) > 30
            || preg_match('/^[A-Za-z0-9 -]+$/', $name) !== 1
        ) {
            throw new ContaboProvisioningException(
                'Snapshot names must use 1–30 letters, numbers, spaces or dashes',
                'snapshot_name_invalid',
                'terminal'
            );
        }
        if (strlen($description) > 255
            || preg_match('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/', $description) === 1
        ) {
            throw new ContaboProvisioningException(
                'The snapshot description is invalid',
                'snapshot_description_invalid',
                'terminal'
            );
        }
        $operation = $this->operations->accept(
            $serviceId,
            null,
            ProviderAccount::id($params),
            'snapshot_create',
            ['name' => $name, 'description' => $description]
        );
        return $this->progress($operation, $params);
    }

    /** @param array<string,mixed> $params */
    public function deleteSnapshot(
        array $params,
        string $snapshotId,
        string $evidenceHash
    ): string {
        $row = $this->snapshotInventoryRow($params, $snapshotId, $evidenceHash);
        $operation = $this->operations->accept(
            (int) ($params['serviceid'] ?? 0),
            null,
            ProviderAccount::id($params),
            'snapshot_delete',
            [
                'snapshot_id' => (string) $row['snapshot_id'],
                'evidence_hash' => (string) $row['payload_hash'],
            ]
        );
        return $this->progress($operation, $params);
    }

    /** @param array<string,mixed> $params */
    public function rollbackSnapshot(
        array $params,
        string $snapshotId,
        string $evidenceHash
    ): string {
        $row = $this->snapshotInventoryRow($params, $snapshotId, $evidenceHash);
        $operation = $this->operations->accept(
            (int) ($params['serviceid'] ?? 0),
            null,
            ProviderAccount::id($params),
            'snapshot_rollback',
            [
                'snapshot_id' => (string) $row['snapshot_id'],
                'evidence_hash' => (string) $row['payload_hash'],
            ]
        );
        return $this->progress($operation, $params);
    }

    /**
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function progressClaimed(array $operation, array $params): array
    {
        return $this->processor->process(
            $operation,
            $params,
            Runtime::instanceService($params)
        );
    }

    /** @param array<string,mixed> $params */
    private function acceptAndProgress(array $params, string $type): string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        OwnershipGuard::assertVerified($serviceId);
        $operation = $this->operations->accept(
            $serviceId,
            null,
            ProviderAccount::id($params),
            $type,
            ['operation' => $type, 'service_id' => $serviceId]
        );
        return $this->progress($operation, $params);
    }

    /**
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $params
     */
    private function progress(array $operation, array $params): string
    {
        if ($this->isWhmcsProjectionComplete($operation)) {
            return 'success';
        }
        if (in_array((string) $operation['state'], ['failed_terminal', 'manual_review'], true)) {
            return $this->operatorMessage($operation);
        }
        $worker = 'http-' . substr(hash('sha256', Uuid::v4()), 0, 20);
        $claimed = $this->operations->claim(
            (string) $operation['operation_uuid'],
            $worker,
            (int) SchemaGuard::setting(
                'operation_lease_seconds',
                (string) OperationRepository::MIN_LEASE_SECONDS
            )
        );
        if ($claimed !== null) {
            $operation = $this->progressClaimed($claimed, $params);
        } else {
            $operation = $this->operations->byUuid((string) $operation['operation_uuid']);
        }
        return $this->isWhmcsProjectionComplete($operation)
            ? 'success'
            : $this->operatorMessage($operation);
    }

    /** @param array<string,mixed> $operation */
    private function operatorMessage(array $operation): string
    {
        $reference = (string) ($operation['correlation_id'] ?? '');
        $state = (string) ($operation['state'] ?? 'pending');
        $safeCode = (string) ($operation['safe_error_code'] ?? '');
        if (in_array($safeCode, [
            'service_record_missing',
            'service_projection_intent_superseded',
            'service_status_projection_conflict',
            'service_status_projection_failed',
        ], true)) {
            return 'VPS provider action completed but WHMCS service state requires administrator review'
                . ' (reference ' . $reference . ')';
        }
        if ($state === 'manual_review' || $state === 'failed_terminal') {
            return 'VPS operation requires administrator review (reference ' . $reference . ')';
        }
        if ($state === 'unknown_outcome') {
            return 'Provider outcome is being reconciled; do not retry manually (reference ' . $reference . ')';
        }
        return 'VPS operation is still in progress (reference ' . $reference . ')';
    }

    /** @param array<string,mixed> $operation */
    private function isWhmcsProjectionComplete(array $operation): bool
    {
        return (string) ($operation['state'] ?? '') === 'succeeded'
            && !in_array((string) ($operation['safe_error_code'] ?? ''), [
                'service_record_missing',
                'service_projection_intent_superseded',
                'service_status_projection_conflict',
                'service_status_projection_failed',
            ], true);
    }

    /** @param array<string,mixed> $params */
    private function assertSnapshotReadCertified(array $params): void
    {
        if (!(new CapabilityRegistry())->canRead(ProviderAccount::id($params), 'snapshot_list')) {
            throw new ContaboProvisioningException(
                'Snapshot inventory is not certified for this Contabo account',
                'snapshot_inventory_not_certified',
                'terminal'
            );
        }
    }

    /**
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    private function snapshotInventoryRow(
        array $params,
        string $snapshotId,
        string $evidenceHash
    ): array {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        OwnershipGuard::assertVerified($serviceId);
        $this->assertSnapshotReadCertified($params);
        if (preg_match('/^[A-Za-z0-9._:-]{1,160}$/', $snapshotId) !== 1
            || preg_match('/^[a-f0-9]{64}$/', $evidenceHash) !== 1
        ) {
            throw new ContaboProvisioningException(
                'The snapshot selection is invalid or stale',
                'snapshot_selection_invalid',
                'terminal'
            );
        }
        $row = Capsule::table('mod_securiacevps_snapshot_inventory')
            ->where('service_id', $serviceId)
            ->where('snapshot_id', $snapshotId)
            ->first();
        $row = $row !== null ? (array) $row : [];
        if ($row === []
            || !hash_equals((string) ($row['payload_hash'] ?? ''), $evidenceHash)
            || !hash_equals(
                ProviderAccount::id($params),
                (string) ($row['provider_account_id'] ?? '')
            )
        ) {
            throw new ContaboProvisioningException(
                'The snapshot inventory changed; refresh before continuing',
                'snapshot_selection_stale',
                'terminal'
            );
        }
        return $row;
    }
}
