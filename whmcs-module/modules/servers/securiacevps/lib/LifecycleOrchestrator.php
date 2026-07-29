<?php
declare(strict_types=1);

namespace SecuriAceVps;

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
        if ((string) $operation['state'] === 'succeeded') {
            return 'success';
        }
        if (in_array((string) $operation['state'], ['failed_terminal', 'manual_review'], true)) {
            return $this->operatorMessage($operation);
        }
        $worker = 'http-' . substr(hash('sha256', Uuid::v4()), 0, 20);
        $claimed = $this->operations->claim(
            (string) $operation['operation_uuid'],
            $worker,
            (int) SchemaGuard::setting('operation_lease_seconds', '120')
        );
        if ($claimed !== null) {
            $operation = $this->progressClaimed($claimed, $params);
        } else {
            $operation = $this->operations->byUuid((string) $operation['operation_uuid']);
        }
        return (string) $operation['state'] === 'succeeded'
            ? 'success'
            : $this->operatorMessage($operation);
    }

    /** @param array<string,mixed> $operation */
    private function operatorMessage(array $operation): string
    {
        $reference = (string) ($operation['correlation_id'] ?? '');
        $state = (string) ($operation['state'] ?? 'pending');
        if ($state === 'manual_review' || $state === 'failed_terminal') {
            return 'VPS operation requires administrator review (reference ' . $reference . ')';
        }
        if ($state === 'unknown_outcome') {
            return 'Provider outcome is being reconciled; do not retry manually (reference ' . $reference . ')';
        }
        return 'VPS operation is still in progress (reference ' . $reference . ')';
    }
}
