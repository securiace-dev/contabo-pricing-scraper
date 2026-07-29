<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class OperationProcessor
{
    /** @var OperationRepository */
    private $operations;
    /** @var CapabilityRegistry */
    private $capabilities;
    /** @var AuditLogger */
    private $audit;

    public function __construct(
        ?OperationRepository $operations = null,
        ?CapabilityRegistry $capabilities = null,
        ?AuditLogger $audit = null
    ) {
        $this->operations = $operations !== null ? $operations : new OperationRepository();
        $this->capabilities = $capabilities !== null ? $capabilities : new CapabilityRegistry();
        $this->audit = $audit !== null ? $audit : new AuditLogger();
    }

    /**
     * Process one already-claimed operation. The lease is always released.
     *
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function process(array $operation, array $params, InstanceService $instances): array
    {
        $uuid = (string) $operation['operation_uuid'];
        $serviceId = (int) $operation['service_id'];
        $token = (int) $operation['fencing_token'];
        $attempt = ((int) ($operation['attempt_count'] ?? 0)) + 1;
        $type = (string) $operation['operation_type'];
        $payload = json_decode((string) ($operation['payload_json'] ?? '{}'), true);
        $payload = is_array($payload) ? $payload : [];
        $startedAt = date('Y-m-d H:i:s');

        try {
            $this->capabilities->assertWriteAllowed(
                (string) $operation['provider_account_id'],
                $this->capabilityForOperation($type)
            );
            if ($type === 'create') {
                $this->processCreate($operation, $params, $instances, $payload);
            } elseif (in_array($type, ['suspend', 'unsuspend', 'start', 'stop', 'restart'], true)) {
                $this->processPower($operation, $params, $instances);
            } elseif ($type === 'terminate') {
                $this->processTerminate($operation, $params, $instances);
            } else {
                throw new ContaboProvisioningException(
                    'Unsupported durable VPS operation',
                    'operation_not_supported',
                    'terminal'
                );
            }

            $latest = $this->operations->byUuid($uuid);
            $this->operations->attempt($uuid, $attempt, $token, (string) $latest['state'], [
                'started_at' => $startedAt,
                'finished_at' => date('Y-m-d H:i:s'),
            ]);
            $this->audit->record(
                'operation.progressed',
                (string) $latest['state'],
                $serviceId,
                (string) ($operation['correlation_id'] ?? ''),
                ['operation_uuid' => $uuid, 'operation_type' => $type, 'attempt' => $attempt]
            );
        } catch (ContaboProvisioningException $e) {
            $this->handleFailure($operation, $e, $attempt, $startedAt);
        } catch (\Throwable $e) {
            $this->handleFailure(
                $operation,
                new ContaboProvisioningException(
                    'Unexpected provisioning worker failure',
                    'worker_unexpected_error',
                    'transient'
                ),
                $attempt,
                $startedAt
            );
            if (function_exists('logActivity')) {
                logActivity(
                    'SecuriAce VPS worker error [' . (string) ($operation['correlation_id'] ?? '') . ']: '
                    . $e->getMessage()
                );
            }
        } finally {
            $this->operations->release($uuid, $serviceId, $token);
        }

        return $this->operations->byUuid($uuid);
    }

    /**
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $params
     * @param array<string,mixed> $payload
     */
    private function processCreate(
        array $operation,
        array $params,
        InstanceService $instances,
        array $payload
    ): void {
        $uuid = (string) $operation['operation_uuid'];
        $token = (int) $operation['fencing_token'];
        $requestIdentity = (string) $operation['idempotency_key'];
        $providerRequest = Capsule::table('mod_securiacevps_provider_requests')
            ->where('operation_uuid', $uuid)
            ->first();
        $providerRequest = $providerRequest !== null ? (array) $providerRequest : null;
        $resourceId = trim((string) ($operation['provider_resource_id'] ?? ''));

        if ($resourceId === '' && $providerRequest !== null) {
            // Any prior submission without a known resource identity is an
            // ambiguous outcome. Reconcile by the deterministic service tag;
            // never submit another create request blindly.
            $resourceId = (string) ($instances->recoverCreateByTag($params) ?? '');
            if ($resourceId === '') {
                $this->schedule(
                    $operation,
                    'unknown_outcome',
                    'provider_create_outcome_unknown',
                    'inspect_before_retry',
                    true,
                    $this->backoff((int) ($operation['attempt_count'] ?? 0), 30, 600)
                );
                return;
            }
            Capsule::table('mod_securiacevps_provider_requests')
                ->where('operation_uuid', $uuid)
                ->update([
                    'provider_resource_id' => $resourceId,
                    'state' => 'reconciled',
                    'unknown_outcome' => 0,
                    'last_checked_at' => date('Y-m-d H:i:s'),
                    'updated_at' => date('Y-m-d H:i:s'),
                ]);
            $this->operations->transition($uuid, $token, 'reconciling', [
                'provider_resource_id' => $resourceId,
                'unknown_outcome' => 0,
            ]);
        }

        if ($resourceId === '') {
            Capsule::table('mod_securiacevps_provider_requests')->insert([
                'operation_uuid' => $uuid,
                'provider_request_id' => null,
                'request_fingerprint' => (string) $operation['request_fingerprint'],
                'idempotency_key' => $requestIdentity,
                'state' => 'submitting',
                'provider_resource_id' => null,
                'unknown_outcome' => 1,
                'submitted_at' => date('Y-m-d H:i:s'),
                'last_checked_at' => null,
                'created_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
            try {
                $result = $instances->submitCreateFromSnapshot($params, $payload, $requestIdentity);
            } catch (ContaboProvisioningException $e) {
                if ($e->hasAmbiguousOutcome()) {
                    Capsule::table('mod_securiacevps_provider_requests')
                        ->where('operation_uuid', $uuid)
                        ->update([
                            'state' => 'unknown_outcome',
                            'unknown_outcome' => 1,
                            'updated_at' => date('Y-m-d H:i:s'),
                        ]);
                } else {
                    // A classified definite rejection is safe to retry only
                    // through the operation policy; remove the submission
                    // marker because no provider side effect occurred.
                    Capsule::table('mod_securiacevps_provider_requests')
                        ->where('operation_uuid', $uuid)
                        ->update([
                            'state' => 'rejected',
                            'unknown_outcome' => 0,
                            'updated_at' => date('Y-m-d H:i:s'),
                        ]);
                }
                throw $e;
            }
            $resourceId = (string) $result['instance_id'];
            Capsule::table('mod_securiacevps_provider_requests')
                ->where('operation_uuid', $uuid)
                ->update([
                    'state' => 'accepted',
                    'provider_resource_id' => $resourceId,
                    'unknown_outcome' => 0,
                    'last_checked_at' => date('Y-m-d H:i:s'),
                    'updated_at' => date('Y-m-d H:i:s'),
                ]);
            $safeResult = [
                'secret_uuid' => $result['secret_uuid'] ?? null,
                'reveal_token_ciphertext' => $result['reveal_token_ciphertext'] ?? null,
                'secret_expires_at' => $result['secret_expires_at'] ?? null,
            ];
            $this->operations->transition($uuid, $token, 'submitted', [
                'provider_resource_id' => $resourceId,
                'submitted_at' => date('Y-m-d H:i:s'),
                'unknown_outcome' => 0,
                'result_json' => CanonicalJson::encode($safeResult),
            ]);
        }

        $verification = $instances->verifyReady($params, $payload);
        if (!$verification['ready']) {
            $this->schedule($operation, 'provider_pending', null, 'provider_pending', false, 30);
            return;
        }
        Capsule::table('mod_securiacevps_resources')
            ->where('service_id', (int) $operation['service_id'])
            ->update([
                'provider_state' => (string) $verification['state'],
                'provisioning_state' => 'ready',
                'ownership_state' => 'verified',
                'last_observed_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
        $this->operations->transition($uuid, $token, 'succeeded', [
            'reconciled_at' => date('Y-m-d H:i:s'),
            'completed_at' => date('Y-m-d H:i:s'),
            'next_attempt_at' => null,
            'safe_error_code' => null,
            'retry_classification' => null,
        ]);
    }

    /**
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $params
     */
    private function processPower(array $operation, array $params, InstanceService $instances): void
    {
        $uuid = (string) $operation['operation_uuid'];
        $token = (int) $operation['fencing_token'];
        $type = (string) $operation['operation_type'];
        $desired = in_array($type, ['suspend', 'stop'], true) ? 'stopped' : 'running';
        $action = $type === 'suspend' ? 'stop' : ($type === 'unsuspend' ? 'start' : $type);
        $providerRequest = Capsule::table('mod_securiacevps_provider_requests')
            ->where('operation_uuid', $uuid)
            ->first();
        if ($providerRequest === null) {
            Capsule::table('mod_securiacevps_provider_requests')->insert([
                'operation_uuid' => $uuid,
                'provider_request_id' => null,
                'request_fingerprint' => (string) $operation['request_fingerprint'],
                'idempotency_key' => (string) $operation['idempotency_key'],
                'state' => 'submitting',
                'provider_resource_id' => (string) ($operation['provider_resource_id'] ?? ''),
                'unknown_outcome' => 1,
                'submitted_at' => date('Y-m-d H:i:s'),
                'created_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
            $instances->submitPowerWithIdentity(
                $params,
                $action,
                (string) $operation['idempotency_key']
            );
            Capsule::table('mod_securiacevps_provider_requests')
                ->where('operation_uuid', $uuid)
                ->update(['state' => 'accepted', 'unknown_outcome' => 0, 'updated_at' => date('Y-m-d H:i:s')]);
            $this->operations->transition($uuid, $token, 'submitted', [
                'submitted_at' => date('Y-m-d H:i:s'),
                'unknown_outcome' => 0,
            ]);
        }
        $snapshot = $instances->sync($params);
        if (strtolower((string) ($snapshot['status'] ?? 'unknown')) !== $desired) {
            $this->schedule($operation, 'provider_pending', null, 'provider_pending', false, 20);
            return;
        }
        $this->operations->transition($uuid, $token, 'succeeded', [
            'reconciled_at' => date('Y-m-d H:i:s'),
            'completed_at' => date('Y-m-d H:i:s'),
            'next_attempt_at' => null,
            'safe_error_code' => null,
            'retry_classification' => null,
        ]);
    }

    /**
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $params
     */
    private function processTerminate(array $operation, array $params, InstanceService $instances): void
    {
        $uuid = (string) $operation['operation_uuid'];
        $token = (int) $operation['fencing_token'];
        $providerRequest = Capsule::table('mod_securiacevps_provider_requests')
            ->where('operation_uuid', $uuid)
            ->first();
        if ($providerRequest === null) {
            Capsule::table('mod_securiacevps_provider_requests')->insert([
                'operation_uuid' => $uuid,
                'provider_request_id' => null,
                'request_fingerprint' => (string) $operation['request_fingerprint'],
                'idempotency_key' => (string) $operation['idempotency_key'],
                'state' => 'submitting',
                'provider_resource_id' => (string) ($operation['provider_resource_id'] ?? ''),
                'unknown_outcome' => 1,
                'submitted_at' => date('Y-m-d H:i:s'),
                'created_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
            $submitted = $instances->submitTerminateWithIdentity(
                $params,
                (string) $operation['idempotency_key']
            );
            Capsule::table('mod_securiacevps_provider_requests')
                ->where('operation_uuid', $uuid)
                ->update([
                    'state' => $submitted ? 'accepted' : 'already_absent',
                    'unknown_outcome' => 0,
                    'updated_at' => date('Y-m-d H:i:s'),
                ]);
        }
        if (!$instances->verifyAbsent($params)) {
            $this->schedule($operation, 'provider_pending', null, 'deletion_pending', false, 300);
            Capsule::table('mod_securiacevps_reconciliation')->updateOrInsert(
                [
                    'service_id' => (int) $operation['service_id'],
                    'finding_type' => 'termination_pending',
                    'state' => 'open',
                ],
                [
                    'finding_uuid' => Uuid::v4(),
                    'provider_account_id' => (string) $operation['provider_account_id'],
                    'provider_resource_id' => (string) ($operation['provider_resource_id'] ?? ''),
                    'severity' => 'warning',
                    'evidence_hash' => hash('sha256', $uuid . '|termination_pending'),
                    'safe_next_action' => 'reconcile',
                    'first_seen_at' => date('Y-m-d H:i:s'),
                    'last_seen_at' => date('Y-m-d H:i:s'),
                    'updated_at' => date('Y-m-d H:i:s'),
                ]
            );
            return;
        }
        $instances->cleanupAfterTermination($params);
        $this->operations->transition($uuid, $token, 'succeeded', [
            'reconciled_at' => date('Y-m-d H:i:s'),
            'completed_at' => date('Y-m-d H:i:s'),
            'next_attempt_at' => null,
            'safe_error_code' => null,
            'retry_classification' => null,
        ]);
    }

    /**
     * @param array<string,mixed> $operation
     */
    private function handleFailure(
        array $operation,
        ContaboProvisioningException $error,
        int $attempt,
        string $startedAt
    ): void {
        $uuid = (string) $operation['operation_uuid'];
        $token = (int) $operation['fencing_token'];
        $max = (int) ($operation['max_attempts'] ?? 8);
        $classification = $error->retryClassification();
        if ($error->hasAmbiguousOutcome()) {
            $state = 'unknown_outcome';
            $next = date('Y-m-d H:i:s', time() + $this->backoff($attempt, 30, 600));
        } elseif ($classification === 'transient' && $attempt < $max) {
            $state = 'retry_scheduled';
            $next = date('Y-m-d H:i:s', time() + $this->backoff($attempt, 15, 900));
        } elseif ($classification === 'manual_review' || $attempt >= $max) {
            $state = 'manual_review';
            $next = null;
        } else {
            $state = 'failed_terminal';
            $next = null;
        }
        $this->operations->transition($uuid, $token, $state, [
            'attempt_count' => $attempt,
            'next_attempt_at' => $next,
            'safe_error_code' => $error->safeCode(),
            'retry_classification' => $classification,
            'unknown_outcome' => $error->hasAmbiguousOutcome() ? 1 : 0,
            'completed_at' => in_array($state, ['manual_review', 'failed_terminal'], true)
                ? date('Y-m-d H:i:s')
                : null,
        ]);
        $this->operations->attempt($uuid, $attempt, $token, $state, [
            'safe_error_code' => $error->safeCode(),
            'retry_classification' => $classification,
            'started_at' => $startedAt,
            'finished_at' => date('Y-m-d H:i:s'),
        ]);
        $this->audit->record(
            'operation.failed',
            $state,
            (int) $operation['service_id'],
            (string) ($operation['correlation_id'] ?? ''),
            [
                'operation_uuid' => $uuid,
                'operation_type' => (string) $operation['operation_type'],
                'safe_error_code' => $error->safeCode(),
                'retry_classification' => $classification,
                'attempt' => $attempt,
            ]
        );
    }

    /**
     * @param array<string,mixed> $operation
     */
    private function schedule(
        array $operation,
        string $state,
        ?string $safeCode,
        string $classification,
        bool $unknown,
        int $delaySeconds
    ): void {
        $this->operations->transition(
            (string) $operation['operation_uuid'],
            (int) $operation['fencing_token'],
            $state,
            [
                'attempt_count' => ((int) ($operation['attempt_count'] ?? 0)) + 1,
                'next_attempt_at' => date('Y-m-d H:i:s', time() + max(5, $delaySeconds)),
                'safe_error_code' => $safeCode,
                'retry_classification' => $classification,
                'unknown_outcome' => $unknown ? 1 : 0,
            ]
        );
    }

    private function backoff(int $attempt, int $base, int $maximum): int
    {
        $power = min(6, max(0, $attempt));
        $delay = min($maximum, $base * (2 ** $power));
        return $delay + random_int(0, max(1, (int) floor($delay / 5)));
    }

    private function capabilityForOperation(string $type): string
    {
        if ($type === 'suspend') {
            return 'stop';
        }
        if ($type === 'unsuspend') {
            return 'start';
        }
        return $type;
    }
}
