<?php
declare(strict_types=1);

namespace ContaboPricing;

use InvalidArgumentException;
use RuntimeException;
use WHMCS\Database\Capsule;

/**
 * Read-oriented operator workbench plus an append-only command producer.
 *
 * This addon class never loads provisioning-module classes and never calls
 * Contabo. Provider-affecting recovery requests are written to the shared
 * operator-command contract for validation by the provisioning cron worker.
 */
final class VpsOperationsWorkbench
{
    /** @var list<string> */
    private const COMMAND_TYPES = [
        'reconcile_operation',
        'retry_operation',
        'cancel_operation',
        'set_global_write_state',
        'set_capability_write_state',
        'approve_adoption',
    ];

    /** @var list<string> */
    private const CAPABILITY_STATES = [
        'supported',
        'unsupported',
        'read_only',
        'requires_polling',
        'requires_manual_action',
        'not_certified',
    ];

    /**
     * @return array<string,mixed>
     */
    public function overview(int $beforeId = 0, int $limit = 50): array
    {
        $limit = max(10, min(100, $limit));
        $query = Capsule::table('mod_securiacevps_operations')->orderByDesc('id');
        if ($beforeId > 0) {
            $query->where('id', '<', $beforeId);
        }
        $operationObjects = $query->limit($limit + 1)->get();
        $operations = [];
        foreach ($operationObjects as $operation) {
            $operations[] = (array) $operation;
        }
        $nextCursor = null;
        if (count($operations) > $limit) {
            array_pop($operations);
            $last = end($operations);
            $nextCursor = is_array($last) ? (int) ($last['id'] ?? 0) : null;
            reset($operations);
        }

        $serviceIds = [];
        $operationUuids = [];
        foreach ($operations as $operation) {
            $serviceId = (int) ($operation['service_id'] ?? 0);
            if ($serviceId > 0) {
                $serviceIds[$serviceId] = $serviceId;
            }
            $uuid = (string) ($operation['operation_uuid'] ?? '');
            if ($uuid !== '') {
                $operationUuids[] = $uuid;
            }
        }

        $services = [];
        if ($serviceIds !== []) {
            foreach (Capsule::table('tblhosting')
                ->whereIn('id', array_values($serviceIds))
                ->get(['id', 'userid', 'packageid', 'domain', 'domainstatus', 'server']) as $service) {
                $row = (array) $service;
                $services[(int) ($row['id'] ?? 0)] = $row;
            }
        }

        $resources = [];
        if ($serviceIds !== []) {
            foreach (Capsule::table('mod_securiacevps_resources')
                ->whereIn('service_id', array_values($serviceIds))
                ->get() as $resource) {
                $row = (array) $resource;
                $resources[(int) ($row['service_id'] ?? 0)] = $row;
            }
        }

        $attemptCounts = [];
        if ($operationUuids !== []) {
            foreach (Capsule::table('mod_securiacevps_operation_attempts')
                ->whereIn('operation_uuid', $operationUuids)
                ->get(['operation_uuid']) as $attempt) {
                $uuid = (string) (((array) $attempt)['operation_uuid'] ?? '');
                $attemptCounts[$uuid] = ($attemptCounts[$uuid] ?? 0) + 1;
            }
        }

        foreach ($operations as &$operation) {
            $serviceId = (int) ($operation['service_id'] ?? 0);
            $operation['service'] = $services[$serviceId] ?? null;
            $operation['resource'] = $resources[$serviceId] ?? null;
            $operation['attempt_timeline_count'] = $attemptCounts[
                (string) ($operation['operation_uuid'] ?? '')
            ] ?? 0;
        }
        unset($operation);

        return [
            'operations' => $operations,
            'next_cursor' => $nextCursor,
            'counts' => $this->operationCounts(),
            'capabilities' => $this->rows('mod_securiacevps_capabilities', 'updated_at', 300),
            'reconciliation' => $this->openFindings(),
            'adoption' => $this->rows('mod_securiacevps_adoption', 'updated_at', 300),
            'commands' => $this->rows('mod_securiacevps_operator_commands', 'id', 100),
            'billing_sagas' => $this->rows('mod_securiacevps_billing_sagas', 'updated_at', 100),
            'communications' => $this->rows('mod_securiacevps_communications', 'id', 100),
            'provider_accounts' => $this->providerAccounts(),
            'global_writes_enabled' => $this->schemaSetting('provider_writes_enabled', '0') === '1',
            'capability_write_settings' => $this->capabilityWriteSettings(),
        ];
    }

    /**
     * @return array<string,mixed>
     */
    public function operationDetail(string $operationUuid): array
    {
        if (!preg_match('/^[a-f0-9-]{36}$/i', $operationUuid)) {
            throw new InvalidArgumentException('A valid operation UUID is required.');
        }
        $operationObject = Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $operationUuid)
            ->first();
        if ($operationObject === null) {
            throw new RuntimeException('The operation was not found.');
        }
        $operation = (array) $operationObject;
        $serviceId = (int) ($operation['service_id'] ?? 0);

        return [
            'operation' => $operation,
            'service' => $serviceId > 0
                ? $this->firstRow('tblhosting', 'id', $serviceId)
                : null,
            'resource' => $serviceId > 0
                ? $this->firstRow('mod_securiacevps_resources', 'service_id', $serviceId)
                : null,
            'attempts' => $this->whereRows(
                'mod_securiacevps_operation_attempts',
                'operation_uuid',
                $operationUuid,
                'id'
            ),
            'provider_requests' => $this->whereRows(
                'mod_securiacevps_provider_requests',
                'operation_uuid',
                $operationUuid,
                'id'
            ),
            'commands' => $this->whereRows(
                'mod_securiacevps_operator_commands',
                'operation_uuid',
                $operationUuid,
                'id'
            ),
            'billing_sagas' => $this->whereRows(
                'mod_securiacevps_billing_sagas',
                'operation_uuid',
                $operationUuid,
                'id'
            ),
            'communications' => $this->whereRows(
                'mod_securiacevps_communications',
                'operation_uuid',
                $operationUuid,
                'id'
            ),
            'audit_events' => $serviceId > 0
                ? $this->whereRows(
                    'mod_securiacevps_audit_events',
                    'service_id',
                    $serviceId,
                    'id'
                )
                : [],
            'findings' => $serviceId > 0
                ? $this->whereRows(
                    'mod_securiacevps_reconciliation',
                    'service_id',
                    $serviceId,
                    'id'
                )
                : [],
        ];
    }

    /**
     * @param array<string,mixed> $payload
     */
    public function queueCommand(
        string $commandType,
        ?int $serviceId,
        ?string $operationUuid,
        array $payload,
        int $adminId
    ): string {
        if (!in_array($commandType, self::COMMAND_TYPES, true)) {
            throw new InvalidArgumentException('The requested operator command is not supported.');
        }
        if ($adminId <= 0) {
            throw new RuntimeException('An authenticated administrator is required.');
        }
        if (in_array($commandType, ['reconcile_operation', 'retry_operation', 'cancel_operation'], true)) {
            if ($operationUuid === null || !preg_match('/^[a-f0-9-]{36}$/i', $operationUuid)) {
                throw new InvalidArgumentException('A valid operation UUID is required.');
            }
            $operation = Capsule::table('mod_securiacevps_operations')
                ->where('operation_uuid', $operationUuid)
                ->first();
            if ($operation === null) {
                throw new RuntimeException('The operation no longer exists.');
            }
            $operation = (array) $operation;
            $serviceId = (int) ($operation['service_id'] ?? 0);
        }
        if ($commandType === 'approve_adoption') {
            if (($serviceId ?? 0) <= 0
                || (string) ($payload['confirmation'] ?? '') !== 'VERIFY OWNERSHIP'
                || !preg_match('/^[a-f0-9]{64}$/', (string) ($payload['evidence_hash'] ?? ''))
                || trim((string) ($payload['provider_resource_id'] ?? '')) === ''
            ) {
                throw new InvalidArgumentException(
                    'A current adoption candidate and typed VERIFY OWNERSHIP confirmation are required.'
                );
            }
            $adoption = Capsule::table('mod_securiacevps_adoption')
                ->where('service_id', (int) $serviceId)
                ->first();
            $adoption = $adoption !== null ? (array) $adoption : [];
            if ((string) ($adoption['state'] ?? '') !== 'probable'
                || !hash_equals(
                    trim((string) ($payload['provider_resource_id'] ?? '')),
                    (string) ($adoption['provider_resource_id'] ?? '')
                )
                || !hash_equals(
                    (string) ($payload['evidence_hash'] ?? ''),
                    hash('sha256', (string) ($adoption['evidence_json'] ?? ''))
                )
            ) {
                throw new InvalidArgumentException('The adoption candidate is stale or no longer probable.');
            }
        }

        if ($commandType === 'set_global_write_state' && !empty($payload['enabled'])) {
            if ((string) ($payload['confirmation'] ?? '') !== 'ENABLE PROVIDER WRITES') {
                throw new InvalidArgumentException('Type ENABLE PROVIDER WRITES to enable provider mutations.');
            }
        }
        if ($commandType === 'set_capability_write_state' && !empty($payload['enabled'])) {
            if ((string) ($payload['confirmation'] ?? '') !== 'ENABLE CAPABILITY WRITE') {
                throw new InvalidArgumentException('Type ENABLE CAPABILITY WRITE to enable this mutation.');
            }
            $providerAccountId = trim((string) ($payload['provider_account_id'] ?? ''));
            $capability = preg_replace(
                '/[^a-z0-9_.-]/',
                '',
                strtolower((string) ($payload['capability'] ?? ''))
            ) ?: '';
            $certified = $providerAccountId !== '' && $capability !== ''
                ? Capsule::table('mod_securiacevps_capabilities')
                    ->where('provider_account_id', $providerAccountId)
                    ->where('capability', $capability)
                    ->whereIn('state', ['supported', 'requires_polling'])
                    ->count()
                : 0;
            if ($certified !== 1) {
                throw new InvalidArgumentException(
                    'Only a certified provider-account capability may be enabled.'
                );
            }
        }

        $payloadJson = CatalogImportService::canonicalJson($payload);
        $uuid = $this->uuid();
        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_operator_commands')->insert([
            'command_uuid' => $uuid,
            'command_type' => $commandType,
            'service_id' => $serviceId !== null && $serviceId > 0 ? $serviceId : null,
            'operation_uuid' => $operationUuid !== null && $operationUuid !== '' ? $operationUuid : null,
            'requested_by_admin_id' => $adminId,
            'state' => 'pending_validation',
            'payload_hash' => hash('sha256', $payloadJson),
            'payload_json' => $payloadJson,
            'safe_error_code' => null,
            'claimed_at' => null,
            'completed_at' => null,
            'created_at' => $now,
            'updated_at' => $now,
        ]);
        return $uuid;
    }

    /**
     * Record explicit provider capability certification. The write controls
     * whether an action may be considered; the separate capability kill switch
     * still has to be enabled before any mutation can execute.
     *
     * @param array<string,mixed> $evidence
     */
    public function certifyCapability(
        string $providerAccountId,
        string $capability,
        string $state,
        string $certificationVersion,
        array $evidence,
        int $adminId,
        string $confirmation
    ): void {
        $providerAccountId = trim($providerAccountId);
        $capability = preg_replace('/[^a-z0-9_.-]/', '', strtolower($capability)) ?: '';
        if ($providerAccountId === '' || $capability === '') {
            throw new InvalidArgumentException('Provider account and capability are required.');
        }
        if (!in_array($state, self::CAPABILITY_STATES, true)) {
            throw new InvalidArgumentException('The capability state is invalid.');
        }
        if ($adminId <= 0) {
            throw new RuntimeException('An authenticated administrator is required.');
        }
        if (in_array($state, ['supported', 'requires_polling'], true)
            && $confirmation !== 'CERTIFY CAPABILITY') {
            throw new InvalidArgumentException('Type CERTIFY CAPABILITY to approve a provider write contract.');
        }
        if (!in_array($providerAccountId, array_keys($this->providerAccounts()), true)) {
            throw new RuntimeException('The provider account is not configured in WHMCS.');
        }

        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_capabilities')->updateOrInsert(
            ['provider_account_id' => $providerAccountId, 'capability' => $capability],
            [
                'state' => $state,
                'certification_version' => trim($certificationVersion) !== ''
                    ? trim($certificationVersion)
                    : null,
                'evidence_json' => CatalogImportService::canonicalJson($evidence),
                'certified_by_admin_id' => $adminId,
                'certified_at' => $now,
                'updated_at' => $now,
                'created_at' => $now,
            ]
        );
    }

    /** @return array<string,int> */
    private function operationCounts(): array
    {
        $states = [
            'accepted',
            'claimed',
            'submitted',
            'provider_pending',
            'reconciling',
            'retry_scheduled',
            'failed_retryable',
            'failed_terminal',
            'unknown_outcome',
            'manual_review',
            'succeeded',
            'cancelled',
        ];
        $counts = [];
        foreach ($states as $state) {
            $counts[$state] = (int) Capsule::table('mod_securiacevps_operations')
                ->where('state', $state)
                ->count();
        }
        $counts['open_findings'] = (int) Capsule::table('mod_securiacevps_reconciliation')
            ->where('state', 'open')
            ->count();
        return $counts;
    }

    /** @return list<array<string,mixed>> */
    private function openFindings(): array
    {
        $out = [];
        foreach (Capsule::table('mod_securiacevps_reconciliation')
            ->where('state', 'open')
            ->orderByDesc('last_seen_at')
            ->limit(300)
            ->get() as $row) {
            $out[] = (array) $row;
        }
        return $out;
    }

    /**
     * @return list<array<string,mixed>>
     */
    private function rows(string $table, string $orderColumn, int $limit): array
    {
        $rows = [];
        foreach (Capsule::table($table)->orderByDesc($orderColumn)->limit($limit)->get() as $row) {
            $rows[] = (array) $row;
        }
        return $rows;
    }

    /** @return array<string,mixed>|null */
    private function firstRow(string $table, string $column, $value): ?array
    {
        $row = Capsule::table($table)->where($column, $value)->first();
        return $row === null ? null : (array) $row;
    }

    /**
     * @param mixed $value
     * @return list<array<string,mixed>>
     */
    private function whereRows(string $table, string $column, $value, string $orderColumn): array
    {
        $rows = [];
        foreach (Capsule::table($table)
            ->where($column, $value)
            ->orderBy($orderColumn)
            ->limit(500)
            ->get() as $item) {
            $rows[] = (array) $item;
        }
        return $rows;
    }

    /**
     * @return array<string,array{id:int,name:string,active:bool}>
     */
    private function providerAccounts(): array
    {
        $accounts = [];
        $servers = Capsule::table('tblservers')
            ->whereIn('type', ['securiacevps', 'contabo_vps'])
            ->get(['id', 'name', 'username', 'active']);
        foreach ($servers as $server) {
            $row = (array) $server;
            $id = hash(
                'sha256',
                'contabo|' . (int) ($row['id'] ?? 0) . '|'
                . strtolower(trim((string) ($row['username'] ?? '')))
            );
            $accounts[$id] = [
                'id' => (int) ($row['id'] ?? 0),
                'name' => (string) ($row['name'] ?? ('Server #' . (int) ($row['id'] ?? 0))),
                'active' => !empty($row['active']),
            ];
        }
        return $accounts;
    }

    private function schemaSetting(string $key, string $default): string
    {
        $value = Capsule::table('mod_securiacevps_schema')->where('key', $key)->value('value');
        return $value === null ? $default : (string) $value;
    }

    /** @return array<string,bool> */
    private function capabilityWriteSettings(): array
    {
        $settings = [];
        foreach (Capsule::table('mod_securiacevps_schema')->get() as $item) {
            $row = (array) $item;
            $key = (string) ($row['key'] ?? '');
            if (preg_match('/^capability\.([a-z0-9_.-]+)\.enabled$/', $key, $match)) {
                $settings[$match[1]] = (string) ($row['value'] ?? '0') === '1';
            }
        }
        ksort($settings);
        return $settings;
    }

    private function uuid(): string
    {
        $bytes = random_bytes(16);
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }
}
