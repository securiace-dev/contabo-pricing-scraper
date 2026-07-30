<?php
/**
 * Contabo VPS — module hooks.
 *
 * DailyCronJob sweep: assess ownership read-only, then reconcile IP/status only
 * for verified active or suspended services. Provider mutations are never
 * performed from a read or view path.
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

require_once __DIR__ . '/securiacevps.php';

if (defined('SECURIACE_VPS_HOOKS_REGISTERED')) {
    return;
}
define('SECURIACE_VPS_HOOKS_REGISTERED', true);

add_hook('AfterCronJob', 30, function () {
    _securiacevps_process_operator_commands();
    _securiacevps_process_operations();
    (new \SecuriAceVps\CommunicationService())->processQueue();
    (new \SecuriAceVps\OneTimeSecretStore())->destroyExpired();
});

add_hook('DailyCronJob', 30, function () {
    $maxServices = 100;

    try {
        $products = \WHMCS\Database\Capsule::table('tblproducts')
            ->whereIn('servertype', ['securiacevps', 'contabo_vps'])
            ->get();
        $productIds = [];
        foreach ($products as $p) {
            $p = (array) $p;
            if ((int) ($p['id'] ?? 0) > 0) {
                $productIds[] = (int) $p['id'];
            }
        }
        if ($productIds === []) {
            return;
        }

        $services = \WHMCS\Database\Capsule::table('tblhosting')
            ->whereIn('packageid', $productIds)
            ->whereIn('domainstatus', ['Active', 'Suspended'])
            ->limit($maxServices)
            ->get();

        // Group by server so each server authenticates ONCE (one token) and its
        // services reuse the same client — instead of a token fetch per service,
        // which would double the call volume and risk Contabo's rate limit.
        $byServer = [];
        foreach ($services as $svc) {
            $svc = (array) $svc;
            $params = _securiacevps_cron_params($svc);
            if ($params === null) {
                continue;
            }
            $byServer[(int) ($svc['server'] ?? 0)][] = $params;
        }

        $synced = 0;
        $failed = 0;
        foreach ($byServer as $serverParamsList) {
            $client = null;
            $instances = null;
            foreach ($serverParamsList as $params) {
                try {
                    if ($client === null) {
                        $client = new \SecuriAceVps\ContaboApiClient(\SecuriAceVps\Runtime::auth($params));
                        $instances = \SecuriAceVps\Runtime::instanceServiceWithClient($client);
                    }
                    $instances->refreshProjection($params);
                    $synced++;
                } catch (\Throwable $e) {
                    $failed++;
                    // Unlinked services and transient API errors are expected here;
                    // the per-service detail lives in the module debug log.
                    _securiacevps_log('CronSync', (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
                }
                // Small pause to stay well under Contabo's rate limit on big fleets.
                usleep(150000);
            }
            if ($client !== null && $serverParamsList !== []) {
                try {
                    $inventory = (new \SecuriAceVps\AdoptionService($client))
                        ->inventoryProviderAccount($serverParamsList[0]);
                    if (($inventory['orphans'] ?? 0) > 0 && function_exists('logActivity')) {
                        logActivity(
                            'SecuriAce VPS: provider inventory found '
                            . (int) $inventory['orphans']
                            . ' tagged orphan resource(s); review the operations workbench.'
                        );
                    }
                } catch (\Throwable $e) {
                    if (function_exists('logActivity')) {
                        logActivity(
                            'SecuriAce VPS: provider inventory incomplete for one account; '
                            . 'review provider health in the operations workbench.'
                        );
                    }
                }
            }
        }

        if (($synced > 0 || $failed > 0) && function_exists('logActivity')) {
            logActivity('Contabo VPS: daily sync reconciled ' . $synced . ' service(s)'
                . ($failed > 0 ? ', ' . $failed . ' failed (see module log)' : '') . '.');
        }
    } catch (\Throwable $e) {
        if (function_exists('logActivity')) {
            logActivity('Contabo VPS: daily sync sweep aborted — ' . $e->getMessage());
        }
    }
});

/**
 * Build the minimal module-params array the sync path needs for a service row,
 * resolving the server credentials from tblservers. Returns null when the
 * service has no usable server assignment.
 *
 * @param array<string,mixed> $service tblhosting row
 * @return array<string,mixed>|null
 */
function _securiacevps_cron_params(array $service): ?array
{
    $serverId = (int) ($service['server'] ?? 0);
    if ($serverId <= 0) {
        return null;
    }
    $server = \WHMCS\Database\Capsule::table('tblservers')->where('id', $serverId)->first();
    $server = $server !== null ? (array) $server : null;
    if ($server === null) {
        return null;
    }

    $password = (string) ($server['password'] ?? '');
    if ($password !== '' && function_exists('decrypt')) {
        $password = decrypt($password);
    }

    return [
        'serviceid'        => (int) ($service['id'] ?? 0),
        'pid'              => (int) ($service['packageid'] ?? 0),
        'packageid'        => (int) ($service['packageid'] ?? 0),
        'domain'           => (string) ($service['domain'] ?? ''),
        'serverid'         => $serverId,
        'server'           => $serverId,
        'serverusername'   => (string) ($server['username'] ?? ''),
        'serverpassword'   => $password,
        'serveraccesshash' => (string) ($server['accesshash'] ?? ''),
    ];
}

/**
 * Claim and progress a bounded batch. Each operation is isolated: one broken
 * service cannot abort the rest of the WHMCS cron run.
 */
function _securiacevps_process_operations(): void
{
    try {
        \SecuriAceVps\SchemaGuard::assertReady();
        $batchSize = max(1, min(100, (int) \SecuriAceVps\SchemaGuard::setting('operation_batch_size', '25')));
        $repo = new \SecuriAceVps\OperationRepository();
        $lifecycle = new \SecuriAceVps\LifecycleOrchestrator($repo);
        $worker = 'cron-' . substr(hash('sha256', gethostname() . '|' . getmypid() . '|' . microtime(true)), 0, 24);
        foreach ($repo->due($batchSize) as $operation) {
            $params = _securiacevps_operation_params($operation);
            if ($params === null) {
                continue;
            }
            $claimed = $repo->claim(
                (string) $operation['operation_uuid'],
                $worker,
                (int) \SecuriAceVps\SchemaGuard::setting(
                    'operation_lease_seconds',
                    (string) \SecuriAceVps\OperationRepository::MIN_LEASE_SECONDS
                )
            );
            if ($claimed === null) {
                continue;
            }
            try {
                $lifecycle->progressClaimed($claimed, $params);
            } catch (\Throwable $e) {
                if (function_exists('logActivity')) {
                    logActivity(
                        'SecuriAce VPS cron operation failed ['
                        . (string) ($operation['correlation_id'] ?? '') . ']: '
                        . $e->getMessage()
                    );
                }
            }
        }
    } catch (\Throwable $e) {
        if (function_exists('logActivity')) {
            logActivity('SecuriAce VPS operation worker unavailable: ' . $e->getMessage());
        }
    }
}

/**
 * @param array<string,mixed> $operation
 * @return array<string,mixed>|null
 */
function _securiacevps_operation_params(array $operation): ?array
{
    $serviceId = (int) ($operation['service_id'] ?? 0);
    if ($serviceId <= 0) {
        return null;
    }
    $service = \WHMCS\Database\Capsule::table('tblhosting')->where('id', $serviceId)->first();
    if ($service === null) {
        return null;
    }
    return _securiacevps_cron_params((array) $service);
}

/**
 * Validate append-only commands produced by the addon. The addon never calls
 * provider APIs or loads provisioning classes; this worker is the only
 * authority that converts a validated command into operation state.
 */
function _securiacevps_process_operator_commands(): void
{
    try {
        \SecuriAceVps\SchemaGuard::assertReady();
        $rows = \WHMCS\Database\Capsule::table('mod_securiacevps_operator_commands')
            ->whereIn('state', ['pending_validation', 'claimed'])
            ->orderBy('id')
            ->limit(75)
            ->get();
        $processed = 0;
        foreach ($rows as $item) {
            $command = (array) $item;
            $state = (string) ($command['state'] ?? '');
            $claimExpiresAt = trim((string) ($command['claim_expires_at'] ?? ''));
            if ($state === 'claimed'
                && $claimExpiresAt !== ''
                && strtotime($claimExpiresAt) > time()
            ) {
                continue;
            }
            if ($processed >= 25) {
                break;
            }
            $claimToken = \SecuriAceVps\Uuid::v4();
            $claimedAt = date('Y-m-d H:i:s');
            $claimQuery = \WHMCS\Database\Capsule::table('mod_securiacevps_operator_commands')
                ->where('id', (int) ($command['id'] ?? 0))
                ->where('state', $state);
            if ($state === 'claimed') {
                $claimQuery->where('claim_token', $command['claim_token'] ?? null)
                    ->where('claim_expires_at', $command['claim_expires_at'] ?? null);
            }
            $claimed = $claimQuery->update([
                'state' => 'claimed',
                'claim_token' => $claimToken,
                'claim_expires_at' => date(
                    'Y-m-d H:i:s',
                    time() + max(
                        60,
                        (int) \SecuriAceVps\SchemaGuard::setting(
                            'operator_command_lease_seconds',
                            '300'
                        )
                    )
                ),
                'claimed_at' => $claimedAt,
                'updated_at' => $claimedAt,
            ]);
            if ($claimed !== 1) {
                continue;
            }
            $processed++;
            $command['state'] = 'claimed';
            $command['claim_token'] = $claimToken;
            try {
                _securiacevps_process_operator_command($command, $claimToken);
            } catch (\Throwable $e) {
                $safeCode = $e instanceof \SecuriAceVps\ContaboProvisioningException
                    ? $e->safeCode()
                    : 'operator_command_execution_failed';
                _securiacevps_finish_operator_command(
                    (int) ($command['id'] ?? 0),
                    'rejected',
                    $safeCode,
                    $claimToken
                );
            }
        }
    } catch (\Throwable $e) {
        if (function_exists('logActivity')) {
            logActivity('SecuriAce VPS operator-command worker unavailable: ' . $e->getMessage());
        }
    }
}

/** @param array<string,mixed> $command */
function _securiacevps_process_operator_command(array $command, string $claimToken): void
{
    $id = (int) ($command['id'] ?? 0);
    $payloadJson = (string) ($command['payload_json'] ?? '{}');
    $payload = json_decode($payloadJson, true);
    $payload = is_array($payload) ? $payload : [];
    if (!hash_equals((string) ($command['payload_hash'] ?? ''), hash('sha256', \SecuriAceVps\CanonicalJson::encode($payload)))) {
        _securiacevps_finish_operator_command(
            $id,
            'rejected',
            'command_payload_hash_mismatch',
            $claimToken
        );
        return;
    }
    $type = (string) ($command['command_type'] ?? '');
    $operationUuid = (string) ($command['operation_uuid'] ?? '');
    $now = date('Y-m-d H:i:s');

    if (in_array($type, ['reconcile_operation', 'retry_operation'], true)) {
        $operation = \WHMCS\Database\Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $operationUuid)
            ->first();
        if ($operation === null) {
            _securiacevps_finish_operator_command(
                $id,
                'rejected',
                'operation_not_found',
                $claimToken
            );
            return;
        }
        $operation = (array) $operation;
        if (in_array((string) ($operation['state'] ?? ''), ['succeeded', 'cancelled', 'superseded'], true)) {
            _securiacevps_finish_operator_command(
                $id,
                'rejected',
                'operation_already_terminal',
                $claimToken
            );
            return;
        }
        // Even an explicit retry is converted to reconciliation when a
        // provider request exists; a create/delete mutation is never replayed
        // blindly.
        $hasProviderRequest = \WHMCS\Database\Capsule::table('mod_securiacevps_provider_requests')
            ->where('operation_uuid', $operationUuid)
            ->count() > 0;
        \WHMCS\Database\Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $operationUuid)
            ->update([
                'state' => $hasProviderRequest ? 'reconciling' : 'retry_scheduled',
                'next_attempt_at' => $now,
                'safe_error_code' => null,
                'updated_at' => $now,
            ]);
        _securiacevps_finish_operator_command($id, 'completed', null, $claimToken);
        return;
    }

    if ($type === 'cancel_operation') {
        $hasProviderRequest = \WHMCS\Database\Capsule::table('mod_securiacevps_provider_requests')
            ->where('operation_uuid', $operationUuid)
            ->count() > 0;
        if ($hasProviderRequest) {
            _securiacevps_finish_operator_command(
                $id,
                'rejected',
                'provider_request_already_submitted',
                $claimToken
            );
            return;
        }
        \WHMCS\Database\Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $operationUuid)
            ->whereIn('state', ['accepted', 'retry_scheduled', 'failed_retryable'])
            ->update(['state' => 'cancelled', 'completed_at' => $now, 'updated_at' => $now]);
        _securiacevps_finish_operator_command($id, 'completed', null, $claimToken);
        return;
    }

    if ($type === 'set_global_write_state') {
        $enabled = !empty($payload['enabled']);
        if ($enabled && (string) ($payload['confirmation'] ?? '') !== 'ENABLE PROVIDER WRITES') {
            _securiacevps_finish_operator_command(
                $id,
                'rejected',
                'write_enable_confirmation_missing',
                $claimToken
            );
            return;
        }
        \WHMCS\Database\Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'provider_writes_enabled'],
            ['value' => $enabled ? '1' : '0', 'updated_at' => $now]
        );
        _securiacevps_finish_operator_command($id, 'completed', null, $claimToken);
        return;
    }

    if ($type === 'set_capability_write_state') {
        $capability = preg_replace('/[^a-z0-9_.-]/', '', strtolower((string) ($payload['capability'] ?? '')));
        $providerAccountId = trim((string) ($payload['provider_account_id'] ?? ''));
        $enabled = !empty($payload['enabled']);
        if ($capability === ''
            || $providerAccountId === ''
            || ($enabled && (string) ($payload['confirmation'] ?? '') !== 'ENABLE CAPABILITY WRITE')
        ) {
            _securiacevps_finish_operator_command(
                $id,
                'rejected',
                'capability_command_invalid',
                $claimToken
            );
            return;
        }
        if ($enabled) {
            $certified = \WHMCS\Database\Capsule::table('mod_securiacevps_capabilities')
                ->where('provider_account_id', $providerAccountId)
                ->where('capability', $capability)
                ->whereIn('state', ['supported', 'requires_polling'])
                ->count();
            if ($certified !== 1) {
                _securiacevps_finish_operator_command(
                    $id,
                    'rejected',
                    'capability_not_certified_for_provider',
                    $claimToken
                );
                return;
            }
        }
        \WHMCS\Database\Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'capability.' . $capability . '.enabled'],
            ['value' => $enabled ? '1' : '0', 'updated_at' => $now]
        );
        _securiacevps_finish_operator_command($id, 'completed', null, $claimToken);
        return;
    }

    if ($type === 'approve_adoption') {
        $serviceId = (int) ($command['service_id'] ?? 0);
        $params = _securiacevps_operation_params(['service_id' => $serviceId]);
        if ($params === null
            || (string) ($payload['confirmation'] ?? '') !== 'VERIFY OWNERSHIP'
        ) {
            _securiacevps_finish_operator_command(
                $id,
                'rejected',
                'adoption_command_invalid',
                $claimToken
            );
            return;
        }
        $client = new \SecuriAceVps\ContaboApiClient(\SecuriAceVps\Runtime::auth($params));
        (new \SecuriAceVps\AdoptionService($client))->approveCandidate(
            $params,
            trim((string) ($payload['provider_resource_id'] ?? '')),
            trim((string) ($payload['evidence_hash'] ?? '')),
            (int) ($command['requested_by_admin_id'] ?? 0)
        );
        _securiacevps_finish_operator_command($id, 'completed', null, $claimToken);
        return;
    }

    _securiacevps_finish_operator_command(
        $id,
        'rejected',
        'command_type_not_supported',
        $claimToken
    );
}

function _securiacevps_finish_operator_command(
    int $id,
    string $state,
    ?string $safeErrorCode,
    string $claimToken
): void
{
    $command = \WHMCS\Database\Capsule::table('mod_securiacevps_operator_commands')
        ->where('id', $id)
        ->first();
    $command = $command !== null ? (array) $command : [];
    $updated = \WHMCS\Database\Capsule::table('mod_securiacevps_operator_commands')
        ->where('id', $id)
        ->where('state', 'claimed')
        ->where('claim_token', $claimToken)
        ->update([
            'state' => $state,
            'safe_error_code' => $safeErrorCode,
            'claim_token' => null,
            'claim_expires_at' => null,
            'completed_at' => date('Y-m-d H:i:s'),
            'updated_at' => date('Y-m-d H:i:s'),
        ]);
    if ($updated === 1 && $command !== []) {
        (new \SecuriAceVps\AuditLogger())->record(
            'operator_command.' . (string) ($command['command_type'] ?? 'unknown'),
            $state,
            (int) ($command['service_id'] ?? 0),
            '',
            [
                'command_uuid' => (string) ($command['command_uuid'] ?? ''),
                'operation_uuid' => (string) ($command['operation_uuid'] ?? ''),
                'safe_error_code' => $safeErrorCode,
            ],
            'admin',
            (int) ($command['requested_by_admin_id'] ?? 0)
        );
    }
}
