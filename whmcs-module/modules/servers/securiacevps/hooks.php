<?php
/**
 * Contabo VPS — module hooks.
 *
 * DailyCronJob sweep: opportunistically reconcile IP/status for every active
 * or suspended securiacevps service (bounded batch, per-service isolation) so
 * tblhosting.dedicatedip converges even for services nobody opens in the UI.
 * View paths (admin tab / client area) sync on render; this is the safety net.
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
            foreach ($serverParamsList as $params) {
                try {
                    if ($client === null) {
                        $client = new \SecuriAceVps\ContaboApiClient(\SecuriAceVps\Runtime::auth($params));
                    }
                    \SecuriAceVps\Runtime::instanceServiceWithClient($client)->sync($params);
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
                (int) \SecuriAceVps\SchemaGuard::setting('operation_lease_seconds', '120')
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
            ->where('state', 'pending_validation')
            ->orderBy('id')
            ->limit(25)
            ->get();
        foreach ($rows as $item) {
            $command = (array) $item;
            _securiacevps_process_operator_command($command);
        }
    } catch (\Throwable $e) {
        if (function_exists('logActivity')) {
            logActivity('SecuriAce VPS operator-command worker unavailable: ' . $e->getMessage());
        }
    }
}

/** @param array<string,mixed> $command */
function _securiacevps_process_operator_command(array $command): void
{
    $id = (int) ($command['id'] ?? 0);
    $payloadJson = (string) ($command['payload_json'] ?? '{}');
    $payload = json_decode($payloadJson, true);
    $payload = is_array($payload) ? $payload : [];
    if (!hash_equals((string) ($command['payload_hash'] ?? ''), hash('sha256', \SecuriAceVps\CanonicalJson::encode($payload)))) {
        _securiacevps_finish_operator_command($id, 'rejected', 'command_payload_hash_mismatch');
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
            _securiacevps_finish_operator_command($id, 'rejected', 'operation_not_found');
            return;
        }
        $operation = (array) $operation;
        if (in_array((string) ($operation['state'] ?? ''), ['succeeded', 'cancelled', 'superseded'], true)) {
            _securiacevps_finish_operator_command($id, 'rejected', 'operation_already_terminal');
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
        _securiacevps_finish_operator_command($id, 'completed', null);
        return;
    }

    if ($type === 'cancel_operation') {
        $hasProviderRequest = \WHMCS\Database\Capsule::table('mod_securiacevps_provider_requests')
            ->where('operation_uuid', $operationUuid)
            ->count() > 0;
        if ($hasProviderRequest) {
            _securiacevps_finish_operator_command($id, 'rejected', 'provider_request_already_submitted');
            return;
        }
        \WHMCS\Database\Capsule::table('mod_securiacevps_operations')
            ->where('operation_uuid', $operationUuid)
            ->whereIn('state', ['accepted', 'retry_scheduled', 'failed_retryable'])
            ->update(['state' => 'cancelled', 'completed_at' => $now, 'updated_at' => $now]);
        _securiacevps_finish_operator_command($id, 'completed', null);
        return;
    }

    if ($type === 'set_global_write_state') {
        $enabled = !empty($payload['enabled']);
        if ($enabled && (string) ($payload['confirmation'] ?? '') !== 'ENABLE PROVIDER WRITES') {
            _securiacevps_finish_operator_command($id, 'rejected', 'write_enable_confirmation_missing');
            return;
        }
        \WHMCS\Database\Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'provider_writes_enabled'],
            ['value' => $enabled ? '1' : '0', 'updated_at' => $now]
        );
        _securiacevps_finish_operator_command($id, 'completed', null);
        return;
    }

    if ($type === 'set_capability_write_state') {
        $capability = preg_replace('/[^a-z0-9_.-]/', '', strtolower((string) ($payload['capability'] ?? '')));
        $enabled = !empty($payload['enabled']);
        if ($capability === '' || ($enabled && (string) ($payload['confirmation'] ?? '') !== 'ENABLE CAPABILITY WRITE')) {
            _securiacevps_finish_operator_command($id, 'rejected', 'capability_command_invalid');
            return;
        }
        \WHMCS\Database\Capsule::table('mod_securiacevps_schema')->updateOrInsert(
            ['key' => 'capability.' . $capability . '.enabled'],
            ['value' => $enabled ? '1' : '0', 'updated_at' => $now]
        );
        _securiacevps_finish_operator_command($id, 'completed', null);
        return;
    }

    _securiacevps_finish_operator_command($id, 'rejected', 'command_type_not_supported');
}

function _securiacevps_finish_operator_command(int $id, string $state, ?string $safeErrorCode): void
{
    \WHMCS\Database\Capsule::table('mod_securiacevps_operator_commands')
        ->where('id', $id)
        ->update([
            'state' => $state,
            'safe_error_code' => $safeErrorCode,
            'claimed_at' => date('Y-m-d H:i:s'),
            'completed_at' => date('Y-m-d H:i:s'),
            'updated_at' => date('Y-m-d H:i:s'),
        ]);
}
