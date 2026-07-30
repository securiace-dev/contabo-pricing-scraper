<?php
/**
 * SecuriAce VPS — WHMCS Server / Provisioning Module
 *
 * Automates certified Contabo compute lifecycle operations through the
 * official Customer API. Provider writes are accepted into a durable,
 * MySQL-backed operation engine and reconciled by WHMCS cron. No external
 * queue, Python service, Redis, Celery or PostgreSQL runtime is required.
 *
 * ── Server config (WHMCS → Setup → Products/Services → Servers) ───────────────
 *   Username     (serverusername)   → Contabo OAuth2 client_id
 *   Password     (serverpassword)   → Contabo OAuth2 client_secret
 *   Access Hash  (serveraccesshash) → "apiUser:apiPassword" (API user email +
 *                                      password), colon-separated. WHMCS stores
 *                                      all three encrypted at rest.
 *
 * ── Per-product compatibility options ───────────────────────────────────────
 *   Existing products retain the six legacy module fields for migration and
 *   read-only display. New provisioning never derives its configuration from
 *   mutable product fields: it requires the sealed paid-order snapshot created
 *   from a published addon mapping.
 *
 * The provider resource identity is projected into module-owned tables and the
 * compatibility custom field. The strict "whmcs-{serviceid}" provider tag and
 * verified adoption record are required for destructive actions. See the
 * addon's docs/PROVISIONING_CONTRACT.md for the complete contract.
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

// Single source of truth for the module version — also read by the release
// workflow (.github/workflows/release-contabo-vps.yml) to tag packages. Bump
// this when cutting a new release.
if (!defined('SECURIACE_VPS_VERSION')) {
    define('SECURIACE_VPS_VERSION', '2.0.0');
}

require_once __DIR__ . '/lib/ContaboProvisioningException.php';
require_once __DIR__ . '/lib/Uuid.php';
require_once __DIR__ . '/lib/CanonicalJson.php';
require_once __DIR__ . '/lib/SchemaGuard.php';
require_once __DIR__ . '/lib/ProviderAccount.php';
require_once __DIR__ . '/lib/CapabilityRegistry.php';
require_once __DIR__ . '/lib/OrderSnapshotRepository.php';
require_once __DIR__ . '/lib/AuditLogger.php';
require_once __DIR__ . '/lib/OneTimeSecretStore.php';
require_once __DIR__ . '/lib/OwnershipGuard.php';
require_once __DIR__ . '/lib/AdoptionService.php';
require_once __DIR__ . '/lib/ClientAreaPresenter.php';
require_once __DIR__ . '/lib/BillingSagaRepository.php';
require_once __DIR__ . '/lib/CommunicationService.php';
require_once __DIR__ . '/lib/OperationRepository.php';
require_once __DIR__ . '/lib/OperationProcessor.php';
require_once __DIR__ . '/lib/LifecycleOrchestrator.php';
require_once __DIR__ . '/lib/HttpExecutor.php';
require_once __DIR__ . '/lib/CurlHttpExecutor.php';
require_once __DIR__ . '/lib/ContaboAuth.php';
require_once __DIR__ . '/lib/ContaboApiClient.php';
require_once __DIR__ . '/lib/BillingCycleMapper.php';
require_once __DIR__ . '/lib/SecretManager.php';
require_once __DIR__ . '/lib/InstanceLinker.php';
require_once __DIR__ . '/lib/ConfigOptionResolver.php';
require_once __DIR__ . '/lib/ImageResolver.php';
require_once __DIR__ . '/lib/ContaboInstanceMapper.php';
require_once __DIR__ . '/lib/InstanceService.php';
require_once __DIR__ . '/lib/Runtime.php';

/** @return array<string,mixed> */
function securiacevps_MetaData(): array
{
    return [
        'DisplayName'    => 'SecuriAce VPS',
        'APIVersion'     => '1.1',
        'RequiresServer' => true,
    ];
}

/** @return array<int,array<string,mixed>> */
function securiacevps_ConfigOptions(): array
{
    return [
        1 => ['FriendlyName' => 'Legacy OS Image ID',  'Type' => 'text',     'Size' => '40', 'Default' => '', 'Description' => 'Migration reference only. Native provisioning uses the sealed paid-order snapshot.'],
        2 => ['FriendlyName' => 'Legacy Region',       'Type' => 'text',     'Size' => '20', 'Default' => 'EU', 'Description' => 'Migration reference only. Native provisioning uses the sealed paid-order snapshot.'],
        3 => ['FriendlyName' => 'Legacy SSH Secret ID','Type' => 'text',     'Size' => '30', 'Default' => '', 'Description' => 'Existing-product compatibility only; never used to bypass the sealed snapshot contract.'],
        4 => ['FriendlyName' => 'Legacy Product ID',   'Type' => 'text',     'Size' => '20', 'Default' => '', 'Description' => 'Migration reference only. Native provisioning requires a published provider SKU in the sealed snapshot.'],
        5 => ['FriendlyName' => 'Legacy Cloud-Init',   'Type' => 'textarea', 'Rows' => '4', 'Cols' => '50', 'Default' => '', 'Description' => 'Existing-product compatibility only; ignored by native sealed provisioning.'],
        6 => ['FriendlyName' => 'Legacy Add-ons JSON', 'Type' => 'textarea', 'Rows' => '2', 'Cols' => '50', 'Default' => '', 'Description' => 'Existing-product compatibility only; ignored by native sealed provisioning.'],
    ];
}

/**
 * @param array<string,mixed> $params
 * @return array<string,mixed>
 */
function securiacevps_TestConnection(array $params): array
{
    try {
        $auth = \SecuriAceVps\Runtime::auth($params);
        $auth->getToken();
    } catch (\Throwable $e) {
        return ['success' => false, 'error' => 'Authentication failed: ' . $e->getMessage()];
    }
    try {
        // Reuse the authenticated $auth so the token is fetched once.
        $client = new \SecuriAceVps\ContaboApiClient($auth);
        $client->setTimeout(10);
        $client->get('/v1/compute/instances?size=1&page=1');
        return ['success' => true, 'error' => ''];
    } catch (\Throwable $e) {
        return ['success' => false, 'error' => 'API unreachable: ' . $e->getMessage()];
    }
}

/** @param array<string,mixed> $params */
function securiacevps_CreateAccount(array $params): string
{
    try {
        return \SecuriAceVps\Runtime::lifecycle()->create($params);
    } catch (\Throwable $e) {
        _securiacevps_log('CreateAccount', $params['domain'] ?? '', $e->getMessage(), 'error');
        return _securiacevps_safe_error($e);
    }
}

/** @param array<string,mixed> $params */
function securiacevps_SuspendAccount(array $params): string
{
    try {
        return \SecuriAceVps\Runtime::lifecycle()->suspend($params);
    } catch (\Throwable $e) {
        _securiacevps_log('SuspendAccount', (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
        return _securiacevps_safe_error($e);
    }
}

/** @param array<string,mixed> $params */
function securiacevps_UnsuspendAccount(array $params): string
{
    try {
        return \SecuriAceVps\Runtime::lifecycle()->unsuspend($params);
    } catch (\Throwable $e) {
        _securiacevps_log('UnsuspendAccount', (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
        return _securiacevps_safe_error($e);
    }
}

/** @param array<string,mixed> $params */
function securiacevps_TerminateAccount(array $params): string
{
    try {
        return \SecuriAceVps\Runtime::lifecycle()->terminate($params);
    } catch (\Throwable $e) {
        _securiacevps_log('TerminateAccount', (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
        return _securiacevps_safe_error($e);
    }
}

/** @param array<string,mixed> $params */
function securiacevps_ChangePackage(array $params): string
{
    // The official API cannot resize an instance (POST /{id}/upgrade covers
    // add-ons only), so an honest error beats a fake success.
    if (function_exists('logActivity')) {
        logActivity('Contabo VPS: package change requested for service #' . (int) ($params['serviceid'] ?? 0)
            . ' — Contabo does not support live resize; cancel and re-provision, or upgrade from the Contabo panel.');
    }
    return 'Package change requires manual intervention: Contabo has no resize API — cancel and re-provision, or upgrade the instance from the Contabo control panel.';
}

// ── Admin UI ──────────────────────────────────────────────────────────────────

/** @return array<string,string> */
function securiacevps_AdminCustomButtonArray(): array
{
    return [
        'Sync from Contabo' => 'buttonSync',
    ];
}

/** @param array<string,mixed> $params */
function securiacevps_buttonStart(array $params): string
{
    return _securiacevps_durable_power($params, 'buttonStart', 'start');
}

/** @param array<string,mixed> $params */
function securiacevps_buttonStop(array $params): string
{
    return _securiacevps_durable_power($params, 'buttonStop', 'stop');
}

/** @param array<string,mixed> $params */
function securiacevps_buttonRestart(array $params): string
{
    return _securiacevps_durable_power($params, 'buttonRestart', 'restart');
}

/** @param array<string,mixed> $params */
function securiacevps_buttonResetPassword(array $params): string
{
    return _securiacevps_lifecycle($params, 'buttonResetPassword', 'resetPassword');
}

/** @param array<string,mixed> $params */
function securiacevps_buttonReinstall(array $params): string
{
    return _securiacevps_lifecycle($params, 'buttonReinstall', 'reinstall');
}

/** @param array<string,mixed> $params */
function securiacevps_buttonSync(array $params): string
{
    return _securiacevps_run($params, 'buttonSync', static function (\SecuriAceVps\InstanceService $svc) use ($params) {
        $svc->sync($params);
        return 'success';
    });
}

/**
 * @param array<string,mixed> $params
 * @return array<string,string>
 */
function securiacevps_AdminServicesTabFields(array $params): array
{
    try {
        $view = (new \SecuriAceVps\ClientAreaPresenter())->present($params);
        $fields = [
            'Instance ID' => htmlspecialchars((string) $view['instance_id']),
            'Provider status' => htmlspecialchars((string) $view['status']),
            'Provisioning' => htmlspecialchars((string) $view['provisioning_state']),
            'Ownership' => htmlspecialchars((string) $view['ownership_state']),
            'Region' => htmlspecialchars((string) $view['region']),
            'IPv4' => htmlspecialchars(implode(', ', (array) $view['ipv4'])),
        ];
        if (!empty($view['ipv6'])) {
            $fields['IPv6'] = htmlspecialchars(implode(', ', (array) $view['ipv6']));
        }
        $fields['Image'] = htmlspecialchars((string) $view['image']);
        $operation = is_array($view['operation'] ?? null) ? $view['operation'] : [];
        if ($operation !== []) {
            $fields['Latest operation'] = htmlspecialchars(
                (string) ($operation['operation_type'] ?? '')
                . ' · ' . (string) ($operation['state'] ?? '')
                . ' · ref ' . (string) ($operation['correlation_id'] ?? '')
            );
        }
        $fields['Last observed'] = htmlspecialchars((string) $view['synced_at']);
        return $fields;
    } catch (\Throwable $e) {
        $cached = _securiacevps_cached_ip($params);
        return [
            'Status' => 'Local VPS projection is unavailable. Use the addon operations workbench.',
            'IPv4'   => htmlspecialchars($cached !== '' ? $cached . ' (last synced)' : '—'),
        ];
    }
}

// ── Client area ───────────────────────────────────────────────────────────────

/** @return array<string,string> */
function securiacevps_ClientAreaCustomButtonArray(): array
{
    return [];
}

/** @param array<string,mixed> $params */
function securiacevps_clientStart(array $params): string
{
    return _securiacevps_durable_power($params, 'clientStart', 'start');
}

/** @param array<string,mixed> $params */
function securiacevps_clientStop(array $params): string
{
    return _securiacevps_durable_power($params, 'clientStop', 'stop');
}

/** @param array<string,mixed> $params */
function securiacevps_clientRestart(array $params): string
{
    return _securiacevps_durable_power($params, 'clientRestart', 'restart');
}

/** @param array<string,mixed> $params */
function securiacevps_clientResetPassword(array $params): string
{
    return _securiacevps_lifecycle($params, 'clientResetPassword', 'resetPassword');
}

/**
 * @param array<string,mixed> $params
 * @return array<string,mixed>
 */
function securiacevps_ClientArea(array $params): array
{
    $flash = '';
    $flashTone = 'info';
    $revealedCredential = '';
    try {
        if (strtoupper((string) ($_SERVER['REQUEST_METHOD'] ?? 'GET')) === 'POST'
            && isset($_POST['securiacevps_action'])
        ) {
            if (function_exists('check_token')) {
                check_token();
            }
            $action = (string) $_POST['securiacevps_action'];
            if ($action === 'refresh') {
                $instances = \SecuriAceVps\Runtime::instanceService($params);
                $instances->refreshProjection($params);
                $providerAccountId = \SecuriAceVps\ProviderAccount::id($params);
                if ((new \SecuriAceVps\CapabilityRegistry())->canRead(
                    $providerAccountId,
                    'snapshot_list'
                )) {
                    try {
                        $instances->refreshSnapshotsProjection($params);
                        $flash = 'Server details and snapshots refreshed.';
                        $flashTone = 'success';
                    } catch (\Throwable $snapshotError) {
                        $flash = 'Server details refreshed, but snapshot inventory is temporarily unavailable.';
                        $flashTone = 'warning';
                        _securiacevps_log(
                            'clientRefreshSnapshots',
                            (int) ($params['serviceid'] ?? 0),
                            $snapshotError->getMessage(),
                            'error'
                        );
                    }
                } else {
                    $flash = 'Server details refreshed.';
                    $flashTone = 'success';
                }
            } elseif (in_array($action, ['start', 'stop', 'restart'], true)) {
                $result = \SecuriAceVps\Runtime::lifecycle()->power($params, $action);
                $flash = $result === 'success'
                    ? 'The server action completed.'
                    : $result;
                $flashTone = $result === 'success' ? 'success' : 'warning';
            } elseif ($action === 'reset_password') {
                if ((string) ($_POST['confirmation'] ?? '') !== 'RESET PASSWORD') {
                    throw new \SecuriAceVps\ContaboProvisioningException(
                        'Type RESET PASSWORD to confirm this action',
                        'confirmation_required'
                    );
                }
                $result = \SecuriAceVps\Runtime::lifecycle()->resetPassword($params);
                $flash = $result === 'success'
                    ? 'Password reset completed. Reveal the new credential once below.'
                    : $result;
                $flashTone = $result === 'success' ? 'success' : 'warning';
            } elseif ($action === 'reinstall') {
                if ((string) ($_POST['confirmation'] ?? '') !== 'REINSTALL') {
                    throw new \SecuriAceVps\ContaboProvisioningException(
                        'Type REINSTALL to confirm data erasure',
                        'confirmation_required'
                    );
                }
                $result = \SecuriAceVps\Runtime::lifecycle()->reinstall($params);
                $flash = $result === 'success'
                    ? 'Reinstall completed. Reveal the new credential once below.'
                    : $result;
                $flashTone = $result === 'success' ? 'success' : 'warning';
            } elseif ($action === 'snapshot_create') {
                $result = \SecuriAceVps\Runtime::lifecycle()->createSnapshot(
                    $params,
                    (string) ($_POST['snapshot_name'] ?? ''),
                    (string) ($_POST['snapshot_description'] ?? '')
                );
                $flash = $result === 'success'
                    ? 'Snapshot created and verified.'
                    : $result;
                $flashTone = $result === 'success' ? 'success' : 'warning';
            } elseif ($action === 'snapshot_delete') {
                if ((string) ($_POST['confirmation'] ?? '') !== 'DELETE SNAPSHOT') {
                    throw new \SecuriAceVps\ContaboProvisioningException(
                        'Type DELETE SNAPSHOT to confirm this action',
                        'confirmation_required'
                    );
                }
                $result = \SecuriAceVps\Runtime::lifecycle()->deleteSnapshot(
                    $params,
                    (string) ($_POST['snapshot_id'] ?? ''),
                    (string) ($_POST['snapshot_evidence'] ?? '')
                );
                $flash = $result === 'success'
                    ? 'Snapshot deletion was verified.'
                    : $result;
                $flashTone = $result === 'success' ? 'success' : 'warning';
            } elseif ($action === 'snapshot_rollback') {
                if ((string) ($_POST['confirmation'] ?? '') !== 'ROLL BACK SNAPSHOT') {
                    throw new \SecuriAceVps\ContaboProvisioningException(
                        'Type ROLL BACK SNAPSHOT to confirm this action',
                        'confirmation_required'
                    );
                }
                $result = \SecuriAceVps\Runtime::lifecycle()->rollbackSnapshot(
                    $params,
                    (string) ($_POST['snapshot_id'] ?? ''),
                    (string) ($_POST['snapshot_evidence'] ?? '')
                );
                $flash = $result === 'success'
                    ? 'Snapshot rollback was accepted and verified in the provider audit.'
                    : $result;
                $flashTone = $result === 'success' ? 'success' : 'warning';
            } elseif ($action === 'reveal_credential') {
                if (!headers_sent()) {
                    header('Cache-Control: no-store, max-age=0');
                    header('Pragma: no-cache');
                    header('Referrer-Policy: no-referrer');
                }
                $revealedCredential = (new \SecuriAceVps\OneTimeSecretStore())->reveal(
                    (int) ($params['serviceid'] ?? 0),
                    (string) ($_POST['reveal_token'] ?? '')
                );
                $flash = 'Credential revealed. Copy it now; it cannot be shown again.';
                $flashTone = 'warning';
            } else {
                throw new \SecuriAceVps\ContaboProvisioningException(
                    'The requested VPS action is not available',
                    'client_action_not_available'
                );
            }
        }
        $view = (new \SecuriAceVps\ClientAreaPresenter())->present($params);
    } catch (\Throwable $e) {
        $flash = _securiacevps_safe_error($e);
        $flashTone = 'danger';
        $cached = _securiacevps_cached_ip($params);
        $view = [
            'instance_id' => '',
            'status' => 'unavailable',
            'provisioning_state' => 'unknown',
            'ownership_state' => 'unassessed',
            'verified_ownership' => false,
            'region' => '',
            'image' => '',
            'ipv4' => $cached !== '' ? [$cached] : [],
            'ipv6' => [],
            'synced_at' => '',
            'operation' => [],
            'busy' => false,
            'actions' => [],
            'credential' => null,
            'writes_enabled' => false,
            'snapshots' => [],
            'snapshot_list_certified' => false,
            'snapshot_actions' => ['create' => false, 'delete' => false, 'rollback' => false],
        ];
    }
    $view['flash'] = $flash;
    $view['flash_tone'] = $flashTone;
    $view['revealed_credential'] = $revealedCredential;
    $view['csrf_field'] = function_exists('generate_token') ? generate_token() : '';
    return ['templatefile' => 'clientarea', 'vars' => $view];
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/** @param array<string,mixed> $params */
function _securiacevps_durable_power(array $params, string $callName, string $action): string
{
    try {
        return \SecuriAceVps\Runtime::lifecycle()->power($params, $action);
    } catch (\Throwable $e) {
        _securiacevps_log($callName, (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
        return _securiacevps_safe_error($e);
    }
}

/** @param array<string,mixed> $params */
function _securiacevps_lifecycle(array $params, string $callName, string $method): string
{
    try {
        $lifecycle = \SecuriAceVps\Runtime::lifecycle();
        if (!in_array($method, ['resetPassword', 'reinstall'], true)) {
            throw new \LogicException('Unsupported lifecycle callback');
        }
        return $lifecycle->{$method}($params);
    } catch (\Throwable $e) {
        _securiacevps_log($callName, (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
        return _securiacevps_safe_error($e);
    }
}

function _securiacevps_safe_error(\Throwable $error): string
{
    if ($error instanceof \SecuriAceVps\ContaboProvisioningException) {
        $safe = [
            'confirmation_required' => $error->getMessage(),
            'client_action_not_available' => 'The requested VPS action is not available.',
            'resource_ownership_not_adopted' => 'This server is awaiting ownership verification.',
            'operation_credential_unavailable' => 'This credential is no longer available.',
            'snapshot_name_invalid' => $error->getMessage(),
            'snapshot_description_invalid' => 'The snapshot description is invalid.',
            'snapshot_selection_invalid' => 'The selected snapshot is invalid. Refresh and try again.',
            'snapshot_selection_stale' => 'The snapshot inventory changed. Refresh before continuing.',
            'snapshot_inventory_not_certified' => 'Snapshot management is not available for this server.',
        ];
        return $safe[$error->safeCode()]
            ?? 'The VPS operation could not be completed safely. An administrator can review the operation record.';
    }
    return 'The VPS operation could not be completed. Check the module log for the correlation reference.';
}

/**
 * Shared wrapper: build the service, run the action, map any throwable to the
 * human-readable error string WHMCS expects, and log the failure.
 *
 * @param array<string,mixed> $params
 * @param callable(\SecuriAceVps\InstanceService):string $fn
 */
function _securiacevps_run(array $params, string $callName, callable $fn): string
{
    try {
        return $fn(\SecuriAceVps\Runtime::instanceService($params));
    } catch (\Throwable $e) {
        _securiacevps_log($callName, (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
        return $e->getMessage();
    }
}

/**
 * Last-synced dedicated IP for graceful degradation when the live API is
 * unreachable. Prefers the model param; falls back to tblhosting.
 *
 * @param array<string,mixed> $params
 */
function _securiacevps_cached_ip(array $params): string
{
    if (isset($params['model']) && is_object($params['model'])) {
        $ip = trim((string) ($params['model']->dedicatedip ?? ''));
        if ($ip !== '') {
            return $ip;
        }
    }
    $serviceId = (int) ($params['serviceid'] ?? 0);
    if ($serviceId > 0) {
        try {
            $row = \WHMCS\Database\Capsule::table('tblhosting')->where('id', $serviceId)->first();
            $row = $row !== null ? (array) $row : [];
            return trim((string) ($row['dedicatedip'] ?? ''));
        } catch (\Throwable $e) {
            return '';
        }
    }
    return '';
}

/**
 * Module-call logger with defence-in-depth secret redaction: WHMCS's
 * replaceVars masks the server credentials, and _securiacevps_sanitize masks
 * any password-bearing keys inside logged request/response structures.
 *
 * @param mixed $request
 * @param mixed $response
 */
function _securiacevps_log(string $action, $request, $response, string $status = 'success'): void
{
    if (function_exists('logModuleCall')) {
        logModuleCall(
            'securiacevps',
            $action,
            _securiacevps_sanitize($request),
            _securiacevps_sanitize($response),
            $status,
            ['serverpassword', 'serveraccesshash']
        );
    }
}

/**
 * Recursively mask secret material in logged structures. Keys that carry
 * credentials ("value" is the Contabo secret-vault payload key) are replaced;
 * numeric rootPassword secretIds are safe and stay readable.
 *
 * @param mixed $data
 * @return mixed
 */
function _securiacevps_sanitize($data)
{
    if (!is_array($data)) {
        return $data;
    }
    $masked = [];
    foreach ($data as $key => $value) {
        $lower = is_string($key) ? strtolower($key) : '';
        if (in_array($lower, ['password', 'value', 'serverpassword', 'serveraccesshash', 'client_secret'], true)
            && is_string($value)
        ) {
            $masked[$key] = '***REDACTED***';
            continue;
        }
        $masked[$key] = is_array($value) ? _securiacevps_sanitize($value) : $value;
    }
    return $masked;
}
