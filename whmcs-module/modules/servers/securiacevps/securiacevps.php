<?php
/**
 * Contabo VPS — WHMCS Server / Provisioning Module
 *
 * Automates the Contabo compute lifecycle (VPS / VDS / Storage VPS) through
 * the official Contabo REST API — the same API the `cntb` CLI wraps. OAuth2
 * password-grant auth (Keycloak). No external dependencies — pure PHP/curl
 * behind an injectable HttpExecutor seam. WHMCS 8.x and 9.x, PHP 7.4+.
 *
 * ── Server config (WHMCS → Setup → Products/Services → Servers) ───────────────
 *   Username     (serverusername)   → Contabo OAuth2 client_id
 *   Password     (serverpassword)   → Contabo OAuth2 client_secret
 *   Access Hash  (serveraccesshash) → "apiUser:apiPassword" (API user email +
 *                                      password), colon-separated. WHMCS stores
 *                                      all three encrypted at rest.
 *
 * ── Per-product config options ───────────────────────────────────────────────
 *   configoption1 → Contabo imageId (fallback when no Image selection)
 *   configoption2 → region slug/label (fallback when no Region selection)
 *   configoption3 → SSH secret id (Contabo vault; optional)
 *   configoption4 → Contabo productId (e.g. V45)
 *   configoption5 → cloud-init user data (optional)
 *   configoption6 → add-ons JSON (optional, merged with selections)
 *
 * Customer selections on configurable products are round-tripped through the
 * contabo_pricing addon's link tables and take precedence over the fallbacks.
 *
 * The created instance id lives in the service custom field
 * "contabo_instance_id" (auto-created on first provision). The instance's
 * Contabo displayName carries a "whmcs-{serviceid}" tag; destructive actions
 * verify it. See docs/PROVISIONING_CONTRACT.md in the addon for the contract.
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

// Single source of truth for the module version — also read by the release
// workflow (.github/workflows/release-contabo-vps.yml) to tag packages. Bump
// this when cutting a new release.
if (!defined('SECURIACE_VPS_VERSION')) {
    define('SECURIACE_VPS_VERSION', '1.0.0');
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
        'DisplayName'    => 'Contabo VPS',
        'APIVersion'     => '1.1',
        'RequiresServer' => true,
    ];
}

/** @return array<int,array<string,mixed>> */
function securiacevps_ConfigOptions(): array
{
    return [
        1 => ['FriendlyName' => 'OS Image ID',   'Type' => 'text',     'Size' => '40', 'Default' => '', 'Description' => 'Contabo imageId — fallback when no Image configurable option is exposed'],
        2 => ['FriendlyName' => 'Region',         'Type' => 'text',     'Size' => '20', 'Default' => 'EU', 'Description' => 'Region slug (EU, US-central, US-east, US-west, SIN, UK, AUS, IND, JPN) — fallback when no Region option is exposed'],
        3 => ['FriendlyName' => 'SSH Secret ID',  'Type' => 'text',     'Size' => '30', 'Default' => '', 'Description' => 'Optional — numeric secretId of an SSH public key in the Contabo vault'],
        4 => ['FriendlyName' => 'Product ID',     'Type' => 'text',     'Size' => '20', 'Default' => '', 'Description' => 'Contabo productId (e.g. V45)'],
        5 => ['FriendlyName' => 'Cloud-Init',     'Type' => 'textarea', 'Rows' => '4', 'Cols' => '50', 'Default' => '', 'Description' => 'Optional cloud-init user data applied at first boot'],
        6 => ['FriendlyName' => 'Add-ons JSON',   'Type' => 'textarea', 'Rows' => '2', 'Cols' => '50', 'Default' => '', 'Description' => 'Optional Contabo addOns object as JSON, e.g. {"privateNetworking":{}}'],
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
        'Start'             => 'buttonStart',
        'Stop'              => 'buttonStop',
        'Restart'           => 'buttonRestart',
        'Reset Password'    => 'buttonResetPassword',
        'Reinstall'         => 'buttonReinstall',
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
    return _securiacevps_run($params, 'buttonResetPassword', static function (\SecuriAceVps\InstanceService $svc) use ($params) {
        return $svc->resetPassword($params);
    });
}

/** @param array<string,mixed> $params */
function securiacevps_buttonReinstall(array $params): string
{
    return _securiacevps_run($params, 'buttonReinstall', static function (\SecuriAceVps\InstanceService $svc) use ($params) {
        return $svc->reinstall($params);
    });
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
        $snapshot = \SecuriAceVps\Runtime::instanceService($params)->sync($params, true);
        $fields = [
            'Instance ID' => htmlspecialchars($snapshot['instance_id']),
            'Status'      => htmlspecialchars($snapshot['status']),
            'Region'      => htmlspecialchars($snapshot['region']),
            'IPv4'        => htmlspecialchars(implode(', ', $snapshot['ipv4'])),
        ];
        if (!empty($snapshot['ipv6'])) {
            $fields['IPv6'] = htmlspecialchars(implode(', ', $snapshot['ipv6']));
        }
        $fields['Image']   = htmlspecialchars($snapshot['image']);
        $fields['Created'] = htmlspecialchars($snapshot['created']);
        $fields['Panel']   = '<a href="https://my.contabo.com" target="_blank" rel="noopener">Open Contabo Panel &#8599;</a>';
        return $fields;
    } catch (\Throwable $e) {
        // Degrade to the last-synced IP instead of a bare error row.
        $cached = _securiacevps_cached_ip($params);
        return [
            'Status' => htmlspecialchars('Live status unavailable: ' . $e->getMessage()),
            'IPv4'   => htmlspecialchars($cached !== '' ? $cached . ' (last synced)' : '—'),
            'Panel'  => '<a href="https://my.contabo.com" target="_blank" rel="noopener">Open Contabo Panel &#8599;</a>',
        ];
    }
}

// ── Client area ───────────────────────────────────────────────────────────────

/** @return array<string,string> */
function securiacevps_ClientAreaCustomButtonArray(): array
{
    return [
        'Start'               => 'clientStart',
        'Stop'                => 'clientStop',
        'Restart'             => 'clientRestart',
        'Reset Root Password' => 'clientResetPassword',
    ];
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
    return _securiacevps_run($params, 'clientResetPassword', static function (\SecuriAceVps\InstanceService $svc) use ($params) {
        return $svc->resetPassword($params);
    });
}

/**
 * @param array<string,mixed> $params
 * @return array<string,mixed>
 */
function securiacevps_ClientArea(array $params): array
{
    try {
        $snapshot = \SecuriAceVps\Runtime::instanceService($params)->sync($params, true);
        return [
            'templatefile' => 'clientarea',
            'vars' => [
                'instance_id' => $snapshot['instance_id'],
                'status'      => $snapshot['status'],
                'region'      => $snapshot['region'],
                'image'       => $snapshot['image'],
                'ipv4'        => $snapshot['ipv4'],
                'ipv6'        => $snapshot['ipv6'],
                'created'     => $snapshot['created'],
                'synced_at'   => $snapshot['synced_at'],
            ],
        ];
    } catch (\Throwable $e) {
        // Degrade to last-known data instead of an error-only page.
        $cached = _securiacevps_cached_ip($params);
        return [
            'templatefile' => 'clientarea',
            'vars' => [
                'stale'       => true,
                'stale_error' => $e->getMessage(),
                'instance_id' => '',
                'status'      => 'unavailable',
                'region'      => '',
                'image'       => '',
                'ipv4'        => $cached !== '' ? [$cached] : [],
                'ipv6'        => [],
                'created'     => '',
                'synced_at'   => '',
            ],
        ];
    }
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

function _securiacevps_safe_error(\Throwable $error): string
{
    if ($error instanceof \SecuriAceVps\ContaboProvisioningException) {
        return $error->getMessage();
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
