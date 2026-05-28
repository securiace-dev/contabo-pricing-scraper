<?php
/**
 * Contabo VPS — WHMCS Server / Provisioning Module
 *
 * Automates Contabo VPS lifecycle through the official Contabo REST API.
 * OAuth2 password-grant auth (Keycloak). No external dependencies — pure
 * PHP/curl, mirroring the contabo_pricing addon's CurlRequestExecutor pattern.
 *
 * ── Server config (WHMCS → Setup → Products/Services → Servers) ───────────────
 *   Username     (serverusername)   → Contabo OAuth2 client_id
 *   Password     (serverpassword)   → Contabo OAuth2 client_secret
 *   Access Hash  (serveraccesshash) → "apiUser:apiPassword" (API user email +
 *                                      password), colon-separated. WHMCS stores
 *                                      all three encrypted at rest.
 *
 * ── Per-product config options ───────────────────────────────────────────────
 *   configoption1 → Contabo imageId (OS image)
 *   configoption2 → region slug (e.g. EU1, US-central)
 *   configoption3 → SSH secret id (from the Contabo secrets vault; optional)
 *   configoption4 → Contabo productId (e.g. V1)
 *
 * The created instance id is stored in a service custom field named
 * "contabo_instance_id" (create it on the product's Custom Fields tab).
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

require_once __DIR__ . '/lib/ContaboProvisioningException.php';
require_once __DIR__ . '/lib/ContaboAuth.php';
require_once __DIR__ . '/lib/ContaboApiClient.php';
require_once __DIR__ . '/lib/ContaboInstanceMapper.php';

/** @return array<string,mixed> */
function contabo_vps_MetaData(): array
{
    return [
        'DisplayName'    => 'Contabo VPS',
        'APIVersion'     => '1.1',
        'RequiresServer' => true,
    ];
}

/** @return array<int,array<string,mixed>> */
function contabo_vps_ConfigOptions(): array
{
    return [
        1 => ['FriendlyName' => 'OS Image ID',  'Type' => 'text', 'Size' => '30', 'Default' => '', 'Description' => 'Contabo imageId'],
        2 => ['FriendlyName' => 'Region',        'Type' => 'text', 'Size' => '20', 'Default' => 'EU1', 'Description' => 'e.g. EU1, US-central'],
        3 => ['FriendlyName' => 'SSH Secret ID', 'Type' => 'text', 'Size' => '30', 'Default' => '', 'Description' => 'Optional — secret id from Contabo vault'],
        4 => ['FriendlyName' => 'Product ID',    'Type' => 'text', 'Size' => '20', 'Default' => 'V1', 'Description' => 'Contabo productId'],
    ];
}

/**
 * @param array<string,mixed> $params
 * @return array<string,mixed>
 */
function contabo_vps_TestConnection(array $params): array
{
    try {
        $auth   = _contabo_vps_auth($params);
        $client = new \ContaboVps\ContaboApiClient($auth);
        // Lightweight authenticated read — verifies creds + API reachability.
        $client->get('/v1/compute/instances?size=1&page=1');
        return ['success' => true, 'error' => ''];
    } catch (\Throwable $e) {
        return ['success' => false, 'error' => $e->getMessage()];
    }
}

/** @param array<string,mixed> $params */
function contabo_vps_CreateAccount(array $params): string
{
    try {
        $auth   = _contabo_vps_auth($params);
        $client = new \ContaboVps\ContaboApiClient($auth);
        $mapper = new \ContaboVps\ContaboInstanceMapper();

        $body = $mapper->mapCreate($params);
        $resp = $client->post('/v1/compute/instances', $body);

        $instanceId = (string) ($resp['data'][0]['instanceId'] ?? '');
        if ($instanceId === '') {
            throw new \ContaboVps\ContaboProvisioningException('API returned no instanceId');
        }

        _contabo_vps_store_instance_id($params, $instanceId);
        _contabo_vps_log('CreateAccount', $body, $resp);

        // Provisioning is async; WHMCS marks the service Active immediately and
        // the admin can watch the build via the Services tab.
        return 'success';
    } catch (\Throwable $e) {
        _contabo_vps_log('CreateAccount', $params['domain'] ?? '', $e->getMessage(), 'error');
        return $e->getMessage();
    }
}

/** @param array<string,mixed> $params */
function contabo_vps_SuspendAccount(array $params): string
{
    // Contabo has no suspend concept — map to a power-off.
    return _contabo_vps_power_action($params, 'stop', 'SuspendAccount');
}

/** @param array<string,mixed> $params */
function contabo_vps_UnsuspendAccount(array $params): string
{
    return _contabo_vps_power_action($params, 'start', 'UnsuspendAccount');
}

/** @param array<string,mixed> $params */
function contabo_vps_TerminateAccount(array $params): string
{
    try {
        $instanceId = _contabo_vps_instance_id($params);
        $auth       = _contabo_vps_auth($params);
        $client     = new \ContaboVps\ContaboApiClient($auth);

        // +1 day so Contabo's billing closes the current day cleanly.
        $terminationDate = date('Y-m-d', strtotime('+1 day'));
        $resp = $client->post(
            '/v1/compute/instances/' . rawurlencode($instanceId) . '/cancel',
            ['terminationDate' => $terminationDate]
        );

        _contabo_vps_log('TerminateAccount', $instanceId, $resp);
        return 'success';
    } catch (\Throwable $e) {
        _contabo_vps_log('TerminateAccount', '', $e->getMessage(), 'error');
        return $e->getMessage();
    }
}

/** @param array<string,mixed> $params */
function contabo_vps_ChangePackage(array $params): string
{
    if (function_exists('logActivity')) {
        logActivity('Contabo VPS: package change requested for service #' . (int) ($params['serviceid'] ?? 0)
            . ' — Contabo does not support live resize; manual intervention required.');
    }
    return 'Package change requires manual intervention: cancel and re-provision, or upgrade the instance from the Contabo control panel.';
}

/** @return array<string,string> */
function contabo_vps_AdminCustomButtonArray(): array
{
    return [
        'Restart'        => 'buttonRestart',
        'Reset Password' => 'buttonResetPassword',
    ];
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonRestart(array $params): string
{
    return _contabo_vps_power_action($params, 'restart', 'buttonRestart');
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonResetPassword(array $params): string
{
    return _contabo_vps_power_action($params, 'resetPassword', 'buttonResetPassword');
}

/**
 * @param array<string,mixed> $params
 * @return array<string,string>
 */
function contabo_vps_AdminServicesTabFields(array $params): array
{
    try {
        $instanceId = _contabo_vps_instance_id($params);
        $auth       = _contabo_vps_auth($params);
        $client     = new \ContaboVps\ContaboApiClient($auth);
        $resp       = $client->get('/v1/compute/instances/' . rawurlencode($instanceId));
        $inst       = $resp['data'][0] ?? [];

        $ipv4 = '';
        if (isset($inst['ipConfig']['v4']) && is_array($inst['ipConfig']['v4'])) {
            $ips  = [];
            foreach ($inst['ipConfig']['v4'] as $ip) {
                $ips[] = (string) ($ip['ip'] ?? '');
            }
            $ipv4 = implode(', ', array_filter($ips));
        }

        return [
            'Instance ID' => htmlspecialchars($instanceId),
            'Status'      => htmlspecialchars((string) ($inst['status'] ?? 'unknown')),
            'Region'      => htmlspecialchars((string) ($inst['region'] ?? '')),
            'IPv4'        => htmlspecialchars($ipv4),
            'Image'       => htmlspecialchars((string) ($inst['imageId'] ?? '')),
            'Created'     => htmlspecialchars((string) ($inst['createdDate'] ?? '')),
            'Panel'       => '<a href="https://my.contabo.com" target="_blank" rel="noopener">Open Contabo Panel &#8599;</a>',
        ];
    } catch (\Throwable $e) {
        return ['Error' => htmlspecialchars($e->getMessage())];
    }
}

/**
 * @param array<string,mixed> $params
 * @return array<string,mixed>
 */
function contabo_vps_ClientArea(array $params): array
{
    try {
        $instanceId = _contabo_vps_instance_id($params);
        $auth       = _contabo_vps_auth($params);
        $client     = new \ContaboVps\ContaboApiClient($auth);
        $resp       = $client->get('/v1/compute/instances/' . rawurlencode($instanceId));
        $inst       = $resp['data'][0] ?? [];

        return [
            'templatefile' => 'clientarea',
            'vars' => [
                'instance_id' => $instanceId,
                'status'      => (string) ($inst['status'] ?? 'unknown'),
                'region'      => (string) ($inst['region'] ?? ''),
                'image'       => (string) ($inst['imageId'] ?? ''),
            ],
        ];
    } catch (\Throwable $e) {
        return [
            'templatefile' => 'clientarea',
            'vars' => ['error' => $e->getMessage()],
        ];
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/**
 * @param array<string,mixed> $params
 * @throws \ContaboVps\ContaboProvisioningException
 */
function _contabo_vps_auth(array $params): \ContaboVps\ContaboAuth
{
    $clientId     = (string) ($params['serverusername'] ?? '');
    $clientSecret = (string) ($params['serverpassword'] ?? '');
    $accessHash   = (string) ($params['serveraccesshash'] ?? '');

    if ($clientId === '' || $clientSecret === '' || $accessHash === '') {
        throw new \ContaboVps\ContaboProvisioningException('Server credentials not configured');
    }

    $parts = array_pad(explode(':', $accessHash, 2), 2, '');
    $apiUser     = (string) $parts[0];
    $apiPassword = (string) $parts[1];
    if ($apiUser === '' || $apiPassword === '') {
        throw new \ContaboVps\ContaboProvisioningException('Access Hash must be "apiUser:apiPassword"');
    }

    return new \ContaboVps\ContaboAuth($clientId, $clientSecret, $apiUser, $apiPassword);
}

/**
 * @param array<string,mixed> $params
 * @throws \ContaboVps\ContaboProvisioningException
 */
function _contabo_vps_instance_id(array $params): string
{
    $fields = $params['customfields'] ?? [];
    if (is_array($fields) && !empty($fields['contabo_instance_id'])) {
        return (string) $fields['contabo_instance_id'];
    }
    // Fallback: read straight from the custom-field values table.
    $serviceId = (int) ($params['serviceid'] ?? 0);
    if ($serviceId > 0) {
        $id = _contabo_vps_read_instance_id_from_db($serviceId);
        if ($id !== '') {
            return $id;
        }
    }
    throw new \ContaboVps\ContaboProvisioningException('Service has no linked Contabo instance — may need manual provisioning');
}

/**
 * Persist the instance id into the "contabo_instance_id" service custom field.
 * The field must exist on the product (Products → Custom Fields). If it does
 * not, we log a warning so the admin can create it — provisioning itself still
 * succeeds.
 *
 * @param array<string,mixed> $params
 */
function _contabo_vps_store_instance_id(array $params, string $instanceId): void
{
    $serviceId = (int) ($params['serviceid'] ?? 0);
    $packageId = (int) ($params['pid'] ?? ($params['packageid'] ?? 0));
    if ($serviceId <= 0) {
        return;
    }

    try {
        $fieldId = \WHMCS\Database\Capsule::table('tblcustomfields')
            ->where('type', 'product')
            ->where('relid', $packageId)
            ->where('fieldname', 'contabo_instance_id')
            ->value('id');

        if ($fieldId === null) {
            if (function_exists('logActivity')) {
                logActivity('Contabo VPS: instance ' . $instanceId . ' created for service #' . $serviceId
                    . ' but no "contabo_instance_id" custom field exists on the product — create it to enable lifecycle actions.');
            }
            return;
        }

        $existing = \WHMCS\Database\Capsule::table('tblcustomfieldsvalues')
            ->where('fieldid', (int) $fieldId)
            ->where('relid', $serviceId)
            ->first();

        if ($existing === null) {
            \WHMCS\Database\Capsule::table('tblcustomfieldsvalues')->insert([
                'fieldid' => (int) $fieldId,
                'relid'   => $serviceId,
                'value'   => $instanceId,
            ]);
        } else {
            \WHMCS\Database\Capsule::table('tblcustomfieldsvalues')
                ->where('fieldid', (int) $fieldId)
                ->where('relid', $serviceId)
                ->update(['value' => $instanceId]);
        }
    } catch (\Throwable $e) {
        if (function_exists('logActivity')) {
            logActivity('Contabo VPS: failed to store instance id for service #' . $serviceId . ' — ' . $e->getMessage());
        }
    }
}

function _contabo_vps_read_instance_id_from_db(int $serviceId): string
{
    try {
        $row = \WHMCS\Database\Capsule::table('tblcustomfieldsvalues')
            ->join('tblcustomfields', 'tblcustomfields.id', '=', 'tblcustomfieldsvalues.fieldid')
            ->where('tblcustomfields.fieldname', 'contabo_instance_id')
            ->where('tblcustomfieldsvalues.relid', $serviceId)
            ->value('tblcustomfieldsvalues.value');
        return $row === null ? '' : (string) $row;
    } catch (\Throwable $e) {
        return '';
    }
}

/**
 * @param array<string,mixed> $params
 */
function _contabo_vps_power_action(array $params, string $action, string $callName): string
{
    try {
        $instanceId = _contabo_vps_instance_id($params);
        $auth       = _contabo_vps_auth($params);
        $client     = new \ContaboVps\ContaboApiClient($auth);
        $resp = $client->post('/v1/compute/instances/' . rawurlencode($instanceId) . '/actions/' . $action, []);
        _contabo_vps_log($callName, $instanceId, $resp);
        return 'success';
    } catch (\Throwable $e) {
        _contabo_vps_log($callName, '', $e->getMessage(), 'error');
        return $e->getMessage();
    }
}

/**
 * @param mixed $request
 * @param mixed $response
 */
function _contabo_vps_log(string $action, $request, $response, string $status = 'success'): void
{
    if (function_exists('logModuleCall')) {
        // Redact the encrypted credential fields from the debug log.
        logModuleCall('contabo_vps', $action, $request, $response, $status, ['serverpassword', 'serveraccesshash']);
    }
}
