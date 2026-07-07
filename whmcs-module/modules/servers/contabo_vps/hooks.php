<?php
/**
 * Contabo VPS — module hooks.
 *
 * DailyCronJob sweep: opportunistically reconcile IP/status for every active
 * or suspended contabo_vps service (bounded batch, per-service isolation) so
 * tblhosting.dedicatedip converges even for services nobody opens in the UI.
 * View paths (admin tab / client area) sync on render; this is the safety net.
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

require_once __DIR__ . '/contabo_vps.php';

add_hook('DailyCronJob', 30, function () {
    $maxServices = 100;

    try {
        $products = \WHMCS\Database\Capsule::table('tblproducts')
            ->where('servertype', 'contabo_vps')
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

        $synced = 0;
        $failed = 0;
        foreach ($services as $svc) {
            $svc = (array) $svc;
            $serviceId = (int) ($svc['id'] ?? 0);
            if ($serviceId <= 0) {
                continue;
            }
            try {
                $params = _contabo_vps_cron_params($svc);
                if ($params === null) {
                    continue;
                }
                \ContaboVps\Runtime::instanceService($params)->sync($params);
                $synced++;
            } catch (\Throwable $e) {
                $failed++;
                // Unlinked services and transient API errors are expected here;
                // the per-service detail lives in the module debug log.
                _contabo_vps_log('CronSync', $serviceId, $e->getMessage(), 'error');
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
function _contabo_vps_cron_params(array $service): ?array
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
        'domain'           => (string) ($service['domain'] ?? ''),
        'serverusername'   => (string) ($server['username'] ?? ''),
        'serverpassword'   => $password,
        'serveraccesshash' => (string) ($server['accesshash'] ?? ''),
    ];
}
