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

        // Group by server so each server authenticates ONCE (one token) and its
        // services reuse the same client — instead of a token fetch per service,
        // which would double the call volume and risk Contabo's rate limit.
        $byServer = [];
        foreach ($services as $svc) {
            $svc = (array) $svc;
            $params = _contabo_vps_cron_params($svc);
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
                        $client = new \ContaboVps\ContaboApiClient(\ContaboVps\Runtime::auth($params));
                    }
                    \ContaboVps\Runtime::instanceServiceWithClient($client)->sync($params);
                    $synced++;
                } catch (\Throwable $e) {
                    $failed++;
                    // Unlinked services and transient API errors are expected here;
                    // the per-service detail lives in the module debug log.
                    _contabo_vps_log('CronSync', (int) ($params['serviceid'] ?? 0), $e->getMessage(), 'error');
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
