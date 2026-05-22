<?php
/**
 * Standalone AJAX endpoint for the Contabo Pricing addon.
 *
 * The addon's regular `_output()` hook is wrapped in the WHMCS admin chrome
 * (header HTML + sidebar + footer), so anything we `echo` there is sandwiched
 * inside an HTML page. `JSON.parse()` on the client end chokes with
 * "Invalid JSON from server (HTTP 200)".
 *
 * This file bootstraps WHMCS just enough to (a) authenticate the admin
 * session, (b) reach the DB + autoloader, then delegates to the existing
 * `AdminController::dispatch()` for `ajax-*` actions. Output is pure JSON.
 *
 * URL pattern (host-absolute):
 *   /modules/addons/contabo_pricing/ajax.php?action=quote        (POST)
 *   /modules/addons/contabo_pricing/ajax.php?action=fx           (GET)
 *   /modules/addons/contabo_pricing/ajax.php?action=meta-probe   (POST)
 *   /modules/addons/contabo_pricing/ajax.php?action=profile-versions&id=42  (GET)
 *   /modules/addons/contabo_pricing/ajax.php?action=profile&id=42           (GET)
 *   /modules/addons/contabo_pricing/ajax.php?action=configurator&plan_slug=cloud-vps-10  (GET)
 *   /modules/addons/contabo_pricing/ajax.php?action=policy-preview&service_id=42        (GET)
 *
 * @license MIT
 */

declare(strict_types=1);

// 1. Bootstrap WHMCS — init.php sets up DB, autoload, sessions, constants.
//    The addon root is 3 levels above this file:
//      modules/addons/contabo_pricing/ajax.php
//      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//      drop 4 path elements (the file itself + 3 dirs) → WHMCS root
$cb_whmcs_root = dirname(__DIR__, 3);
$cb_init = $cb_whmcs_root . '/init.php';
if (!is_file($cb_init)) {
    http_response_code(500);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'WHMCS init.php not found — wrong install path?']);
    exit;
}
require_once $cb_init;

// 2. Authenticate the admin session.
//    WHMCS sets $_SESSION['adminid'] on admin login and validates it via the
//    standard admin entry points. We mirror that check rather than calling
//    checkAdminLogin() (which is meant for full-chrome admin pages and
//    redirects on failure).
if (empty($_SESSION['adminid'])) {
    http_response_code(403);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'not authenticated — admin login required']);
    exit;
}

// 3. Autoload the addon's namespaced classes. Mirror the stub autoloader
//    pattern from contabo_pricing.php so a missing vendor/ doesn't break us.
$cb_autoload = __DIR__ . '/vendor/autoload.php';
if (is_file($cb_autoload)) {
    require_once $cb_autoload;
} else {
    spl_autoload_register(static function (string $class): void {
        if (strpos($class, 'ContaboPricing\\') === 0) {
            $rel = str_replace(['ContaboPricing\\', '\\'], ['', '/'], $class);
            $path = __DIR__ . '/lib/' . $rel . '.php';
            if (is_file($path)) {
                require_once $path;
            }
        }
    });
}

// 4. Hydrate Settings from tbladdonmodules. WHMCS doesn't pass $vars to
//    standalone files, so we read the rows ourselves.
$cb_rows = \WHMCS\Database\Capsule::table('tbladdonmodules')
    ->where('module', 'contabo_pricing')
    ->get(['setting', 'value']);
$cb_vars = ['modulelink' => 'addonmodules.php?module=contabo_pricing'];
foreach ($cb_rows as $cb_r) {
    $cb_vars[(string) $cb_r->setting] = (string) $cb_r->value;
}

// 5. Dispatch. The dispatcher already knows the ajax-* actions; we just need
//    to normalise the action name (clients may send either form).
try {
    $cb_settings = \ContaboPricing\Settings::fromVars($cb_vars);
    $cb_controller = new \ContaboPricing\AdminController(
        $cb_settings,
        __DIR__ . '/templates/admin'
    );

    $cb_action = (string) ($_REQUEST['action'] ?? '');
    if ($cb_action !== '' && strpos($cb_action, 'ajax-') !== 0) {
        $_REQUEST['action'] = 'ajax-' . $cb_action;
    }
    if (($_REQUEST['action'] ?? '') === '') {
        http_response_code(400);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode(['error' => 'missing action parameter']);
        exit;
    }

    $cb_controller->dispatch($_REQUEST);
} catch (\Throwable $e) {
    if (!headers_sent()) {
        http_response_code(500);
        header('Content-Type: application/json; charset=utf-8');
    }
    if (function_exists('logActivity')) {
        logActivity('Contabo Pricing ajax error: ' . $e->getMessage());
    }
    echo json_encode(['error' => $e->getMessage()]);
}
