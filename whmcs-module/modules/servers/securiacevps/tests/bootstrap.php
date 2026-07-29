<?php
declare(strict_types=1);

// WHMCS-environment shims required before the module file is loaded.
if (!defined('WHMCS')) {
    define('WHMCS', true);
}

/**
 * Recording logActivity stub — tests assert against $GLOBALS['__activity_log'].
 */
if (!function_exists('logActivity')) {
    function logActivity(string $message): void
    {
        $GLOBALS['__activity_log'][] = $message;
    }
}

/**
 * Recording logModuleCall stub — the redaction tests assert that no secret
 * material ever reaches the debug log.
 */
if (!function_exists('logModuleCall')) {
    /**
     * @param mixed $request
     * @param mixed $response
     * @param mixed $processed
     * @param mixed $replaceVars
     */
    function logModuleCall(string $module, string $action, $request, $response, $processed = '', $replaceVars = []): void
    {
        $GLOBALS['__module_log'][] = [
            'module'   => $module,
            'action'   => $action,
            'request'  => $request,
            'response' => $response,
            'status'   => $processed,
        ];
    }
}

// Reversible stand-ins for WHMCS's global encrypt()/decrypt() helpers (same
// pattern as the addon suite): decrypt(encrypt($s)) === $s, and the value is
// visibly transformed so tests can prove it went through.
if (!function_exists('encrypt')) {
    function encrypt(string $plaintext): string
    {
        return strrev(base64_encode($plaintext));
    }
}

if (!function_exists('decrypt')) {
    function decrypt(string $cipher): string
    {
        return (string) base64_decode(strrev($cipher), true);
    }
}

if (!function_exists('localAPI')) {
    /**
     * Deterministic WHMCS LocalAPI stand-in. Individual tests can install a
     * callable in $__local_api_handler and inspect $__local_api_calls.
     *
     * @param array<string,mixed> $parameters
     * @return array<string,mixed>
     */
    function localAPI(string $command, array $parameters, string $adminUsername = ''): array
    {
        $GLOBALS['__local_api_calls'][] = [
            'command' => $command,
            'parameters' => $parameters,
            'admin_username' => $adminUsername,
        ];
        $handler = $GLOBALS['__local_api_handler'] ?? null;
        if (is_callable($handler)) {
            return (array) $handler($command, $parameters, $adminUsername);
        }
        return ['result' => 'error', 'message' => 'No test LocalAPI handler installed'];
    }
}

// In-memory Capsule stand-in — shared with the addon suite (both modules ship
// together; the relative path is stable inside the repo).
require __DIR__ . '/../../../addons/contabo_pricing/tests/FakeCapsule.php';

// Hook-registration stub so hooks.php can be loaded (and its helpers tested).
if (!function_exists('add_hook')) {
    /** @param mixed $priority */
    function add_hook(string $hookPoint, $priority, callable $fn): void
    {
        $GLOBALS['__hooks'][$hookPoint][] = $fn;
    }
}

// Module entry file (loads every lib class via require_once) + hooks.
require __DIR__ . '/../securiacevps.php';
require __DIR__ . '/../hooks.php';

// Scripted HttpExecutor + shared flow-test wiring.
require __DIR__ . '/FakeHttpExecutor.php';
require __DIR__ . '/Harness.php';
