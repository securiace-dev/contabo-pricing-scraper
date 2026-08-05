<?php
declare(strict_types=1);

// WHMCS-environment shims required before the addon file is loaded.
if (!defined('WHMCS')) {
    define('WHMCS', true);
}

if (!function_exists('logActivity')) {
    function logActivity(string $message): void
    {
        $GLOBALS['contabo_test_activity_log'][] = $message;
    }
}

if (!function_exists('sendAdminNotification')) {
    function sendAdminNotification(...$_args): void
    {
        // no-op in tests
    }
}

// Pass-through stand-ins for WHMCS's global encrypt() / decrypt() helpers.
// The real helpers wrap a configuration-key-keyed AES routine; for tests we
// only need decrypt(encrypt($s)) === $s. Reversing the base64 keeps the
// stub from looking like a no-op so tests can prove the value really went
// through both directions.
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

// WHMCS-CSRF helpers stand-ins. The real implementations belong to WHMCS's
// admin entry point and validate against $_SESSION. In tests we don't drive a
// real admin session, so generate_token() emits a stable hidden input and
// check_token() always passes. AdminController code paths that require these
// are exercised end-to-end in MappingFormTest below.
if (!function_exists('generate_token')) {
    function generate_token(string $_mode = 'plain'): string
    {
        return '<input type="hidden" name="token" value="testtoken">';
    }
}

if (!function_exists('check_token')) {
    function check_token(string $_mode = 'POST'): bool
    {
        return true;
    }
}

// Minimal stand-in for WHMCS\Database\Capsule. Records every where()+update()
// pair into a static $calls array that tests can assert against.
require __DIR__ . '/FakeCapsule.php';

// Use the addon's stub PSR-4 autoloader so tests don't depend on composer's
// autoload picking up the lib/ classes (it does, but loading the addon entry
// file also installs the same stub for symmetry with production).
require __DIR__ . '/../contabo_pricing.php';
