<?php
/**
 * Real-WHMCS smoke for the canonical provisioning entrypoint and shared schema.
 *
 * This test performs no provider request. It migrates the isolated development
 * database, proves the module loads under the real WHMCS runtime, and proves
 * provider writes remain fail-closed.
 */

declare(strict_types=1);

chdir('/var/www/html');
require 'init.php';

$addonLib = getenv('CONTABO_ADDON_LIB_DIR');
$moduleEntry = getenv('SECURIACE_MODULE_ENTRY');
$shimEntry = getenv('SECURIACE_SHIM_ENTRY');

if (!$addonLib || !$moduleEntry || !$shimEntry) {
    fwrite(STDERR, "Required integration paths are missing\n");
    exit(2);
}

spl_autoload_register(static function (string $class) use ($addonLib): void {
    if (strpos($class, 'ContaboPricing\\') !== 0) {
        return;
    }
    $relative = str_replace(array('ContaboPricing\\', '\\'), array('', '/'), $class);
    $path = rtrim($addonLib, '/') . '/' . $relative . '.php';
    if (is_file($path)) {
        require_once $path;
    }
}, true, true);

$failures = 0;
$passes = 0;

/**
 * @param mixed $condition
 */
function native_smoke_assert($condition, string $message): void
{
    global $failures, $passes;
    if ($condition) {
        $passes++;
        echo "  PASS  $message\n";
        return;
    }
    $failures++;
    echo "  FAIL  $message\n";
}

echo "SecuriAce VPS — real-WHMCS native smoke\n";

$firstMigration = \ContaboPricing\SchemaHealth::assertOrMigrate();
native_smoke_assert(!empty($firstMigration['ok']), 'shared schema migration succeeds');
$secondMigration = \ContaboPricing\SchemaHealth::assertOrMigrate();
native_smoke_assert(!empty($secondMigration['ok']), 'shared schema migration is idempotent');
native_smoke_assert(
    (int) ($secondMigration['to'] ?? -1) === \ContaboPricing\Installer::SCHEMA_VERSION,
    'schema reaches the current addon version'
);

require_once $moduleEntry;

$metadata = securiacevps_MetaData();
native_smoke_assert(($metadata['DisplayName'] ?? '') === 'SecuriAce VPS', 'canonical module metadata loads');
native_smoke_assert(($metadata['RequiresServer'] ?? false) === true, 'module requires WHMCS server credentials');
native_smoke_assert(count(securiacevps_ConfigOptions()) === 6, 'legacy product fields remain migration-only compatibility fields');

foreach (array(
    'securiacevps_CreateAccount',
    'securiacevps_SuspendAccount',
    'securiacevps_UnsuspendAccount',
    'securiacevps_TerminateAccount',
    'securiacevps_ChangePackage',
    'securiacevps_ClientArea',
) as $callback) {
    native_smoke_assert(function_exists($callback), "$callback is registered");
}

\SecuriAceVps\SchemaGuard::assertReady();
native_smoke_assert(\SecuriAceVps\SchemaGuard::installationId() !== '', 'stable WHMCS installation identity exists');

$writeBlocked = false;
try {
    \SecuriAceVps\SchemaGuard::assertProviderWriteEnabled('create');
} catch (\SecuriAceVps\ContaboProvisioningException $exception) {
    $writeBlocked = strpos($exception->getMessage(), 'safety switch') !== false;
}
native_smoke_assert($writeBlocked, 'provider writes default to fail-closed');

require_once $shimEntry;
$shimMetadata = contabo_vps_MetaData();
native_smoke_assert(
    strpos((string) ($shimMetadata['DisplayName'] ?? ''), 'legacy compatibility') !== false,
    'legacy system name delegates through an explicit compatibility entrypoint'
);
native_smoke_assert(function_exists('contabo_vps_CreateAccount'), 'legacy CreateAccount callback delegates');

echo "\npasses=$passes failures=$failures\n";
exit($failures === 0 ? 0 : 1);
