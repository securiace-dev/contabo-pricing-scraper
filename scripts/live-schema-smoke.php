<?php
/**
 * Read-only LIVE-SCHEMA smoke for the WHMCS-native contabo_pricing addon.
 *
 * Confirms the live WHMCS schema matches what the addon's code expects, so a
 * real-vs-FakeCapsule divergence (like the tblhosting.recurringamount→amount bug
 * found in the production currency audit) FAILS the pre-deploy gate instead of
 * hiding behind the schemaless test double.
 *
 * Safety: SELECTs from information_schema ONLY. No INSERT/UPDATE/DELETE/DDL, no
 * migration, no sync, no module action. Gated behind
 * CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 and skips safely (exit 0) when the flag is
 * unset or no DB credentials are resolvable.
 *
 * Credential resolution (read-only): explicit LIVE_DB_* env vars first, else a
 * WHMCS configuration.php under CONTABO_WHMCS_ROOT (or the cwd).
 */
ini_set('display_errors', '0');
error_reporting(0);

if (getenv('CONTABO_PRICING_LIVE_SCHEMA_SMOKE') !== '1') {
    fwrite(STDOUT, "SKIP: set CONTABO_PRICING_LIVE_SCHEMA_SMOKE=1 to run the live-schema smoke.\n");
    exit(0);
}

$host = getenv('LIVE_DB_HOST') ?: null;
$user = getenv('LIVE_DB_USER') ?: null;
$pass = getenv('LIVE_DB_PASS');
$name = getenv('LIVE_DB_NAME') ?: null;
$port = (int) (getenv('LIVE_DB_PORT') ?: 0);

if ($name === null) {
    $root = getenv('CONTABO_WHMCS_ROOT') ?: getcwd();
    $cfg  = rtrim((string) $root, '/') . '/configuration.php';
    if (is_file($cfg)) {
        require $cfg; // defines $db_host, $db_username, $db_password, $db_name, maybe $db_port
        $host = isset($db_host) ? $db_host : $host;
        $user = isset($db_username) ? $db_username : $user;
        $pass = isset($db_password) ? $db_password : $pass;
        $name = isset($db_name) ? $db_name : $name;
        $port = (isset($db_port) && $db_port) ? (int) $db_port : $port;
    }
}
if ($port <= 0) {
    $port = 3306;
}
if (!$name) {
    fwrite(STDOUT, "SKIP: no DB credentials (set LIVE_DB_* env or CONTABO_WHMCS_ROOT to a WHMCS install).\n");
    exit(0);
}

try {
    $pdo = new PDO(
        "mysql:host={$host};port={$port};dbname={$name};charset=utf8mb4",
        $user, $pass,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC]
    );
} catch (Throwable $e) {
    fwrite(STDOUT, "SKIP: cannot connect to DB (" . $e->getMessage() . ").\n");
    exit(0);
}

$colExists = function (string $table, string $col) use ($pdo, $name): bool {
    $st = $pdo->prepare(
        "SELECT COUNT(*) AS n FROM information_schema.columns
         WHERE table_schema = ? AND table_name = ? AND column_name = ?"
    );
    $st->execute([$name, $table, $col]); // read-only SELECT
    $row = $st->fetch();
    return (int) ($row['n'] ?? 0) > 0;
};
$tableExists = function (string $table) use ($pdo, $name): bool {
    $st = $pdo->prepare(
        "SELECT COUNT(*) AS n FROM information_schema.tables
         WHERE table_schema = ? AND table_name = ?"
    );
    $st->execute([$name, $table]);
    $row = $st->fetch();
    return (int) ($row['n'] ?? 0) > 0;
};

$fail  = 0;
$lines = [];

// REQUIRED: the real recurring-charge column the addon reads.
$hasAmount = $colExists('tblhosting', 'amount');
$lines[] = ($hasAmount ? '[PASS]' : '[FAIL]') . ' tblhosting.amount exists (real recurring-charge column)';
if (!$hasAmount) { $fail++; }

// REQUIRED: firstpaymentamount (the setup/first charge column).
$hasFpa = $colExists('tblhosting', 'firstpaymentamount');
$lines[] = ($hasFpa ? '[PASS]' : '[FAIL]') . ' tblhosting.firstpaymentamount exists';
if (!$hasFpa) { $fail++; }

// INFORMATIONAL: recurringamount must NOT be required as a raw column.
$hasRecurring = $colExists('tblhosting', 'recurringamount');
$lines[] = '[INFO] tblhosting.recurringamount present=' . ($hasRecurring ? 'yes' : 'no')
    . ' — the addon must NOT depend on this raw column (it is an API/model field, not a column)';

// REQUIRED: addon drift columns from schema v6.
foreach (['mod_contabo_config_group_link', 'mod_contabo_config_option_link', 'mod_contabo_config_option_value_link'] as $t) {
    $ok = $colExists($t, 'expected_hash');
    $lines[] = ($ok ? '[PASS]' : '[FAIL]') . " {$t}.expected_hash exists (drift baseline, schema v6)";
    if (!$ok) { $fail++; }
}

// REQUIRED: WHMCS-native suite tables. This remains information_schema-only;
// row content and credentials are never read.
$suiteTables = [
    'mod_securiacevps_schema',
    'mod_securiacevps_order_snapshots',
    'mod_securiacevps_resources',
    'mod_securiacevps_operations',
    'mod_securiacevps_operation_attempts',
    'mod_securiacevps_provider_requests',
    'mod_securiacevps_service_locks',
    'mod_securiacevps_capabilities',
    'mod_securiacevps_reconciliation',
    'mod_securiacevps_adoption',
    'mod_securiacevps_billing_sagas',
    'mod_securiacevps_audit_events',
    'mod_securiacevps_operator_commands',
    'mod_securiacevps_secrets',
    'mod_securiacevps_communications',
    'mod_securiacevps_snapshot_inventory',
    'mod_contabo_catalog_versions',
    'mod_contabo_catalog_items',
    'mod_contabo_mapping_publications',
    'mod_contabo_publication_approvals',
];
foreach ($suiteTables as $table) {
    $ok = $tableExists($table);
    $lines[] = ($ok ? '[PASS]' : '[FAIL]') . " {$table} exists";
    if (!$ok) { $fail++; }
}

$suiteColumns = [
    ['mod_securiacevps_order_snapshots', 'cart_total_hash'],
    ['mod_securiacevps_operations', 'fencing_token'],
    ['mod_securiacevps_operations', 'operation_generation'],
    ['mod_securiacevps_operations', 'payload_json'],
    ['mod_securiacevps_provider_requests', 'unknown_outcome'],
    ['mod_securiacevps_adoption', 'confidence'],
    ['mod_securiacevps_billing_sagas', 'compensation_state'],
    ['mod_securiacevps_secrets', 'operation_uuid'],
    ['mod_securiacevps_secrets', 'reveal_token_ciphertext'],
    ['mod_securiacevps_communications', 'safe_error_code'],
    ['mod_securiacevps_communications', 'claim_token'],
    ['mod_securiacevps_communications', 'claim_expires_at'],
    ['mod_securiacevps_operator_commands', 'claim_token'],
    ['mod_securiacevps_operator_commands', 'claim_expires_at'],
    ['mod_securiacevps_snapshot_inventory', 'provider_account_id'],
    ['mod_securiacevps_snapshot_inventory', 'provider_resource_id'],
    ['mod_securiacevps_snapshot_inventory', 'snapshot_id'],
    ['mod_securiacevps_snapshot_inventory', 'payload_hash'],
    ['mod_contabo_catalog_versions', 'payload_hash'],
    ['mod_contabo_catalog_items', 'machine_id'],
    ['mod_contabo_mapping_publications', 'mapping_version'],
];
foreach ($suiteColumns as $definition) {
    [$table, $column] = $definition;
    $ok = $colExists($table, $column);
    $lines[] = ($ok ? '[PASS]' : '[FAIL]') . " {$table}.{$column} exists";
    if (!$ok) { $fail++; }
}

echo implode("\n", $lines) . "\n";
if ($fail > 0) {
    fwrite(STDERR, "FAIL: {$fail} required schema check(s) failed.\n");
    exit(1);
}
echo "OK: live WHMCS schema matches addon expectations.\n";
exit(0);
