<?php
/**
 * Read-only LIVE-SCHEMA smoke for the contabo_pricing addon (0.5.1).
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

echo implode("\n", $lines) . "\n";
if ($fail > 0) {
    fwrite(STDERR, "FAIL: {$fail} required schema check(s) failed.\n");
    exit(1);
}
echo "OK: live WHMCS schema matches addon expectations.\n";
exit(0);
