#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * Round-2 scorer.
 *
 *   php scripts/round2-score.php --mode=A --provider=treg-anyapi --input=page.html
 *   php scripts/round2-score.php --mode=B --provider=alterlab-schema --input=extracted.json
 *
 * Mode A: input is a raw Contabo page; the __SAPPER__ blob is decoded with the
 *         addon's SapperLiteralDecoder and shaped with CatalogStructure.
 * Mode B: input is the provider's JSON already shaped like
 *         docs/round2-2026-10-01/scorecard.schema.json.
 *
 * Options: --ground-truth=<file> --scorecard-out=<file> --json (machine output)
 *          --notes="free text for the decision-table row"
 */

$root = dirname(__DIR__);
$lib = $root . '/whmcs-module/modules/addons/contabo_pricing/lib';
spl_autoload_register(function (string $c) use ($lib): void {
    if (strpos($c, 'ContaboPricing\\') === 0) {
        $f = $lib . '/' . str_replace('\\', '/', substr($c, strlen('ContaboPricing\\'))) . '.php';
        if (is_file($f)) {
            require_once $f;
        }
    }
});

$opt = getopt('', ['mode:', 'provider:', 'input:', 'ground-truth::', 'scorecard-out::', 'json', 'notes::']);
foreach (['mode', 'provider', 'input'] as $req) {
    if (!isset($opt[$req])) {
        fwrite(STDERR, "usage: round2-score.php --mode=A|B --provider=<name> --input=<file> [--ground-truth=f] [--scorecard-out=f] [--json] [--notes=t]\n");
        exit(2);
    }
}
$mode = strtoupper((string) $opt['mode']);
if (!in_array($mode, ['A', 'B'], true)) {
    fwrite(STDERR, "--mode must be A or B\n");
    exit(2);
}
$input = (string) $opt['input'];
if (!is_file($input)) {
    fwrite(STDERR, "input file does not exist: $input\n");
    exit(2);
}
$gtFile = isset($opt['ground-truth']) ? (string) $opt['ground-truth'] : $root . '/docs/round2-2026-10-01/ground-truth.json';
if (!is_file($gtFile)) {
    fwrite(STDERR, "ground truth file does not exist: $gtFile\n");
    exit(2);
}
$gt = json_decode((string) file_get_contents($gtFile), true);
if (!is_array($gt)) {
    fwrite(STDERR, "ground truth is not valid JSON\n");
    exit(2);
}

$raw = (string) file_get_contents($input);
$bytes = strlen($raw);
$decodeNeeded = false;
$extra = [];

if ($mode === 'A') {
    $decodeNeeded = true;
    try {
        $decoded = (new ContaboPricing\Scrape\SapperLiteralDecoder())->decodeFromHtml($raw);
    } catch (\Throwable $e) {
        fwrite(STDERR, 'decode failed: ' . get_class($e) . ': ' . $e->getMessage() . "\n");
        exit(3);
    }
    if (!isset($decoded['preloaded'][0]) || !is_array($decoded['preloaded'][0])) {
        fwrite(STDERR, "decoded blob has no preloaded[0]\n");
        exit(3);
    }
    $struct = (new ContaboPricing\Scrape\CatalogStructure())->build($decoded['preloaded'][0]);
    $extra['legacy'] = count($struct['legacy']);
    $extra['addon_groups'] = count($struct['addon_groups']);
    $card = ['families' => $struct['families']];
} else {
    $card = json_decode($raw, true);
    if (!is_array($card)) {
        fwrite(STDERR, "mode B input is not valid JSON (json_last_error_msg: " . json_last_error_msg() . ")\n");
        exit(3);
    }
    if (isset($card['data']) && is_array($card['data']) && !isset($card['families'])) {
        $card = $card['data']; // tolerate {data:{...}} provider envelopes
    }
    if (!isset($card['families']) || !is_array($card['families'])) {
        fwrite(STDERR, "mode B input has no families[]\n");
        exit(3);
    }
}
if (isset($opt['scorecard-out'])) {
    file_put_contents((string) $opt['scorecard-out'], json_encode($card, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
}

/** Resolve a path like `options.regions[name=Asia (India)].monthly_price`. */
function r2_resolve(array $plan, string $path)
{
    $cur = $plan;
    // split on dots that are not inside [...]
    $parts = [];
    $buf = '';
    $depth = 0;
    foreach (str_split($path) as $ch) {
        if ($ch === '[') {
            $depth++;
        } elseif ($ch === ']') {
            $depth--;
        }
        if ($ch === '.' && $depth === 0) {
            $parts[] = $buf;
            $buf = '';
        } else {
            $buf .= $ch;
        }
    }
    $parts[] = $buf;
    foreach ($parts as $part) {
        $sel = null;
        if (preg_match('/^([^\[]+)\[([^=]+)=(.*)\]$/', $part, $m) === 1) {
            $part = $m[1];
            $sel = [$m[2], $m[3]];
        }
        if (!is_array($cur) || !array_key_exists($part, $cur)) {
            return null;
        }
        $cur = $cur[$part];
        if ($sel !== null) {
            $found = null;
            if (is_array($cur)) {
                foreach ($cur as $row) {
                    if (is_array($row) && isset($row[$sel[0]]) && (string) $row[$sel[0]] === $sel[1]) {
                        $found = $row;
                        break;
                    }
                }
            }
            if ($found === null) {
                return null;
            }
            $cur = $found;
        }
    }
    return $cur;
}

function r2_filled($v): bool
{
    if ($v === null || $v === '') {
        return false;
    }
    if (is_array($v)) {
        return count($v) > 0;
    }
    return true;
}

function r2_norm(string $s): string
{
    return strtolower(trim(preg_replace('/[^a-z0-9]+/i', ' ', $s) ?? ''));
}

function r2_eq($a, $b, float $tol): bool
{
    if (is_numeric($a) && is_numeric($b)) {
        return abs((float) $a - (float) $b) <= $tol;
    }
    return is_string($a) && is_string($b) && strtolower(trim($a)) === strtolower(trim($b));
}

// ---- index output plans by family key and slug ----
$outFam = [];
foreach ($card['families'] as $f) {
    if (!is_array($f)) {
        continue;
    }
    $plans = isset($f['plans']) && is_array($f['plans']) ? $f['plans'] : [];
    $outFam[] = ['key' => isset($f['key']) ? (string) $f['key'] : '', 'label' => isset($f['label']) ? (string) $f['label'] : '', 'plans' => $plans];
}
$allPlans = [];
foreach ($outFam as $f) {
    foreach ($f['plans'] as $p) {
        if (is_array($p)) {
            $allPlans[] = $p;
        }
    }
}
function r2_find_plan(array $family, string $slug, array $gtPlanTitleHints, array $outFam): ?array
{
    // 1. same family by key/label, slug match
    $cands = [];
    foreach ($outFam as $f) {
        if ($f['key'] === $family['key'] || r2_norm($f['label']) === r2_norm($family['label'])) {
            $cands[] = $f;
        }
    }
    if ($cands === []) {
        $cands = $outFam;
    }
    foreach ($cands as $f) {
        foreach ($f['plans'] as $p) {
            if (is_array($p) && isset($p['slug']) && r2_norm((string) $p['slug']) === r2_norm($slug)) {
                return $p;
            }
        }
    }
    return null;
}

$tol = isset($gt['tolerance']) ? (float) $gt['tolerance'] : 0.005;
$vat = isset($gt['vat_rate']) ? (float) $gt['vat_rate'] : 0.18;

$perFamily = [];
$totReq = 0;
$totFilled = 0;
$missingByFamily = [];
foreach ($gt['families'] as $key => $fam) {
    $req = 0;
    $filled = 0;
    $missing = [];
    $found = 0;
    foreach ($fam['plans'] as $slug) {
        $plan = r2_find_plan(['key' => $key, 'label' => $fam['label']], $slug, [], $outFam);
        $isConfigurator = isset($gt['configurator_plans'][$slug]);
        // field list for this plan
        $fields = ['title', 'slug', 'availability', 'prices.EUR', 'prices.USD', 'prices.GBP'];
        foreach ($fam['required_specs'] as $s) {
            $fields[] = 'specs.' . $s;
        }
        foreach ($fam['periods_months'] as $m) {
            foreach (['discount', 'effective_monthly', 'total'] as $pf) {
                $fields[] = 'periods[months=' . $m . '].' . $pf;
            }
        }
        if ($isConfigurator) {
            foreach ($gt['configurator_plans'][$slug]['required_options'] as $o) {
                $fields[] = 'options.' . $o;
            }
        }
        if ($plan !== null) {
            $found++;
        }
        foreach ($fields as $path) {
            $req++;
            $v = $plan === null ? null : r2_resolve($plan, $path);
            if (r2_filled($v)) {
                $filled++;
            } else {
                $missing[] = $slug . ':' . $path;
            }
        }
    }
    $perFamily[$key] = [
        'label' => $fam['label'],
        'plans_expected' => count($fam['plans']),
        'plans_found' => $found,
        'required' => $req,
        'filled' => $filled,
        'pct' => $req > 0 ? round(100 * $filled / $req, 1) : 0.0,
    ];
    $missingByFamily[$key] = $missing;
    $totReq += $req;
    $totFilled += $filled;
}
$overallPct = $totReq > 0 ? round(100 * $totFilled / $totReq, 1) : 0.0;

// ---- correctness vs ground truth ----
$checksTotal = 0;
$checksOk = 0;
$mismatches = [];
foreach ($gt['configurator_plans'] as $slug => $spec) {
    $plan = null;
    foreach ($allPlans as $p) {
        if (isset($p['slug']) && r2_norm((string) $p['slug']) === r2_norm($slug)) {
            $plan = $p;
            break;
        }
    }
    foreach ($spec['checks'] as $path => $expected) {
        $checksTotal++;
        $got = $plan === null ? null : r2_resolve($plan, $path);
        if (r2_eq($got, $expected, $tol)) {
            $checksOk++;
        } else {
            $mismatches[] = "$slug $path expected " . json_encode($expected) . ' got ' . json_encode($got);
        }
    }
    foreach (isset($spec['gross_checks']) ? $spec['gross_checks'] : [] as $gc) {
        $checksTotal++;
        $net = $plan === null ? null : r2_resolve($plan, $gc['path']);
        $gross = is_numeric($net) ? round((float) $net * (1 + $vat), 2) : null;
        if ($gross !== null && abs($gross - (float) $gc['expected_gross']) <= 0.0051) {
            $checksOk++;
        } else {
            $mismatches[] = "$slug gross({$gc['path']}) expected {$gc['expected_gross']} got " . json_encode($gross);
        }
    }
    foreach (isset($spec['percent_checks']) ? $spec['percent_checks'] : [] as $pc) {
        $checksTotal++;
        $price = $plan === null ? null : r2_resolve($plan, 'prices.EUR');
        $disc = $plan === null ? null : r2_resolve($plan, 'periods[months=' . $pc['months'] . '].discount');
        $pct = (is_numeric($price) && is_numeric($disc) && (float) $price > 0) ? round(100 * (float) $disc / ((float) $price * $pc['months']), 2) : null;
        if ($pct !== null && abs($pct - (float) $pc['percent']) <= 0.05) {
            $checksOk++;
        } else {
            $mismatches[] = "$slug discount% at {$pc['months']}m expected {$pc['percent']} got " . json_encode($pct);
        }
    }
}
$correctPct = $checksTotal > 0 ? round(100 * $checksOk / $checksTotal, 1) : 0.0;

$result = [
    'provider' => (string) $opt['provider'],
    'mode' => $mode,
    'input' => basename($input),
    'bytes' => $bytes,
    'decode_needed' => $decodeNeeded,
    'completeness_pct' => $overallPct,
    'required_total' => $totReq,
    'filled_total' => $totFilled,
    'families' => $perFamily,
    'correctness_pct' => $correctPct,
    'checks_ok' => $checksOk,
    'checks_total' => $checksTotal,
    'mismatches' => $mismatches,
    'missing' => $missingByFamily,
    'extra' => $extra,
];
$notes = isset($opt['notes']) ? (string) $opt['notes'] : '';
$row = sprintf(
    '| %s | %s | %s %% | %s %% (%d/%d) | %s | %s | %s |',
    $result['provider'],
    $mode,
    number_format($overallPct, 1),
    number_format($correctPct, 1),
    $checksOk,
    $checksTotal,
    number_format($bytes),
    $decodeNeeded ? 'yes (SapperLiteralDecoder)' : 'no',
    $notes === '' ? '-' : str_replace('|', '/', $notes)
);
$result['markdown_row'] = $row;

if (isset($opt['json'])) {
    echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES), "\n";
    exit(0);
}

echo "provider={$result['provider']} mode=$mode input={$result['input']} bytes=" . number_format($bytes) . ' decode_needed=' . ($decodeNeeded ? 'yes' : 'no') . "\n";
echo "completeness: {$overallPct} % ({$totFilled}/{$totReq} required values)\n";
foreach ($perFamily as $k => $f) {
    printf("  %-18s %-22s plans %d/%d  %5.1f %% (%d/%d)\n", $k, $f['label'], $f['plans_found'], $f['plans_expected'], $f['pct'], $f['filled'], $f['required']);
}
echo "correctness: {$correctPct} % ({$checksOk}/{$checksTotal} ground-truth checks)\n";
foreach ($mismatches as $m) {
    echo "  MISMATCH $m\n";
}
foreach ($missingByFamily as $k => $m) {
    if ($m !== []) {
        echo "  missing in $k (" . count($m) . '): ' . implode(', ', array_slice($m, 0, 12)) . (count($m) > 12 ? ' ...' : '') . "\n";
    }
}
if ($extra !== []) {
    echo 'extra: ' . json_encode($extra) . "\n";
}
echo "\n| Provider | Mode | Completeness | Correctness | Bytes | Decode | Notes |\n|---|---|---|---|---|---|---|\n$row\n";
