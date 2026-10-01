#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * SPIKE-0: probe every scrape provider from the REAL deploy egress before
 * building on it. Fetches Contabo plan pages through each provider adapter,
 * runs the sapper decoder + plan extractor on what comes back, and records
 * one JSON per attempt plus a summary table. Nothing here touches the DB.
 *
 *   php scrape-spike0.php [--providers=tinyfish_fetch,treg,alterlab]
 *                         [--urls=<url>,<url>] [--record-dir=<dir>]
 *                         [--save-html] [--allow-agent] [--timeout=60]
 *
 * Keys come ONLY from the environment (never argv, never written to disk):
 *   SPIKE0_TINYFISH_KEY   SPIKE0_TREG_TOKEN   SPIKE0_ALTERLAB_KEY
 *
 * Providers: tinyfish_fetch (also runs the markdown + json format variants),
 * treg, alterlab, tinyfish_agent (billed per step; needs --allow-agent).
 * Default URLs: the first plan page of each family (3 pages).
 * Only needs lib/ (no composer vendor): a tiny autoloader is registered below.
 */

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "CLI only\n");
    exit(2);
}

spl_autoload_register(static function (string $class): void {
    $prefix = 'ContaboPricing\\';
    if (strpos($class, $prefix) !== 0) {
        return;
    }
    $path = __DIR__ . '/../lib/' . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
    if (is_file($path)) {
        require_once $path;
    }
});

use ContaboPricing\CurlRequestExecutor;
use ContaboPricing\Scrape\AlterLabSource;
use ContaboPricing\Scrape\DecoderException;
use ContaboPricing\Scrape\FetchResult;
use ContaboPricing\Scrape\PlanExtractor;
use ContaboPricing\Scrape\PlanUrlList;
use ContaboPricing\Scrape\SapperLiteralDecoder;
use ContaboPricing\Scrape\SourceConfig;
use ContaboPricing\Scrape\SourceException;
use ContaboPricing\Scrape\SourceInterface;
use ContaboPricing\Scrape\TinyFishAgentSource;
use ContaboPricing\Scrape\TinyFishFetchSource;
use ContaboPricing\Scrape\TregRoutedSource;

/** @return array<string,string> */
function spike_args(array $argv): array
{
    $out = [];
    foreach (array_slice($argv, 1) as $a) {
        if (strpos($a, '--') !== 0) {
            fwrite(STDERR, "Ignoring argument: $a\n");
            continue;
        }
        $kv = explode('=', substr($a, 2), 2);
        $out[$kv[0]] = $kv[1] ?? '1';
    }
    return $out;
}

/** Replaces every secret value with *** so nothing sensitive reaches stdout or disk. */
function spike_scrub(string $text, array $secrets): string
{
    foreach ($secrets as $s) {
        if ($s !== '') {
            $text = str_replace($s, '***', $text);
        }
    }
    return $text;
}

$args = spike_args($argv);
$timeout = max(5, (int) ($args['timeout'] ?? 60));
$saveHtml = isset($args['save-html']);
$allowAgent = isset($args['allow-agent']);
$recordDir = isset($args['record-dir']) ? rtrim((string) $args['record-dir'], '/') : '';
if ($recordDir !== '' && !is_dir($recordDir) && !mkdir($recordDir, 0700, true) && !is_dir($recordDir)) {
    fwrite(STDERR, "Cannot create record dir\n");
    exit(2);
}

$keys = [
    'tinyfish_fetch' => (string) getenv('SPIKE0_TINYFISH_KEY'),
    'tinyfish_agent' => (string) getenv('SPIKE0_TINYFISH_KEY'),
    'treg' => (string) getenv('SPIKE0_TREG_TOKEN'),
    'alterlab' => (string) getenv('SPIKE0_ALTERLAB_KEY'),
];
$secrets = array_values(array_filter($keys, static function (string $k): bool {
    return $k !== '';
}));

$urlList = new PlanUrlList();
$urls = isset($args['urls'])
    ? array_values(array_filter(array_map('trim', explode(',', (string) $args['urls']))))
    : array_values($urlList->firstUrlPerFamily());
foreach ($urls as $u) {
    if (preg_match('#^https://contabo\.com/#', $u) !== 1) {
        fwrite(STDERR, "Refusing non-Contabo URL\n");
        exit(2);
    }
}

$wanted = isset($args['providers'])
    ? array_values(array_filter(array_map('trim', explode(',', (string) $args['providers']))))
    : ['tinyfish_fetch', 'treg', 'alterlab'];

$executor = new CurlRequestExecutor();

/** @var list<array{label:string, source:SourceInterface, opts:array<string,mixed>}> $jobs */
$jobs = [];
foreach ($wanted as $p) {
    if (!isset($keys[$p])) {
        fwrite(STDERR, "Unknown provider: $p\n");
        exit(2);
    }
    if ($keys[$p] === '') {
        fwrite(STDERR, "Skipping $p: its SPIKE0_* environment variable is not set\n");
        continue;
    }
    switch ($p) {
        case 'tinyfish_fetch':
            $src = new TinyFishFetchSource(new SourceConfig('tinyfish_fetch', '', $keys[$p], []), $executor, $timeout);
            $jobs[] = ['label' => 'tinyfish_fetch:html', 'source' => $src, 'opts' => ['format' => 'html']];
            $jobs[] = ['label' => 'tinyfish_fetch:markdown', 'source' => $src, 'opts' => ['format' => 'markdown']];
            $jobs[] = ['label' => 'tinyfish_fetch:json', 'source' => $src, 'opts' => ['format' => 'json']];
            break;
        case 'treg':
            $src = new TregRoutedSource(new SourceConfig('treg', '', $keys[$p], []), $executor, $timeout);
            $jobs[] = ['label' => 'treg', 'source' => $src, 'opts' => []];
            // Second route: litescrape browser engine. Path/endpoint are the OPEN QUESTION in TregRoutedSource.
            $lite = new TregRoutedSource(
                new SourceConfig('treg', '', $keys[$p], [
                    'endpoint_id' => 'litescrape.web.fetch',
                    'query' => ['respond_with' => 'html', 'engine' => 'browser'],
                ]),
                $executor,
                $timeout
            );
            $jobs[] = ['label' => 'treg:litescrape', 'source' => $lite, 'opts' => []];
            break;
        case 'alterlab':
            $jobs[] = [
                'label' => 'alterlab',
                'source' => new AlterLabSource(new SourceConfig('alterlab', '', $keys[$p], []), $executor, $timeout),
                'opts' => [],
            ];
            break;
        case 'tinyfish_agent':
            if (!$allowAgent) {
                fwrite(STDERR, "Skipping tinyfish_agent: billed per step, pass --allow-agent to run it\n");
                break;
            }
            $jobs[] = [
                'label' => 'tinyfish_agent',
                'source' => new TinyFishAgentSource(new SourceConfig('tinyfish_agent', '', $keys[$p], []), $executor, max($timeout, 180)),
                'opts' => [],
            ];
            break;
        default:
            fwrite(STDERR, "Unknown provider: $p\n");
            exit(2);
    }
}

if ($jobs === []) {
    fwrite(STDERR, "Nothing to run (no provider with a key set).\n");
    exit(2);
}

$extractor = new PlanExtractor();
$decoder = new SapperLiteralDecoder();
$rows = [];

foreach ($jobs as $job) {
    foreach ($urls as $url) {
        $slug = PlanUrlList::slugFromUrl($url);
        $row = [
            'provider' => $job['label'],
            'url' => $url,
            'http_status' => null,
            'ok' => false,
            'sapper_present' => false,
            'decoder_ok' => false,
            'plan_count' => 0,
            'family_plan_count' => '',
            'strategy' => null,
            'latency_ms' => 0,
            'served_by' => null,
            'cost_micro' => 0,
            'body_bytes' => 0,
            'sha256' => null,
            'error' => null,
            'recorded_at' => gmdate('c'),
        ];
        $html = null;
        $t0 = microtime(true);
        try {
            /** @var FetchResult $r */
            $r = $job['source']->fetchFamilyPage($url, $job['opts']);
            $row['ok'] = true;
            $row['http_status'] = $r->httpStatus;
            $row['latency_ms'] = $r->latencyMs;
            $row['served_by'] = $r->servedBy;
            $row['cost_micro'] = $r->costMicro;
            $html = $r->html;
            $body = $html ?? (string) json_encode($r->json);
            $row['body_bytes'] = strlen($body);
            $row['sha256'] = hash('sha256', $body);

            if ($html !== null && $html !== '') {
                $row['sapper_present'] = SapperLiteralDecoder::present($html);
                if ($row['sapper_present']) {
                    try {
                        $decoder->decodeFromHtml($html);
                        $row['decoder_ok'] = true;
                    } catch (DecoderException $e) {
                        $row['error'] = 'decoder: ' . $e->getMessage();
                    }
                }
            }
            $ex = $extractor->extract($html, $r->json);
            $row['strategy'] = $ex->strategy;
            $row['plan_count'] = count($ex->plans);
            $fam = [];
            foreach ($ex->plans as $plan) {
                $fam[$plan['family']] = ($fam[$plan['family']] ?? 0) + 1;
            }
            $row['family_plan_count'] = implode('/', array_map(
                static function (string $f, int $n): string {
                    return substr($f, 0, 3) . $n;
                },
                array_keys($fam),
                array_values($fam)
            ));
        } catch (SourceException $e) {
            $row['http_status'] = $e->httpStatus() ?: null;
            $row['latency_ms'] = (int) round((microtime(true) - $t0) * 1000);
            $row['error'] = $e->getMessage();
        } catch (\Throwable $e) {
            $row['latency_ms'] = (int) round((microtime(true) - $t0) * 1000);
            $row['error'] = get_class($e) . ': ' . $e->getMessage();
        }
        if ($row['error'] !== null) {
            $row['error'] = spike_scrub((string) $row['error'], $secrets);
        }

        if ($recordDir !== '') {
            $base = $recordDir . '/' . preg_replace('/[^a-z0-9_.-]+/i', '_', $job['label'] . '__' . $slug);
            file_put_contents(
                $base . '.json',
                json_encode($row, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n"
            );
            if ($saveHtml && $html !== null && $html !== '') {
                file_put_contents($base . '.html', $html);
            }
        }
        $rows[] = $row;
    }
}

// ── Table ───────────────────────────────────────────────────────────────────
$cols = [
    'provider' => 24, 'slug' => 16, 'http' => 5, 'sapper' => 6, 'decode' => 6, 'plans' => 5,
    'families' => 12, 'ms' => 7, 'served_by' => 22, 'cost_u$' => 8, 'bytes' => 9, 'sha256' => 10, 'error' => 40,
];
$line = static function (array $cells) use ($cols): string {
    $out = '';
    $i = 0;
    foreach ($cols as $name => $w) {
        $out .= str_pad(substr((string) ($cells[$i++] ?? ''), 0, $w), $w + 1);
    }
    return rtrim($out) . "\n";
};
echo $line(array_keys($cols));
echo str_repeat('-', array_sum($cols) + count($cols)) . "\n";
foreach ($rows as $r) {
    echo $line([
        $r['provider'],
        PlanUrlList::slugFromUrl($r['url']),
        $r['http_status'] ?? '-',
        $r['sapper_present'] ? 'yes' : 'no',
        $r['decoder_ok'] ? 'ok' : 'no',
        $r['plan_count'],
        $r['family_plan_count'],
        $r['latency_ms'],
        $r['served_by'] ?? '',
        $r['cost_micro'],
        $r['body_bytes'],
        $r['sha256'] === null ? '' : substr((string) $r['sha256'], 0, 10),
        $r['error'] ?? '',
    ]);
}
$ok = count(array_filter($rows, static function (array $r): bool {
    return $r['ok'];
}));
echo "\n$ok/" . count($rows) . " fetches succeeded";
echo $recordDir !== '' ? "; records in $recordDir\n" : "\n";
exit($ok > 0 ? 0 : 1);
