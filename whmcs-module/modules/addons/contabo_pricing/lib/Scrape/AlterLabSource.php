<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * AlterLab scraping API. POST {base}/api/v1/scrape {url, mode} with
 * mode 'js' by default (Spike-0 2026-10-01: tier 4, ~$0.004, full page incl.
 * the __SAPPER__ blob; mode 'auto' lands on tier 1 with a truncated page and
 * is not viable). Override with the `mode` option.
 *
 * Synchronous response: {job_id, url, final_url, redirected, status_code,
 * content{html,markdown,json,...}, billing{final_cost_microcents, tier_used},
 * ...}. HTML comes from content.html; cost from billing.final_cost_microcents
 * (numerically equal to micro-USD at list price). A 202 (async job) was never
 * observed but is still polled at GET {base}/api/v1/jobs/{id} every 5 s for
 * at most 85 s.
 */
final class AlterLabSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://api.alterlab.io';
    private const POLL_INTERVAL_SEC = 5;
    private const POLL_MAX_SEC = 85;
    private const PRIOR_COST_MICRO = 4000;
    private const DEFAULT_MODE = 'js';

    /** @var list<string> */
    private const HTML_PATHS = ['content.html', 'html', 'content', 'result.html', 'result.content', 'data.html', 'data.content'];

    public function priorSuccessRate(): float
    {
        return 0.95;
    }

    public function priceMicroPerPage(): int
    {
        return self::PRIOR_COST_MICRO;
    }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $base = $this->base(self::DEFAULT_BASE);
        $headers = [
            'X-API-Key: ' . $this->config->apiKey,
            'Content-Type: application/json',
            'Accept: application/json',
        ];
        $mode = isset($this->config->options['mode']) && is_string($this->config->options['mode']) && $this->config->options['mode'] !== ''
            ? $this->config->options['mode'] : self::DEFAULT_MODE;
        $payload = ['url' => $url, 'mode' => $mode];
        $schema = $this->config->options['extraction_schema'] ?? null;
        if (is_array($schema) && $schema !== []) {
            $payload['extraction_schema'] = $schema;
        }

        $first = $this->call('POST', $base . '/api/v1/scrape', $headers, $this->encodeJson($payload));
        $latency = $first['latency_ms'];
        $status = $first['status'];
        $data = $this->decodeJson($first['body']);

        if ($status === 202) {
            $jobId = $this->firstString($data, ['job_id', 'id', 'jobId']);
            if ($jobId === null) {
                throw new SourceException('alterlab accepted the job but returned no job id');
            }
            $waited = 0;
            while (true) {
                if ($waited >= self::POLL_MAX_SEC) {
                    throw new SourceException('alterlab job ' . $jobId . ' did not finish within ' . self::POLL_MAX_SEC . 's');
                }
                ($this->sleep)(self::POLL_INTERVAL_SEC);
                $waited += self::POLL_INTERVAL_SEC;
                $poll = $this->call(
                    'GET',
                    $base . '/api/v1/jobs/' . rawurlencode($jobId),
                    ['X-API-Key: ' . $this->config->apiKey, 'Accept: application/json'],
                    null
                );
                $latency += $poll['latency_ms'];
                $status = $poll['status'];
                $data = $this->decodeJson($poll['body']);
                $state = strtolower((string) ($data['status'] ?? ''));
                if (in_array($state, ['failed', 'error', 'cancelled', 'canceled'], true)) {
                    throw new SourceException('alterlab job ' . $jobId . ' ' . $state . ': '
                        . $this->scrub(substr((string) ($data['error'] ?? ''), 0, 200)));
                }
                if (in_array($state, ['completed', 'complete', 'succeeded', 'success', 'done', 'finished'], true)
                    || $this->firstString($data, self::HTML_PATHS) !== null
                ) {
                    break;
                }
            }
        }

        $html = $this->firstString($data, self::HTML_PATHS);
        $json = null;
        foreach (['extracted', 'extraction', 'result.extracted', 'data.extracted', 'json'] as $path) {
            $cur = $data;
            foreach (explode('.', $path) as $seg) {
                $cur = is_array($cur) && array_key_exists($seg, $cur) ? $cur[$seg] : null;
            }
            if (is_array($cur) && $cur !== []) {
                $json = $cur;
                break;
            }
        }
        if ($html === null && $json === null) {
            throw new SourceException('alterlab returned neither html nor extracted data');
        }

        return new FetchResult(
            $this->id(),
            null,
            $html,
            $json,
            $this->costMicro($data),
            $latency,
            $status,
            $html === null ? 'provider-json' : null,
            $this->firstString($data, ['final_url'])
        );
    }

    /** @param array<string,mixed> $data */
    private function costMicro(array $data): int
    {
        foreach (['billing.final_cost_microcents', 'cost_micro', 'cost_micros', 'usage.cost_micro'] as $path) {
            $cur = $data;
            foreach (explode('.', $path) as $seg) {
                $cur = is_array($cur) && array_key_exists($seg, $cur) ? $cur[$seg] : null;
            }
            if (is_int($cur) || (is_string($cur) && ctype_digit($cur))) {
                return (int) $cur;
            }
        }
        foreach (['cost_usd', 'cost', 'usage.cost_usd'] as $path) {
            $cur = $data;
            foreach (explode('.', $path) as $seg) {
                $cur = is_array($cur) && array_key_exists($seg, $cur) ? $cur[$seg] : null;
            }
            if (is_int($cur) || is_float($cur)) {
                return (int) round($cur * 1000000);
            }
        }
        return self::PRIOR_COST_MICRO;
    }
}
