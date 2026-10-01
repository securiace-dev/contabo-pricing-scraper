<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * AlterLab scraping API. POST {base}/api/v1/scrape {url, mode:'auto'}; a 202
 * means an async job that is polled at GET {base}/api/v1/jobs/{id} every 5 s
 * for at most 85 s.
 *
 * Response field names beyond the documented request shape are read
 * tolerantly (see HTML_PATHS / COST_*); confirm against a recorded spike.
 */
final class AlterLabSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://api.alterlab.io';
    private const POLL_INTERVAL_SEC = 5;
    private const POLL_MAX_SEC = 85;
    private const PRIOR_COST_MICRO = 200;

    /** @var list<string> */
    private const HTML_PATHS = ['html', 'content', 'result.html', 'result.content', 'data.html', 'data.content'];

    public function priorSuccessRate(): float
    {
        return 0.90;
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
        $payload = ['url' => $url, 'mode' => 'auto'];
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
            $html === null ? 'provider-json' : null
        );
    }

    /** @param array<string,mixed> $data */
    private function costMicro(array $data): int
    {
        foreach (['cost_micro', 'cost_micros', 'usage.cost_micro'] as $path) {
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
