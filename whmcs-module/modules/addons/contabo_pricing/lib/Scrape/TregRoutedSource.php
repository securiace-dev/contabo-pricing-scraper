<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * treg.to web fetch. POST https://treg.to/call/<endpoint_id> with header
 * X-Treg-Token (Spike-0 2026-10-01).
 *
 * Default endpoint `anyapi.web.scrape`, body
 * {url, formats:["html"], onlyMainContent:false, waitFor:3000}; the page is in
 * `output.data.html` (2.1 MB incl. <script>, the __SAPPER__ blob present) at
 * ~700 micro-USD (X-Treg-Cost-Micro). Routed ids (treg.web.extract,
 * tinyfish.web.fetch) strip scripts and are NOT usable for S1. A second row
 * (`treg_litescrape`) targets litescrape.web.fetch.post via the options
 * endpoint_id / query {timeout:90} / body_params {respond_with:"html",
 * engine:"browser", page_timeout:60}; it timed out (503) once and is seeded
 * disabled.
 *
 * Content fallbacks for other ids: output.pages.0.html, output, output.html,
 * output.content, output.body, raw. X-Treg-Served-By (routed ids only) or
 * _treg.served_by names the upstream.
 *
 * Options: endpoint_id, path, query, body_params, route_prefer, route_exclude,
 * max_cost, prior_cost_micro.
 */
final class TregRoutedSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://treg.to';
    public const DEFAULT_ENDPOINT = 'anyapi.web.scrape';
    private const PRIOR_COST_MICRO = 700;

    public function priorSuccessRate(): float
    {
        return 0.80;
    }

    public function priceMicroPerPage(): int
    {
        $c = $this->config->options['prior_cost_micro'] ?? null;
        return is_int($c) || (is_string($c) && ctype_digit($c)) ? (int) $c : self::PRIOR_COST_MICRO;
    }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $o = $this->config->options;
        $endpoint = (string) ($o['endpoint_id'] ?? self::DEFAULT_ENDPOINT);
        $path = (string) ($o['path'] ?? ('/call/' . $endpoint));
        if ($path === '' || $path[0] !== '/') {
            $path = '/' . $path;
        }
        $query = isset($o['query']) && is_array($o['query']) && $o['query'] !== []
            ? '?' . http_build_query($o['query'])
            : '';

        $headers = [
            'X-Treg-Token: ' . $this->config->apiKey,
            'Content-Type: application/json',
            'Accept: application/json',
        ];
        $map = [
            'route_prefer' => 'X-Treg-Route-Prefer',
            'route_exclude' => 'X-Treg-Route-Exclude',
            'max_cost' => 'X-Treg-Max-Cost',
        ];
        foreach ($map as $opt => $header) {
            if (isset($o[$opt]) && $o[$opt] !== '' && $o[$opt] !== null) {
                $v = is_array($o[$opt]) ? implode(',', array_map('strval', $o[$opt])) : (string) $o[$opt];
                $headers[] = $header . ': ' . preg_replace('/[\r\n]+/', ' ', $v);
            }
        }

        $payload = ['url' => $url];
        if (isset($o['body_params']) && is_array($o['body_params'])) {
            $payload = array_merge($o['body_params'], $payload);
        } elseif ($endpoint === self::DEFAULT_ENDPOINT) {
            $payload = ['url' => $url, 'formats' => ['html'], 'onlyMainContent' => false, 'waitFor' => 3000];
        }

        $r = $this->call('POST', $this->base(self::DEFAULT_BASE) . $path . $query, $headers, $this->encodeJson($payload));
        $data = $this->decodeJson($r['body']);

        $html = $this->firstString($data, [
            'output.data.html', 'output.pages.0.html', 'output', 'output.html', 'output.content', 'output.body', 'raw',
        ]);
        if ($html === null) {
            throw new SourceException('treg returned no page content');
        }

        $served = $r['headers']['x-treg-served-by'] ?? null;
        if ($served === null || $served === '') {
            $served = $this->firstString($data, ['_treg.served_by']);
        }
        $cost = $this->priceMicroPerPage();
        $hdr = $r['headers']['x-treg-cost-micro'] ?? null;
        if ($hdr !== null && ctype_digit(trim($hdr))) {
            $cost = (int) trim($hdr);
        }

        return new FetchResult($this->id(), $served, $html, null, $cost, $r['latency_ms'], $r['status']);
    }
}
