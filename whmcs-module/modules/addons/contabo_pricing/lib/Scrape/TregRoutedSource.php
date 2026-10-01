<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * treg.to routed web-fetch. Auth header X-Treg-Token. Routing hints from
 * options map to X-Treg-Route-Prefer / -Exclude / X-Treg-Max-Cost.
 *
 * OPEN QUESTION: the exact REST path for catalog endpoint ids is not
 * confirmed. The request path is the `path` option, defaulting to
 * "/call/<endpoint_id>"; endpoint_id defaults to "web.fetch". A second config
 * row can target litescrape.web.fetch by setting endpoint_id and
 * body_params {"respond_with":"html","engine":"browser"} (or `query`).
 *
 * Response: JSON with the page in `output` (string, or object holding
 * html/content) or `raw`; `_treg.served_by` names the upstream; cost comes
 * from the X-Treg-Cost-Micro response header.
 */
final class TregRoutedSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://treg.to';
    private const DEFAULT_ENDPOINT = 'web.fetch';
    private const PRIOR_COST_MICRO = 1000;

    public function priorSuccessRate(): float
    {
        return 0.80;
    }

    public function priceMicroPerPage(): int
    {
        return self::PRIOR_COST_MICRO;
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
        }

        $r = $this->call('POST', $this->base(self::DEFAULT_BASE) . $path . $query, $headers, $this->encodeJson($payload));
        $data = $this->decodeJson($r['body']);

        $html = $this->firstString($data, ['output', 'output.html', 'output.content', 'output.body', 'raw']);
        if ($html === null) {
            throw new SourceException('treg returned no page content');
        }

        $served = $this->firstString($data, ['_treg.served_by']);
        $cost = self::PRIOR_COST_MICRO;
        $hdr = $r['headers']['x-treg-cost-micro'] ?? null;
        if ($hdr !== null && ctype_digit(trim($hdr))) {
            $cost = (int) trim($hdr);
        }

        return new FetchResult($this->id(), $served, $html, null, $cost, $r['latency_ms'], $r['status']);
    }
}
