<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * TinyFish Fetch API. POST https://api.fetch.tinyfish.ai {urls:[url],
 * format:'html', ttl:0}. Free tier here, so cost is reported as 0.
 *
 * Spike-0 2026-10-01: results[0] = {url, final_url, title, description,
 * language, text, author, published_date, latency_ms, format}. `text` is ~2 KB
 * of rendered content with scripts stripped, so there is no __SAPPER__ blob:
 * supportsSapper() is false and the source is only useful for the S3 visible
 * price/title cross-check. The format:'json' variant returns no content and
 * is not offered. Per-URL failures arrive in errors[].
 */
final class TinyFishFetchSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://api.fetch.tinyfish.ai';

    public function priorSuccessRate(): float
    {
        return 0.999;
    }

    public function priceMicroPerPage(): int
    {
        return 0;
    }

    public function supportsSapper(): bool
    {
        return false;
    }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $format = (string) ($opts['format'] ?? 'html');
        if (!in_array($format, ['html', 'markdown'], true)) {
            throw new SourceException('tinyfish_fetch: unsupported format ' . $format);
        }
        $r = $this->call(
            'POST',
            $this->base(self::DEFAULT_BASE),
            [
                'X-API-Key: ' . $this->config->apiKey,
                'Content-Type: application/json',
                'Accept: application/json',
            ],
            $this->encodeJson(['urls' => [$url], 'format' => $format, 'ttl' => 0])
        );
        $data = $this->decodeJson($r['body']);

        foreach ((array) ($data['errors'] ?? []) as $err) {
            if (is_array($err) && (($err['url'] ?? $url) === $url)) {
                throw new SourceException('tinyfish_fetch failed for URL: '
                    . $this->scrub(substr((string) ($err['error'] ?? $err['message'] ?? 'unknown'), 0, 200)));
            }
        }

        $results = $data['results'] ?? null;
        if (!is_array($results) || $results === []) {
            throw new SourceException('tinyfish_fetch returned no result for the URL');
        }
        $hit = null;
        foreach ($results as $item) {
            if (is_array($item) && ($item['url'] ?? null) === $url) {
                $hit = $item;
                break;
            }
        }
        if ($hit === null) {
            $hit = is_array($results[0] ?? null) ? $results[0] : null;
        }
        if ($hit === null) {
            throw new SourceException('tinyfish_fetch returned a malformed result');
        }

        $html = $this->firstString($hit, ['text', 'html', 'content']);
        if ($html === null) {
            throw new SourceException('tinyfish_fetch result had no content');
        }

        return new FetchResult(
            $this->id(),
            null,
            $html,
            null,
            0,
            $r['latency_ms'],
            $r['status'],
            null,
            $this->firstString($hit, ['final_url'])
        );
    }
}
