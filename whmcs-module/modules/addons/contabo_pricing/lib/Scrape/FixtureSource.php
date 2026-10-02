<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Serves recorded HTML from disk (tests, staging, offline spikes). $path is
 * either a single file (served for every URL) or a directory holding
 * "<slug>.html" (or "<slug_with_underscores>.html") per plan page.
 */
final class FixtureSource implements SourceInterface
{
    /** @var string */
    private $path;

    public function __construct(string $path)
    {
        $this->path = $path;
    }

    public function id(): string
    {
        return 'fixture';
    }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $t0 = microtime(true);
        $file = $this->path;
        if (is_dir($this->path)) {
            $slug = PlanUrlList::slugFromUrl($url);
            $file = rtrim($this->path, '/') . '/' . $slug . '.html';
            if (!is_file($file)) {
                // recorded fixtures may use underscores (cloud_vps_10.html)
                $file = rtrim($this->path, '/') . '/' . str_replace('-', '_', $slug) . '.html';
            }
        }
        if (!is_file($file)) {
            throw new SourceException('fixture not found for URL');
        }
        $html = file_get_contents($file);
        if (!is_string($html) || $html === '') {
            throw new SourceException('fixture is empty or unreadable');
        }
        return new FetchResult(
            'fixture',
            'fixture',
            $html,
            null,
            0,
            (int) round((microtime(true) - $t0) * 1000),
            200
        );
    }

    public function testConnection(): array
    {
        $ok = is_file($this->path) || is_dir($this->path);
        return ['ok' => $ok, 'latency_ms' => 0, 'message' => $ok ? 'fixture path exists' : 'fixture path missing', 'cost_micro' => 0];
    }

    public function priorSuccessRate(): float
    {
        return 1.0;
    }

    public function priceMicroPerPage(): int
    {
        return 0;
    }

    public function manualOnly(): bool
    {
        return false;
    }

    public function supportsSapper(): bool
    {
        return true;
    }
}
