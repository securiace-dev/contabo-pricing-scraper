<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/** Builds the provider adapter for a mod_contabo_scrape_sources row. */
final class SourceFactory
{
    /** @var HeaderAwareExecutor */
    private $executor;
    /** @var SourceConfigRepository */
    private $repo;

    public function __construct(HeaderAwareExecutor $executor, ?SourceConfigRepository $repo = null)
    {
        $this->executor = $executor;
        $this->repo = $repo ?? new SourceConfigRepository();
    }

    /** @param array<string,mixed> $row */
    public function build(array $row): SourceInterface
    {
        $id = (string) ($row['source_id'] ?? '');
        $cfg = $this->repo->toConfig($row);
        if (strpos($id, 'treg') === 0) {
            return new TregRoutedSource($cfg, $this->executor);
        }
        switch ($id) {
            case 'alterlab':
                return new AlterLabSource($cfg, $this->executor);
            case 'tinyfish_fetch':
                return new TinyFishFetchSource($cfg, $this->executor);
            case 'tinyfish_agent':
                return new TinyFishAgentSource($cfg, $this->executor);
        }
        throw new \InvalidArgumentException('No adapter for scrape source: ' . $id);
    }
}
