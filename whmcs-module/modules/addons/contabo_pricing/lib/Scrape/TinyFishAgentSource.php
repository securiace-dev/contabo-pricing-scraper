<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * TinyFish browser agent. POST https://agent.tinyfish.ai/v1/automation/run
 * with an output_schema from options and max_steps (default 40). Priced per
 * step, so it is manual-only and never runs in unattended scheduled runs.
 * Returns structured JSON only (S2 provider-json); there is no page HTML.
 */
final class TinyFishAgentSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://agent.tinyfish.ai';
    private const DEFAULT_MAX_STEPS = 40;
    private const MICRO_PER_STEP = 16000;

    public function priorSuccessRate(): float
    {
        return 0.90;
    }

    public function priceMicroPerPage(): int
    {
        return self::DEFAULT_MAX_STEPS * self::MICRO_PER_STEP;
    }

    public function manualOnly(): bool
    {
        return true;
    }

    public function testConnection(): array
    {
        // A probe would burn agent steps; credential check is left to a manual run.
        return [
            'ok' => $this->config->apiKey !== '',
            'latency_ms' => 0,
            'message' => $this->config->apiKey !== ''
                ? 'Key present; agent runs are billed per step, so no live probe is made'
                : 'No API key configured',
            'cost_micro' => 0,
        ];
    }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $o = $this->config->options;
        $maxSteps = (int) ($o['max_steps'] ?? self::DEFAULT_MAX_STEPS);
        if ($maxSteps < 1) {
            $maxSteps = self::DEFAULT_MAX_STEPS;
        }
        $payload = [
            'url' => $url,
            'goal' => (string) ($o['goal'] ?? 'Extract every plan on this page: slug, title, type, monthly EUR price, contract periods (length, discount EUR, setup EUR) and specs.'),
            'max_steps' => $maxSteps,
        ];
        if (isset($o['output_schema']) && is_array($o['output_schema'])) {
            $payload['output_schema'] = $o['output_schema'];
        }

        $r = $this->call(
            'POST',
            $this->base(self::DEFAULT_BASE) . '/v1/automation/run',
            [
                'X-API-Key: ' . $this->config->apiKey,
                'Content-Type: application/json',
                'Accept: application/json',
            ],
            $this->encodeJson($payload)
        );
        $data = $this->decodeJson($r['body']);

        $state = strtolower((string) ($data['status'] ?? 'completed'));
        if (in_array($state, ['failed', 'error'], true)) {
            throw new SourceException('tinyfish_agent run ' . $state . ': '
                . $this->scrub(substr((string) ($data['error'] ?? ''), 0, 200)));
        }
        $result = $data['result'] ?? ($data['output'] ?? null);
        if (!is_array($result) || $result === []) {
            throw new SourceException('tinyfish_agent returned no structured result');
        }
        $steps = (int) ($data['steps'] ?? $data['num_steps'] ?? $data['step_count'] ?? $maxSteps);

        return new FetchResult(
            $this->id(),
            null,
            null,
            $result,
            $steps * self::MICRO_PER_STEP,
            $r['latency_ms'],
            $r['status'],
            'provider-json'
        );
    }
}
