<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * TinyFish browser agent (manual-only, billed per step).
 *
 * Non-idempotent paid call, so:
 *  - the run is launched EXACTLY ONCE with POST {base}/v1/automation/run-async
 *    -> {run_id}. A transport error, timeout or 5xx on that POST is never
 *    retried (a retry launched duplicate paid runs: 4 billed instead of 2);
 *  - the run is then followed with GET {base}/v1/runs/{run_id} every
 *    poll_interval_sec (default 10) until COMPLETED (result in `result`),
 *    FAILED / CANCELLED (SourceException), or the poll budget
 *    (poll_budget_sec, default 600) or max_steps is exceeded, in which case the
 *    run is cancelled with POST /v1/runs/{run_id}/cancel (best effort, once).
 *    Polling is idempotent and does not launch anything; up to 3 consecutive
 *    poll failures are tolerated before the run is cancelled and abandoned;
 *  - cost = num_of_steps x $0.016 (an int on terminal runs, a list of step
 *    objects while RUNNING: counted then). The run id is attached to every
 *    failure (SourceException::providerRunId) and recorded on the attempt row
 *    so a timed-out run can be recovered instead of relaunched;
 *  - output_schema is sanitised (sanitizeSchema) before sending.
 *
 * Returns structured JSON only (S2 provider-json); there is no page HTML.
 */
final class TinyFishAgentSource extends AbstractSource
{
    private const DEFAULT_BASE = 'https://agent.tinyfish.ai';
    private const DEFAULT_MAX_STEPS = 40;
    private const MICRO_PER_STEP = 16000;
    private const DEFAULT_POLL_INTERVAL_SEC = 10;
    private const DEFAULT_POLL_BUDGET_SEC = 600;
    private const MAX_CONSECUTIVE_POLL_FAILURES = 3;

    /** Keys TinyFish rejects in output_schema (removed at every level, except as property names). */
    public const REJECTED_SCHEMA_KEYS = ['$schema', '$id', 'title', 'description', 'examples', 'default', '$comment'];

    /** Maps whose KEYS are names (property names etc.), not keywords. */
    private const NAME_MAPS = ['properties', 'patternProperties', 'definitions', '$defs', 'dependentSchemas'];
    private const SCHEMA_KEYS = ['items', 'additionalProperties', 'not', 'contains', 'propertyNames', 'if', 'then', 'else'];
    private const SCHEMA_LISTS = ['anyOf', 'oneOf', 'allOf', 'prefixItems'];

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

    /**
     * Removes the keys TinyFish rejects ($schema, $id, title, description,
     * examples, default, $comment) at every schema level while keeping property
     * NAMES (a property may legitimately be called "title"), and drops any
     * `required` name that is not declared in `properties`.
     *
     * @param array<mixed> $schema
     * @return array<mixed>
     */
    public static function sanitizeSchema(array $schema): array
    {
        $out = [];
        foreach ($schema as $k => $v) {
            if (is_string($k) && in_array($k, self::REJECTED_SCHEMA_KEYS, true)) {
                continue;
            }
            if (is_string($k) && in_array($k, self::NAME_MAPS, true) && is_array($v)) {
                $m = [];
                foreach ($v as $name => $sub) {
                    $m[$name] = is_array($sub) ? self::sanitizeSchema($sub) : $sub;
                }
                $out[$k] = $m === [] ? new \stdClass() : $m;
            } elseif (is_string($k) && in_array($k, self::SCHEMA_KEYS, true) && is_array($v)) {
                $out[$k] = self::isList($v) ? array_map([self::class, 'sanitizeNode'], $v) : self::sanitizeSchema($v);
            } elseif (is_string($k) && in_array($k, self::SCHEMA_LISTS, true) && is_array($v)) {
                $out[$k] = array_map([self::class, 'sanitizeNode'], $v);
            } else {
                $out[$k] = $v; // type, enum, const, format, bounds ...: literals are never rewritten
            }
        }
        if (isset($out['required']) && is_array($out['required'])) {
            $props = isset($out['properties']) && is_array($out['properties']) ? $out['properties'] : [];
            $req = [];
            foreach ($out['required'] as $name) {
                if (is_string($name) && array_key_exists($name, $props)) {
                    $req[] = $name;
                }
            }
            if ($req === []) {
                unset($out['required']);
            } else {
                $out['required'] = $req;
            }
        }
        return $out;
    }

    /** @param mixed $n @return mixed */
    private static function sanitizeNode($n)
    {
        return is_array($n) ? self::sanitizeSchema($n) : $n;
    }

    /** @param array<mixed> $a */
    private static function isList(array $a): bool
    {
        $i = 0;
        foreach ($a as $k => $_) {
            if ($k !== $i++) {
                return false;
            }
        }
        return true;
    }

    public function fetchFamilyPage(string $url, array $opts = []): FetchResult
    {
        $o = $this->config->options;
        $maxSteps = (int) ($o['max_steps'] ?? self::DEFAULT_MAX_STEPS);
        if ($maxSteps < 1) {
            $maxSteps = self::DEFAULT_MAX_STEPS;
        }
        $interval = max(1, (int) ($o['poll_interval_sec'] ?? self::DEFAULT_POLL_INTERVAL_SEC));
        $budget = max($interval, (int) ($o['poll_budget_sec'] ?? self::DEFAULT_POLL_BUDGET_SEC));
        $payload = [
            'url' => $url,
            'goal' => (string) ($o['goal'] ?? 'Extract every plan on this page: slug, title, type, monthly EUR price, contract periods (length, discount EUR, setup EUR) and specs.'),
            'max_steps' => $maxSteps,
        ];
        if (isset($o['output_schema']) && is_array($o['output_schema'])) {
            $payload['output_schema'] = self::sanitizeSchema($o['output_schema']);
        }
        $headers = [
            'X-API-Key: ' . $this->config->apiKey,
            'Content-Type: application/json',
            'Accept: application/json',
        ];
        $base = $this->base(self::DEFAULT_BASE);

        // ONE launch. Any failure here propagates: never retried, never re-sent.
        $t0 = microtime(true);
        $start = $this->call('POST', $base . '/v1/automation/run-async', $headers, $this->encodeJson($payload));
        $started = $this->decodeJson($start['body']);
        $runId = isset($started['run_id']) && is_scalar($started['run_id']) ? (string) $started['run_id'] : '';
        if ($runId === '' || preg_match('/^[A-Za-z0-9_.:-]{1,120}$/', $runId) !== 1) {
            throw new SourceException('tinyfish_agent run-async returned no usable run_id (the run may still be billed; check the TinyFish console)');
        }

        $elapsed = 0;
        $pollFailures = 0;
        $steps = 0;
        $status = 0;
        while (true) {
            try {
                $poll = $this->call('GET', $base . '/v1/runs/' . rawurlencode($runId), $headers, null);
            } catch (SourceException $e) {
                $h = $e->httpStatus();
                $transient = $h === 0 || $h >= 500;
                if ($transient && ++$pollFailures < self::MAX_CONSECUTIVE_POLL_FAILURES) {
                    ($this->sleep)($interval);
                    $elapsed += $interval;
                    if ($elapsed >= $budget) {
                        $this->cancel($base, $runId, $headers);
                        throw (new SourceException('tinyfish_agent run ' . $runId . ' not followed to the end within ' . $budget . ' s (cancelled): ' . $e->getMessage(), $h))
                            ->withProviderRun($runId, $steps * self::MICRO_PER_STEP);
                    }
                    continue;
                }
                if ($transient) {
                    $this->cancel($base, $runId, $headers);
                }
                throw (new SourceException('tinyfish_agent run ' . $runId . ' lost: ' . $e->getMessage(), $h, $e))
                    ->withProviderRun($runId, $steps * self::MICRO_PER_STEP);
            }
            $pollFailures = 0;
            $status = $poll['status'];
            $data = $this->decodeJson($poll['body']);
            $steps = self::stepCount($data);
            $state = strtoupper((string) ($data['status'] ?? ''));

            if ($state === 'COMPLETED') {
                $result = $data['result'] ?? null;
                if (!is_array($result) || $result === []) {
                    throw (new SourceException('tinyfish_agent run ' . $runId . ' completed without a structured result'))
                        ->withProviderRun($runId, $steps * self::MICRO_PER_STEP);
                }
                $cost = (isset($data['num_of_steps']) && is_numeric($data['num_of_steps']) ? (int) $data['num_of_steps'] : ($steps > 0 ? $steps : $maxSteps)) * self::MICRO_PER_STEP;
                return new FetchResult(
                    $this->id(),
                    'tinyfish-run:' . $runId,
                    null,
                    $result,
                    $cost,
                    (int) round((microtime(true) - $t0) * 1000),
                    $status,
                    'provider-json'
                );
            }
            if ($state === 'FAILED' || $state === 'CANCELLED') {
                $why = $this->scrub(substr((string) ($data['error'] ?? ($data['message'] ?? '')), 0, 200));
                throw (new SourceException('tinyfish_agent run ' . $runId . ' ' . strtolower($state) . ($why !== '' ? ': ' . $why : '')))
                    ->withProviderRun($runId, $steps * self::MICRO_PER_STEP);
            }
            // PENDING / RUNNING (or anything unrecognised): keep polling within the limits
            if ($steps > $maxSteps) {
                $this->cancel($base, $runId, $headers);
                throw (new SourceException('tinyfish_agent run ' . $runId . ' exceeded max_steps (' . $steps . ' > ' . $maxSteps . '); cancelled'))
                    ->withProviderRun($runId, $steps * self::MICRO_PER_STEP);
            }
            if ($elapsed >= $budget) {
                $this->cancel($base, $runId, $headers);
                throw (new SourceException('tinyfish_agent run ' . $runId . ' still ' . strtolower($state) . ' after ' . $budget . ' s; cancelled'))
                    ->withProviderRun($runId, $steps * self::MICRO_PER_STEP);
            }
            ($this->sleep)($interval);
            $elapsed += $interval;
        }
    }

    /** num_of_steps: an int on terminal runs, a list of step objects while RUNNING. @param array<string,mixed> $data */
    private static function stepCount(array $data): int
    {
        foreach (['num_of_steps', 'steps', 'num_steps', 'step_count'] as $k) {
            if (!array_key_exists($k, $data)) {
                continue;
            }
            $v = $data[$k];
            if (is_array($v)) {
                return count($v);
            }
            if (is_numeric($v)) {
                return max(0, (int) $v);
            }
        }
        return 0;
    }

    /** Best-effort, single attempt; a failure to cancel must not mask the real error. @param array<int,string> $headers */
    private function cancel(string $base, string $runId, array $headers): void
    {
        try {
            $this->call('POST', $base . '/v1/runs/' . rawurlencode($runId) . '/cancel', $headers, '{}');
        } catch (SourceException $e) {
            // swallowed: the run id is on the attempt row for manual recovery
        }
    }
}
