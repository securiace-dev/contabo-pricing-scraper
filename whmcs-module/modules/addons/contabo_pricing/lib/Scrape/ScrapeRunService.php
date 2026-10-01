<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use ContaboPricing\AdminController;
use ContaboPricing\CatalogImportService;
use ContaboPricing\CurlRequestExecutor;
use ContaboPricing\Lock;

/**
 * Orchestrates one scrape run: lock -> budget pre-check -> waterfall fetch ->
 * extract -> validate (deterministic gates) -> optional Jev veto -> outcome ->
 * envelope -> (auto_import only) CatalogImportService::import().
 *
 * Outcomes: rejected (any anomaly or failed gate), needs_review (risky diff,
 * Jev doubt, or scrape.require_human_review=1), auto_import. A dry run does
 * everything except the import and ends in state dry_run.
 */
final class ScrapeRunService
{
    public const LOCK_NAME = 'contabo_scrape_run';
    public const LOCK_TTL = 900;

    /** @var ScrapeSettings */
    private $settings;
    /** @var SourceConfigRepository */
    private $sourceRepo;
    /** @var HeaderAwareExecutor */
    private $executor;
    /** @var CostLedger */
    private $ledger;
    /** @var RunRepository */
    private $runs;
    /** @var CatalogImportService */
    private $importer;
    /** @var Lock */
    private $lock;
    /** @var DecisionRecorder */
    private $decisions;

    public function __construct(
        ?ScrapeSettings $settings = null,
        ?HeaderAwareExecutor $executor = null,
        ?CostLedger $ledger = null,
        ?CatalogImportService $importer = null,
        ?Lock $lock = null
    ) {
        $this->settings = $settings ?? new ScrapeSettings();
        $this->sourceRepo = new SourceConfigRepository();
        $this->executor = $executor ?? new CurlRequestExecutor();
        $this->ledger = $ledger ?? new CostLedger();
        $this->runs = new RunRepository();
        $this->importer = $importer ?? new CatalogImportService();
        $this->lock = $lock ?? new Lock();
        $this->decisions = new DecisionRecorder();
    }

    /**
     * @param array<string,mixed> $opts dry_run(bool), sources(list<SourceInterface> test/override),
     *        only_source(string id; may be manual-only), per_run_cap_micro(int override, used by the confirmed agent run)
     * @return array<string,mixed> summary
     */
    public function run(string $trigger, int $adminId = 0, array $opts = []): array
    {
        $dry = !empty($opts['dry_run']);

        $token = $this->lock->acquire(self::LOCK_NAME, self::LOCK_TTL);
        if ($token === null) {
            $id = $this->runs->create($trigger, $adminId, $dry, RunRepository::STATE_SKIPPED_LOCKED);
            $this->runs->finish($id, RunRepository::STATE_SKIPPED_LOCKED, ['error' => 'another scrape run holds the lock']);
            return ['run_id' => $id, 'state' => RunRepository::STATE_SKIPPED_LOCKED, 'outcome' => null, 'plan_count' => 0, 'total_cost_micro' => 0];
        }

        $runId = 0;
        try {
            $runId = $this->runs->create($trigger, $adminId, $dry);
            $summary = $this->execute($runId, $trigger, $adminId, $dry, $opts);
        } catch (\Throwable $e) {
            if ($runId > 0) {
                $this->runs->finish($runId, RunRepository::STATE_FAILED, ['error' => $e->getMessage()]);
            }
            $summary = ['run_id' => $runId, 'state' => RunRepository::STATE_FAILED, 'outcome' => null, 'plan_count' => 0, 'total_cost_micro' => 0, 'error' => $e->getMessage()];
        } finally {
            $this->lock->release(self::LOCK_NAME, $token);
        }

        if (!$dry) {
            try {
                $this->settings->touchLastRun();
            } catch (\Throwable $e) {
                // bookkeeping only
            }
            $this->notify($summary);
        }
        return $summary;
    }

    /**
     * @param array<string,mixed> $opts
     * @return array<string,mixed>
     */
    private function execute(int $runId, string $trigger, int $adminId, bool $dry, array $opts): array
    {
        $s = $this->settings;
        $perRunCap = isset($opts['per_run_cap_micro']) ? (int) $opts['per_run_cap_micro'] : $s->perRunCapMicro();

        // ── budget pre-check ────────────────────────────────────────────────
        $monthSpend = $this->ledger->monthSpendMicro();
        if ($monthSpend >= $s->monthlyBudgetMicro()) {
            return $this->finishRejected($runId, 'budget_exhausted', [
                'month_spend_micro' => $monthSpend,
                'monthly_budget_micro' => $s->monthlyBudgetMicro(),
            ], $dry, $adminId);
        }

        // ── sources ─────────────────────────────────────────────────────────
        $includeManual = $trigger !== 'cron' && !empty($opts['only_source']);
        [$rows, $adapters] = $this->resolveSources($opts, $trigger);
        $ordered = (new SourceRanker())->order(
            $rows,
            $this->ledger,
            isset($opts['sources']) ? 'manual' : $s->rankMode(),
            $includeManual
        );
        if ($ordered === []) {
            return $this->finishRejected($runId, 'no_usable_source', ['reason' => 'no enabled, keyed, sapper-capable source (manual-only sources never run on cron)'], $dry, $adminId);
        }
        $ordered = array_values(array_filter($ordered, function (array $r) use ($adapters, $includeManual): bool {
            $a = $adapters[(string) $r['source_id']] ?? null;
            if ($a === null) {
                return false;
            }
            return $includeManual || (!$a->manualOnly() && $a->supportsSapper());
        }));
        if ($ordered === []) {
            return $this->finishRejected($runId, 'no_usable_source', ['reason' => 'no sapper-capable, non-manual source'], $dry, $adminId);
        }
        $perSourceMonth = $this->ledger->perSourceMonthSpend();
        $ordered = array_values(array_filter($ordered, static function (array $r) use ($perSourceMonth): bool {
            return ($perSourceMonth[(string) $r['source_id']] ?? 0) < (int) ($r['monthly_budget_micro'] ?? PHP_INT_MAX);
        }));
        if ($ordered === []) {
            return $this->finishRejected($runId, 'budget_exhausted', ['reason' => 'every usable source has spent its monthly budget'], $dry, $adminId);
        }

        // ── fetch waterfall ─────────────────────────────────────────────────
        $urlList = new PlanUrlList($s->planUrls());
        $extractor = new PlanExtractor($urlList);
        $min = $s->minPlansByFamily();

        $attempts = [];
        $merged = [];
        $families = [];
        $runSpend = 0;
        $runSourceSpend = [];
        $jevHtml = null;
        $anyFetchOk = false;

        foreach ($urlList->firstUrlPerFamily() as $family => $url) {
            $need = $min[$family] ?? 0;
            if ($this->familyCount($merged, $family) >= $need && $merged !== []) {
                $families[$family] = ['url' => $url, 'min' => $need, 'count' => $this->familyCount($merged, $family), 'fetched' => false];
                continue;
            }
            $families[$family] = ['url' => $url, 'min' => $need, 'count' => 0, 'fetched' => true, 'skipped' => []];
            foreach ($ordered as $row) {
                $id = (string) $row['source_id'];
                $adapter = $adapters[$id] ?? null;
                if ($adapter === null) {
                    continue;
                }
                $price = $adapter->priceMicroPerPage();
                $why = $this->unaffordable($price, $id, $row, $runSpend, $runSourceSpend, $perRunCap, $monthSpend, $perSourceMonth);
                if ($why !== null) {
                    $families[$family]['skipped'][] = $id . ': ' . $why;
                    continue;
                }

                $attempt = $this->fetchOne($runId, $family, $url, $adapter, $extractor);
                $attempts[] = $attempt;
                $runSpend += $attempt['cost_micro'];
                $runSourceSpend[$id] = ($runSourceSpend[$id] ?? 0) + $attempt['cost_micro'];
                $familyPlans = array_filter($attempt['plans'], static function (array $p) use ($family): bool {
                    return ($p['family'] ?? '') === $family;
                });
                $sufficient = $attempt['importable'] && count($familyPlans) >= $need;
                $this->persistAttempt($runId, $attempt, $sufficient);
                $this->sourceRepo->recordAttemptOutcome($id, $sufficient, $sufficient ? null : ($attempt['error'] ?? 'insufficient plans'));
                if ($attempt['fetch_ok']) {
                    $anyFetchOk = true;
                    if ($jevHtml === null && $attempt['html'] !== null && $attempt['importable']) {
                        $jevHtml = $attempt['html'];
                    }
                }
                if ($sufficient) {
                    foreach ($attempt['plans'] as $p) {
                        if (!isset($merged[$p['product_slug']])) {
                            $merged[$p['product_slug']] = $p;
                        }
                    }
                    $families[$family]['count'] = $this->familyCount($merged, $family);
                    $families[$family]['served_by'] = $attempt['served_by'] ?? $id;
                    break;
                }
            }
        }

        $plans = array_values($merged);
        usort($plans, static function (array $a, array $b): int {
            return [(int) ($a['plan_rank'] ?? 0), (string) $a['product_slug']] <=> [(int) ($b['plan_rank'] ?? 0), (string) $b['product_slug']];
        });
        $cost = $runSpend;

        if ($attempts === []) {
            return $this->finishRejected($runId, 'budget_exhausted', ['reason' => 'per-run or per-source cap prevented every fetch', 'families' => $families], $dry, $adminId);
        }
        if (!$anyFetchOk) {
            $err = 'every provider attempt failed: ' . implode(' | ', array_map(static function (array $a): string {
                return $a['source_id'] . ': ' . (string) ($a['error'] ?? 'unknown');
            }, $attempts));
            $this->runs->finish($runId, RunRepository::STATE_FAILED, [
                'error' => $err, 'total_cost_micro' => $cost, 'families_json' => $families,
            ]);
            return ['run_id' => $runId, 'state' => RunRepository::STATE_FAILED, 'outcome' => null, 'plan_count' => 0, 'total_cost_micro' => $cost, 'error' => $err];
        }

        // ── validate ────────────────────────────────────────────────────────
        $last = $this->runs->latestSucceeded();
        $validation = (new RunValidator())->evaluate($plans, $attempts, $last === null ? null : $last['plans'], $s);

        if (!$validation['passed'] || $validation['anomalies'] !== []) {
            $outcome = 'rejected';
        } elseif ($validation['risky'] !== [] || $s->requireHumanReview()) {
            $outcome = 'needs_review';
        } else {
            $outcome = 'auto_import';
        }

        // ── Jev (advisory; may only downgrade auto_import) ──────────────────
        $jev = null;
        $decidedBy = DecisionRecorder::BY_RULES;
        if ($outcome === 'auto_import' && $s->jevEnabled() && $s->hasJevApiKey() && $jevHtml !== null) {
            $jev = (new JevJudge($s, $this->executor))->judge($jevHtml);
            $after = JevJudge::apply($outcome, $jev);
            if ($after !== $outcome) {
                $outcome = $after;
                $decidedBy = DecisionRecorder::BY_JEV;
            }
        }

        // ── envelope ────────────────────────────────────────────────────────
        $envelope = null;
        if ($plans !== []) {
            $envelope = (new CatalogEnvelopeBuilder())->build(
                $plans,
                ['plans' => []],
                ['dimensions' => []],
                gmdate('Y-m-d\TH:i:s\Z'),
                'addon-' . AdminController::VERSION
            );
        }

        $decisionId = $this->decisions->record(
            $runId,
            $outcome,
            [
                'gates' => $validation['gates'],
                'anomalies' => $validation['anomalies'],
                'risky' => $validation['risky'],
                'require_human_review' => $s->requireHumanReview(),
                'warnings' => $validation['warnings'],
            ],
            $jev,
            $jev !== null ? $s->jevConfidenceMin() : null,
            $decidedBy,
            0
        );

        $fields = [
            'plan_count' => count($plans),
            'families_json' => $families,
            'total_cost_micro' => $cost,
            'gates_json' => $validation + ['jev' => $jev, 'outcome' => $outcome],
            'decision_id' => $decisionId,
        ];
        if ($envelope !== null) {
            $fields['envelope_json'] = $envelope;
            $fields['envelope_hash'] = (string) $envelope['payload_hash'];
            $fields['catalog_version'] = (string) $envelope['catalog_version'];
        }

        // ── terminal state ──────────────────────────────────────────────────
        if ($dry) {
            $state = RunRepository::STATE_DRY_RUN;
        } elseif ($outcome === 'auto_import' && $envelope !== null) {
            $res = $this->importer->import($envelope, $adminId);
            $fields['catalog_version'] = $res['catalog_version'];
            $state = RunRepository::STATE_SUCCEEDED;
        } else {
            $state = $outcome === 'rejected' ? RunRepository::STATE_REJECTED : RunRepository::STATE_NEEDS_REVIEW;
        }
        $this->runs->finish($runId, $state, $fields);

        return [
            'run_id' => $runId,
            'state' => $state,
            'outcome' => $outcome,
            'plan_count' => count($plans),
            'total_cost_micro' => $cost,
            'catalog_version' => $fields['catalog_version'] ?? null,
            'gates' => $validation['gates'],
            'anomalies' => count($validation['anomalies']),
            'risky' => count($validation['risky']),
            'safe' => count($validation['safe']),
            'jev' => $jev,
            'dry_run' => $dry,
        ];
    }

    /**
     * Imports a needs_review run's stored envelope after re-hashing it.
     *
     * @return array{catalog_version:string,payload_hash:string,item_count:int,created:bool}
     */
    public function importStored(int $runId, int $adminId): array
    {
        $run = $this->runs->find($runId);
        if ($run === null) {
            throw new \RuntimeException('Unknown scrape run.');
        }
        if (($run['state'] ?? '') !== RunRepository::STATE_NEEDS_REVIEW) {
            throw new \RuntimeException('Only a needs_review run can be imported.');
        }
        $env = $run['envelope_json'] ?? null;
        if (!is_array($env) || !isset($env['payload_hash'])) {
            throw new \RuntimeException('The run has no stored envelope.');
        }
        $hashable = $env;
        unset($hashable['payload_hash']);
        $computed = hash('sha256', CatalogImportService::canonicalJson($hashable));
        if (!hash_equals((string) $env['payload_hash'], $computed)
            || !hash_equals((string) ($run['envelope_hash'] ?? ''), $computed)
        ) {
            throw new \RuntimeException('Stored envelope failed its integrity check; refusing to import.');
        }
        $res = $this->importer->import($env, $adminId);
        $this->decisions->record($runId, 'auto_import', ['approved_by_admin' => true], null, null, DecisionRecorder::BY_ADMIN, $adminId);
        $this->runs->update($runId, ['state' => RunRepository::STATE_SUCCEEDED, 'catalog_version' => $res['catalog_version']]);
        return $res;
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /**
     * @param array<string,mixed> $opts
     * @return array{0:list<array<string,mixed>>, 1:array<string,SourceInterface>}
     */
    private function resolveSources(array $opts, string $trigger): array
    {
        $rows = [];
        $adapters = [];
        if (isset($opts['sources']) && is_array($opts['sources'])) {
            $i = 0;
            foreach ($opts['sources'] as $src) {
                $i++;
                $adapters[$src->id()] = $src;
                $rows[] = [
                    'source_id' => $src->id(), 'enabled' => 1, 'priority' => $i, 'consecutive_failures' => 0,
                    'monthly_budget_micro' => 500000, 'per_run_cap_micro' => 50000, 'last_fail_at' => null,
                ];
            }
            return [$rows, $adapters];
        }
        $factory = new SourceFactory($this->executor, $this->sourceRepo);
        $only = isset($opts['only_source']) ? (string) $opts['only_source'] : '';
        foreach ($this->sourceRepo->enabled() as $row) {
            $id = (string) $row['source_id'];
            if ($only !== '' && $id !== $only) {
                continue;
            }
            if (trim((string) ($row['api_key_enc'] ?? '')) === '') {
                continue; // no API key configured: never a usable source
            }
            try {
                $adapters[$id] = $factory->build($row);
                $rows[] = $row;
            } catch (\InvalidArgumentException $e) {
                // unknown source id: ignore
            }
        }
        return [$rows, $adapters];
    }

    /**
     * @param array<string,mixed> $row
     * @param array<string,int> $runSourceSpend
     * @param array<string,int> $perSourceMonth
     */
    private function unaffordable(int $price, string $id, array $row, int $runSpend, array $runSourceSpend, int $perRunCap, int $monthSpend, array $perSourceMonth): ?string
    {
        if ($price <= 0) {
            return null; // free calls are never blocked by budgets
        }
        if ($runSpend + $price > $perRunCap) {
            return 'per-run cap';
        }
        if (($runSourceSpend[$id] ?? 0) + $price > (int) ($row['per_run_cap_micro'] ?? PHP_INT_MAX)) {
            return 'source per-run cap';
        }
        if (($perSourceMonth[$id] ?? 0) + ($runSourceSpend[$id] ?? 0) + $price > (int) ($row['monthly_budget_micro'] ?? PHP_INT_MAX)) {
            return 'source monthly budget';
        }
        if ($monthSpend + $runSpend + $price > $this->settings->monthlyBudgetMicro()) {
            return 'monthly budget';
        }
        return null;
    }

    /**
     * @param array<string,array<string,mixed>> $merged
     */
    private function familyCount(array $merged, string $family): int
    {
        $n = 0;
        foreach ($merged as $p) {
            if (($p['family'] ?? '') === $family) {
                $n++;
            }
        }
        return $n;
    }

    /** @return array<string,mixed> */
    private function fetchOne(int $runId, string $family, string $url, SourceInterface $adapter, PlanExtractor $extractor): array
    {
        $a = [
            'family' => $family, 'url' => $url, 'source_id' => $adapter->id(), 'served_by' => null, 'http_status' => 0,
            'fetch_ok' => false, 'importable' => false, 'sapper_present' => false, 'strategy' => null, 'plans' => [],
            'warnings' => [], 'cost_micro' => 0, 'latency_ms' => 0, 'html' => null, 'html_sha256' => null,
            'html_bytes' => 0, 'error' => null, 'final_url' => null,
        ];
        $t0 = microtime(true);
        try {
            $f = $adapter->fetchFamilyPage($url);
        } catch (SourceException $e) {
            $a['latency_ms'] = (int) round((microtime(true) - $t0) * 1000);
            $a['http_status'] = $e->httpStatus();
            $a['error'] = $e->getMessage();
            return $a;
        } catch (\Throwable $e) {
            $a['latency_ms'] = (int) round((microtime(true) - $t0) * 1000);
            $a['error'] = get_class($e) . ': ' . $e->getMessage();
            return $a;
        }
        $a['fetch_ok'] = true;
        $a['served_by'] = $f->servedBy;
        $a['http_status'] = $f->httpStatus;
        $a['cost_micro'] = $f->costMicro;
        $a['latency_ms'] = $f->latencyMs;
        $a['final_url'] = $f->finalUrl;
        $a['html'] = $f->html;
        if ($f->html !== null) {
            $a['html_sha256'] = hash('sha256', $f->html);
            $a['html_bytes'] = strlen($f->html);
        }
        $x = $extractor->extract($f->html, $f->json);
        $a['importable'] = $x->importable();
        $a['sapper_present'] = $x->sapperPresent;
        $a['strategy'] = $x->strategy;
        $a['plans'] = $x->plans;
        $a['warnings'] = $x->warnings;
        if (!$x->importable()) {
            $a['error'] = 'no importable plans (' . $x->strategy . '): ' . implode('; ', array_slice($x->warnings, 0, 3));
        }
        return $a;
    }

    /** @param array<string,mixed> $a */
    private function persistAttempt(int $runId, array $a, bool $ok): void
    {
        $this->runs->addAttempt($runId, [
            'family' => $a['family'], 'url' => $a['url'], 'source_id' => $a['source_id'], 'served_by' => $a['served_by'],
            'http_status' => $a['http_status'], 'ok' => $ok, 'sapper_present' => $a['sapper_present'], 'strategy' => $a['strategy'],
            'plan_count' => count($a['plans']), 'cost_micro' => $a['cost_micro'], 'latency_ms' => $a['latency_ms'],
            'html_sha256' => $a['html_sha256'], 'html_bytes' => $a['html_bytes'], 'error' => $a['error'], 'final_url' => $a['final_url'],
        ]);
    }

    /**
     * @param array<string,mixed> $evidence
     * @return array<string,mixed>
     */
    private function finishRejected(int $runId, string $reason, array $evidence, bool $dry, int $adminId): array
    {
        $decisionId = $this->decisions->record($runId, 'rejected', ['reason' => $reason] + $evidence, null, null, DecisionRecorder::BY_RULES, 0);
        $this->runs->finish($runId, $dry ? RunRepository::STATE_DRY_RUN : RunRepository::STATE_REJECTED, [
            'error' => $reason,
            'decision_id' => $decisionId,
        ]);
        return [
            'run_id' => $runId,
            'state' => $dry ? RunRepository::STATE_DRY_RUN : RunRepository::STATE_REJECTED,
            'outcome' => 'rejected',
            'reason' => $reason,
            'plan_count' => 0,
            'total_cost_micro' => 0,
            'error' => $reason,
            'dry_run' => $dry,
        ];
    }

    /** @param array<string,mixed> $summary */
    private function notify(array $summary): void
    {
        $state = (string) ($summary['state'] ?? '');
        if (!in_array($state, ['failed', 'rejected', 'needs_review'], true) || !function_exists('sendAdminNotification')) {
            return;
        }
        $msg = 'Scrape run #' . (int) ($summary['run_id'] ?? 0) . ' ended as ' . $state;
        if (!empty($summary['error'])) {
            $msg .= ': ' . (string) $summary['error'];
        }
        $failed = [];
        foreach ((array) ($summary['gates'] ?? []) as $name => $g) {
            if (is_array($g) && empty($g['ok'])) {
                $failed[] = (string) $name;
            }
        }
        if ($failed !== []) {
            $msg .= ' Failed gates: ' . implode(', ', $failed) . '.';
        }
        try {
            \sendAdminNotification('system', 'Contabo Pricing scrape ' . $state, $msg);
        } catch (\Throwable $e) {
            // notification is best effort
        }
    }
}
