<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Walks `mod_contabo_price_change_schedule` rows whose effective_at has been
 * reached and routes each row through the SAME applyWithGuards() surface that
 * the cron RenewalEngine uses.
 *
 * NON-NEGOTIABLE CONTRACT — read this before touching the code:
 *
 *   - Scheduled changes funnel through RenewalEngine::decideForScheduledChange()
 *     → decideInternal() → applyWithGuards(). There is intentionally NO second
 *     write path. ServicePriceWriter is never called from this file directly;
 *     it's the engine's job (Phase B, when wired). This file's only side
 *     effects today are: emit decision rows via DecisionLog, emit pricing-
 *     action rows for the schedule lifecycle (defer / apply markers), and
 *     mark schedule.status / applied_at.
 *
 *   - Phase A.5 stays observe-only on the renewal side. Even when this
 *     processor "decides apply" for a service, the engine forces
 *     applied=false unless repricing_phase has been flipped to opt_in/enforce.
 *
 *   - Catalog-affecting schedules (applies_to_catalog=1) are NOT lit up in
 *     Phase A.5. We emit a catalog_audit sidecar row with
 *     skipped_reason='phase_a5_scheduled_catalog_pending' so admins see the
 *     intent without us silently writing tblpricing. Agent B's SyncEngine
 *     will adopt this path in a follow-up.
 *
 *   - Cycle gating: a schedule's `cycles_mask` selects which cycles this
 *     change applies to. For each enabled cycle we evaluate only services
 *     whose `tblhosting.billingcycle` matches that cycle. Other cycles are
 *     out of scope for the same schedule row.
 *
 * PHP 7.4 polyglot.
 */
final class ScheduledChangeProcessor
{
    /** @var array<string,mixed> Resolved settings bag (same shape RenewalEngine reads). */
    private $settings;

    /** @var PolicyResolver */
    private $policyResolver;

    /** @var DecisionLog */
    private $decisionLog;

    /** @var callable Returns `list<array<string,mixed>>` of pending schedule rows. */
    private $scheduleFetcher;

    /** @var callable Returns `list<array<string,mixed>>` of services affected by a schedule row. */
    private $servicesFetcher;

    /**
     * @param array<string,mixed>                                                  $settings
     * @param PolicyResolver|null                                                  $policyResolver
     * @param DecisionLog|null                                                     $decisionLog
     * @param callable(\DateTimeImmutable):list<array<string,mixed>>|null         $scheduleFetcher
     *        Optional override. Defaults to a Capsule query for pending/notified
     *        rows with `effective_at <= now`.
     * @param callable(array<string,mixed>):list<array<string,mixed>>|null         $servicesFetcher
     *        Optional override. Defaults to a Capsule query that joins
     *        `tblhosting` + `mod_contabo_mapping`. Tests inject fixture data.
     */
    public function __construct(
        array $settings,
        ?PolicyResolver $policyResolver = null,
        ?DecisionLog $decisionLog = null,
        ?callable $scheduleFetcher = null,
        ?callable $servicesFetcher = null
    ) {
        $this->settings        = $settings;
        $this->policyResolver  = $policyResolver !== null ? $policyResolver : new PolicyResolver();
        $this->decisionLog     = $decisionLog !== null ? $decisionLog : new DecisionLog();
        $this->scheduleFetcher = $scheduleFetcher !== null ? $scheduleFetcher : [$this, 'defaultScheduleFetcher'];
        $this->servicesFetcher = $servicesFetcher !== null ? $servicesFetcher : [$this, 'defaultServicesFetcher'];
    }

    /**
     * Process all due scheduled changes. Returns a summary blob the caller
     * (DailyCronJob) can log.
     *
     * @return array{
     *   schedules_processed:int,
     *   schedules_applied:int,
     *   schedules_deferred:int,
     *   services_evaluated:int,
     *   catalog_intents_logged:int,
     *   decisions:list<array<string,mixed>>
     * }
     */
    public function run(?\DateTimeImmutable $now = null): array
    {
        if ($now === null) {
            $now = new \DateTimeImmutable('now');
        }

        $summary = [
            'schedules_processed'    => 0,
            'schedules_applied'      => 0,
            'schedules_deferred'     => 0,
            'services_evaluated'     => 0,
            'catalog_intents_logged' => 0,
            'decisions'              => [],
        ];

        $schedules = ($this->scheduleFetcher)($now);
        if (!is_array($schedules)) {
            return $summary;
        }

        $engine = new RenewalEngine($this->settings, $this->policyResolver);

        foreach ($schedules as $change) {
            $summary['schedules_processed']++;
            $scheduleId = (int) ($change['id'] ?? 0);

            $cyclesMask = (int) ($change['cycles_mask'] ?? 0);
            // Treat zero mask as "all recurring cycles" so legacy rows written
            // before Phase A.5 keep working. New admin UI should always send
            // an explicit mask.
            $cycleSet = $cyclesMask === 0
                ? CycleSet::fromCycles(CycleSet::allCycles())
                : CycleSet::fromMask($cyclesMask);

            $appliesToCatalog  = !empty($change['applies_to_catalog']);
            $appliesToRenewals = isset($change['applies_to_renewals'])
                ? !empty($change['applies_to_renewals'])
                : true;

            $anyApplied = false;
            $anyDeferred = false;

            foreach ($cycleSet->enabledCycles() as $cycle) {
                if ($appliesToCatalog) {
                    // Phase A.5: log intent only — no tblpricing mutation here.
                    // ScheduledChangeProcessor delegates the eventual catalog
                    // write to SyncEngine. We mark the intent with a sentinel
                    // row in mod_contabo_catalog_audit so admins can see it.
                    $this->recordCatalogIntent($change, $cycle, $now);
                    $summary['catalog_intents_logged']++;
                }

                if ($appliesToRenewals) {
                    $services = ($this->servicesFetcher)([
                        'schedule' => $change,
                        'cycle'    => $cycle,
                    ]);
                    if (!is_array($services)) {
                        $services = [];
                    }

                    foreach ($services as $service) {
                        $serviceCycle = (string) ($service['billingcycle'] ?? '');
                        if ($serviceCycle !== $cycle) {
                            // Service does not match the cycle we're iterating
                            // — skip without recording (some fetchers may pass
                            // an unfiltered list).
                            continue;
                        }
                        if (CycleNormalizer::monthsForCycle($serviceCycle) === null) {
                            continue;
                        }

                        $summary['services_evaluated']++;

                        $decision = $engine->decideForScheduledChange(
                            $service,
                            (float) ($change['new_price'] ?? 0.0),
                            $now
                        );

                        try {
                            $decisionId = $this->decisionLog->insert($decision);
                        } catch (\Throwable $e) {
                            // Don't let a single bad row tank the whole batch.
                            // Log and move on.
                            $this->logIfPossible(
                                'ScheduledChangeProcessor: DecisionLog::insert failed for service '
                                . (int) ($service['id'] ?? 0) . ' on schedule ' . $scheduleId
                                . ' — ' . $e->getMessage()
                            );
                            continue;
                        }

                        $decision['_decision_id'] = $decisionId;
                        $summary['decisions'][]   = $decision;

                        if (!empty($decision['applied'])) {
                            $anyApplied = true;
                            $this->insertAction([
                                'action_type' => 'apply',
                                'service_id'  => (int) ($service['id'] ?? 0),
                                'decision_id' => $decisionId,
                                'schedule_id' => $scheduleId,
                                'reason'      => 'scheduled_change_applied',
                            ], $now);
                        } else {
                            $anyDeferred = true;
                            $this->insertAction([
                                'action_type' => 'defer',
                                'service_id'  => (int) ($service['id'] ?? 0),
                                'decision_id' => $decisionId,
                                'schedule_id' => $scheduleId,
                                'reason'      => (string) ($decision['skip_reason'] ?? 'deferred'),
                            ], $now);
                        }
                    }
                }
            }

            // Update schedule.status. If ANY service applied, mark the schedule
            // applied so it doesn't repeatedly fire on the next cron tick.
            // Mixed batches (some applied, some deferred) are conservatively
            // marked applied — the deferred services have their own audit
            // rows admins can re-target via a new schedule entry.
            if ($anyApplied) {
                $summary['schedules_applied']++;
                $this->markScheduleApplied($scheduleId, $now);
            } elseif ($anyDeferred) {
                $summary['schedules_deferred']++;
                // Leave status as 'pending'/'notified' so the next cron retries.
            }
        }

        return $summary;
    }

    /**
     * Defaults to a Capsule query against `mod_contabo_price_change_schedule`.
     * Returns rows whose effective_at has passed and whose status is one of
     * the still-actionable values.
     *
     * @return list<array<string,mixed>>
     */
    private function defaultScheduleFetcher(\DateTimeImmutable $now): array
    {
        try {
            $rows = Capsule::table('mod_contabo_price_change_schedule')
                ->where('effective_at', '<=', $now->format('Y-m-d H:i:s'))
                ->where('status', 'pending')
                ->get();
            $rows2 = Capsule::table('mod_contabo_price_change_schedule')
                ->where('effective_at', '<=', $now->format('Y-m-d H:i:s'))
                ->where('status', 'notified')
                ->get();
        } catch (\Throwable $e) {
            return [];
        }

        $out = [];
        foreach (array_merge((array) $rows, (array) $rows2) as $row) {
            $out[] = (array) $row;
        }
        return $out;
    }

    /**
     * Default services-for-schedule fetcher. Loads candidates from
     * `tblhosting` joined to `mod_contabo_mapping`. Tests inject a fixture
     * fetcher to skip the DB entirely.
     *
     * Returns service rows shaped for RenewalEngine::decideForScheduledChange()
     * — meaning they already carry the `mapping`, `profile`, `profile_version`
     * sub-arrays the engine reads. The default implementation is intentionally
     * simple; production wiring should hydrate these via PolicyResolver +
     * ProfileManager so cycle masks and overrides are present.
     *
     * @param array{schedule:array<string,mixed>, cycle:string} $args
     * @return list<array<string,mixed>>
     */
    private function defaultServicesFetcher(array $args): array
    {
        // Production wiring is left for the DailyCronJob hook integration.
        // Returning [] here means: without an injected fetcher, no services
        // are evaluated. This keeps the default safe (no surprise reads /
        // writes during the test or a half-wired install).
        return [];
    }

    private function insertAction(array $row, \DateTimeImmutable $now): void
    {
        try {
            Capsule::table('mod_contabo_pricing_action')->insert(array_merge(
                ['admin_id' => 0, 'created_at' => $now->format('Y-m-d H:i:s')],
                $row
            ));
        } catch (\Throwable $e) {
            $this->logIfPossible('ScheduledChangeProcessor: pricing_action insert failed: ' . $e->getMessage());
        }
    }

    private function recordCatalogIntent(array $change, string $cycle, \DateTimeImmutable $now): void
    {
        try {
            $cycleMonths = CycleNormalizer::monthsForCycle($cycle);
            Capsule::table('mod_contabo_catalog_audit')->insert([
                'sync_batch_id'          => self::uuidV4(),
                'product_id'             => 0,
                'currency_id'            => 0,
                'cycle'                  => $cycle,
                'cycle_months'           => $cycleMonths !== null ? $cycleMonths : 0,
                'recurring_column'       => (string) CyclePricingMap::getRecurringColumn($cycle),
                'setup_fee_column'       => (string) CyclePricingMap::getSetupFeeColumn($cycle),
                'old_price'              => null,
                'new_price'              => (float) ($change['new_price'] ?? 0.0),
                'old_setup_fee'          => null,
                'new_setup_fee'          => null,
                'price_status_before'    => 'absent',
                'markup_strategy_used'   => null,
                'markup_value_used'      => null,
                'pre_round_price'        => null,
                'rounded_price'          => null,
                'rounding_mode'          => null,
                'skipped_reason'         => 'phase_a5_scheduled_catalog_pending',
                'applied'                => 0,
                'created_at'             => $now->format('Y-m-d H:i:s'),
            ]);
        } catch (\Throwable $e) {
            $this->logIfPossible('ScheduledChangeProcessor: catalog intent insert failed: ' . $e->getMessage());
        }
    }

    private function markScheduleApplied(int $scheduleId, \DateTimeImmutable $now): void
    {
        if ($scheduleId <= 0) {
            return;
        }
        try {
            Capsule::table('mod_contabo_price_change_schedule')
                ->where('id', $scheduleId)
                ->update([
                    'status'      => 'applied',
                    'applied_at'  => $now->format('Y-m-d H:i:s'),
                ]);
        } catch (\Throwable $e) {
            $this->logIfPossible('ScheduledChangeProcessor: status update failed: ' . $e->getMessage());
        }
    }

    private function logIfPossible(string $msg): void
    {
        if (function_exists('logActivity')) {
            \logActivity('Contabo Pricing ' . $msg);
        }
    }

    /**
     * RFC 4122 v4 UUID. Copied from RenewalEngine / Lock to keep this class
     * dependency-free.
     */
    private static function uuidV4(): string
    {
        try {
            $bytes = random_bytes(16);
        } catch (\Throwable $e) {
            $bytes = '';
            for ($i = 0; $i < 16; $i++) {
                $bytes .= chr(mt_rand(0, 255));
            }
        }
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }
}
