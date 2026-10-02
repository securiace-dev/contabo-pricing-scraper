<?php
declare(strict_types=1);

namespace ContaboPricing;

final class DailyOperationsDigestBuilder
{
    /**
     * @param array<string,mixed> $syncSummary
     * @param array<string,mixed> $observeSummary
     * @param array<string,mixed> $context
     * @return array{severity:string,subject:string,message:string,should_send:bool}
     */
    public function build(array $syncSummary, array $observeSummary, array $context = []): array
    {
        $severity = $this->resolveSeverity($syncSummary, $observeSummary, $context);
        $lines = [];

        $lines[] = 'Unified daily Contabo operations digest';
        $lines[] = 'Overall status: ' . $severity;
        $lines[] = $this->buildRunLine($syncSummary, $observeSummary);
        $lines[] = '';

        $lines[] = 'Executive summary';
        $lines[] = $this->buildExecutiveLine($syncSummary, $observeSummary);
        $snapshot = (string) ($syncSummary['snapshot_generated_at'] ?? '');
        if ($snapshot !== '') {
            $lines[] = 'Snapshot generated at: ' . $snapshot;
        }
        $sourceVersion = (string) ($syncSummary['catalog_scraper_version'] ?? '');
        if ($sourceVersion !== '') {
            $lines[] = 'Catalog source version: ' . $sourceVersion;
        }

        $lines[] = '';
        $lines[] = 'What happened today';
        $lines = array_merge($lines, $this->buildWhatHappenedLines($syncSummary));

        $lines[] = '';
        $lines[] = 'Repricing and renewal insight';
        $lines = array_merge($lines, $this->buildObserveLines($observeSummary));

        $failureLines = $this->buildFailureLines($syncSummary, $observeSummary, $context);
        if ($failureLines !== []) {
            $lines[] = '';
            $lines[] = 'Failures and degraded behavior';
            $lines = array_merge($lines, $failureLines);
        }

        $lines[] = '';
        $lines[] = 'Next actions';
        $lines = array_merge($lines, $this->buildNextActionLines($severity, $syncSummary, $observeSummary));

        return [
            'severity' => $severity,
            'subject' => 'Contabo Pricing daily digest [' . $severity . ']',
            'message' => implode("\n", $lines),
            'should_send' => true,
        ];
    }

    /**
     * @param array<string,mixed> $syncSummary
     * @param array<string,mixed> $observeSummary
     * @param array<string,mixed> $context
     */
    private function resolveSeverity(array $syncSummary, array $observeSummary, array $context): string
    {
        $syncStatus = (string) ($syncSummary['status'] ?? '');
        $observeStatus = (string) ($observeSummary['status'] ?? '');
        if ($syncStatus === 'failed' || $observeStatus === 'failed' || !empty($context['hook_errors'])) {
            return 'failed';
        }
        if (
            !empty($syncSummary['errors'])
            || !empty($observeSummary['errors'])
            || $observeStatus === 'warning'
            || (int) (($observeSummary['scheduled_changes']['schedules_deferred'] ?? 0)) > 0
        ) {
            return 'warning';
        }
        if (
            (int) ($syncSummary['profiles_changed'] ?? 0) > 0
            || (int) ($syncSummary['products_updated'] ?? 0) > 0
            || (int) (($observeSummary['scheduled_changes']['schedules_applied'] ?? 0)) > 0
            || (int) (($observeSummary['scheduled_changes']['catalog_intents_logged'] ?? 0)) > 0
        ) {
            return 'changed';
        }
        return 'healthy';
    }

    /**
     * @param array<string,mixed> $syncSummary
     * @param array<string,mixed> $observeSummary
     */
    private function buildRunLine(array $syncSummary, array $observeSummary): string
    {
        $startedAt = (string) ($syncSummary['started_at'] ?? ($observeSummary['started_at'] ?? ''));
        $finishedAt = (string) ($observeSummary['finished_at'] ?? ($syncSummary['finished_at'] ?? ''));
        $duration = $this->durationLabel($startedAt, $finishedAt);
        $parts = [];
        if ($startedAt !== '') {
            $parts[] = 'Started ' . $startedAt;
        }
        if ($finishedAt !== '') {
            $parts[] = 'Finished ' . $finishedAt;
        }
        if ($duration !== '') {
            $parts[] = 'Duration ' . $duration;
        }
        return $parts === [] ? 'Run timing unavailable.' : implode(' | ', $parts);
    }

    /**
     * @param array<string,mixed> $syncSummary
     * @param array<string,mixed> $observeSummary
     */
    private function buildExecutiveLine(array $syncSummary, array $observeSummary): string
    {
        $productsWord = !empty($syncSummary['observe_only']) ? 'products planned' : 'products updated';
        return sprintf(
            'Sync status %s; profiles checked %d, changed %d, %s %d, cycles applied %d, cycles skipped %d, cron candidate services %d.',
            (string) ($syncSummary['status'] ?? 'unknown'),
            (int) ($syncSummary['profiles_checked'] ?? 0),
            (int) ($syncSummary['profiles_changed'] ?? 0),
            $productsWord,
            (int) (!empty($syncSummary['observe_only']) ? ($syncSummary['products_planned'] ?? 0) : ($syncSummary['products_updated'] ?? 0)),
            (int) ($syncSummary['cycles_applied'] ?? 0),
            (int) ($syncSummary['cycles_skipped'] ?? 0),
            (int) ($observeSummary['candidate_mapped_services'] ?? 0)
        );
    }

    /**
     * @param array<string,mixed> $syncSummary
     * @return list<string>
     */
    private function buildWhatHappenedLines(array $syncSummary): array
    {
        $lines = [];
        $status = (string) ($syncSummary['status'] ?? 'unknown');
        if ($status === 'no-change') {
            $lines[] = 'Sync completed without upstream or catalog-visible changes.';
        } elseif ($status === 'preview') {
            $lines[] = 'Sync ran in observe-only mode; no catalog rows were changed.';
        } else {
            $lines[] = sprintf(
                'Profiles checked %d; profile versions changed %d; products touched %d.',
                (int) ($syncSummary['profiles_checked'] ?? 0),
                (int) ($syncSummary['profiles_changed'] ?? 0),
                (int) (!empty($syncSummary['observe_only']) ? ($syncSummary['products_planned'] ?? 0) : ($syncSummary['products_updated'] ?? 0))
            );
        }

        if (!empty($syncSummary['snapshot_unchanged'])) {
            $lines[] = 'Upstream snapshot timestamp matched the last successful sync; catalog traversal still ran in case local mapping flags changed.';
        }

        $changes = isset($syncSummary['change_list']) && is_array($syncSummary['change_list'])
            ? array_slice($syncSummary['change_list'], 0, 3)
            : [];
        if ($changes === []) {
            $lines[] = 'Top profile changes: none.';
        } else {
            foreach ($changes as $change) {
                $oldValue = isset($change['previous_final']) && $change['previous_final'] !== null
                    ? number_format((float) $change['previous_final'], 2, '.', '')
                    : 'new';
                $newValue = number_format((float) ($change['new_final'] ?? 0.0), 2, '.', '');
                $lines[] = sprintf(
                    'Profile %s (%s, %d mo): %s -> %s %s.',
                    (string) ($change['profile_slug'] ?? 'unknown'),
                    (string) ($change['plan_slug'] ?? 'unknown'),
                    (int) ($change['period_months'] ?? 0),
                    $oldValue,
                    $newValue,
                    (string) ($change['currency'] ?? '')
                );
            }
        }

        $plannedWrites = isset($syncSummary['planned_writes']) && is_array($syncSummary['planned_writes'])
            ? count($syncSummary['planned_writes'])
            : 0;
        if (!empty($syncSummary['observe_only'])) {
            $lines[] = 'Planned catalog writes: ' . $plannedWrites . '.';
        } else {
            $lines[] = sprintf(
                'Catalog cycles evaluated %d; applied %d; skipped %d.',
                (int) ($syncSummary['cycles_evaluated'] ?? 0),
                (int) ($syncSummary['cycles_applied'] ?? 0),
                (int) ($syncSummary['cycles_skipped'] ?? 0)
            );
        }

        return $lines;
    }

    /**
     * @param array<string,mixed> $observeSummary
     * @return list<string>
     */
    private function buildObserveLines(array $observeSummary): array
    {
        $scheduled = isset($observeSummary['scheduled_changes']) && is_array($observeSummary['scheduled_changes'])
            ? $observeSummary['scheduled_changes']
            : [];
        $lines = [];
        $lines[] = 'Active mapped service candidates: ' . (int) ($observeSummary['candidate_mapped_services'] ?? 0) . '.';

        $mode = (string) ($observeSummary['renewal_evaluation_mode'] ?? 'inactive');
        $modeMessage = (string) ($observeSummary['renewal_evaluation_message'] ?? '');
        if ($modeMessage !== '') {
            $lines[] = $modeMessage;
        } elseif ($mode === 'inactive') {
            $lines[] = 'Repricing observation stayed inactive.';
        }

        $lines[] = sprintf(
            'Scheduled changes: processed %d, applied %d, deferred %d, services evaluated %d, catalog intents logged %d.',
            (int) ($scheduled['schedules_processed'] ?? 0),
            (int) ($scheduled['schedules_applied'] ?? 0),
            (int) ($scheduled['schedules_deferred'] ?? 0),
            (int) ($scheduled['services_evaluated'] ?? 0),
            (int) ($scheduled['catalog_intents_logged'] ?? 0)
        );

        $skipSummary = $this->topSkipReasons($scheduled);
        if ($skipSummary !== '') {
            $lines[] = 'Top repricing skip/defer reasons: ' . $skipSummary . '.';
        }

        $notes = isset($observeSummary['notes']) && is_array($observeSummary['notes'])
            ? array_slice($observeSummary['notes'], 0, 2)
            : [];
        foreach ($notes as $note) {
            $lines[] = (string) $note;
        }

        return $lines;
    }

    /**
     * @param array<string,mixed> $syncSummary
     * @param array<string,mixed> $observeSummary
     * @param array<string,mixed> $context
     * @return list<string>
     */
    private function buildFailureLines(array $syncSummary, array $observeSummary, array $context): array
    {
        $lines = [];
        foreach ($this->bucketMessages($syncSummary['errors'] ?? []) as $bucket => $messages) {
            $lines[] = ucfirst($bucket) . ': ' . implode(' | ', array_slice($messages, 0, 3));
        }
        foreach ($this->bucketMessages($observeSummary['errors'] ?? []) as $bucket => $messages) {
            $lines[] = ucfirst($bucket) . ': ' . implode(' | ', array_slice($messages, 0, 3));
        }
        if (!empty($context['hook_errors']) && is_array($context['hook_errors'])) {
            $lines[] = 'Hook failures: ' . implode(' | ', array_slice($context['hook_errors'], 0, 3));
        }
        return $lines;
    }

    /**
     * @param string $severity
     * @param array<string,mixed> $syncSummary
     * @param array<string,mixed> $observeSummary
     * @return list<string>
     */
    private function buildNextActionLines(string $severity, array $syncSummary, array $observeSummary): array
    {
        $actions = [];
        if ($severity === 'failed') {
            $actions[] = 'Review the listed errors before trusting catalog or repricing state.';
        }
        if (!empty($syncSummary['errors'])) {
            $actions[] = 'Open the addon sync history and inspect failed profile rows or API reachability.';
        }
        if ((int) (($observeSummary['scheduled_changes']['schedules_deferred'] ?? 0)) > 0) {
            $actions[] = 'Review deferred scheduled changes and their skip reasons in repricing diagnostics.';
        }
        if ((string) ($observeSummary['renewal_evaluation_mode'] ?? '') === 'phase_b_pending') {
            $actions[] = 'Treat the repricing section as observational only; recurring amount automation is not active from this daily cron yet.';
        }
        if ((string) ($observeSummary['renewal_evaluation_mode'] ?? '') === 'schema_unavailable') {
            $actions[] = 'Load the addon admin page or run the installer migration path so repricing schema tables are created.';
        }
        if ($actions === []) {
            $actions[] = 'No action needed.';
        }
        return $actions;
    }

    /**
     * @param mixed $messages
     * @return array<string,list<string>>
     */
    private function bucketMessages($messages): array
    {
        $buckets = [];
        if (!is_array($messages)) {
            return $buckets;
        }
        foreach ($messages as $message) {
            $text = (string) $message;
            if (strpos($text, 'profile ') === 0) {
                $bucket = 'profile issues';
            } elseif (stripos($text, 'scheduledchangeprocessor') !== false) {
                $bucket = 'scheduled changes';
            } elseif (stripos($text, 'crondriver') !== false) {
                $bucket = 'cron driver';
            } else {
                $bucket = 'general';
            }
            if (!isset($buckets[$bucket])) {
                $buckets[$bucket] = [];
            }
            $buckets[$bucket][] = $text;
        }
        return $buckets;
    }

    /**
     * @param array<string,mixed> $scheduled
     */
    private function topSkipReasons(array $scheduled): string
    {
        if (empty($scheduled['decisions']) || !is_array($scheduled['decisions'])) {
            return '';
        }
        $counts = [];
        foreach ($scheduled['decisions'] as $decision) {
            if (!is_array($decision) || !empty($decision['applied'])) {
                continue;
            }
            $reason = (string) ($decision['skip_reason'] ?? 'deferred');
            if (!isset($counts[$reason])) {
                $counts[$reason] = 0;
            }
            $counts[$reason]++;
        }
        if ($counts === []) {
            return '';
        }
        arsort($counts);
        $parts = [];
        foreach (array_slice($counts, 0, 3, true) as $reason => $count) {
            $parts[] = $reason . ' x' . $count;
        }
        return implode(', ', $parts);
    }

    private function durationLabel(string $startedAt, string $finishedAt): string
    {
        if ($startedAt === '' || $finishedAt === '') {
            return '';
        }
        $ta = strtotime($startedAt);
        $tb = strtotime($finishedAt);
        if ($ta === false || $tb === false || $tb < $ta) {
            return '';
        }
        $delta = $tb - $ta;
        if ($delta < 60) {
            return $delta . 's';
        }
        if ($delta < 3600) {
            return floor($delta / 60) . 'm ' . ($delta % 60) . 's';
        }
        return floor($delta / 3600) . 'h ' . floor(($delta % 3600) / 60) . 'm';
    }
}
