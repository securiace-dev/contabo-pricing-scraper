<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\DailyOperationsDigestBuilder;
use PHPUnit\Framework\TestCase;

final class DailyOperationsDigestBuilderTest extends TestCase
{
    public function testBuildsChangedDigestWithProfileSummary(): void
    {
        $digest = (new DailyOperationsDigestBuilder())->build(
            [
                'status' => 'succeeded',
                'started_at' => '2026-09-07 10:00:00',
                'finished_at' => '2026-09-07 10:00:30',
                'snapshot_generated_at' => '2026-09-07T09:58:00Z',
                'catalog_scraper_version' => 'scrape-test-1',
                'profiles_checked' => 4,
                'profiles_changed' => 1,
                'products_updated' => 2,
                'cycles_applied' => 8,
                'cycles_skipped' => 1,
                'cycles_evaluated' => 9,
                'change_list' => [
                    [
                        'profile_slug' => 'vps-1',
                        'plan_slug' => 'cloud-vps',
                        'period_months' => 12,
                        'previous_final' => 499.00,
                        'new_final' => 549.00,
                        'currency' => 'INR',
                    ],
                ],
                'errors' => [],
            ],
            [
                'status' => 'healthy',
                'started_at' => '2026-09-07 10:00:31',
                'finished_at' => '2026-09-07 10:00:35',
                'candidate_mapped_services' => 0,
                'renewal_evaluation_mode' => 'no_candidates',
                'renewal_evaluation_message' => 'No active mapped services needed repricing observation today.',
                'scheduled_changes' => [
                    'schedules_processed' => 0,
                    'schedules_applied' => 0,
                    'schedules_deferred' => 0,
                    'services_evaluated' => 0,
                    'catalog_intents_logged' => 0,
                    'decisions' => [],
                ],
                'errors' => [],
                'notes' => [],
            ]
        );

        $this->assertSame('changed', $digest['severity']);
        $this->assertStringContainsString('[changed]', $digest['subject']);
        $this->assertStringContainsString('Profile vps-1 (cloud-vps, 12 mo): 499.00 -> 549.00 INR.', $digest['message']);
        $this->assertStringContainsString('No action needed.', $digest['message']);
    }

    public function testBuildsHealthyNoChangeDigest(): void
    {
        $digest = (new DailyOperationsDigestBuilder())->build(
            [
                'status' => 'no-change',
                'started_at' => '2026-09-07 10:00:00',
                'finished_at' => '2026-09-07 10:00:20',
                'profiles_checked' => 2,
                'profiles_changed' => 0,
                'products_updated' => 0,
                'cycles_applied' => 0,
                'cycles_skipped' => 0,
                'cycles_evaluated' => 0,
                'snapshot_unchanged' => true,
                'change_list' => [],
                'errors' => [],
            ],
            [
                'status' => 'healthy',
                'started_at' => '2026-09-07 10:00:21',
                'finished_at' => '2026-09-07 10:00:25',
                'candidate_mapped_services' => 0,
                'renewal_evaluation_mode' => 'no_candidates',
                'renewal_evaluation_message' => 'No active mapped services needed repricing observation today.',
                'scheduled_changes' => [
                    'schedules_processed' => 0,
                    'schedules_applied' => 0,
                    'schedules_deferred' => 0,
                    'services_evaluated' => 0,
                    'catalog_intents_logged' => 0,
                    'decisions' => [],
                ],
                'errors' => [],
                'notes' => [],
            ]
        );

        $this->assertSame('healthy', $digest['severity']);
        $this->assertStringContainsString('Sync completed without upstream or catalog-visible changes.', $digest['message']);
        $this->assertStringContainsString('Upstream snapshot timestamp matched the last successful sync', $digest['message']);
    }

    public function testBuildsWarningDigestForObserveOnlyDormantRepricing(): void
    {
        $digest = (new DailyOperationsDigestBuilder())->build(
            [
                'status' => 'succeeded',
                'started_at' => '2026-09-07 10:00:00',
                'finished_at' => '2026-09-07 10:00:20',
                'observe_only' => false,
                'profiles_checked' => 1,
                'profiles_changed' => 0,
                'products_updated' => 0,
                'cycles_applied' => 0,
                'cycles_skipped' => 0,
                'cycles_evaluated' => 0,
                'change_list' => [],
                'errors' => [],
            ],
            [
                'status' => 'warning',
                'started_at' => '2026-09-07 10:00:21',
                'finished_at' => '2026-09-07 10:00:22',
                'candidate_mapped_services' => 14,
                'renewal_evaluation_mode' => 'phase_b_pending',
                'renewal_evaluation_message' => 'Active mapped services were identified, but per-service RenewalEngine evaluation is intentionally inactive in the daily cron until Phase B is wired.',
                'scheduled_changes' => [
                    'schedules_processed' => 2,
                    'schedules_applied' => 0,
                    'schedules_deferred' => 1,
                    'services_evaluated' => 3,
                    'catalog_intents_logged' => 1,
                    'decisions' => [
                        ['applied' => false, 'skip_reason' => 'phase_observe_only'],
                        ['applied' => false, 'skip_reason' => 'phase_observe_only'],
                        ['applied' => false, 'skip_reason' => 'awaiting_admin_approval'],
                    ],
                ],
                'errors' => [],
                'notes' => ['Mapped-service counts are observational only; no recurring amounts were recalculated.'],
            ]
        );

        $this->assertSame('warning', $digest['severity']);
        $this->assertStringContainsString('per-service RenewalEngine evaluation is intentionally inactive', $digest['message']);
        $this->assertStringContainsString('Top repricing skip/defer reasons: phase_observe_only x2, awaiting_admin_approval x1.', $digest['message']);
        $this->assertStringContainsString('Treat the repricing section as observational only', $digest['message']);
    }

    public function testBuildsFailedDigestWithActionableErrors(): void
    {
        $digest = (new DailyOperationsDigestBuilder())->build(
            [
                'status' => 'failed',
                'started_at' => '2026-09-07 10:00:00',
                'finished_at' => '2026-09-07 10:00:05',
                'profiles_checked' => 2,
                'profiles_changed' => 0,
                'products_updated' => 0,
                'cycles_applied' => 0,
                'cycles_skipped' => 0,
                'cycles_evaluated' => 0,
                'change_list' => [],
                'errors' => [
                    'profile starter: API timeout talking to Contabo',
                    'profile business: period 3 mo not offered by Contabo for cloud-vps',
                ],
            ],
            [
                'status' => 'warning',
                'started_at' => '2026-09-07 10:00:06',
                'finished_at' => '2026-09-07 10:00:08',
                'candidate_mapped_services' => 4,
                'renewal_evaluation_mode' => 'schema_unavailable',
                'renewal_evaluation_message' => 'Repricing observation skipped because required Contabo Pricing tables are not available yet.',
                'scheduled_changes' => [
                    'schedules_processed' => 0,
                    'schedules_applied' => 0,
                    'schedules_deferred' => 0,
                    'services_evaluated' => 0,
                    'catalog_intents_logged' => 0,
                    'decisions' => [],
                ],
                'errors' => ['ScheduledChangeProcessor failed: table missing'],
                'notes' => [],
            ],
            ['hook_errors' => ['Daily cron hook failed before the digest completed: example exception']]
        );

        $this->assertSame('failed', $digest['severity']);
        $this->assertStringContainsString('Profile issues:', $digest['message']);
        $this->assertStringContainsString('Scheduled changes:', $digest['message']);
        $this->assertStringContainsString('Hook failures:', $digest['message']);
        $this->assertStringContainsString('Review the listed errors before trusting catalog or repricing state.', $digest['message']);
    }
}
