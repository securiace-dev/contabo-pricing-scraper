<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Lock;
use ContaboPricing\ProfileManager;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class SyncEngineLockTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_sync_log'] = [];
    }

    private function lock(?string $token): object
    {
        return new class($token) extends Lock {
            public $acquired = [];
            public $released = [];
            private $token;
            public function __construct(?string $t) { $this->token = $t; }
            public function acquire(string $name, int $ttlSeconds): ?string
            {
                $this->acquired[] = [$name, $ttlSeconds];
                return $this->token;
            }
            public function release(string $name, string $token): void
            {
                $this->released[] = [$name, $token];
            }
        };
    }

    private function engine(object $lock, ?\Throwable $metaThrows = null): SyncEngine
    {
        $settings = new Settings('manual', 'EUR', false, 0.0, 365, '');
        $api = new class($metaThrows) extends StubPlanSource {
            private $ex;
            public function __construct(?\Throwable $e) { $this->ex = $e; }
            public function meta(): array
            {
                if ($this->ex !== null) { throw $this->ex; }
                return ['snapshot_meta' => ['generated_at' => '2026-07-30T00:00:00Z']];
            }
        };
        $pm = new class($settings) extends ProfileManager {
            public function listProfiles(bool $activeOnly = true, bool $includeTrashed = false): array { return []; }
        };
        return new SyncEngine($settings, $api, $pm, null, $lock);
    }

    public function testContentionSkipsWithoutTouchingSyncLog(): void
    {
        $lock = $this->lock(null);
        $summary = $this->engine($lock)->run('cron');
        $this->assertSame('skipped_locked', $summary['status']);
        $this->assertSame([['contabo_sync_run', 900]], $lock->acquired);
        $this->assertSame([], $lock->released);
        $this->assertSame([], Capsule::$tables['mod_contabo_sync_log']);
    }

    public function testLockReleasedAfterRun(): void
    {
        $lock = $this->lock('tok-1');
        $summary = $this->engine($lock)->run('manual');
        $this->assertNotSame('skipped_locked', $summary['status']);
        $this->assertSame([['contabo_sync_run', 'tok-1']], $lock->released);
        $this->assertCount(1, Capsule::$tables['mod_contabo_sync_log']);
    }

    public function testObserveOnlyDoesNotTakeLock(): void
    {
        $lock = $this->lock(null);
        $summary = $this->engine($lock)->run('manual', true);
        $this->assertSame('preview', $summary['status']);
        $this->assertSame([], $lock->acquired);
    }
}
