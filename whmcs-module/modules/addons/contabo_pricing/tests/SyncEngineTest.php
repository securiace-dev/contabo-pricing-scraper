<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

final class SyncEngineTest extends TestCase
{
    public function testSyncEngineNoChangeShortCircuit(): void
    {
        $this->markTestIncomplete(
            'needs Capsule fixture (in-memory SQLite via illuminate/database); '
            . 'add when composer dep weight is acceptable.'
        );
    }
}
