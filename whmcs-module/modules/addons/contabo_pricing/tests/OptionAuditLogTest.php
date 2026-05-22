<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\OptionAuditLog;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.2 — OptionAuditLog.
 *
 * Uses a capturing subclass (override storeRow) so no DB is touched, mirroring
 * the DecisionLog test pattern.
 */
final class OptionAuditLogTest extends TestCase
{
    /** @var array<int,array<string,mixed>> */
    private $rows = [];

    private function logger(string $batch = 'batch-123'): OptionAuditLog
    {
        $captured = &$this->rows;
        return new class($batch, $captured) extends OptionAuditLog {
            /** @var array<int,array<string,mixed>> */
            private $sink;
            public function __construct(string $batch, array &$sink)
            {
                parent::__construct($batch);
                $this->sink = &$sink;
            }
            protected function storeRow(array $row): int
            {
                $this->sink[] = $row;
                return count($this->sink);
            }
        };
    }

    public function testRecordBuildsCanonicalRow(): void
    {
        $log = $this->logger('batch-abc');
        $id = $log->record(
            42,
            'Networking:IPv4',
            'tblproductconfigoptionssub',
            null,
            OptionAuditLog::ACTION_OBSERVED,
            null,
            ['label' => 'Additional IPv4', 'price' => 103.5],
            'preview'
        );

        $this->assertSame(1, $id);
        $this->assertCount(1, $this->rows);
        $row = $this->rows[0];
        $this->assertSame('batch-abc', $row['sync_batch_id']);
        $this->assertSame(42, $row['profile_id']);
        $this->assertSame('Networking:IPv4', $row['dimension_key']);
        $this->assertSame('tblproductconfigoptionssub', $row['target_table']);
        $this->assertNull($row['target_id']);
        $this->assertSame('observed', $row['action']);
        $this->assertNull($row['old_value_json']);
        $this->assertSame('{"label":"Additional IPv4","price":103.5}', $row['new_value_json']);
        $this->assertSame('preview', $row['note']);
        $this->assertNotEmpty($row['created_at']);
    }

    public function testNullValuesStayNullNotEmptyJson(): void
    {
        $log = $this->logger();
        $log->record(1, null, 'tblproductconfiggroups', 7, OptionAuditLog::ACTION_INSERT, null, null);
        $row = $this->rows[0];
        $this->assertNull($row['old_value_json']);
        $this->assertNull($row['new_value_json']);
        $this->assertNull($row['dimension_key']);
        $this->assertSame(7, $row['target_id']);
    }

    public function testObserveHelperUsesObservedAction(): void
    {
        $log = $this->logger();
        $log->observe(5, 'Image', 'tblproductconfigoptions', ['optionname' => 'Image']);
        $this->assertSame('observed', $this->rows[0]['action']);
        $this->assertSame('{"optionname":"Image"}', $this->rows[0]['new_value_json']);
    }

    public function testNoteIsTruncatedTo255(): void
    {
        $log = $this->logger();
        $log->record(1, null, 't', null, OptionAuditLog::ACTION_ERROR, null, null, str_repeat('x', 400));
        $this->assertSame(255, mb_strlen((string) $this->rows[0]['note']));
    }

    public function testBatchIdExposed(): void
    {
        $this->assertSame('batch-123', $this->logger()->batchId());
    }
}
