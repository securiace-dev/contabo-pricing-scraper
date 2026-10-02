<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\DecisionRecorder;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class DecisionRecorderTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testRecordsRulesAndJevAsJson(): void
    {
        $rec = new DecisionRecorder();
        $id = $rec->record(7, 'needs_review', ['gates' => ['price_sanity' => ['ok' => true]]], ['status' => 'ok', 'veto' => true], 0.8, DecisionRecorder::BY_JEV);
        $row = $rec->find($id);
        $this->assertSame(7, (int) $row['run_id']);
        $this->assertSame('needs_review', $row['outcome']);
        $this->assertSame('jev', $row['decided_by']);
        $this->assertSame(true, $row['rules']['gates']['price_sanity']['ok']);
        $this->assertSame(true, $row['jev']['veto']);
        $this->assertEquals(0.8, $row['threshold']);
        $this->assertNull($row['admin_id']);
    }

    public function testAdminDecisionWithoutJev(): void
    {
        $rec = new DecisionRecorder();
        $id = $rec->record(9, 'auto_import', '{"manual":true}', null, null, DecisionRecorder::BY_ADMIN, 42);
        $row = $rec->find($id);
        $this->assertNull($row['jev']);
        $this->assertSame(42, (int) $row['admin_id']);
        $this->assertSame('{"manual":true}', $row['rules_json']);
        $this->assertNull($rec->find(9999));
    }
}
