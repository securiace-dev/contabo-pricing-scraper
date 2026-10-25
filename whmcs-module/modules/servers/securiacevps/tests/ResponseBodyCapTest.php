<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use SecuriAceVps\ResponseBodyCap;

final class ResponseBodyCapTest extends TestCase
{
    public function testBufferNeverGrowsPastFourMebibytes(): void
    {
        $cap = new ResponseBodyCap();
        $chunk = str_repeat('a', 1024 * 1024);
        for ($i = 0; $i < 4; $i++) {
            $this->assertSame(strlen($chunk), $cap->write($chunk));
            $this->assertLessThanOrEqual(ResponseBodyCap::MAX_BYTES, $cap->size());
            $this->assertLessThanOrEqual(ResponseBodyCap::MAX_BYTES, $cap->peak());
        }
        $this->assertSame(ResponseBodyCap::MAX_BYTES, $cap->size());
        $this->assertFalse($cap->overflowed());

        $this->assertSame(0, $cap->write('x'));
        $this->assertTrue($cap->overflowed());
        $this->assertSame('', $cap->body());
        $this->assertSame(0, $cap->size());
        $this->assertLessThanOrEqual(ResponseBodyCap::MAX_BYTES, $cap->peak());
    }

    public function testSingleOversizeChunkIsNotStored(): void
    {
        $cap = new ResponseBodyCap();
        $this->assertSame(0, $cap->write(str_repeat('b', ResponseBodyCap::MAX_BYTES + 1)));
        $this->assertTrue($cap->overflowed());
        $this->assertSame('', $cap->body());
        $this->assertSame(0, $cap->peak());
        $this->assertSame(0, $cap->size());
    }
}
