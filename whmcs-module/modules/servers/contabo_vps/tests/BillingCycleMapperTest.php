<?php
declare(strict_types=1);

use ContaboVps\BillingCycleMapper;
use PHPUnit\Framework\TestCase;

final class BillingCycleMapperTest extends TestCase
{
    protected function setUp(): void
    {
        $GLOBALS['__activity_log'] = [];
    }

    /** @return array<string, array{string,int}> */
    public static function cycles(): array
    {
        return [
            'monthly'          => ['Monthly', 1],
            'quarterly'        => ['Quarterly', 3],
            'semiannually'     => ['Semi-Annually', 6],
            'semiannually alt' => ['Semiannually', 6],
            'annually'         => ['Annually', 12],
            'biennially'       => ['Biennially', 12],
            'triennially'      => ['Triennially', 12],
            'free'             => ['Free Account', 1],
            'onetime'          => ['One Time', 1],
            'unknown'          => ['whatever', 1],
            'empty'            => ['', 1],
            'case+space'       => ['  ANNUALLY ', 12],
        ];
    }

    /** @dataProvider cycles */
    public function testMapsWhmcsCycleToContaboPeriod(string $cycle, int $expected): void
    {
        $this->assertSame($expected, BillingCycleMapper::toPeriod($cycle));
    }

    public function testOverlongCyclesAreLogged(): void
    {
        BillingCycleMapper::toPeriod('Biennially');
        $this->assertNotEmpty($GLOBALS['__activity_log']);
        $this->assertStringContainsString('12-month', $GLOBALS['__activity_log'][0]);
    }

    public function testUnknownCycleIsLogged(): void
    {
        BillingCycleMapper::toPeriod('mystery');
        $this->assertNotEmpty($GLOBALS['__activity_log']);
        $this->assertStringContainsString('unrecognised', $GLOBALS['__activity_log'][0]);
    }
}
