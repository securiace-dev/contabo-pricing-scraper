<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\ChangeClassifier;
use PHPUnit\Framework\TestCase;

/** Replays docs/test-vectors/classifier.json. */
final class ChangeClassifierTest extends TestCase
{
    /** @return array<string,array{0:array<string,mixed>}> */
    public function vectors(): array
    {
        $doc = json_decode((string) file_get_contents(__DIR__ . '/../fixtures/vectors/classifier.json'), true);
        $out = [];
        foreach ($doc['cases'] as $c) {
            $out[$c['name']] = [$c];
        }
        return $out;
    }

    /** @dataProvider vectors
     * @param array<string,mixed> $case */
    public function testVector(array $case): void
    {
        $this->assertSame($case['expected']['bucket'], ChangeClassifier::classify($case['input']), $case['name']);
    }

    public function testOptionPriceBoundaryIsStrictTenPercent(): void
    {
        $d = static function ($b, $a) {
            return ['kind' => 'mandatory_option_price_change', 'cost_before' => $b, 'cost_after' => $a];
        };
        $this->assertSame('safe', ChangeClassifier::classify($d('10.00', '11.00')));
        $this->assertSame('safe', ChangeClassifier::classify($d('10.00', '9.00')));
        $this->assertSame('risky', ChangeClassifier::classify($d('10.00', '11.01')));
        $this->assertSame('risky', ChangeClassifier::classify($d('0', '1')));
    }

    public function testUnresolvedOsImageRemovedFailsClosed(): void
    {
        $this->assertSame('anomaly', ChangeClassifier::classify(['kind' => 'os_image_removed']));
        $this->assertSame('anomaly', ChangeClassifier::classify([]));
    }
}
