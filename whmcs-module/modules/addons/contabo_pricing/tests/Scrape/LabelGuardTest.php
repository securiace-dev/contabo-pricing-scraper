<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\LabelGuard;
use PHPUnit\Framework\TestCase;

/** Replays docs/test-vectors/whitelabel.json 'safe' cases through LabelGuard. */
final class LabelGuardTest extends TestCase
{
    /** @return array<string,array{0:array<string,mixed>}> */
    public function vectors(): array
    {
        $doc = json_decode((string) file_get_contents(__DIR__ . '/../fixtures/vectors/whitelabel.json'), true);
        $out = [];
        foreach ($doc['cases'] as $c) {
            if (($c['input']['fn'] ?? '') === 'safe') {
                $out[$c['name']] = [$c, $doc['config']['forbidden_terms']];
            }
        }
        return $out;
    }

    /** @dataProvider vectors
     * @param array<string,mixed> $case
     * @param list<string> $defaultTerms */
    public function testVector(array $case, array $defaultTerms): void
    {
        $terms = $case['input']['config']['forbidden_terms'] ?? $defaultTerms;
        $guard = new LabelGuard($terms);
        $clean = $guard->isClean($case['input']['text']);
        $this->assertSame($case['expected']['outcome'] === 'pass', $clean, $case['name']);
    }

    public function testDefaultTermList(): void
    {
        $g = new LabelGuard();
        foreach (['Powered by Contabo', 'Cloud VPS 10', 'cloud_vps', 'Cloud-VDS L', 'Storage  VPS', 'CB-10', 'via 20i', 'VULTR'] as $bad) {
            $this->assertFalse($g->isClean($bad), $bad);
        }
        foreach (['120i', 'SecuriAce Cloud Compute S', 'CBSE results', 'Cloud Compute'] as $ok) {
            $this->assertTrue($g->isClean($ok), $ok);
        }
        $this->assertSame(['contabo'], $g->scan('Hosted at contabo'));
    }
}
