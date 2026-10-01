<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use ContaboPricing\Scrape\SourceConfigRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class SourceConfigRepositoryTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        (new Installer())->migrateTo15();
    }

    public function testSeededRowsAreDisabledAndListed(): void
    {
        $repo = new SourceConfigRepository();
        $this->assertCount(4, $repo->all());
        $this->assertSame([], $repo->enabled());
    }

    public function testSaveSealsKeyAndBlankKeepsExisting(): void
    {
        $repo = new SourceConfigRepository();
        $repo->save('alterlab', [
            'enabled' => 1,
            'base_url' => 'https://alterlab.example',
            'api_key' => 'sk-live-abcdef123456',
            'priority' => 2,
            'options' => ['extraction_schema' => ['a' => 1]],
        ]);
        $row = $repo->find('alterlab');
        $this->assertStringStartsWith('ENC:', (string) $row['api_key_enc']);
        $this->assertStringNotContainsString('abcdef123456', (string) $row['api_key_enc']);
        $this->assertCount(1, $repo->enabled());

        $repo->save('alterlab', ['api_key' => '', 'priority' => 1]);
        $cfg = $repo->toConfig((array) $repo->find('alterlab'));
        $this->assertSame('sk-live-abcdef123456', $cfg->apiKey);
        $this->assertSame('https://alterlab.example', $cfg->baseUrl);
        $this->assertSame(['extraction_schema' => ['a' => 1]], $cfg->options);
        $this->assertSame(1, (int) $repo->find('alterlab')['priority']);
    }

    public function testPriorityOrdering(): void
    {
        $repo = new SourceConfigRepository();
        $repo->save('treg', ['priority' => 1]);
        $repo->save('alterlab', ['priority' => 5]);
        $ids = array_map(static function (array $r): string { return (string) $r['source_id']; }, $repo->all());
        $this->assertSame(['treg', 'alterlab'], array_slice($ids, 0, 2));
    }

    public function testUnknownSourceRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new SourceConfigRepository())->save('nope', ['enabled' => 1]);
    }

    public function testRecordAttemptOutcome(): void
    {
        $repo = new SourceConfigRepository();
        $repo->recordAttemptOutcome('treg', false, 'HTTP 500');
        $repo->recordAttemptOutcome('treg', false, 'HTTP 502');
        $row = $repo->find('treg');
        $this->assertSame(2, (int) $row['consecutive_failures']);
        $this->assertSame('HTTP 502', $row['last_error']);
        $repo->recordAttemptOutcome('treg', true);
        $row = $repo->find('treg');
        $this->assertSame(0, (int) $row['consecutive_failures']);
        $this->assertNotEmpty($row['last_ok_at']);
    }
}
