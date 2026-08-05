<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ApiClient;
use ContaboPricing\ProposalMaker;
use ContaboPricing\RequestExecutor;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;

final class ProposalMakerRequestExecutor implements RequestExecutor
{
    /** @var list<array{0:int,1:string,2:int,3:string}> */
    public array $queue = [];
    /** @var list<string> */
    public array $bodies = [];

    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        if ($body !== null) {
            $this->bodies[] = $body;
        }
        $response = array_shift($this->queue);
        if ($response === null) {
            return [200, '{}', 0, ''];
        }
        return $response;
    }
}

final class ProposalMakerTest extends TestCase
{
    private function settings(): Settings
    {
        return new Settings(
            'http://api.local/v1',
            'api-token',
            'notify',
            'INR',
            true,
            3.5,
            365,
            'addonmodules.php?module=contabo_pricing'
        );
    }

    /** @return array<string,mixed> */
    private function request(): array
    {
        return [
            'client_id' => '42',
            'client_name' => 'Acme <Operations>',
            'proposal_title' => 'Secure server proposal',
            'plan_slug' => 'core-vps-1',
            'period_months' => '12',
            'region' => 'Asia (India)',
            'os' => 'Ubuntu 24.04',
            'canonical_family' => 'Core VPS',
            'selections_json' => '{"Storage":"SSD"}',
            'fx_rate' => '90',
            'managed_tier' => 'growth-managed',
            'managed_visibility' => 'show',
            'owner_markup_pct' => '10',
            'owner_markup_scope' => 'provider_and_managed',
            'owner_visibility' => 'internal_only',
            'comparison_visibility' => 'exclude',
            'source_visibility' => 'internal_only',
            'client_notes' => 'Please include the migration assumptions.',
            'internal_notes' => 'Do not send this internal review note.',
        ];
    }

    public function testBuildKeepsOwnerAndInternalFactsOutOfClientJson(): void
    {
        $executor = new ProposalMakerRequestExecutor();
        $executor->queue[] = [200, '{"final_monthly":100,"final_setup":20,"base_monthly_eur":80}', 0, ''];
        $maker = new ProposalMaker($this->settings(), new ApiClient($this->settings(), 8, $executor));

        $result = $maker->build($this->request());
        $snapshot = $result['snapshot'];
        $clientJson = base64_decode((string) $result['attachments'][1]['data'], true);

        self::assertIsString($clientJson);
        $client = json_decode($clientJson, true);
        self::assertIsArray($client);
        self::assertSame(10.0, (float) $snapshot['pricing']['owner_markup_pct']);
        self::assertArrayNotHasKey('api_quote', $client['pricing']);
        self::assertArrayNotHasKey('owner_provider', $client['pricing']);
        self::assertArrayNotHasKey('owner_managed', $client['pricing']);
        self::assertSame('', (string) ($client['notes']['internal'] ?? ''));
        self::assertStringNotContainsString('internal review note', (string) $clientJson);
        self::assertStringNotContainsString('approval required', $result['html']);
        self::assertStringNotContainsString('99.95% SLA', $result['html']);
        self::assertStringContainsString('Growth Managed', $result['html']);
    }

    public function testSilentManagedServiceCountsButIsNotNamedInClientArtifact(): void
    {
        $executor = new ProposalMakerRequestExecutor();
        $executor->queue[] = [200, '{"final_monthly":100,"final_setup":0}', 0, ''];
        $maker = new ProposalMaker($this->settings(), new ApiClient($this->settings(), 8, $executor));

        $request = $this->request();
        $request['managed_visibility'] = 'silent_include';
        $result = $maker->build($request);

        self::assertGreaterThan(1000, (float) $result['snapshot']['pricing']['managed_subtotal']);
        self::assertStringNotContainsString('Growth Managed', $result['html']);
        self::assertStringNotContainsString('Growth Managed', base64_decode((string) $result['attachments'][1]['data'], true));
    }

    public function testBlankOwnerMarkupDefaultsToZero(): void
    {
        $executor = new ProposalMakerRequestExecutor();
        $executor->queue[] = [200, '{"final_monthly":100,"final_setup":0}', 0, ''];
        $maker = new ProposalMaker($this->settings(), new ApiClient($this->settings(), 8, $executor));

        $request = $this->request();
        $request['owner_markup_pct'] = '';
        $result = $maker->build($request);

        self::assertSame(0.0, (float) $result['snapshot']['pricing']['owner_markup_pct']);
        self::assertSame(0.0, (float) $result['snapshot']['pricing']['owner_provider']);
        self::assertSame(0.0, (float) $result['snapshot']['pricing']['owner_managed']);
    }

    public function testEmptySelectionsAreSentAsJsonObjectsToRustApi(): void
    {
        $executor = new ProposalMakerRequestExecutor();
        $executor->queue[] = [200, '{"final_monthly":100,"final_setup":0}', 0, ''];
        $executor->queue[] = [200, '{"final_monthly":120,"final_setup":0}', 0, ''];
        $maker = new ProposalMaker($this->settings(), new ApiClient($this->settings(), 8, $executor));

        $request = $this->request();
        $request['selections_json'] = '{}';
        $request['comparison_plan_slug'] = 'core-vps-2';
        $maker->build($request);

        self::assertCount(2, $executor->bodies);
        foreach ($executor->bodies as $body) {
            $decoded = json_decode($body);
            self::assertIsObject($decoded);
            self::assertIsObject($decoded->selections);
        }
    }

    public function testCodexDocumentOnlyReplacesSafeNarrativeWording(): void
    {
        $executor = new ProposalMakerRequestExecutor();
        $executor->queue[] = [200, '{"final_monthly":100,"final_setup":0}', 0, ''];
        $executor->queue[] = [202, '{"job_id":"proposal-job-1","status":"queued"}', 0, ''];
        $executor->queue[] = [200, '{"status":"succeeded","provider":"codex-cli-safe","document":{"schema_version":"proposal.v1","title":"Codex draft","sections":[{"id":"summary","title":"Summary","blocks":[{"type":"paragraph","text":"A carefully scoped infrastructure recommendation."}]},{"id":"next_steps","title":"Next steps","blocks":[{"type":"list","items":["Confirm the intended operating scope."]}]}]}}', 0, ''];
        $maker = new ProposalMaker($this->settings(), new ApiClient($this->settings(), 8, $executor));

        $result = $maker->generateWithCodex($this->request(), $maker->build($this->request()));

        self::assertSame('codex-cli-safe', $result['report_generation_provider']);
        self::assertSame('codex-cli', $result['narrative']['mode']);
        self::assertStringContainsString('carefully scoped infrastructure recommendation', $result['html']);
        self::assertStringContainsString('Confirm the intended operating scope.', $result['text']);
        self::assertStringNotContainsString('₹', $result['narrative']['content']['opening']);
    }
}
