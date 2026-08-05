<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ApiClient;
use ContaboPricing\ProposalMaker;
use ContaboPricing\RequestExecutor;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;

final class ProposalRequestExecutor implements RequestExecutor
{
    /** @var array<int,array{0:int,1:string,2:int,3:string}> */
    public $queue = [];
    /** @var array<int,array<string,mixed>> */
    public $calls = [];

    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $this->calls[] = [
            'method' => $method,
            'url' => $url,
            'headers' => $headers,
            'body' => $body,
            'timeout' => $timeoutSec,
        ];
        $response = array_shift($this->queue);
        return $response === null ? [500, '{}', 0, 'missing test response'] : $response;
    }
}

final class ProposalMakerTest extends TestCase
{
    /** @param array<string,mixed> $overrides */
    private function settings(array $overrides = []): Settings
    {
        $v = array_merge([
            'ai_enabled' => false,
            'ai_provider' => 'openai',
            'ai_base' => 'https://api.openai.com/v1',
            'ai_key' => '',
            'ai_model' => 'gpt-5.6-luna',
            'structured' => true,
            'tokens' => 1200,
            'timeout' => 30,
            'retries' => 0,
            'advisory' => 0.10,
            'delivery' => false,
            'provider_tax_charged' => false,
            'provider_prices_include_tax' => false,
            'provider_tax_rate' => 0.0,
            'provider_tax_recoverable' => false,
            'payment_buffer' => 0.0,
            'output_tax_enabled' => false,
            'output_tax_verified' => false,
            'output_tax_mode' => 'all_inclusive_no_separate_tax',
            'output_tax_rate' => 18.0,
        ], $overrides);

        return new Settings(
            'http://api.local/v1',
            '',
            'notify',
            'INR',
            false,
            0.0,
            365,
            'addonmodules.php?module=contabo_pricing',
            (bool) $v['ai_enabled'],
            (string) $v['ai_provider'],
            (string) $v['ai_base'],
            (string) $v['ai_key'],
            (string) $v['ai_model'],
            (string) ($v['ai_provider'] === 'openai' ? 'responses' : 'chat_completions'),
            (bool) $v['structured'],
            (int) $v['tokens'],
            (int) $v['timeout'],
            (int) $v['retries'],
            (float) $v['advisory'],
            (bool) $v['delivery'],
            (bool) $v['provider_tax_charged'],
            (bool) $v['provider_prices_include_tax'],
            (float) $v['provider_tax_rate'],
            (bool) $v['provider_tax_recoverable'],
            (float) $v['payment_buffer'],
            (bool) $v['output_tax_enabled'],
            (bool) $v['output_tax_verified'],
            (string) $v['output_tax_mode'],
            (float) $v['output_tax_rate']
        );
    }

    /** @return array<string,mixed> */
    private function request(array $overrides = []): array
    {
        return array_merge([
            'client_name' => 'Example Client',
            'proposal_title' => 'Infrastructure proposal',
            'plan_slug' => 'cloud-vps-10',
            'period_months' => 1,
            'currency' => 'INR',
            'fx_rate' => 100,
            'fx_card_markup_pct' => 0,
            'owner_margin_pct' => 0,
            'owner_margin_scope' => 'provider_only',
            'managed_tier' => '',
            'provider_visibility' => 'show',
            'configuration_visibility' => 'show',
            'managed_visibility' => 'exclude',
            'owner_visibility' => 'internal_only',
            'tax_visibility' => 'total_only',
            'comparison_visibility' => 'exclude',
            'client_notes_visibility' => 'show',
            'selections_json' => '{}',
            'narrative_mode' => 'deterministic',
        ], $overrides);
    }

    /** @return array{0:ProposalMaker,1:ProposalRequestExecutor,2:ProposalRequestExecutor} */
    private function maker(Settings $settings, float $monthlyEur = 10.0, float $setupEur = 0.0): array
    {
        $apiExecutor = new ProposalRequestExecutor();
        $apiExecutor->queue[] = [200, json_encode([
            'plan_slug' => 'cloud-vps-10',
            'period_months' => 1,
            'currency' => 'EUR',
            'configured_monthly_eur' => $monthlyEur,
            'setup_fee_eur' => $setupEur,
            'gst_amount_eur' => 0,
            'fx_markup' => 0,
        ]), 0, ''];
        $aiExecutor = new ProposalRequestExecutor();
        $api = new ApiClient($settings, 8, $apiExecutor);
        return [new ProposalMaker($settings, $api, $aiExecutor), $apiExecutor, $aiExecutor];
    }

    public function testOpenAiDefaultsAndAdminOverrideAreExplicit(): void
    {
        $defaults = $this->settings();
        $this->assertSame('openai', $defaults->proposalAiProvider);
        $this->assertSame('gpt-5.6-luna', $defaults->proposalAiModel);
        $this->assertSame('responses', $defaults->proposalAiRequestStyle);

        $override = $this->settings(['ai_model' => 'admin-deployment-2']);
        $this->assertSame('admin-deployment-2', $override->proposalAiModel);
    }

    public function testFortyFivePercentOwnerAdjustmentStaysFortyFivePercent(): void
    {
        [$maker] = $this->maker($this->settings());
        $result = $maker->build($this->request(['owner_margin_pct' => 45]));

        $this->assertSame(1000.0, $result['internal']['pricing']['provider_landed_period']);
        $this->assertSame(450.0, $result['internal']['pricing']['provider_owner_adjustment']);
        $this->assertSame(1450.0, $result['client']['pricing']['totals'][0]['amount']);
    }

    public function testProviderTaxExclusiveCashAndRecoverabilityRemainSeparate(): void
    {
        [$nonRecoverable] = $this->maker($this->settings([
            'provider_tax_charged' => true,
            'provider_tax_rate' => 18,
        ]), 100.0);
        $nonRecoverableResult = $nonRecoverable->build($this->request());
        $this->assertSame(18.0, $nonRecoverableResult['internal']['pricing']['provider_tax_cash_eur']);
        $this->assertSame(11800.0, $nonRecoverableResult['internal']['pricing']['provider_landed_period']);

        [$recoverable] = $this->maker($this->settings([
            'provider_tax_charged' => true,
            'provider_tax_rate' => 18,
            'provider_tax_recoverable' => true,
        ]), 100.0);
        $recoverableResult = $recoverable->build($this->request());
        $this->assertSame(18.0, $recoverableResult['internal']['pricing']['provider_tax_cash_eur']);
        $this->assertSame(10000.0, $recoverableResult['internal']['pricing']['provider_landed_period']);
    }

    public function testTaxInclusiveProviderPriceIsDecomposedBeforeRecoverability(): void
    {
        [$nonRecoverable] = $this->maker($this->settings([
            'provider_tax_charged' => true,
            'provider_prices_include_tax' => true,
            'provider_tax_rate' => 18,
        ]), 118.0);
        $result = $nonRecoverable->build($this->request());
        $this->assertSame(118.0, $result['internal']['pricing']['provider_quoted_monthly_eur']);
        $this->assertSame(100.0, $result['internal']['pricing']['provider_net_monthly_eur']);
        $this->assertSame(18.0, $result['internal']['pricing']['provider_tax_cash_eur']);
        $this->assertSame(11800.0, $result['internal']['pricing']['provider_landed_period']);

        [$recoverable] = $this->maker($this->settings([
            'provider_tax_charged' => true,
            'provider_prices_include_tax' => true,
            'provider_tax_rate' => 18,
            'provider_tax_recoverable' => true,
        ]), 118.0);
        $this->assertSame(10000.0, $recoverable->build($this->request())['internal']['pricing']['provider_landed_period']);
    }

    public function testContradictoryTaxInclusiveProviderConfigurationFailsClosed(): void
    {
        [$maker] = $this->maker($this->settings(['provider_prices_include_tax' => true]));
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('tax-inclusive');
        $maker->build($this->request());
    }

    public function testNonEmptySelectionsRequireExplicitApiCertification(): void
    {
        [$maker] = $this->maker($this->settings());
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('has not certified');
        $maker->build($this->request(['selections_json' => '{"region":"India"}']));
    }

    public function testOutputTaxRequiresEnabledVerifiedAndGstExclusiveMode(): void
    {
        [$unverified] = $this->maker($this->settings(['output_tax_enabled' => true]));
        try {
            $unverified->build($this->request());
            $this->fail('Unverified output GST must fail closed.');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('registration evidence', $e->getMessage());
        }

        [$wrongMode] = $this->maker($this->settings([
            'output_tax_enabled' => true,
            'output_tax_verified' => true,
        ]));
        try {
            $wrongMode->build($this->request());
            $this->fail('Non-exclusive output GST mode must fail closed.');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('GST-exclusive', $e->getMessage());
        }

        [$effective] = $this->maker($this->settings([
            'output_tax_enabled' => true,
            'output_tax_verified' => true,
            'output_tax_mode' => 'gst_exclusive',
            'output_tax_rate' => 18,
        ]));
        $result = $effective->build($this->request());
        $this->assertTrue($result['internal']['pricing']['output_tax_enabled']);
        $this->assertSame(180.0, $result['internal']['pricing']['provider_output_tax']);
        $this->assertSame(1180.0, $result['client']['pricing']['totals'][0]['amount']);
    }

    public function testCalculatedOnlyContributesWithoutLeakingItsLineLabel(): void
    {
        [$maker] = $this->maker($this->settings());
        $result = $maker->build($this->request([
            'managed_tier' => 'solo-managed',
            'managed_visibility' => 'calculated_only',
        ]));

        $this->assertTrue($result['client']['pricing']['mixed_billing_terms']);
        $this->assertSame(15400.0, $result['client']['pricing']['combined_initial_commitment']);
        $this->assertNull($result['client']['managed_service']);
        $client = $result['client_json'] . $result['client_html'] . $result['client_text'];
        $this->assertStringNotContainsString('Solo Managed', $client);
        $this->assertStringContainsString('Additional annual commitment', $client);
    }

    public function testManagedQuantityScalesAnnualAmountAndFounderMinutes(): void
    {
        [$maker] = $this->maker($this->settings());
        $result = $maker->build($this->request([
            'managed_tier' => 'solo-managed',
            'managed_quantity' => 3,
            'managed_visibility' => 'show',
        ]));

        $managed = $result['internal']['managed_service'];
        $this->assertSame(3, $managed['quantity']);
        $this->assertSame(1440000, $managed['annual_price_per_server_minor']);
        $this->assertSame(4320000, $managed['annual_price_total_minor']);
        $this->assertSame(60, $managed['founder_minutes_per_server_per_month']);
        $this->assertSame(180, $managed['founder_minutes_per_month_total']);
        $this->assertSame(43200.0, $result['internal']['pricing']['managed_base_annual_inr']);
        $this->assertSame(44200.0, $result['client']['pricing']['combined_initial_commitment']);
        $this->assertSame(3, $result['client']['managed_service']['quantity']);
        $this->assertSame(180, $result['client']['managed_service']['founder_minutes_per_month_total']);
        $this->assertStringContainsString('3 managed server(s)', $result['client_html']);
    }

    public function testManagedQuantityRejectsValuesOutsideOneToNinetyNine(): void
    {
        foreach ([0, 100, '1.5', ''] as $quantity) {
            [$maker] = $this->maker($this->settings());
            try {
                $maker->build($this->request([
                    'managed_tier' => 'solo-managed',
                    'managed_quantity' => $quantity,
                ]));
                $this->fail('Expected invalid managed quantity to be rejected.');
            } catch (\InvalidArgumentException $e) {
                $this->assertStringContainsString('Managed server quantity', $e->getMessage());
            }
        }
    }

    public function testAuthoritativePlanComparisonShowsSameTermTotalAndFamilyWarning(): void
    {
        [$maker, $apiExecutor] = $this->maker($this->settings());
        $apiExecutor->queue[] = [200, '{"slug":"cloud-vps-10","canonical_family":"Core VPS"}', 0, ''];
        $apiExecutor->queue[] = [200, json_encode([
            'plan_slug' => 'performance-vps-20',
            'period_months' => 1,
            'currency' => 'EUR',
            'configured_monthly_eur' => 20,
            'setup_fee_eur' => 0,
            'gst_amount_eur' => 0,
            'fx_markup' => 0,
        ]), 0, ''];
        $apiExecutor->queue[] = [200, '{"slug":"performance-vps-20","canonical_family":"Performance VPS"}', 0, ''];

        $result = $maker->build($this->request([
            'comparison_plan_slugs' => 'performance-vps-20',
            'comparison_visibility' => 'show',
        ]));

        $this->assertSame(1000.0, $result['client']['pricing']['totals'][0]['amount']);
        $this->assertCount(1, $result['internal']['alternatives']);
        $this->assertTrue($result['internal']['alternatives'][0]['family_mismatch']);
        $this->assertSame(2000.0, $result['internal']['alternatives'][0]['selected_term_total']);
        $this->assertSame('performance-vps-20', $result['client']['comparisons'][0]['label']);
        $this->assertSame('1 month term', $result['client']['comparisons'][0]['cycle']);
        $this->assertTrue($result['client']['comparisons'][0]['non_billing_comparison']);
        $this->assertStringContainsString('different provider family', $result['client']['comparisons'][0]['warning']);
        $this->assertStringContainsString('Performance VPS', implode("\n", $result['internal']['warnings']));
        $this->assertStringContainsString('informational and do not add', $result['client_html']);
        $this->assertCount(4, $apiExecutor->calls);
        $this->assertStringEndsWith('/quote', $apiExecutor->calls[2]['url']);
        $alternativeRequest = json_decode((string) $apiExecutor->calls[2]['body'], true);
        $this->assertSame('performance-vps-20', $alternativeRequest['plan_slug']);
        $this->assertSame(1, $alternativeRequest['period_months']);
    }

    public function testTotalOnlyComparisonHidesAlternateSlugButPreservesAuthoritativeTotal(): void
    {
        [$maker, $apiExecutor] = $this->maker($this->settings());
        $apiExecutor->queue[] = [200, '{"slug":"cloud-vps-10","canonical_family":"Core VPS"}', 0, ''];
        $apiExecutor->queue[] = [200, json_encode([
            'plan_slug' => 'cloud-vps-20',
            'period_months' => 1,
            'currency' => 'EUR',
            'configured_monthly_eur' => 15,
            'setup_fee_eur' => 0,
            'gst_amount_eur' => 0,
            'fx_markup' => 0,
        ]), 0, ''];
        $apiExecutor->queue[] = [200, '{"slug":"cloud-vps-20","canonical_family":"Core VPS"}', 0, ''];

        $result = $maker->build($this->request([
            'comparison_plan_slugs' => 'cloud-vps-20',
            'comparison_visibility' => 'total_only',
        ]));
        $this->assertSame('Alternative 1', $result['client']['comparisons'][0]['label']);
        $this->assertSame(1500.0, $result['client']['comparisons'][0]['selected_term_total']);
        $clientArtifacts = $result['client_json'] . $result['client_html'] . $result['client_text'];
        $this->assertStringNotContainsString('cloud-vps-20', $clientArtifacts);
        $this->assertStringNotContainsString('Core VPS', $clientArtifacts);
    }

    public function testComparisonInputIsBoundedAndVisibilityIsFieldSpecific(): void
    {
        [$tooMany] = $this->maker($this->settings());
        try {
            $tooMany->build($this->request([
                'comparison_plan_slugs' => 'a-1,b-2,c-3,d-4,e-5',
                'comparison_visibility' => 'show',
            ]));
            $this->fail('Expected more than four alternatives to fail.');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString('no more than four', $e->getMessage());
        }

        [$unsafeMode] = $this->maker($this->settings());
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Alternatives visibility');
        $unsafeMode->build($this->request(['comparison_visibility' => 'silent_include']));
    }

    public function testFieldSpecificVisibilityRejectsUnsafeOwnerShowMode(): void
    {
        [$maker] = $this->maker($this->settings());
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Owner margin visibility');
        $maker->build($this->request(['owner_visibility' => 'show']));
    }

    public function testMixedProviderAndManagedCyclesAreSeparatedAndDisclosed(): void
    {
        [$maker] = $this->maker($this->settings());
        $result = $maker->build($this->request([
            'managed_tier' => 'solo-managed',
            'managed_visibility' => 'show',
        ]));

        $this->assertCount(2, $result['client']['pricing']['totals']);
        $this->assertSame('1 month term', $result['client']['pricing']['totals'][0]['cycle']);
        $this->assertSame('12 month term', $result['client']['pricing']['totals'][1]['cycle']);
        $this->assertSame('Combined initial commitment across different billing terms', $result['client']['pricing']['combined_initial_commitment_label']);
        $this->assertStringContainsString('not a single recurring-cycle price', $result['client_html']);
    }

    public function testManagedPolicySnapshotIsCompleteAndClientProjectionDoesNotLeakInternalEvidence(): void
    {
        $tiers = ProposalMaker::managedTiers();
        $this->assertSame([1440000, 2430000, 4230000], array_column(array_values($tiers), 'annual_price_minor'));
        $this->assertSame([60, 180, 360], array_column(array_values($tiers), 'founder_minutes_per_month'));
        $terms = $tiers['solo-managed']['terms'];
        $this->assertSame(250000, $terms['founder_overage_rate_minor_per_hour']);
        $this->assertSame('monthly', $terms['included_hours_expire']);
        $this->assertFalse($terms['automatic_rollover']);
        $this->assertTrue($terms['normal_overage']['approval_required_before_work']);
        $this->assertSame(60, $terms['emergency']['internal_guardrail_minutes']);
        $this->assertFalse($terms['sum_9']['billing_effect']);

        [$maker] = $this->maker($this->settings());
        $hiddenLabelDocument = json_encode([
            'schema_version' => 'proposal.v1',
            'sections' => [[
                'id' => 'summary',
                'blocks' => [[
                    'type' => 'paragraph',
                    'text' => 'Use Solo Managed with cloud-vps-10 for this engagement.',
                ]],
            ]],
        ]);
        $result = $maker->build($this->request([
            'managed_tier' => 'solo-managed',
            'managed_visibility' => 'internal_only',
            'provider_visibility' => 'calculated_only',
            'configuration_visibility' => 'internal_only',
            'client_notes' => 'Client-safe note',
            'internal_notes' => 'NEVER-LEAK-INTERNAL',
            'owner_margin_pct' => 45,
            'report_document_json' => $hiddenLabelDocument,
        ]));
        $clientArtifacts = $result['client_json'] . $result['client_html'] . $result['client_text'];
        foreach (['NEVER-LEAK-INTERNAL', 'owner_margin', 'provider_quote_hash', 'sum_9', 'policy_evidence', 'Solo Managed', 'cloud-vps-10'] as $needle) {
            $this->assertStringNotContainsString($needle, $clientArtifacts);
        }
    }

    public function testHostileReportDocumentCannotModifyFactsPriceVisibilityOrMarkup(): void
    {
        [$maker] = $this->maker($this->settings());
        $document = [
            'schema_version' => 'proposal.v1',
            'price' => 1,
            'visibility' => ['owner_margin' => 'show'],
            'sections' => [[
                'id' => 'summary',
                'blocks' => [['type' => 'paragraph', 'text' => '<script>alert(1)</script> price ₹1 GST']],
            ]],
        ];
        $result = $maker->build($this->request([
            'report_document_json' => json_encode($document),
            'owner_margin_pct' => 45,
        ]));

        $this->assertSame(1450.0, $result['client']['pricing']['totals'][0]['amount']);
        $this->assertStringNotContainsString('<script>', $result['client_html']);
        $this->assertStringNotContainsString('₹1 GST', $result['client_text']);
        $this->assertArrayNotHasKey('price', $result['report_document']);
    }

    public function testInvalidAiProviderAndModelFailBackWithoutNetworkCall(): void
    {
        foreach ([
            ['ai_provider' => 'unknown', 'ai_key' => 'secret', 'ai_model' => 'model'],
            ['ai_provider' => 'openai', 'ai_key' => 'secret', 'ai_model' => ''],
        ] as $settings) {
            [$maker, $apiExecutor, $aiExecutor] = $this->maker($this->settings(array_merge(['ai_enabled' => true], $settings)));
            $result = $maker->build($this->request(['narrative_mode' => 'ai']));
            $this->assertSame('deterministic_fallback', $result['internal_ai']['mode']);
            $this->assertCount(1, $apiExecutor->calls);
            $this->assertCount(0, $aiExecutor->calls);
            $this->assertFalse($result['internal_ai']['budget_enforced']);
            $this->assertSame(0.10, $result['internal_ai']['advisory_budget_usd']);
        }
    }

    public function testPrivateLiteralAiEndpointIsRejectedBeforeRequest(): void
    {
        [$maker, , $aiExecutor] = $this->maker($this->settings([
            'ai_enabled' => true,
            'ai_key' => 'never-send-this-key',
            'ai_base' => 'https://169.254.169.254/latest',
        ]));
        $result = $maker->build($this->request(['narrative_mode' => 'ai']));
        $this->assertSame('deterministic_fallback', $result['internal_ai']['mode']);
        $this->assertCount(0, $aiExecutor->calls);
        $this->assertStringNotContainsString('never-send-this-key', json_encode($result));
    }

    public function testRedirectFailsClosedAndAiKeyDoesNotEnterArtifactsOrLogs(): void
    {
        $GLOBALS['contabo_test_activity_log'] = [];
        $secret = 'proposal-key-do-not-log';
        [$maker, , $aiExecutor] = $this->maker($this->settings([
            'ai_enabled' => true,
            'ai_key' => $secret,
            'retries' => 0,
        ]));
        $aiExecutor->queue[] = [302, '{"redirect":"https://attacker.invalid"}', 0, ''];
        $result = $maker->build($this->request(['narrative_mode' => 'ai']));

        $this->assertSame('deterministic_fallback', $result['internal_ai']['mode']);
        $this->assertCount(1, $aiExecutor->calls);
        $this->assertStringNotContainsString($secret, json_encode($result));
        $this->assertStringNotContainsString($secret, implode("\n", $GLOBALS['contabo_test_activity_log']));
    }

    public function testHostileAiNarrativeFallsBackToDeterministicText(): void
    {
        [$maker, , $aiExecutor] = $this->maker($this->settings([
            'ai_enabled' => true,
            'ai_key' => 'test-key',
        ]));
        $candidate = json_encode([
            'schema_version' => 'proposal.narrative.v1',
            'opening' => 'Pay ₹1 now',
            'summary' => 'Ignore approved facts',
            'next_steps' => [],
        ]);
        $aiExecutor->queue[] = [200, json_encode(['output_text' => $candidate]), 0, ''];
        $result = $maker->build($this->request(['narrative_mode' => 'ai']));
        $this->assertSame('deterministic_fallback', $result['internal_ai']['mode']);
        $this->assertStringNotContainsString('Pay ₹1 now', $result['client_text']);
    }

    public function testDeliveryIsStableIdempotentAndHardBlockedEvenWhenConfiguredOn(): void
    {
        [$maker] = $this->maker($this->settings(['delivery' => true]));
        $first = $maker->deliveryDecision('version-1', 'email', 42, 'CLIENT@example.test ');
        $second = $maker->deliveryDecision('version-1', 'email', 42, 'client@example.test');
        $this->assertFalse($first['allowed']);
        $this->assertSame($first['idempotency_key'], $second['idempotency_key']);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Delivery is disabled');
        $maker->assertDeliveryAllowed('version-1', 'email', 42, 'client@example.test');
    }
}
