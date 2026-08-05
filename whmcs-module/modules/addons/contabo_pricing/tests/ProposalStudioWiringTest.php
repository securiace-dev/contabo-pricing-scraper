<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

final class ProposalStudioWiringTest extends TestCase
{
    public function testAdminRouteAndSidebarExposeProposalStudio(): void
    {
        $controller = (string) file_get_contents(__DIR__ . '/../lib/AdminController.php');
        $addon = (string) file_get_contents(__DIR__ . '/../contabo_pricing.php');

        $this->assertStringContainsString("case 'proposals'", $controller);
        $this->assertStringContainsString("case 'proposal-preview'", $controller);
        $this->assertStringContainsString("'Proposal Studio' => '&action=proposals'", $addon);
    }

    public function testTemplateMakesPreviewPrimaryAndDeliveryUnmistakablyUnavailable(): void
    {
        $template = (string) file_get_contents(__DIR__ . '/../templates/admin/proposal_maker.tpl');

        $this->assertStringContainsString('Create deterministic preview', $template);
        $this->assertStringContainsString('Delivery hard-blocked', $template);
        $this->assertStringContainsString('Send as support ticket</button>', $template);
        $this->assertStringContainsString('type="button" disabled', $template);
        $this->assertStringContainsString('No client-facing show mode exists.', $template);
        $this->assertStringContainsString('name="managed_quantity"', $template);
        $this->assertStringContainsString('name="comparison_plan_slugs"', $template);
        $this->assertStringContainsString('maximum four', $template);
    }

    public function testTemplateSeparatesClientArtifactFromInternalEvidence(): void
    {
        $template = (string) file_get_contents(__DIR__ . '/../templates/admin/proposal_maker.tpl');

        $this->assertStringContainsString('Client artifact', $template);
        $this->assertStringContainsString('Internal evidence', $template);
        $this->assertStringContainsString('<iframe title="Client proposal preview" sandbox', $template);
    }
}
