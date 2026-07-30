<?php
declare(strict_types=1);

use SecuriAceVps\ContaboInstanceMapper;
use SecuriAceVps\ContaboProvisioningException;
use PHPUnit\Framework\TestCase;

final class InstanceMapperTest extends TestCase
{
    private ContaboInstanceMapper $mapper;

    protected function setUp(): void
    {
        $GLOBALS['__activity_log'] = [];
        $this->mapper = new ContaboInstanceMapper();
    }

    /** @return array<string,mixed> */
    private function params(array $overrides = []): array
    {
        return array_merge([
            'serviceid'     => 101,
            'domain'        => 'vps.example.com',
            'billingcycle'  => 'Annually',
            'configoption1' => 'afecbb85-e2fc-46f0-9684-b46b1faf00bb',
            'configoption2' => 'EU',
            'configoption3' => '',
            'configoption4' => 'V45',
            'configoption5' => '',
            'configoption6' => '',
        ], $overrides);
    }

    public function testMapsRequiredFieldsIncludingPeriod(): void
    {
        $body = $this->mapper->mapCreate($this->params());

        $this->assertSame('afecbb85-e2fc-46f0-9684-b46b1faf00bb', $body['imageId']);
        $this->assertSame('V45', $body['productId']);
        $this->assertSame('EU', $body['region']);
        $this->assertSame(12, $body['period'], 'annual WHMCS cycle must buy a 12-month Contabo contract');
        $this->assertSame('whmcs-101 vps.example.com', $body['displayName']);
        $this->assertArrayNotHasKey('sshKeys', $body);
        $this->assertArrayNotHasKey('rootPassword', $body);
    }

    public function testDisplayNameIsTaggedSanitizedAndBounded(): void
    {
        $body = $this->mapper->mapCreate($this->params([
            'domain' => "bad\"domain'؟" . str_repeat('x', 300),
        ]));
        $this->assertStringStartsWith('whmcs-101 ', $body['displayName']);
        $this->assertLessThanOrEqual(255, strlen($body['displayName']));
        $this->assertMatchesRegularExpression('/^[A-Za-z0-9 ._-]+$/', $body['displayName']);
    }

    public function testSelectionsBeatConfigOptionFallbacks(): void
    {
        $body = $this->mapper->mapCreate(
            $this->params(),
            ['imageId' => '11111111-2222-4333-8444-555555555555', 'region' => 'SIN']
        );
        $this->assertSame('11111111-2222-4333-8444-555555555555', $body['imageId']);
        $this->assertSame('SIN', $body['region']);
    }

    public function testRootPasswordSecretIdIsPassedThrough(): void
    {
        $body = $this->mapper->mapCreate($this->params(), [], 777);
        $this->assertSame(777, $body['rootPassword']);
    }

    public function testValidSshSecretIdBecomesSshKeys(): void
    {
        $body = $this->mapper->mapCreate($this->params(['configoption3' => '4242']));
        $this->assertSame([4242], $body['sshKeys']);
    }

    public function testNonNumericSshSecretIdFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('SSH secret id');
        $this->mapper->mapCreate($this->params(['configoption3' => 'ssh-rsa AAAA...']));
    }

    public function testUserDataAndAddOnsPassThrough(): void
    {
        $body = $this->mapper->mapCreate($this->params([
            'configoption5' => "#cloud-config\npackages: [nginx]",
            'configoption6' => '{"privateNetworking":{}}',
        ]));
        $this->assertSame("#cloud-config\npackages: [nginx]", $body['userData']);
        $this->assertSame(['privateNetworking' => []], $body['addOns']);
    }

    public function testInvalidAddOnsJsonFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('not valid JSON');
        $this->mapper->mapCreate($this->params(['configoption6' => '{nope']));
    }

    public function testMissingImageFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('image');
        $this->mapper->mapCreate($this->params(['configoption1' => '']));
    }

    public function testMissingProductIdFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('product id');
        $this->mapper->mapCreate($this->params(['configoption4' => '']));
    }

    // ── region resolution ────────────────────────────────────────────────────

    /** @return array<string, array{string,string}> */
    public static function regions(): array
    {
        return [
            'slug passthrough'   => ['EU', 'EU'],
            'slug case'          => ['us-central', 'US-central'],
            'label'              => ['European Union', 'EU'],
            'label case+space'   => ['  singapore ', 'SIN'],
            'value key'          => ['Europe:European Union', 'EU'],
            'us east label'      => ['United States (East)', 'US-east'],
            'uk'                 => ['United Kingdom', 'UK'],
            'india'              => ['India', 'IND'],
            'japan'              => ['Japan', 'JPN'],
            'australia'          => ['Australia', 'AUS'],
        ];
    }

    /** @dataProvider regions */
    public function testRegionResolution(string $selection, string $slug): void
    {
        $this->assertSame($slug, ContaboInstanceMapper::resolveRegionSlug($selection));
    }

    public function testUnknownRegionFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('Cannot resolve region');
        ContaboInstanceMapper::resolveRegionSlug('Atlantis');
    }
}
