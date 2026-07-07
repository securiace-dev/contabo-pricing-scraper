<?php
declare(strict_types=1);

use ContaboVps\ContaboApiClient;
use ContaboVps\ContaboAuth;
use ContaboVps\ContaboProvisioningException;
use ContaboVps\ImageResolver;
use ContaboVps\Tests\FakeHttpExecutor;
use PHPUnit\Framework\TestCase;

final class ImageResolverTest extends TestCase
{
    private FakeHttpExecutor $http;
    private ImageResolver $resolver;

    protected function setUp(): void
    {
        $this->http = new FakeHttpExecutor();
        $this->http->stubToken();
        $this->http->stub('GET /v1/compute/images', 200, ['data' => [
            ['imageId' => 'img-ubuntu-2404', 'name' => 'Ubuntu 24.04'],
            ['imageId' => 'img-ubuntu-2204', 'name' => 'Ubuntu 22.04'],
            ['imageId' => 'img-debian-12',   'name' => 'Debian 12'],
            ['imageId' => 'img-cpanel',      'name' => 'cPanel'],
        ]]);
        $auth = new ContaboAuth('cid', 'cs', 'u@example.com', 'pw', $this->http);
        $client = new ContaboApiClient($auth, $this->http, static function (int $s): void {});
        $this->resolver = new ImageResolver($client);
    }

    public function testUuidSelectionPassesThroughWithoutApiCall(): void
    {
        $id = $this->resolver->resolveImageId('afecbb85-e2fc-46f0-9684-b46b1faf00bb');
        $this->assertSame('afecbb85-e2fc-46f0-9684-b46b1faf00bb', $id);
        $this->assertCount(0, $this->http->callsMatching('/v1/compute/images'));
    }

    public function testValueKeyResolvesToImageId(): void
    {
        $this->assertSame('img-ubuntu-2404', $this->resolver->resolveImageId('OS:Ubuntu 24.04'));
    }

    public function testPrefixedLabelResolves(): void
    {
        $this->assertSame('img-cpanel', $this->resolver->resolveImageId('[Panel] cPanel'));
    }

    public function testBareLabelIsCaseInsensitive(): void
    {
        $this->assertSame('img-debian-12', $this->resolver->resolveImageId('debian 12'));
    }

    public function testAmbiguousSelectionFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('Ambiguous');
        $this->resolver->resolveImageId('OS:Ubuntu');
    }

    public function testUnknownSelectionFailsClosed(): void
    {
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('No Contabo standard image matches');
        $this->resolver->resolveImageId('OS:TempleOS');
    }

    public function testCatalogIsMemoizedPerRequest(): void
    {
        $this->resolver->resolveImageId('OS:Ubuntu 24.04');
        $this->resolver->resolveImageId('Debian 12');
        $this->assertCount(1, $this->http->callsMatching('/v1/compute/images'));
    }
}
