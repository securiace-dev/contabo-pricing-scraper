<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CurlRequestExecutor;
use PHPUnit\Framework\TestCase;

final class CurlRequestExecutorTest extends TestCase
{
    public function testDefaultFollowsHttpsRedirectsOnly(): void
    {
        $exec = new CurlRequestExecutor();
        $this->assertTrue($exec->followsRedirects());
        $opts = $exec->transportOptions(8);
        $this->assertTrue($opts[CURLOPT_FOLLOWLOCATION]);
        $this->assertSame(CurlRequestExecutor::MAX_REDIRECTS, $opts[CURLOPT_MAXREDIRS]);
        $this->assertSame(CURLPROTO_HTTPS, $opts[CURLOPT_REDIR_PROTOCOLS]);
        $this->assertTrue($opts[CURLOPT_SSL_VERIFYPEER]);
        $this->assertSame(2, $opts[CURLOPT_SSL_VERIFYHOST]);
    }

    public function testOptOutDoesNotFollowRedirects(): void
    {
        $exec = new CurlRequestExecutor(false);
        $this->assertFalse($exec->followsRedirects());
        $opts = $exec->transportOptions(8);
        $this->assertFalse($opts[CURLOPT_FOLLOWLOCATION]);
        $this->assertArrayNotHasKey(CURLOPT_MAXREDIRS, $opts);
        $this->assertArrayNotHasKey(CURLOPT_REDIR_PROTOCOLS, $opts);
    }

    public function testProposalMakerDefaultExecutorMustNotFollowRedirects(): void
    {
        $src = (string) file_get_contents(dirname(__DIR__) . '/lib/ProposalMaker.php');
        $this->assertStringContainsString(
            'new CurlRequestExecutor(false)',
            $src,
            'AI Bearer tokens must not be forwarded across a 302'
        );
    }

    public function testAppliedOptionsLetTheCappedWriterOverrideReturnTransfer(): void
    {
        // curl handles cannot be read back, so pin the layering in source. The left
        // operand of the union wins, which keeps RETURNTRANSFER off while the redirect
        // policy still comes from transportOptions().
        $src = (string) file_get_contents(dirname(__DIR__) . '/lib/CurlRequestExecutor.php');
        $this->assertMatchesRegularExpression('/CURLOPT_RETURNTRANSFER\s*=>\s*false/', $src);
        $this->assertMatchesRegularExpression('/\]\s*\+\s*\$this->transportOptions\(\$timeoutSec\)\s*;/', $src);
        $this->assertDoesNotMatchRegularExpression('/\$this->transportOptions\(\$timeoutSec\)\s*\+\s*\[/', $src);
        $this->assertTrue((new CurlRequestExecutor())->transportOptions(8)[CURLOPT_RETURNTRANSFER]);
    }
}
