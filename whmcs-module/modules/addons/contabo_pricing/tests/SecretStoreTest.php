<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\SecretStore;
use ContaboPricing\SecretUnreadableException;
use PHPUnit\Framework\TestCase;

final class SecretStoreTest extends TestCase
{
    public function testSealOpenRoundTrip(): void
    {
        $sealed = SecretStore::seal('tf-secret-value-1234');
        $this->assertStringStartsWith('ENC:', $sealed);
        $this->assertStringNotContainsString('tf-secret-value-1234', $sealed);
        $this->assertTrue(SecretStore::isSealed($sealed));
        $this->assertSame('tf-secret-value-1234', SecretStore::open($sealed));
    }

    public function testOpenThrowsAndLogsOnDecryptFailureInsteadOfReturningEmpty(): void
    {
        $GLOBALS['contabo_pricing_test_activity_log'] = [];
        try {
            SecretStore::open('ENC:!!!not-valid!!!');
            $this->fail('an undecryptable sealed value must not look like "no key configured"');
        } catch (SecretUnreadableException $e) {
            $this->assertStringContainsString('SECRET_RESEAL.md', $e->getMessage());
            $this->assertStringNotContainsString('not-valid', $e->getMessage(), 'the stored value is never echoed');
        }
        $this->assertCount(1, $GLOBALS['contabo_pricing_test_activity_log']);
        $this->assertStringContainsString('cannot be decrypted', $GLOBALS['contabo_pricing_test_activity_log'][0]);
        $this->assertStringNotContainsString('not-valid', $GLOBALS['contabo_pricing_test_activity_log'][0]);
    }

    public function testTryOpenReportsUnreadableAsNullAndStillLogs(): void
    {
        $GLOBALS['contabo_pricing_test_activity_log'] = [];
        $this->assertNull(SecretStore::tryOpen('ENC:!!!not-valid!!!'));
        $this->assertCount(1, $GLOBALS['contabo_pricing_test_activity_log']);
        $this->assertSame('', SecretStore::tryOpen(''));
        $this->assertSame('k', SecretStore::tryOpen(SecretStore::seal('k')));
    }

    public function testOpenPassesThroughLegacyPlaintextAndEmpty(): void
    {
        $this->assertFalse(SecretStore::isSealed('plain'));
        $this->assertSame('plain', SecretStore::open('plain'));
        $this->assertSame('', SecretStore::open(''));
    }

    public function testMaskShowsOnlyLastFour(): void
    {
        $this->assertSame("\u{2022}\u{2022}\u{2022}\u{2022}7890", SecretStore::mask('abcdef7890'));
        $this->assertSame("\u{2022}\u{2022}\u{2022}\u{2022}", SecretStore::mask('abc'));
        $this->assertSame('', SecretStore::mask(''));
    }
}
