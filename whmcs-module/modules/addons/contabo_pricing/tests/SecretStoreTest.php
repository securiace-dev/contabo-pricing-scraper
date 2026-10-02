<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\SecretStore;
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

    public function testOpenReturnsEmptyOnDecryptFailure(): void
    {
        $this->assertSame('', SecretStore::open('ENC:!!!not-valid!!!'));
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
