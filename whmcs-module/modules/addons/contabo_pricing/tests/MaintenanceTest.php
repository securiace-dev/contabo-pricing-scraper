<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\SchemaHealth;
use PHPUnit\Framework\TestCase;

/**
 * Contract for the destructive-purge typed-confirmation guard.
 *
 * The maintenance-purge handler (orchestrator-owned, in AdminController) MUST
 * route the posted `purge_confirmation_phrase` through
 * SchemaHealth::isPurgeConfirmed() before truncating any data. This proves the
 * shared validator accepts only the exact phrase.
 */
final class MaintenanceTest extends TestCase
{
    public function testPurgeRequiresTypedConfirmation(): void
    {
        // The canonical phrase is accepted.
        $this->assertTrue(SchemaHealth::isPurgeConfirmed('PURGE CONTABO PRICING DATA'));
        $this->assertSame('PURGE CONTABO PRICING DATA', SchemaHealth::PURGE_CONFIRMATION_PHRASE);

        // Surrounding whitespace is tolerated (trimmed) — admins copy/paste.
        $this->assertTrue(SchemaHealth::isPurgeConfirmed('  PURGE CONTABO PRICING DATA  '));
        $this->assertTrue(SchemaHealth::isPurgeConfirmed("PURGE CONTABO PRICING DATA\n"));

        // Anything else is rejected.
        $this->assertFalse(SchemaHealth::isPurgeConfirmed(''));
        $this->assertFalse(SchemaHealth::isPurgeConfirmed('purge contabo pricing data'), 'case-sensitive');
        $this->assertFalse(SchemaHealth::isPurgeConfirmed('PURGE CONTABO PRICING'), 'partial phrase rejected');
        $this->assertFalse(SchemaHealth::isPurgeConfirmed('PURGE  CONTABO  PRICING  DATA'), 'inner spacing must match exactly');
        $this->assertFalse(SchemaHealth::isPurgeConfirmed('DELETE EVERYTHING'));
        $this->assertFalse(SchemaHealth::isPurgeConfirmed('PURGE CONTABO PRICING DATA!'), 'no extra punctuation');
    }
}
