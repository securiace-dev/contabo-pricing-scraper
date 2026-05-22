<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionCapabilityRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * A.5.2 amendment #6 — ConfigOptionCapabilityRepository: chokepoint for the
 * per-(plan, dimension, value) capability matrix, plus the capability-source
 * auto-apply gate. Runs against FakeCapsule (no DB).
 */
final class ConfigOptionCapabilityRepositoryTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testUpsertInsertsThenIsIdempotentBySameKey(): void
    {
        $repo = new ConfigOptionCapabilityRepository();

        $a = $repo->upsertCapability('contabo-cloud-vps-10', 'Storage', 'ssd:400', [
            'allowed_on_upgrade' => true,
            'destructive_change' => false,
        ]);
        $this->assertSame('contabo-cloud-vps-10', (string) $a['contabo_plan_slug']);
        $this->assertSame('Storage', (string) $a['dimension_key']);
        $this->assertSame('ssd:400', (string) $a['value_key']);
        $this->assertArrayHasKey('id', $a);
        $this->assertNotEmpty($a['last_verified_at']);
        $firstId = (int) $a['id'];

        // Second upsert with the SAME key updates in place — one row, not two.
        $b = $repo->upsertCapability('contabo-cloud-vps-10', 'Storage', 'ssd:400', [
            'allowed_on_upgrade' => true,
            'data_loss_expected' => true,
        ]);
        $this->assertSame($firstId, (int) $b['id'], 'idempotent: same row, not a duplicate');
        $this->assertSame(1, (int) $b['data_loss_expected']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_option_capability']);
    }

    public function testDistinctTriplesAreDistinctRows(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', []);
        $repo->upsertCapability('plan-a', 'Storage', 'ssd:800', []); // different value
        $repo->upsertCapability('plan-b', 'Storage', 'ssd:400', []); // different plan
        $this->assertCount(3, Capsule::$tables['mod_contabo_option_capability']);
    }

    public function testFlagWhitelistDropsUnknownKeysAndCastsBooleans(): void
    {
        $repo = new ConfigOptionCapabilityRepository();

        $row = $repo->upsertCapability('plan-a', 'Networking', 'ipv4:1', [
            'allowed_on_create'   => true,
            'requires_recreate'   => 1,
            'destructive_change'  => false,
            'provisioning_action' => 'reinstall',
            'i_am_not_a_column'   => true,   // must be dropped
            'drop_table'          => 'oops', // must be dropped
        ]);

        $this->assertSame(1, (int) $row['allowed_on_create']);
        $this->assertSame(1, (int) $row['requires_recreate']);
        $this->assertSame(0, (int) $row['destructive_change']);
        $this->assertSame('reinstall', (string) $row['provisioning_action']);
        $this->assertArrayNotHasKey('i_am_not_a_column', $row);
        $this->assertArrayNotHasKey('drop_table', $row);
    }

    public function testInsertDefaultsCapabilitySourceToManualAssumption(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $row = $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', ['allowed_on_create' => true]);
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION,
            (string) $row['capability_source']
        );
    }

    public function testCapabilitySourceWhitelistRejectsInvalidValue(): void
    {
        $repo = new ConfigOptionCapabilityRepository();

        $valid = $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', [
            'capability_source' => ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED,
        ]);
        $this->assertSame('api_verified', (string) $valid['capability_source']);

        // An invalid source on an existing row is dropped — the stored value
        // (api_verified) is preserved rather than overwritten with garbage.
        $after = $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', [
            'capability_source' => 'totally_made_up',
        ]);
        $this->assertSame('api_verified', (string) $after['capability_source']);
    }

    public function testFindReturnsNullWhenAbsent(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $this->assertNull($repo->find('nope', 'Storage', 'ssd:400'));
    }

    public function testFindReturnsRow(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', ['allowed_on_upgrade' => true]);
        $found = $repo->find('plan-a', 'Storage', 'ssd:400');
        $this->assertNotNull($found);
        $this->assertSame(1, (int) $found['allowed_on_upgrade']);
    }

    public function testListForPlanReturnsOnlyThatPlan(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', []);
        $repo->upsertCapability('plan-a', 'Networking', 'ipv4:1', []);
        $repo->upsertCapability('plan-b', 'Storage', 'ssd:400', []);

        $list = $repo->listForPlan('plan-a');
        $this->assertCount(2, $list);
        foreach ($list as $r) {
            $this->assertSame('plan-a', (string) $r['contabo_plan_slug']);
        }
    }

    public function testListForPlanEmptyWhenNoRows(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $this->assertSame([], $repo->listForPlan('ghost-plan'));
    }

    /**
     * Amendment #6 truth table: for a destructive/in-place change ONLY
     * api_verified may auto-apply; every weaker source needs admin approval.
     */
    public function testCanAutoApplyDestructiveChangePerSource(): void
    {
        $repo = new ConfigOptionCapabilityRepository();

        $expected = [
            ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED      => true,
            ConfigOptionCapabilityRepository::SOURCE_SCRAPE_VERIFIED   => false,
            ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION => false,
            ConfigOptionCapabilityRepository::SOURCE_ADMIN_OVERRIDE    => false,
            ConfigOptionCapabilityRepository::SOURCE_UNKNOWN           => false,
        ];

        foreach ($expected as $source => $allowed) {
            $row = ['capability_source' => $source];
            $this->assertSame(
                $allowed,
                $repo->canAutoApply($row, true),
                "destructive auto-apply for source={$source}"
            );
        }
    }

    /**
     * A non-destructive metadata-only change may auto-apply for EVERY source.
     */
    public function testCanAutoApplyNonDestructiveChangeAlwaysTrue(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        foreach (ConfigOptionCapabilityRepository::VALID_SOURCES as $source) {
            $row = ['capability_source' => $source];
            $this->assertTrue(
                $repo->canAutoApply($row, false),
                "non-destructive auto-apply for source={$source}"
            );
        }
    }

    public function testCanAutoApplyMissingOrInvalidSourceIsConservative(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        // Missing capability_source → treated as untrusted → needs approval.
        $this->assertFalse($repo->canAutoApply([], true));
        // Invalid source → needs approval.
        $this->assertFalse($repo->canAutoApply(['capability_source' => 'bogus'], true));
        // But a non-destructive change is fine even with no source at all.
        $this->assertTrue($repo->canAutoApply([], false));
    }

    public function testCanAutoApplyAfterRealUpsertRoundTrip(): void
    {
        $repo = new ConfigOptionCapabilityRepository();

        $manual = $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', [
            'destructive_change' => true,
            'data_loss_expected' => true,
        ]);
        // Defaulted to manual_assumption → destructive change must NOT auto-apply.
        $this->assertFalse($repo->canAutoApply($manual, true));

        $verified = $repo->upsertCapability('plan-a', 'Storage', 'ssd:400', [
            'capability_source' => ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED,
        ]);
        // Promoted to api_verified → now it may auto-apply.
        $this->assertTrue($repo->canAutoApply($verified, true));
    }
}
