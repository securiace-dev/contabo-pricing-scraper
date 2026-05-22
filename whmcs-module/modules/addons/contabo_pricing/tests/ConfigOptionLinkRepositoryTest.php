<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionLinkRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * A.6.3 — ConfigOptionLinkRepository (idempotency + ownership chokepoint for the
 * config link tables). Runs against FakeCapsule (no DB).
 */
final class ConfigOptionLinkRepositoryTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testGroupLinkInsertThenIdempotentUpdate(): void
    {
        $repo = new ConfigOptionLinkRepository();

        $a = $repo->upsertGroupLink(7, 501, 'contabo-cloud-vps-10', null);
        $this->assertSame(7, (int) $a['profile_id']);
        $this->assertSame(501, (int) $a['whmcs_product_id']);
        $this->assertArrayHasKey('id', $a);
        $firstId = (int) $a['id'];

        // Second upsert with the SAME key records the whmcs id, reuses the row.
        $b = $repo->upsertGroupLink(7, 501, 'contabo-cloud-vps-10', 8800);
        $this->assertSame($firstId, (int) $b['id'], 'idempotent: same row, not a duplicate');
        $this->assertSame(8800, (int) $b['whmcs_group_id']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_config_group_link']);
    }

    public function testDifferentProductIsADistinctGroupLink(): void
    {
        $repo = new ConfigOptionLinkRepository();
        $repo->upsertGroupLink(7, 501, 'g', 10);
        $repo->upsertGroupLink(7, 502, 'g', 20); // different product → new row (amendment 7)
        $this->assertCount(2, Capsule::$tables['mod_contabo_config_group_link']);
    }

    public function testOptionLinkExposureWhitelist(): void
    {
        $repo = new ConfigOptionLinkRepository();

        $row = $repo->upsertOptionLink(7, 'Networking:IPv4', 4, 9100, [
            'expose_to_customer'   => true,
            'allowed_on_create'    => true,
            'i_am_not_a_column'    => true, // must be dropped
        ]);

        $this->assertSame('Networking:IPv4', (string) $row['dimension_key']);
        $this->assertSame(4, (int) $row['optiontype']);
        $this->assertSame(9100, (int) $row['whmcs_option_id']);
        $this->assertSame(1, (int) $row['expose_to_customer']);
        $this->assertSame(1, (int) $row['allowed_on_create']);
        $this->assertArrayNotHasKey('i_am_not_a_column', $row);
    }

    public function testOptionLinkIdempotentByProfileAndDimension(): void
    {
        $repo = new ConfigOptionLinkRepository();
        $r1 = $repo->upsertOptionLink(7, 'Image', 1, null);
        $r2 = $repo->upsertOptionLink(7, 'Image', 1, 9200); // same key → update
        $this->assertSame((int) $r1['id'], (int) $r2['id']);
        $this->assertSame(9200, (int) $r2['whmcs_option_id']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_config_option_link']);
    }

    public function testListOptionLinksForProfileOrderedByDimensionAndScoped(): void
    {
        $repo = new ConfigOptionLinkRepository();
        // Two dimensions for profile 7 (inserted out of alphabetical order)…
        $repo->upsertOptionLink(7, 'Networking:IPv4', 4, 9100, ['expose_to_customer' => true]);
        $repo->upsertOptionLink(7, 'Image', 1, 9200, ['hidden' => true]);
        // …and one for a DIFFERENT profile, which must be excluded.
        $repo->upsertOptionLink(8, 'Storage', 1, 9300);

        $rows = $repo->listOptionLinksForProfile(7);
        $this->assertCount(2, $rows, 'only profile 7 links');
        // orderBy('dimension_key'): 'Image' before 'Networking:IPv4'.
        $this->assertSame('Image', (string) $rows[0]['dimension_key']);
        $this->assertSame('Networking:IPv4', (string) $rows[1]['dimension_key']);
        // Each row is a plain associative array carrying the exposure flags.
        $this->assertIsArray($rows[0]);
        $this->assertSame(1, (int) $rows[0]['hidden']);
        $this->assertSame(1, (int) $rows[1]['expose_to_customer']);
    }

    public function testListOptionLinksForProfileEmptyWhenNone(): void
    {
        $repo = new ConfigOptionLinkRepository();
        $this->assertSame([], $repo->listOptionLinksForProfile(999));
    }

    public function testValueLinkUpsertAndRoundTripLookup(): void
    {
        $repo = new ConfigOptionLinkRepository();
        $opt = $repo->upsertOptionLink(7, 'Image', 1, 9200);
        $optId = (int) $opt['id'];

        $v = $repo->upsertValueLink($optId, 'os:windows', '[OS] Windows', 9300, false, 10.0);
        $this->assertSame($optId, (int) $v['option_link_id']);
        $this->assertSame('[OS] Windows', (string) $v['contabo_label']);
        $this->assertSame(9300, (int) $v['whmcs_sub_id']);
        $this->assertEqualsWithDelta(10.0, (float) $v['monthly_eur_delta'], 0.0001);

        // Round-trip: provisioning maps a WHMCS sub id back to the Contabo value.
        $back = $repo->findValueLinkByWhmcsSubId(9300);
        $this->assertNotNull($back);
        $this->assertSame('os:windows', (string) $back['contabo_value_key']);
    }

    public function testValueLinkIdempotentByOptionAndValueKey(): void
    {
        $repo = new ConfigOptionLinkRepository();
        $opt = $repo->upsertOptionLink(7, 'Image', 1, 9200);
        $optId = (int) $opt['id'];
        $repo->upsertValueLink($optId, 'os:ubuntu', '[OS] Ubuntu', null, true, 0.0);
        $repo->upsertValueLink($optId, 'os:ubuntu', '[OS] Ubuntu', 9400, true, 0.0); // same key → update
        $this->assertCount(1, Capsule::$tables['mod_contabo_config_option_value_link']);
        $this->assertSame(9400, (int) $repo->findValueLink($optId, 'os:ubuntu')['whmcs_sub_id']);
    }
}
