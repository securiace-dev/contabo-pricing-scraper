<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProfileIdentityResolver;
use ContaboPricing\ProfileRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class ProfileCreateTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /**
     * @return array<string,mixed>
     */
    private function input(array $overrides = []): array
    {
        return array_merge([
            'name'          => 'Cloud VPS 10 — EU — Ubuntu — 12mo',
            'plan_slug'     => 'cloud-vps-10',
            'period_months' => 12,
            'region'        => 'EU',
            'os'            => 'Ubuntu 24.04',
            'sync_strategy' => 'notify',
            'options'       => [
                'Image:OS' => ['label' => 'Ubuntu 24.04'],
                'Region'   => ['label' => 'EU'],
                'Storage'  => ['label' => '200 GB NVMe'],
            ],
        ], $overrides);
    }

    public function testFirstCreateInsertsWithIdentityColumns(): void
    {
        $repo = new ProfileRepository('notify');
        $result = $repo->createOrResolve($this->input());

        $this->assertSame('created', $result['status']);
        $this->assertIsInt($result['profile_id']);

        // The inserted row carries the v4 identity columns.
        $row = $repo->findBySlug('cloud-vps-10-12mo-eu-ubuntu-24-04');
        $this->assertNotNull($row);
        $this->assertSame(ProfileIdentityResolver::MODE_FIXED, $row['profile_mode']);
        $this->assertNotEmpty($row['profile_fingerprint_hash']);
        $this->assertNotEmpty($row['profile_identity_json']);
        // identity_json must be valid JSON (LONGTEXT column).
        $this->assertIsArray(json_decode((string) $row['profile_identity_json'], true));
    }

    public function testDuplicateSlugSameFingerprintLoadsExisting(): void
    {
        $repo = new ProfileRepository('notify');
        $first = $repo->createOrResolve($this->input());
        $this->assertSame('created', $first['status']);

        // Re-create the exact same profile → loads existing, no second insert.
        $second = $repo->createOrResolve($this->input());
        $this->assertSame('loaded_existing', $second['status']);
        $this->assertSame($first['profile_id'], $second['profile_id']);

        // Exactly one row exists for the slug.
        $count = Capsule::table('mod_contabo_profile')
            ->where('slug', 'cloud-vps-10-12mo-eu-ubuntu-24-04')->count();
        $this->assertSame(1, $count, 'duplicate same-fingerprint create must not insert a second row');
    }

    public function testDuplicateSlugDifferentFingerprintReturnsConflict(): void
    {
        $repo = new ProfileRepository('notify');
        $first = $repo->createOrResolve($this->input());
        $this->assertSame('created', $first['status']);

        // Same slug (explicit), but a DIFFERENT config (different storage).
        $conflicting = $this->input([
            'slug'    => 'cloud-vps-10-12mo-eu-ubuntu-24-04',
            'options' => [
                'Image:OS' => ['label' => 'Ubuntu 24.04'],
                'Region'   => ['label' => 'EU'],
                'Storage'  => ['label' => '800 GB NVMe'], // changed
            ],
        ]);
        $result = $repo->createOrResolve($conflicting);

        $this->assertSame('conflict', $result['status']);
        $this->assertNull($result['profile_id'], 'conflict must NOT insert');
        $this->assertNotNull($result['existing']);
        $this->assertSame('cloud-vps-10-12mo-eu-ubuntu-24-04-2', $result['suffix_suggestion']);

        // Still only one row.
        $count = Capsule::table('mod_contabo_profile')
            ->where('slug', 'cloud-vps-10-12mo-eu-ubuntu-24-04')->count();
        $this->assertSame(1, $count);
    }

    public function testSlugSuffixGeneration(): void
    {
        $repo = new ProfileRepository('notify');

        // Seed existing slugs `x` and `x-2`; expect suggestion `x-3`.
        Capsule::table('mod_contabo_profile')->insertGetId(['slug' => 'x']);
        Capsule::table('mod_contabo_profile')->insertGetId(['slug' => 'x-2']);

        $this->assertSame('x-3', $repo->nextFreeSuffix('x'));

        // With only `x` present, suggestion is `x-2`.
        Capsule::reset();
        Capsule::table('mod_contabo_profile')->insertGetId(['slug' => 'x']);
        $this->assertSame('x-2', $repo->nextFreeSuffix('x'));
    }

    public function testLegacyRowWithoutFingerprintPrefersLoadedExisting(): void
    {
        // A pre-A.5.1 row has no profile_fingerprint_hash. Re-creating must
        // NOT block on a conflict — prefer loaded_existing.
        Capsule::table('mod_contabo_profile')->insertGetId([
            'slug'          => 'legacy-12mo-eu-ubuntu',
            'name'          => 'Legacy profile',
            'plan_slug'     => 'legacy',
            'period_months' => 12,
            'region'        => 'EU',
            'os'            => 'Ubuntu',
            // no profile_fingerprint_hash / profile_identity_json columns
        ]);

        $repo = new ProfileRepository('notify');
        $result = $repo->createOrResolve([
            'slug'          => 'legacy-12mo-eu-ubuntu',
            'name'          => 'Legacy profile re-create',
            'plan_slug'     => 'legacy',
            'period_months' => 12,
            'region'        => 'EU',
            'os'            => 'Ubuntu',
            // Even with a slightly different config, legacy rows must not block.
            'options'       => ['Storage' => ['label' => 'whatever']],
        ]);

        $this->assertSame('loaded_existing', $result['status'], 'legacy rows must never block on conflict');
    }
}
