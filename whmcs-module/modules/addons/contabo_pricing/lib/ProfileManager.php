<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Thrown by ProfileManager::create() when a slug already exists with a
 * DIFFERENT identity fingerprint. Carries the existing row + a free
 * `-N` suffix suggestion so AdminController::profileCreate() can render the
 * duplicate-conflict chooser instead of leaking a raw SQLSTATE.
 *
 * Note: this is NOT thrown for an idempotent re-create (same slug + same
 * fingerprint) — that returns the existing profile id normally.
 */
class ProfileSlugConflictException extends \RuntimeException
{
    /** @var array<string,mixed> */
    public $existing;

    /** @var string */
    public $suffixSuggestion;

    /**
     * @param array<string,mixed> $existing
     */
    public function __construct(array $existing, string $suffixSuggestion)
    {
        $slug = (string) ($existing['slug'] ?? '');
        parent::__construct(sprintf(
            'Profile slug "%s" already exists with a different configuration.',
            $slug
        ));
        $this->existing         = $existing;
        $this->suffixSuggestion = $suffixSuggestion;
    }
}

/**
 * CRUD for profiles and immutable profile versions.
 *
 * A Profile is a named template (e.g. "cloud-vps-10-eu-ubuntu-prod") that
 * captures (plan_slug, period_months, region, OS, options[]).
 * A ProfileVersion is a frozen pricing snapshot of that profile at a point
 * in time. Every sync that detects a delta creates a new version and points
 * `latest_version_id` at it. Old versions are kept indefinitely (no purge).
 */
class ProfileManager
{
    /** @var Settings */
    private $settings;

    public function __construct(Settings $settings)
    {
        $this->settings = $settings;
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function listProfiles(bool $activeOnly = true): array
    {
        $q = Capsule::table('mod_contabo_profile');
        if ($activeOnly) {
            $q->where('active', true);
        }
        return $q->orderBy('plan_slug')->orderBy('period_months')->get()->map(static fn ($r) => (array) $r)->all();
    }

    /**
     * @return array<string, mixed>|null
     */
    public function find(int $id): ?array
    {
        $row = Capsule::table('mod_contabo_profile')->where('id', $id)->first();
        return $row ? (array) $row : null;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function findBySlug(string $slug): ?array
    {
        $row = Capsule::table('mod_contabo_profile')->where('slug', $slug)->first();
        return $row ? (array) $row : null;
    }

    /**
     * Create a new profile. Returns the new id.
     *
     * Routes through ProfileRepository::createOrResolve() for graceful
     * duplicate handling (Phase A.5.1):
     *   - status 'created'         → returns the new profile id.
     *   - status 'loaded_existing' → returns the existing profile id (the
     *                                same slug + same fingerprint, i.e. an
     *                                idempotent re-create).
     *   - status 'conflict'        → throws ProfileSlugConflictException so the
     *                                caller can render the conflict chooser.
     *
     * @param array{slug?: string, name: string, plan_slug: string, period_months: int, region?: string, os?: string, options?: array<string, mixed>, tags?: string, sync_strategy?: string, profile_mode?: string} $input
     * @throws ProfileSlugConflictException
     */
    public function create(array $input): int
    {
        $repo   = new ProfileRepository($this->settings->defaultSyncStrategy);
        $result = $repo->createOrResolve($input);

        if ($result['status'] === 'conflict') {
            throw new ProfileSlugConflictException(
                $result['existing'] ?? [],
                (string) ($result['suffix_suggestion'] ?? '')
            );
        }

        // 'created' or 'loaded_existing' both yield a usable id.
        return (int) ($result['profile_id'] ?? 0);
    }

    /** @param array<string, mixed> $patch */
    public function update(int $id, array $patch): void
    {
        $patch['updated_at'] = date('Y-m-d H:i:s');
        if (isset($patch['options']) && is_array($patch['options'])) {
            $patch['options'] = json_encode($patch['options']);
        }
        Capsule::table('mod_contabo_profile')->where('id', $id)->update($patch);
    }

    public function setActive(int $id, bool $active): void
    {
        $this->update($id, ['active' => $active ? 1 : 0]);
    }

    /** Append a new immutable version snapshot. Returns the new version id. */
    public function appendVersion(int $profileId, ProfileVersionInput $v): int
    {
        $nextVersion = ((int) Capsule::table('mod_contabo_profile_version')
            ->where('profile_id', $profileId)
            ->max('version')) + 1;

        $now = date('Y-m-d H:i:s');
        $id = (int) Capsule::table('mod_contabo_profile_version')->insertGetId([
            'profile_id'             => $profileId,
            'version'                => $nextVersion,
            'base_monthly_eur'       => $v->baseMonthlyEur,
            'configured_monthly_eur' => $v->configuredMonthlyEur,
            'setup_fee_eur'          => $v->setupFeeEur,
            'options_snapshot'       => json_encode($v->optionsSnapshot),
            'specs_snapshot'         => json_encode($v->specsSnapshot),
            'fx_rate'                => $v->fxRate,
            'fx_source'              => $v->fxSource,
            'fx_markup_pct'          => $v->fxMarkupPct,
            'gst_pct'                => $v->gstPct,
            'currency_iso'           => $v->currencyIso,
            'final_monthly'          => $v->finalMonthly,
            'final_setup'            => $v->finalSetup,
            'snapshot_generated_at'  => $v->snapshotGeneratedAt,
            'created_at'             => $now,
            'updated_at'             => $now,
        ]);

        Capsule::table('mod_contabo_profile')
            ->where('id', $profileId)
            ->update(['latest_version_id' => $id, 'updated_at' => $now]);

        return $id;
    }

    /** @return array<string, mixed>|null */
    public function latestVersion(int $profileId): ?array
    {
        $row = Capsule::table('mod_contabo_profile_version')
            ->where('profile_id', $profileId)
            ->orderByDesc('version')
            ->first();
        return $row ? (array) $row : null;
    }

    /** @return list<array<string, mixed>> */
    public function listVersions(int $profileId): array
    {
        return Capsule::table('mod_contabo_profile_version')
            ->where('profile_id', $profileId)
            ->orderByDesc('version')
            ->get()->map(static fn ($r) => (array) $r)->all();
    }

    /**
     * Backward-compat shim. The slug-building logic now lives in
     * ProfileIdentityResolver::buildSlug(array). This positional-arg form is
     * preserved because existing callers (and tests) use it; it simply adapts
     * the positional args to the array shape and delegates.
     */
    public static function buildSlug(string $planSlug, int $period, string $region, string $os): string
    {
        return ProfileIdentityResolver::buildSlug([
            'plan_slug'     => $planSlug,
            'period_months' => $period,
            'region'        => $region,
            'os'            => $os,
        ]);
    }
}
