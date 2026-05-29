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
     * Soft-deleted (trashed) profiles are EXCLUDED by default — they live in the
     * Trash until restored or purged. Pass $includeTrashed=true only for the
     * Trash view / admin tooling.
     *
     * @return list<array<string, mixed>>
     */
    public function listProfiles(bool $activeOnly = true, bool $includeTrashed = false): array
    {
        $q = Capsule::table('mod_contabo_profile');
        if ($activeOnly) {
            $q->where('active', true);
        }
        if (!$includeTrashed) {
            $q->whereNull('deleted_at');
        }
        $rows = $q->orderBy('plan_slug')->orderBy('period_months')->get();
        $out = [];
        foreach ($rows as $r) {
            $out[] = (array) $r;
        }
        return $out;
    }

    /**
     * Profiles currently in the Trash (soft-deleted), newest-deleted first.
     *
     * @return list<array<string, mixed>>
     */
    public function listTrashed(): array
    {
        $rows = Capsule::table('mod_contabo_profile')
            ->whereNotNull('deleted_at')
            ->orderByDesc('deleted_at')
            ->get();
        $out = [];
        foreach ($rows as $r) {
            $out[] = (array) $r;
        }
        return $out;
    }

    /**
     * Soft-delete (Trash) a profile: stamp deleted_at. Reversible via restore().
     * Writes the column directly (it is intentionally outside the generic
     * update() whitelist so a normal edit can never trash/untrash a profile).
     */
    public function softDelete(int $id): void
    {
        Capsule::table('mod_contabo_profile')
            ->where('id', $id)
            ->update(['deleted_at' => date('Y-m-d H:i:s'), 'updated_at' => date('Y-m-d H:i:s')]);
    }

    /** Restore a trashed profile (the Undo): clear deleted_at. */
    public function restore(int $id): void
    {
        Capsule::table('mod_contabo_profile')
            ->where('id', $id)
            ->update(['deleted_at' => null, 'updated_at' => date('Y-m-d H:i:s')]);
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

    /**
     * Updatable columns for a profile row. Anything outside this set is dropped
     * by update() so a malformed or hostile $patch can never mass-assign
     * arbitrary columns. `slug` is intentionally excluded — it is identity and
     * must not change through the generic edit path.
     *
     * @var list<string>
     */
    private const UPDATABLE_COLUMNS = [
        'name', 'plan_slug', 'period_months', 'published_cycles_mask',
        'region', 'os', 'options', 'tags',
        'sync_strategy', 'active', 'profile_mode', 'expose_configurable_options',
        'profile_fingerprint_hash', 'profile_identity_json', 'latest_version_id',
        'default_policy', 'margin_floor_pct', 'fx_buffer_pct',
        'large_increase_threshold_pct', 'max_increase_pct', 'notice_days_default',
        'allow_auto_decrease',
    ];

    /** @param array<string, mixed> $patch */
    public function update(int $id, array $patch): void
    {
        $clean = array_intersect_key($patch, array_flip(self::UPDATABLE_COLUMNS));
        if (isset($clean['options']) && is_array($clean['options'])) {
            $clean['options'] = json_encode($clean['options']);
        }
        $clean['updated_at'] = date('Y-m-d H:i:s');
        Capsule::table('mod_contabo_profile')->where('id', $id)->update($clean);
    }

    public function setActive(int $id, bool $active): void
    {
        $this->update($id, ['active' => $active ? 1 : 0]);
    }

    /** Append a new immutable version snapshot. Returns the new version id. */
    public function appendVersion(int $profileId, ProfileVersionInput $v): int
    {
        $id = 0;
        Capsule::connection()->transaction(function () use ($profileId, $v, &$id) {
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
                'period_prices_json'     => $v->periodPricesEur === [] ? null : json_encode($v->periodPricesEur),
                'snapshot_generated_at'  => $v->snapshotGeneratedAt,
                'created_at'             => $now,
                'updated_at'             => $now,
            ]);

            Capsule::table('mod_contabo_profile')
                ->where('id', $profileId)
                ->update(['latest_version_id' => $id, 'updated_at' => $now]);
        });
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
