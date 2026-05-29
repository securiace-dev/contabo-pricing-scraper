<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Slug-aware persistence for profiles, with graceful duplicate handling.
 *
 * The legacy create path did a blind insertGetId() against a UNIQUE `slug`
 * column, so creating the same profile twice surfaced a raw
 * SQLSTATE[23000] Duplicate entry … to the admin UI. This repository
 * pre-checks the slug and returns a structured status instead of ever
 * throwing on a duplicate.
 *
 * createOrResolve() outcomes:
 *   - created          — slug was free; row inserted.
 *   - loaded_existing  — slug taken AND fingerprint matches; same thing,
 *                        return the existing row (idempotent re-create).
 *   - conflict         — slug taken but fingerprint differs; NO insert. The
 *                        caller decides (open existing / suffix / update /
 *                        cancel). A free `-N` suffix suggestion is included.
 */
final class ProfileRepository
{
    private const TABLE = 'mod_contabo_profile';

    /** @var string */
    private $defaultSyncStrategy;

    public function __construct(string $defaultSyncStrategy = 'notify')
    {
        $this->defaultSyncStrategy = $defaultSyncStrategy;
    }

    /**
     * @return array<string,mixed>|null
     */
    public function findBySlug(string $slug): ?array
    {
        $row = Capsule::table(self::TABLE)->where('slug', $slug)->first();
        return $row ? (array) $row : null;
    }

    /**
     * Create the profile, or resolve a slug collision without throwing.
     *
     * @param array<string,mixed> $input
     * @return array{status:string, profile_id:?int, existing:?array<string,mixed>, suffix_suggestion:?string}
     */
    public function createOrResolve(array $input): array
    {
        $slug = isset($input['slug']) && (string) $input['slug'] !== ''
            ? (string) $input['slug']
            : ProfileIdentityResolver::buildSlug($input);

        // Ensure the projection sees the resolved slug + a concrete mode.
        $input['slug']         = $slug;
        $input['profile_mode'] = (string) ($input['profile_mode'] ?? ProfileIdentityResolver::MODE_FIXED);

        $fingerprint = ProfileIdentityResolver::buildFingerprint($input);
        $existing    = $this->findBySlug($slug);

        // (4) Slug free → insert.
        if ($existing === null) {
            $id = $this->insert($input, $slug, $fingerprint);
            return [
                'status'            => 'created',
                'profile_id'        => $id,
                'existing'          => null,
                'suffix_suggestion' => null,
            ];
        }

        // (5)/(6) Slug taken → compare fingerprints.
        if ($this->fingerprintMatches($existing, $fingerprint)) {
            return [
                'status'            => 'loaded_existing',
                'profile_id'        => isset($existing['id']) ? (int) $existing['id'] : null,
                'existing'          => $existing,
                'suffix_suggestion' => null,
            ];
        }

        return [
            'status'            => 'conflict',
            'profile_id'        => null,
            'existing'          => $existing,
            'suffix_suggestion' => $this->nextFreeSuffix($slug),
        ];
    }

    /**
     * Compare the candidate fingerprint against an existing row's stored hash.
     *
     * Legacy rows (created before A.5.1) have a null/empty
     * profile_fingerprint_hash. We never want to BLOCK on a legacy row, so:
     *   1. recompute the existing row's fingerprint from its stored columns;
     *   2. if that recomputation matches the candidate → treat as match;
     *   3. if the stored hash is empty AND recomputation is inconclusive,
     *      prefer "match" (load existing) over "conflict".
     *
     * @param array<string,mixed> $existing
     */
    private function fingerprintMatches(array $existing, string $candidate): bool
    {
        $stored = isset($existing['profile_fingerprint_hash'])
            ? (string) $existing['profile_fingerprint_hash']
            : '';

        if ($stored !== '') {
            return hash_equals($stored, $candidate);
        }

        // Legacy / unknown stored hash → recompute from the stored row.
        $recomputed = ProfileIdentityResolver::buildFingerprint(
            $this->rowToIdentityInput($existing)
        );
        if (hash_equals($recomputed, $candidate)) {
            return true;
        }

        // Inconclusive legacy row: don't block. Prefer loaded_existing.
        return true;
    }

    /**
     * Map a stored profile row back to the identity input shape so its
     * fingerprint can be recomputed for legacy comparison.
     *
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    private function rowToIdentityInput(array $row): array
    {
        $options = null;
        if (isset($row['options']) && is_string($row['options']) && $row['options'] !== '') {
            $decoded = json_decode($row['options'], true);
            if (is_array($decoded)) {
                $options = $decoded;
            }
        }
        return [
            'profile_mode'  => (string) ($row['profile_mode'] ?? ProfileIdentityResolver::MODE_FIXED),
            'plan_slug'     => (string) ($row['plan_slug'] ?? ''),
            'period_months' => (int) ($row['period_months'] ?? 0),
            'region'        => (string) ($row['region'] ?? ''),
            'os'            => (string) ($row['os'] ?? ''),
            'options'       => $options,
            'slug'          => (string) ($row['slug'] ?? ''),
        ];
    }

    /**
     * Probe `$slug-2`, `$slug-3`, … until a free slug is found.
     */
    public function nextFreeSuffix(string $slug): string
    {
        $n = 2;
        // Bounded loop guards against a pathological run; 9999 is far past any
        // realistic number of duplicates.
        while ($n < 10000) {
            $candidate = $slug . '-' . $n;
            if ($this->findBySlug($candidate) === null) {
                return $candidate;
            }
            $n++;
        }
        // Extremely unlikely fallback: random suffix.
        return $slug . '-' . substr(sha1((string) microtime(true)), 0, 6);
    }

    /**
     * Insert a new profile row with the v4 identity columns populated.
     *
     * @param array<string,mixed> $input
     */
    private function insert(array $input, string $slug, string $fingerprint): int
    {
        $now  = date('Y-m-d H:i:s');
        $mode = (string) ($input['profile_mode'] ?? ProfileIdentityResolver::MODE_FIXED);
        $identityJson = (string) json_encode(ProfileIdentityResolver::identityProjection($input));

        return (int) Capsule::table(self::TABLE)->insertGetId([
            'slug'                     => $slug,
            'name'                     => (string) ($input['name'] ?? $slug),
            'plan_slug'                => (string) ($input['plan_slug'] ?? ''),
            'period_months'            => (int) ($input['period_months'] ?? 0),
            // v8: which cycles this profile SOURCES (offered superset; the mapping
            // narrows to the customer-facing set). Default 63 = all six.
            'published_cycles_mask'    => isset($input['published_cycles_mask'])
                ? (int) $input['published_cycles_mask'] : 63,
            'region'                   => $input['region'] ?? null,
            'os'                       => $input['os'] ?? null,
            'options'                  => $this->encodeOptions($input['options'] ?? null),
            'tags'                     => $input['tags'] ?? null,
            'sync_strategy'            => (string) ($input['sync_strategy'] ?? $this->defaultSyncStrategy),
            'active'                   => 1,
            'profile_mode'             => $mode,
            // v7 master switch (default 1 = exposed, matching the column default).
            'expose_configurable_options' => isset($input['expose_configurable_options'])
                ? (int) $input['expose_configurable_options'] : 1,
            'profile_fingerprint_hash' => $fingerprint,
            'profile_identity_json'    => $identityJson,
            'created_at'               => $now,
            'updated_at'               => $now,
        ]);
    }

    /**
     * @param mixed $options
     */
    private function encodeOptions($options): ?string
    {
        if ($options === null) {
            return null;
        }
        if (is_string($options)) {
            return $options === '' ? null : $options;
        }
        return (string) json_encode($options);
    }
}
