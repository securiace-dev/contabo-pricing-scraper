<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use ContaboPricing\SecretStore;
use WHMCS\Database\Capsule;

/**
 * Typed accessors for the scrape.* keys in mod_contabo_settings. Writes use
 * the same updateOrInsert pattern as AdminController::taxSettingsSave().
 * scrape.jev_api_key is stored sealed and is never returned by all().
 */
final class ScrapeSettings
{
    private const TABLE = 'mod_contabo_settings';
    public const KEY_JEV_API_KEY = 'scrape.jev_api_key';

    /** @return array<string,string> */
    public static function defaults(): array
    {
        return [
            'scrape.enabled' => '0',
            'scrape.rank_mode' => 'auto',
            'scrape.monthly_budget_micro' => '500000',
            'scrape.per_run_cap_micro' => '50000',
            'scrape.min_plans_cloud_vps' => '6',
            'scrape.min_plans_storage_vps' => '5',
            'scrape.min_plans_cloud_vds' => '5',
            'scrape.max_drop_pct' => '20',
            'scrape.max_price_change_pct' => '50',
            'scrape.require_human_review' => '1',
            'scrape.jev_enabled' => '0',
            self::KEY_JEV_API_KEY => '',
            'scrape.jev_model' => 'jev-1.13.0',
            'scrape.jev_confidence_min' => '0.80',
            'scrape.plan_source' => 'local',
            // explicit fetch-target override only; [] = discover from the family registry
            'scrape.plan_urls_json' => '[]',
            // product slugs outside every discovered family that may still be imported
            'scrape.legacy_allowlist_json' => '[]',
            'scrape.last_run_at' => '',
        ];
    }

    public function get(string $key): string
    {
        $defaults = self::defaults();
        $default = $defaults[$key] ?? '';
        $value = Capsule::table(self::TABLE)->where('key', $key)->value('value');
        return $value === null ? $default : (string) $value;
    }

    public function set(string $key, string $value): void
    {
        if (!array_key_exists($key, self::defaults())) {
            throw new \InvalidArgumentException('Unknown scrape setting: ' . $key);
        }
        Capsule::table(self::TABLE)->updateOrInsert(
            ['key' => $key],
            ['value' => $value, 'updated_at' => date('Y-m-d H:i:s')]
        );
    }

    /** @return array<string,string> every key with defaults applied; the JEV key is masked out. */
    public function all(): array
    {
        $out = [];
        foreach (array_keys(self::defaults()) as $key) {
            $out[$key] = $key === self::KEY_JEV_API_KEY ? '' : $this->get($key);
        }
        return $out;
    }

    public function enabled(): bool
    {
        return $this->get('scrape.enabled') === '1';
    }

    public function rankMode(): string
    {
        return $this->get('scrape.rank_mode') === 'manual' ? 'manual' : 'auto';
    }

    public function monthlyBudgetMicro(): int
    {
        return (int) $this->get('scrape.monthly_budget_micro');
    }

    public function perRunCapMicro(): int
    {
        return (int) $this->get('scrape.per_run_cap_micro');
    }

    /** @return array<string,int> family display name => minimum plan count */
    public function minPlansByFamily(): array
    {
        return [
            PlanUrlList::FAMILY_VPS => (int) $this->get('scrape.min_plans_cloud_vps'),
            PlanUrlList::FAMILY_STORAGE => (int) $this->get('scrape.min_plans_storage_vps'),
            PlanUrlList::FAMILY_VDS => (int) $this->get('scrape.min_plans_cloud_vds'),
        ];
    }

    public function maxDropPct(): float
    {
        return (float) $this->get('scrape.max_drop_pct');
    }

    public function maxPriceChangePct(): float
    {
        return (float) $this->get('scrape.max_price_change_pct');
    }

    public function requireHumanReview(): bool
    {
        return $this->get('scrape.require_human_review') !== '0';
    }

    public function jevEnabled(): bool
    {
        return $this->get('scrape.jev_enabled') === '1';
    }

    public function jevModel(): string
    {
        return $this->get('scrape.jev_model');
    }

    public function jevConfidenceMin(): float
    {
        return (float) $this->get('scrape.jev_confidence_min');
    }

    public function planSource(): string
    {
        return $this->get('scrape.plan_source') === 'native' ? 'native' : 'local';
    }

    /** Stores the key sealed. A blank key is ignored (keeps the existing one). */
    public function setJevApiKey(string $plain): void
    {
        if ($plain === '') {
            return;
        }
        $this->set(self::KEY_JEV_API_KEY, SecretStore::seal($plain));
    }

    public function jevApiKey(): string
    {
        return SecretStore::open($this->get(self::KEY_JEV_API_KEY));
    }

    public function hasJevApiKey(): bool
    {
        return $this->get(self::KEY_JEV_API_KEY) !== '';
    }

    /**
     * Explicit fetch-target override. Empty (the default) means the targets are
     * discovered from the family registry. The pre-v16 built-in 16-URL list is
     * ignored so a stale default can never pin the run to legacy slugs.
     *
     * @return list<string>
     */
    public function planUrls(): array
    {
        $decoded = json_decode($this->get('scrape.plan_urls_json'), true);
        $urls = [];
        if (is_array($decoded)) {
            foreach ($decoded as $u) {
                if (is_string($u) && preg_match('#^https://contabo\.com/#', $u) === 1) {
                    $urls[] = $u;
                }
            }
        }
        if (count($urls) === 16 && strpos($urls[0], '/en/vps/cloud-vps-10/') !== false) {
            return [];
        }
        return $urls;
    }

    /** @return list<string> product slugs allowed although they belong to no family */
    public function legacyAllowlist(): array
    {
        $decoded = json_decode($this->get('scrape.legacy_allowlist_json'), true);
        $out = [];
        if (is_array($decoded)) {
            foreach ($decoded as $s) {
                if (is_string($s) && preg_match('/^[a-z0-9-]{1,60}$/', $s) === 1) {
                    $out[] = $s;
                }
            }
        }
        return $out;
    }

    public function touchLastRun(): void
    {
        $this->set('scrape.last_run_at', date('Y-m-d H:i:s'));
    }
}
