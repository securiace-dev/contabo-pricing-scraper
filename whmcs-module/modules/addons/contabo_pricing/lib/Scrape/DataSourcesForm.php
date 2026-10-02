<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Validation, coercion and persistence for the Data sources admin form.
 * Pure of HTTP concerns (method/CSRF/redirect live in AdminController) so it
 * can be unit tested. Budgets are entered in USD and stored as micro-USD.
 * API key fields: blank keeps the stored key; a value is sealed by the
 * repositories (SecretStore) and never echoed back.
 */
final class DataSourcesForm
{
    /** @var ScrapeSettings */
    private $settings;
    /** @var SourceConfigRepository */
    private $repo;

    public function __construct(?ScrapeSettings $settings = null, ?SourceConfigRepository $repo = null)
    {
        $this->settings = $settings ?? new ScrapeSettings();
        $this->repo = $repo ?? new SourceConfigRepository();
    }

    /**
     * @param array<string,mixed> $req
     * @return array{ok:bool, message:string, errors:list<string>, keys_replaced:list<string>}
     */
    public function apply(array $req): array
    {
        $errors = [];
        $replaced = [];
        $s = $this->settings;

        // ── decision / budget settings ──────────────────────────────────────
        $set = [];
        $set['scrape.enabled'] = !empty($req['scrape_enabled']) ? '1' : '0';
        $set['scrape.rank_mode'] = (($req['rank_mode'] ?? '') === 'manual') ? 'manual' : 'auto';
        $set['scrape.plan_source'] = (($req['plan_source'] ?? '') === 'native') ? 'native' : 'local';
        $set['scrape.require_human_review'] = !empty($req['require_human_review']) ? '1' : '0';
        $set['scrape.jev_enabled'] = !empty($req['jev_enabled']) ? '1' : '0';

        $micro = self::usdToMicro($req['monthly_budget_usd'] ?? null);
        if ($micro !== null) {
            $set['scrape.monthly_budget_micro'] = (string) $micro;
        }
        $micro = self::usdToMicro($req['per_run_cap_usd'] ?? null);
        if ($micro !== null) {
            $set['scrape.per_run_cap_micro'] = (string) $micro;
        }
        foreach (['cloud_vps', 'storage_vps', 'cloud_vds'] as $fam) {
            $n = self::intIn($req['min_plans_' . $fam] ?? null, 0, 100);
            if ($n !== null) {
                $set['scrape.min_plans_' . $fam] = (string) $n;
            }
        }
        $f = self::floatIn($req['max_drop_pct'] ?? null, 0.0, 100.0);
        if ($f !== null) {
            $set['scrape.max_drop_pct'] = self::num($f);
        }
        $f = self::floatIn($req['max_price_change_pct'] ?? null, 0.0, 1000.0);
        if ($f !== null) {
            $set['scrape.max_price_change_pct'] = self::num($f);
        }
        $f = self::floatIn($req['jev_confidence_min'] ?? null, 0.0, 1.0);
        if ($f !== null) {
            $set['scrape.jev_confidence_min'] = self::num($f);
        }
        $model = trim((string) ($req['jev_model'] ?? ''));
        if ($model !== '') {
            if (preg_match('/^[A-Za-z0-9._-]{1,40}$/', $model) === 1) {
                $set['scrape.jev_model'] = $model;
            } else {
                $errors[] = 'Jev model name contains invalid characters.';
            }
        }

        // ── per-source rows (validated first so a bad row saves nothing) ────
        $rows = [];
        $src = isset($req['src']) && is_array($req['src']) ? $req['src'] : [];
        foreach ($src as $sourceId => $f) {
            $sourceId = (string) $sourceId;
            if (!is_array($f) || $this->repo->find($sourceId) === null) {
                $errors[] = 'Unknown data source: ' . substr(preg_replace('/[^A-Za-z0-9_.-]/', '', $sourceId), 0, 40);
                continue;
            }
            $data = ['enabled' => !empty($f['enabled']) ? 1 : 0];

            $base = trim((string) ($f['base_url'] ?? ''));
            if ($base !== '' && preg_match('#^https://[^\s]+$#i', $base) !== 1) {
                $errors[] = $sourceId . ': base URL must start with https://';
                continue;
            }
            $data['base_url'] = $base;

            $prio = trim((string) ($f['priority'] ?? ''));
            $data['priority'] = $prio === '' ? null : (ctype_digit($prio) ? (int) $prio : null);

            $m = self::usdToMicro($f['monthly_budget_usd'] ?? null);
            if ($m !== null) {
                $data['monthly_budget_micro'] = $m;
            }
            $m = self::usdToMicro($f['per_run_cap_usd'] ?? null);
            if ($m !== null) {
                $data['per_run_cap_micro'] = $m;
            }

            $opts = trim((string) ($f['options_json'] ?? ''));
            if ($opts !== '') {
                $decoded = json_decode($opts, true);
                if (!is_array($decoded)) {
                    $errors[] = $sourceId . ': options must be a JSON object.';
                    continue;
                }
                $data['options'] = $decoded;
            } else {
                $data['options_json'] = '';
            }

            $key = trim((string) ($f['api_key'] ?? ''));
            if ($key !== '') {
                if (strlen($key) > 512 || preg_match('/[\x00-\x20\x7f]/', $key) === 1) {
                    $errors[] = $sourceId . ': API key has invalid characters or is too long.';
                    continue;
                }
                $data['api_key'] = $key;
                $replaced[] = $sourceId;
            }
            $rows[$sourceId] = $data;
        }

        $jevKey = trim((string) ($req['jev_api_key'] ?? ''));
        if ($jevKey !== '' && (strlen($jevKey) > 512 || preg_match('/[\x00-\x20\x7f]/', $jevKey) === 1)) {
            $errors[] = 'Jev API key has invalid characters or is too long.';
            $jevKey = '';
        }

        foreach ($set as $k => $v) {
            $s->set($k, $v);
        }
        foreach ($rows as $id => $data) {
            $this->repo->save($id, $data);
        }
        if ($jevKey !== '') {
            $s->setJevApiKey($jevKey);
            $replaced[] = 'jev';
        }

        $ok = $errors === [];
        $msg = $ok ? 'Data sources saved.' : 'Saved with problems: ' . implode(' ', $errors);
        return ['ok' => $ok, 'message' => $msg, 'errors' => $errors, 'keys_replaced' => $replaced];
    }

    /** USD string/number -> micro-USD; null when absent or non-numeric; negatives clamp to 0. @param mixed $v */
    public static function usdToMicro($v): ?int
    {
        if ($v === null || $v === '' || !is_numeric($v)) {
            return null;
        }
        return max(0, (int) round(((float) $v) * 1000000));
    }

    /** @param mixed $v */
    private static function intIn($v, int $min, int $max): ?int
    {
        if ($v === null || $v === '' || !is_numeric($v)) {
            return null;
        }
        return max($min, min($max, (int) $v));
    }

    /** @param mixed $v */
    private static function floatIn($v, float $min, float $max): ?float
    {
        if ($v === null || $v === '' || !is_numeric($v)) {
            return null;
        }
        return max($min, min($max, (float) $v));
    }

    private static function num(float $f): string
    {
        return rtrim(rtrim(number_format($f, 4, '.', ''), '0'), '.') ?: '0';
    }
}
