<?php
declare(strict_types=1);

namespace ContaboPricing\Fx;

use ContaboPricing\CurlRequestExecutor;
use ContaboPricing\RequestExecutor;
use WHMCS\Database\Capsule;

/**
 * EUR->INR rate from Frankfurter with a persistent 300 s cache in
 * mod_contabo_settings (fx.cache_json / fx.cached_at). Port of GET /fx in the
 * retired Rust API, plus the normalised keys the addon consumers read:
 * eurInr (SyncEngine), rate|mid + source + age_minutes (assets/app.js).
 *
 * PHP 7.4 polyglot.
 */
final class FxService
{
    public const URL = 'https://api.frankfurter.app/latest?base=EUR&symbols=INR';
    public const TTL_SECONDS = 300;
    private const TABLE = 'mod_contabo_settings';
    private const KEY_JSON = 'fx.cache_json';
    private const KEY_AT = 'fx.cached_at';

    /** @var RequestExecutor */ private $executor;
    /** @var callable */        private $clock;

    public function __construct(?RequestExecutor $executor = null, ?callable $clock = null)
    {
        $this->executor = $executor ?? new CurlRequestExecutor();
        $this->clock = $clock ?? 'time';
    }

    /**
     * @return array<string,mixed>
     * @throws \RuntimeException when no fresh fetch and no cached copy exist
     */
    public function rates(): array
    {
        $now = (int) call_user_func($this->clock);
        $cached = $this->readCache();

        if ($cached !== null && ($now - $cached['at']) < self::TTL_SECONDS && ($now - $cached['at']) >= 0) {
            return $this->decorate($cached['body'], $cached['at'], $now, false);
        }

        $err = '';
        try {
            [$code, $resp, $errno, $emsg] = $this->executor->execute(
                'GET',
                self::URL,
                ['Accept: application/json'],
                null,
                8
            );
            $body = json_decode((string) $resp, true);
            if ($errno === 0 && $code >= 200 && $code < 300 && is_array($body) && self::inr($body) !== null) {
                $this->writeCache($body, $now);
                return $this->decorate($body, $now, $now, false);
            }
            $err = $errno !== 0 ? "transport error {$emsg}" : "HTTP {$code} or unparseable body";
        } catch (\Throwable $e) {
            $err = $e->getMessage();
        }

        if ($cached !== null) {
            return $this->decorate($cached['body'], $cached['at'], $now, true);
        }
        throw new \RuntimeException('FX rates unavailable: ' . $err);
    }

    /** @param array<string,mixed> $body */
    private static function inr(array $body): ?float
    {
        $v = $body['rates']['INR'] ?? null;
        return (is_int($v) || is_float($v) || (is_string($v) && is_numeric($v))) && (float) $v > 0
            ? (float) $v
            : null;
    }

    /**
     * @param array<string,mixed> $body
     * @return array<string,mixed>
     */
    private function decorate(array $body, int $fetchedAt, int $now, bool $stale): array
    {
        $rate = self::inr($body);
        $out = $body;
        $out['eurInr'] = $rate;
        $out['rate'] = $rate;
        $out['mid'] = $rate;
        $out['source'] = 'frankfurter';
        $out['fetched_at'] = gmdate('Y-m-d\TH:i:s\Z', $fetchedAt);
        $out['age_minutes'] = max(0, (int) round(($now - $fetchedAt) / 60));
        if ($stale) {
            $out['stale'] = true;
        }
        return $out;
    }

    /** @return array{body: array<string,mixed>, at: int}|null */
    private function readCache(): ?array
    {
        try {
            $json = Capsule::table(self::TABLE)->where('key', self::KEY_JSON)->value('value');
            $at = Capsule::table(self::TABLE)->where('key', self::KEY_AT)->value('value');
        } catch (\Throwable $e) {
            return null;
        }
        if ($json === null || $at === null) {
            return null;
        }
        $body = json_decode((string) $json, true);
        if (!is_array($body) || self::inr($body) === null || !is_numeric($at)) {
            return null;
        }
        return ['body' => $body, 'at' => (int) $at];
    }

    /** @param array<string,mixed> $body */
    private function writeCache(array $body, int $now): void
    {
        try {
            $ts = date('Y-m-d H:i:s', $now);
            Capsule::table(self::TABLE)->updateOrInsert(
                ['key' => self::KEY_JSON],
                ['value' => (string) json_encode($body, JSON_UNESCAPED_SLASHES), 'updated_at' => $ts]
            );
            Capsule::table(self::TABLE)->updateOrInsert(
                ['key' => self::KEY_AT],
                ['value' => (string) $now, 'updated_at' => $ts]
            );
        } catch (\Throwable $e) {
            // Cache is best effort; the fresh value is still returned.
        }
    }
}
