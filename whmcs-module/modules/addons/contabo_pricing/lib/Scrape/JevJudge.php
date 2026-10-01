<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Second-opinion judge (typesafe.ai "systemone"). It is advisory only: it can
 * downgrade auto_import to needs_review, never upgrade an outcome, and never
 * touches any number. Every failure mode (no key, HTTP error, timeout, bad
 * JSON, low confidence) yields status "skipped" so the deterministic gates
 * remain the sole authority.
 */
final class JevJudge
{
    public const ENDPOINT = 'https://api.typesafe.ai/v1/systemone';
    public const STATE_CAP_BYTES = 90000;

    /** @var ScrapeSettings */
    private $settings;
    /** @var HeaderAwareExecutor */
    private $executor;
    /** @var int */
    private $timeoutSec;

    public function __construct(ScrapeSettings $settings, HeaderAwareExecutor $executor, int $timeoutSec = 30)
    {
        $this->settings = $settings;
        $this->executor = $executor;
        $this->timeoutSec = $timeoutSec;
    }

    /** Trimmed visible text of a page: scripts/styles dropped, tags stripped, whitespace collapsed, capped. */
    public static function visibleText(string $html, int $cap = self::STATE_CAP_BYTES): string
    {
        $t = (string) preg_replace('#<(script|style|noscript|template)\b[^>]*>.*?</\1>#is', ' ', $html);
        $t = (string) preg_replace('#<!--.*?-->#s', ' ', $t);
        $t = strip_tags($t);
        $t = html_entity_decode($t, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $t = trim((string) preg_replace('/\s+/u', ' ', $t));
        if (strlen($t) > $cap) {
            $t = function_exists('mb_strcut') ? mb_strcut($t, 0, $cap, 'UTF-8') : substr($t, 0, $cap);
        }
        return $t;
    }

    /**
     * @param string|null $secondHtml page text from a disagreeing second source (adds the same_product_list question)
     * @return array{status:string, reason?:string, veto?:bool, answers?:array<string,array<string,mixed>>, model?:string, threshold?:float}
     */
    public function judge(string $html, ?string $secondHtml = null): array
    {
        $key = $this->settings->jevApiKey();
        if ($key === '') {
            return ['status' => 'skipped', 'reason' => 'no api key configured'];
        }
        $threshold = $this->settings->jevConfidenceMin();
        $model = $this->settings->jevModel();

        $state = self::visibleText($html);
        if ($state === '') {
            return ['status' => 'skipped', 'reason' => 'page has no visible text'];
        }
        $questions = [
            'is_pricing_page' => [
                'type' => 'choice',
                'instructions' => 'Does this text look like a hosting product page that lists VPS/VDS plans with monthly prices?',
                'criteria' => [
                    'yes' => 'The text describes server plans with CPU, RAM, storage and a recurring price.',
                    'no' => 'The text is an error page, a captcha/bot wall, a login page, or unrelated content.',
                ],
            ],
        ];
        if ($secondHtml !== null) {
            $second = self::visibleText($secondHtml, (int) (self::STATE_CAP_BYTES / 2));
            $state = self::visibleText($html, (int) (self::STATE_CAP_BYTES / 2)) . "\n\n=== SECOND SOURCE ===\n\n" . $second;
            $questions['same_product_list'] = [
                'type' => 'choice',
                'instructions' => 'The text has two sources separated by "=== SECOND SOURCE ===". Do both list the same set of products and the same prices?',
                'criteria' => [
                    'yes' => 'Both sources list the same plans with the same prices.',
                    'no' => 'The plans or the prices differ between the two sources.',
                ],
            ];
        }

        $body = json_encode(['state' => $state, 'model' => $model, 'questions' => $questions], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if (!is_string($body)) {
            return ['status' => 'skipped', 'reason' => 'could not encode request'];
        }

        try {
            $r = $this->executor->executeWithHeaders(
                'POST',
                self::ENDPOINT,
                ['Authorization: Bearer ' . $key, 'Content-Type: application/json', 'Accept: application/json'],
                $body,
                $this->timeoutSec
            );
        } catch (\Throwable $e) {
            return ['status' => 'skipped', 'reason' => 'transport exception: ' . str_replace($key, '***', $e->getMessage())];
        }
        if ($r['errno'] !== 0) {
            return ['status' => 'skipped', 'reason' => 'transport error: ' . str_replace($key, '***', (string) $r['error'])];
        }
        if ($r['status'] < 200 || $r['status'] >= 300) {
            return ['status' => 'skipped', 'reason' => 'HTTP ' . $r['status']];
        }
        $data = json_decode($r['body'], true);
        $answers = is_array($data) ? ($data['answers'] ?? null) : null;
        if (!is_array($answers)) {
            return ['status' => 'skipped', 'reason' => 'unparseable response'];
        }

        $accepted = [];
        $veto = false;
        foreach (array_keys($questions) as $q) {
            $a = $answers[$q] ?? null;
            if (!is_array($a) || !isset($a['choice']) || !is_string($a['choice']) || !isset($a['confidence']) || !is_numeric($a['confidence'])) {
                return ['status' => 'skipped', 'reason' => 'answer missing or malformed: ' . $q];
            }
            $conf = (float) $a['confidence'];
            if ($conf < $threshold) {
                continue; // below the bar: ignored entirely
            }
            $accepted[$q] = [
                'choice' => $a['choice'],
                'confidence' => $conf,
                'probabilities' => isset($a['probabilities']) && is_array($a['probabilities']) ? $a['probabilities'] : [],
            ];
            if (strtolower($a['choice']) === 'no') {
                $veto = true;
            }
        }
        if ($accepted === []) {
            return ['status' => 'skipped', 'reason' => 'confidence below threshold', 'threshold' => $threshold, 'model' => $model];
        }
        return ['status' => 'ok', 'veto' => $veto, 'answers' => $accepted, 'model' => $model, 'threshold' => $threshold];
    }

    /**
     * Jev may only move auto_import -> needs_review. Every other outcome, and
     * every non-veto verdict, passes through untouched.
     *
     * @param array<string,mixed> $verdict result of judge()
     */
    public static function apply(string $outcome, array $verdict): string
    {
        if ($outcome === 'auto_import' && ($verdict['status'] ?? '') === 'ok' && !empty($verdict['veto'])) {
            return 'needs_review';
        }
        return $outcome;
    }
}
