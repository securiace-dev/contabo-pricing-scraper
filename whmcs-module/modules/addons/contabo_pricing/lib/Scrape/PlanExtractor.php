<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Turns a fetched page (and/or provider-extracted JSON) into normalized plans.
 *
 *   S1 sapper     decode the page's `__SAPPER__` literal (no JS evaluation)
 *   S2 provider   provider JSON of the fixed shape
 *                 {products:[{slug,title,type,price_eur,
 *                             periods:[{length,discount_eur,setup_eur}],
 *                             specs:[{type,title,subtitle}]}]}
 *                 -- every derived number is recomputed here
 *   S3 dom_probe  visible title/price only; cross-check, NEVER importable
 *
 * One Contabo page embeds the whole product catalogue, so a single page yields
 * every wanted plan; the extractor picks the wanted slugs out of it.
 */
final class PlanExtractor
{
    /** @var PlanUrlList */
    private $urls;
    /** @var PlanNormalizer */
    private $normalizer;
    /** @var DomPlanProbe */
    private $probe;
    /** @var SapperLiteralDecoder */
    private $decoder;

    public function __construct(
        ?PlanUrlList $urls = null,
        ?DomPlanProbe $probe = null,
        ?SapperLiteralDecoder $decoder = null
    ) {
        $this->urls = $urls ?? new PlanUrlList();
        $this->normalizer = new PlanNormalizer($this->urls);
        $this->probe = $probe ?? new DomPlanProbe();
        $this->decoder = $decoder ?? new SapperLiteralDecoder();
    }

    /**
     * @param array<string,mixed>|null $providerJson
     * @param string|null $fetchedAt ISO-8601 UTC; defaults to now
     */
    public function extract(?string $html, ?array $providerJson = null, ?string $fetchedAt = null): ExtractionResult
    {
        $fetchedAt = $fetchedAt ?? gmdate('Y-m-d\TH:i:s.') . substr(sprintf('%.3F', microtime(true)), -3) . 'Z';
        $warnings = [];
        $html = $html ?? '';
        $sapperPresent = $html !== '' && SapperLiteralDecoder::present($html);
        $wanted = $this->wantedSlugs();

        // ── S1: sapper decoder ──────────────────────────────────────────────
        if ($sapperPresent) {
            try {
                $payload = $this->decoder->decodeFromHtml($html);
                $products = $payload['preloaded'][0]['products'] ?? null;
                if (!is_array($products)) {
                    $warnings[] = 'sapper: preloaded[0].products missing';
                } else {
                    $rules = PlanNormalizer::extractPasswordRules($html);
                    $plans = $this->collect($products, $wanted, $fetchedAt, $rules, $warnings);
                    if ($plans !== []) {
                        $probe = $this->probe->probe($html);
                        $this->crossCheck($plans, $probe, $warnings);
                        return new ExtractionResult($plans, ExtractionResult::STRATEGY_SAPPER, true, $warnings, $probe);
                    }
                    $warnings[] = 'sapper: no wanted plan found in products';
                }
            } catch (DecoderException $e) {
                $warnings[] = 'sapper: ' . $e->getMessage();
            }
        } elseif ($html !== '') {
            $warnings[] = 'sapper: __SAPPER__ not present in page';
        }

        // ── S2: provider JSON ───────────────────────────────────────────────
        if ($providerJson !== null) {
            $products = $this->productsFromProviderJson($providerJson, $warnings);
            if ($products !== []) {
                $plans = $this->collect($products, $wanted, $fetchedAt, null, $warnings);
                if ($plans !== []) {
                    return new ExtractionResult($plans, ExtractionResult::STRATEGY_PROVIDER_JSON, $sapperPresent, $warnings);
                }
            }
        }

        // ── S3: DOM probe (cross-check only) ────────────────────────────────
        if ($html !== '') {
            $warnings[] = 'dom_probe: visible-DOM data only; not importable';
            return new ExtractionResult(
                [],
                ExtractionResult::STRATEGY_DOM_PROBE,
                $sapperPresent,
                $warnings,
                $this->probe->probe($html)
            );
        }

        return new ExtractionResult([], ExtractionResult::STRATEGY_NONE, $sapperPresent, $warnings);
    }

    /** @return list<string> slugs in PlanUrlList order */
    private function wantedSlugs(): array
    {
        $out = [];
        foreach ($this->urls->urls() as $u) {
            $out[] = PlanUrlList::slugFromUrl($u);
        }
        return $out;
    }

    /**
     * @param array<mixed> $products map or list of product records
     * @param list<string> $wanted
     * @param array{min_length:int,max_length:int,alphanumeric_only:bool,no_special_chars:bool}|null $rules
     * @param list<string> $warnings
     * @return list<array<string,mixed>>
     */
    private function collect(array $products, array $wanted, string $fetchedAt, ?array $rules, array &$warnings): array
    {
        $bySlug = [];
        foreach ($products as $p) {
            if (is_array($p) && isset($p['slug']) && is_string($p['slug']) && !isset($bySlug[$p['slug']])) {
                $bySlug[$p['slug']] = $p;
            }
        }
        $plans = [];
        foreach ($wanted as $slug) {
            if (!isset($bySlug[$slug])) {
                $warnings[] = 'missing plan: ' . $slug;
                continue;
            }
            try {
                $plans[] = $this->normalizer->normalize($bySlug[$slug], $fetchedAt, $rules);
            } catch (\InvalidArgumentException $e) {
                $warnings[] = 'rejected plan: ' . $e->getMessage();
            }
        }
        return $plans;
    }

    /**
     * Validates the fixed S2 shape and converts it into Contabo-product shape.
     *
     * @param array<string,mixed> $json
     * @param list<string> $warnings
     * @return list<array<string,mixed>>
     */
    private function productsFromProviderJson(array $json, array &$warnings): array
    {
        $list = $json['products'] ?? null;
        if (!is_array($list)) {
            $warnings[] = 'provider_json: no products list';
            return [];
        }
        $out = [];
        foreach ($list as $p) {
            if (!is_array($p) || !isset($p['slug'], $p['type'], $p['price_eur'])
                || !is_string($p['slug']) || !is_string($p['type'])
                || !(is_int($p['price_eur']) || is_float($p['price_eur']))
            ) {
                $warnings[] = 'provider_json: malformed product skipped';
                continue;
            }
            $periods = [];
            foreach ((array) ($p['periods'] ?? []) as $per) {
                if (!is_array($per) || !isset($per['length'])) {
                    continue;
                }
                $period = ['length' => $per['length']];
                if (isset($per['discount_eur']) && (is_int($per['discount_eur']) || is_float($per['discount_eur']))) {
                    $period['discount'] = ['EUR' => $per['discount_eur']];
                }
                if (isset($per['setup_eur']) && (is_int($per['setup_eur']) || is_float($per['setup_eur']))) {
                    $period['setup'] = ['EUR' => $per['setup_eur']];
                }
                $periods[] = $period;
            }
            $specs = [];
            foreach ((array) ($p['specs'] ?? []) as $sp) {
                if (is_array($sp) && isset($sp['type']) && is_string($sp['type'])) {
                    $specs[] = [
                        'type' => $sp['type'],
                        'title' => isset($sp['title']) && is_string($sp['title']) ? $sp['title'] : null,
                        'subtitle' => isset($sp['subtitle']) && is_string($sp['subtitle']) ? $sp['subtitle'] : null,
                    ];
                }
            }
            $out[] = [
                'slug' => $p['slug'],
                'title' => isset($p['title']) && is_string($p['title']) ? $p['title'] : $p['slug'],
                'type' => $p['type'],
                'price' => ['EUR' => $p['price_eur']],
                'periods' => $periods,
                'specs' => $specs,
            ];
        }
        return $out;
    }

    /**
     * Compares the page's own visible plan with the decoded plan of the same name.
     *
     * @param list<array<string,mixed>> $plans
     * @param array{title:?string, monthly_eur:?float} $probe
     * @param list<string> $warnings
     */
    private function crossCheck(array $plans, array $probe, array &$warnings): void
    {
        if ($probe['title'] === null || $probe['monthly_eur'] === null) {
            $warnings[] = 'dom_probe: could not read the page plan title/price';
            return;
        }
        foreach ($plans as $plan) {
            if (strcasecmp((string) $plan['product_name'], $probe['title']) === 0) {
                if (abs((float) $plan['base_monthly_price'] - $probe['monthly_eur']) > 0.005) {
                    $warnings[] = sprintf(
                        'dom_probe mismatch for %s: payload %.2f vs page %.2f',
                        $plan['product_slug'],
                        (float) $plan['base_monthly_price'],
                        $probe['monthly_eur']
                    );
                }
                return;
            }
        }
        $warnings[] = 'dom_probe: page plan "' . $probe['title'] . '" not among extracted plans';
    }
}
