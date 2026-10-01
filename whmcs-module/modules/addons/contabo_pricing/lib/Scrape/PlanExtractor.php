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
 * One Contabo product page embeds the whole catalogue (categories, products,
 * navItems), so a single page yields every plan. Which plans belong to which
 * family is DISCOVERED from that blob by CatalogStructure (nav-linked
 * categories, membership from category.products); products outside every
 * family are excluded unless their slug is in the legacy allow-list.
 */
final class PlanExtractor
{
    /** @var list<string> product slugs allowed through although they belong to no family */
    private $legacyAllowlist;
    /** @var PlanNormalizer */
    private $normalizer;
    /** @var DomPlanProbe */
    private $probe;
    /** @var SapperLiteralDecoder */
    private $decoder;

    /** @param list<string> $legacyAllowlist */
    public function __construct(
        array $legacyAllowlist = [],
        ?DomPlanProbe $probe = null,
        ?SapperLiteralDecoder $decoder = null
    ) {
        $this->legacyAllowlist = array_values(array_filter($legacyAllowlist, 'is_string'));
        $this->normalizer = new PlanNormalizer();
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

        // ── S1: sapper decoder ──────────────────────────────────────────────
        if ($sapperPresent) {
            try {
                $payload = $this->decoder->decodeFromHtml($html);
                $pre = $payload['preloaded'][0] ?? null;
                if (!is_array($pre) || !isset($pre['products']) || !is_array($pre['products'])) {
                    $warnings[] = 'sapper: preloaded[0].products missing';
                } else {
                    $rules = PlanNormalizer::extractPasswordRules($html);
                    $structure = (new CatalogStructure($pre))->discover();
                    $plans = $this->collectDiscovered($pre, $structure, $fetchedAt, $rules, $warnings);
                    $navTitles = [];
                    foreach ($structure['nav'] as $n) {
                        if ($n['category_id'] !== null) {
                            $navTitles[] = (string) $n['title'];
                        }
                    }
                    if ($plans !== []) {
                        $probe = $this->probe->probe($html);
                        $this->crossCheck($plans, $probe, $warnings);
                        return new ExtractionResult($plans, ExtractionResult::STRATEGY_SAPPER, true, $warnings, $probe, $structure, $navTitles);
                    }
                    $warnings[] = 'sapper: no plan products found under any nav-linked category';
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
                $plans = $this->collectProviderJson($products, $fetchedAt, $warnings);
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

    /**
     * Plans of every discovered family (nav order, then category member order),
     * plus allow-listed legacy products.
     *
     * @param array<string,mixed> $pre
     * @param array{categories:array<string,array<string,mixed>>,nav:list<array<string,mixed>>} $structure
     * @param array{min_length:int,max_length:int,alphanumeric_only:bool,no_special_chars:bool}|null $rules
     * @param list<string> $warnings
     * @return list<array<string,mixed>>
     */
    private function collectDiscovered(array $pre, array $structure, string $fetchedAt, ?array $rules, array &$warnings): array
    {
        $products = $pre['products'];
        $units = (new CatalogStructure())->objectStorageUnits($products);
        $plans = [];
        $rank = 0;
        $claimed = [];
        foreach (CatalogStructure::familyCategories($structure) as $cat) {
            $label = $cat['nav_title'] !== null && $cat['nav_title'] !== '' ? (string) $cat['nav_title'] : (string) $cat['title'];
            $famRank = 0;
            foreach ($cat['plan_ids'] as $pid) {
                $claimed[$pid] = true;
                try {
                    $plans[] = $this->normalizer->normalize($products[$pid], $fetchedAt, $rules, [
                        'family' => $label, 'family_key' => $cat['slug'], 'category_id' => $cat['category_id'],
                        'plan_rank' => ++$rank, 'plan_family_rank' => ++$famRank, 'object_units' => $units,
                    ]);
                } catch (\InvalidArgumentException $e) {
                    $rank--;
                    $famRank--;
                    $warnings[] = 'rejected plan: ' . $e->getMessage();
                }
            }
        }
        if ($this->legacyAllowlist !== []) {
            $famRank = 0;
            foreach ($products as $pid => $p) {
                if (!is_array($p) || isset($claimed[$pid]) || !isset($p['slug']) || !in_array($p['slug'], $this->legacyAllowlist, true)) {
                    continue;
                }
                try {
                    $plans[] = $this->normalizer->normalize($p, $fetchedAt, $rules, [
                        'family' => 'Legacy', 'family_key' => 'legacy', 'category_id' => '',
                        'plan_rank' => ++$rank, 'plan_family_rank' => ++$famRank, 'object_units' => $units,
                    ]);
                } catch (\InvalidArgumentException $e) {
                    $rank--;
                    $famRank--;
                    $warnings[] = 'rejected plan: ' . $e->getMessage();
                }
            }
        }
        return $plans;
    }

    /**
     * @param array<mixed> $products list of product records (provider-JSON shape converted to Contabo shape)
     * @param list<string> $warnings
     * @return list<array<string,mixed>>
     */
    private function collectProviderJson(array $products, string $fetchedAt, array &$warnings): array
    {
        $plans = [];
        $seen = [];
        $rank = 0;
        foreach ($products as $p) {
            if (!is_array($p) || !isset($p['slug']) || !is_string($p['slug']) || isset($seen[$p['slug']])) {
                continue;
            }
            $seen[$p['slug']] = true;
            try {
                $ctx = ['plan_rank' => $rank + 1];
                if (isset($p['family']) && is_string($p['family']) && $p['family'] !== '') {
                    $ctx['family'] = $p['family'];
                }
                $plans[] = $this->normalizer->normalize($p, $fetchedAt, null, $ctx);
                $rank++;
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
                'family' => isset($p['family']) && is_string($p['family']) ? $p['family'] : null,
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
