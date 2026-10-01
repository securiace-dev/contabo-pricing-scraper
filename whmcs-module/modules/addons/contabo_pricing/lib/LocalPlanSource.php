<?php
declare(strict_types=1);

namespace ContaboPricing;

use ContaboPricing\Catalog\CatalogReader;
use ContaboPricing\Fx\FxService;
use ContaboPricing\Quote\QuoteService;

/**
 * PlanSource backed by the addon's own imported catalog, Frankfurter FX and the
 * PHP quote port. No HTTP to any pricing service.
 */
final class LocalPlanSource implements PlanSource
{
    /** @var CatalogReader */ private $catalog;
    /** @var FxService */     private $fx;
    /** @var QuoteService */  private $quote;

    public function __construct(CatalogReader $catalog, FxService $fx, QuoteService $quote)
    {
        $this->catalog = $catalog;
        $this->fx      = $fx;
        $this->quote   = $quote;
    }

    public function meta(): array
    {
        return $this->catalog->meta();
    }

    public function plans(?string $family = null): array
    {
        return $this->catalog->plans($family);
    }

    public function plan(string $slug): array
    {
        $plan = $this->catalog->plan($slug);
        if ($plan === null) {
            throw new \RuntimeException("plan {$slug} not found in the local catalog");
        }
        return $plan;
    }

    public function configurator(string $slug): array
    {
        $cfg = $this->catalog->configurator($slug);
        $plan = $this->catalog->plan($slug);
        if ($plan !== null) {
            if (!isset($cfg['contract_periods']) && isset($plan['periods'])) {
                $cfg['contract_periods'] = $plan['periods'];
            }
            if (!isset($cfg['title']) && isset($plan['product_name'])) {
                $cfg['title'] = $plan['product_name'];
            }
            if (!isset($cfg['family']) && isset($plan['family'])) {
                $cfg['family'] = $plan['family'];
            }
        }
        return $cfg;
    }

    public function fx(): array
    {
        return $this->fx->rates();
    }

    public function quote(array $body): array
    {
        $req = $body;
        // Accept the admin UI's key names alongside the Rust wire names.
        if (!isset($req['currency']) && isset($body['currency_iso'])) {
            $req['currency'] = (string) $body['currency_iso'];
        }
        if (!isset($req['gst']) && isset($body['apply_gst'])) {
            $req['gst'] = (bool) $body['apply_gst'];
        }
        if (!isset($req['fx_markup']) && isset($body['fx_markup_pct'])) {
            $req['fx_markup'] = ((float) $body['fx_markup_pct']) / 100.0;
        }
        if (($req['currency'] ?? 'EUR') === 'INR' && !isset($req['fx_rate'])) {
            $rate = $this->fx->rates()['eurInr'] ?? null;
            if ($rate !== null) {
                $req['fx_rate'] = (float) $rate;
            }
        }
        $slug = (string) ($req['plan_slug'] ?? '');
        return $this->quote->quote($this->plan($slug), $req);
    }
}
