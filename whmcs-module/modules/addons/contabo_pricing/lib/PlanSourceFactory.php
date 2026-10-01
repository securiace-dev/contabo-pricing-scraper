<?php
declare(strict_types=1);

namespace ContaboPricing;

use ContaboPricing\Catalog\CatalogReader;
use ContaboPricing\Fx\FxService;
use ContaboPricing\Quote\QuoteService;

/**
 * Single construction point for the PlanSource. Always local: the Rust API
 * wrapper is intentionally not offered (that service is being retired).
 */
final class PlanSourceFactory
{
    public static function fromSettings(?Settings $settings = null, ?RequestExecutor $executor = null): PlanSource
    {
        return new LocalPlanSource(new CatalogReader(), new FxService($executor), new QuoteService());
    }
}
