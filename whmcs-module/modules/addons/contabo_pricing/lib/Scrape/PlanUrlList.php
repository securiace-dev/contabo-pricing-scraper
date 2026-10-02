<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Where a run fetches from, backed by the FamilyRegistry.
 *
 * One PRODUCT page embeds the whole catalogue blob (categories, products,
 * navItems); category landing pages do not carry it. So fetch targets are
 * product pages:
 *
 *  - override: an explicit scrape.plan_urls_json list wins (operator escape hatch);
 *  - learned:  the registry's active families' sample_product_url, nav order
 *              (the first member product of each family, built exactly as
 *              PlanNormalizer builds product_url);
 *  - default:  before the first accepted run, the Core VPS entry product page.
 *
 * Landing pages (nav_href of the active families; before the first run the
 * six known nav links) are metadata and an optional DOM cross-check, never the
 * primary fetch.
 */
final class PlanUrlList
{
    /** Legacy scrape.min_plans_* family keys (settings fallback only; the registry is the source of truth). */
    public const FAMILY_VPS = 'Cloud VPS';
    public const FAMILY_STORAGE = 'Storage VPS';
    public const FAMILY_VDS = 'Cloud VDS';

    public const SITE = 'https://contabo.com/en';
    public const DEFAULT_PRODUCT_URL = 'https://contabo.com/en/vps/cloud-vps-core-4/';

    /** @var list<string> nav landing links known before the first run (metadata only) */
    public const DEFAULT_LANDING_PATHS = [
        '/vps/', '/vps-performance/', '/vps-dedicated/', '/storage-vps/', '/dedicated-servers/', '/gpu-vps/',
    ];

    /** @var list<string> */
    private $override;
    /** @var FamilyRegistry */
    private $registry;

    /** @param list<string> $override explicit URL list; empty = discover from the registry */
    public function __construct(array $override = [], ?FamilyRegistry $registry = null)
    {
        $this->override = array_values($override);
        $this->registry = $registry ?? new FamilyRegistry();
    }

    public function isOverride(): bool
    {
        return $this->override !== [];
    }

    /**
     * Pages to fetch, in order: the first is the primary, the rest are the
     * fall-back / verification pages (each one carries the full blob).
     *
     * @return list<string>
     */
    public function fetchTargets(): array
    {
        if ($this->override !== []) {
            return $this->override;
        }
        $out = [];
        foreach ($this->registry->active() as $row) {
            $u = (string) ($row['sample_product_url'] ?? '');
            if ($u !== '' && !in_array($u, $out, true)) {
                $out[] = $u;
            }
        }
        return $out === [] ? [self::DEFAULT_PRODUCT_URL] : $out;
    }

    /** Landing pages, for metadata and the optional DOM cross-check. @return list<string> */
    public function landingUrls(): array
    {
        $out = [];
        foreach ($this->registry->active() as $row) {
            $h = (string) ($row['nav_href'] ?? '');
            if ($h !== '') {
                $out[] = self::SITE . $h;
            }
        }
        if ($out === []) {
            foreach (self::DEFAULT_LANDING_PATHS as $p) {
                $out[] = self::SITE . $p;
            }
        }
        return array_values(array_unique($out));
    }

    public static function slugFromUrl(string $url): string
    {
        $parts = explode('/', rtrim($url, '/'));
        return (string) end($parts);
    }
}
