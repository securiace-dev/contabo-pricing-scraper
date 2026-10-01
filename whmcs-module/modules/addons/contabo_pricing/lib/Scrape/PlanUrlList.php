<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * The ordered list of Contabo plan URLs and the rank/family helpers derived
 * from it. Mirrors ALL_PLAN_URLS in src/main.rs (rank = 1-based position;
 * family rank = 1-based position among URLs of the same family).
 */
final class PlanUrlList
{
    public const FAMILY_VPS = 'Cloud VPS';
    public const FAMILY_STORAGE = 'Storage VPS';
    public const FAMILY_VDS = 'Cloud VDS';

    /** @var list<string> */
    public const DEFAULT_URLS = [
        'https://contabo.com/en/vps/cloud-vps-10/',
        'https://contabo.com/en/vps/cloud-vps-20/',
        'https://contabo.com/en/vps/cloud-vps-30/',
        'https://contabo.com/en/vps/cloud-vps-40/',
        'https://contabo.com/en/vps/cloud-vps-50/',
        'https://contabo.com/en/vps/cloud-vps-60/',
        'https://contabo.com/en/storage-vps/storage-vps-10/',
        'https://contabo.com/en/storage-vps/storage-vps-20/',
        'https://contabo.com/en/storage-vps/storage-vps-30/',
        'https://contabo.com/en/storage-vps/storage-vps-40/',
        'https://contabo.com/en/storage-vps/storage-vps-50/',
        'https://contabo.com/en/vds/vds-s/',
        'https://contabo.com/en/vds/vds-m/',
        'https://contabo.com/en/vds/vds-l/',
        'https://contabo.com/en/vds/vds-xl/',
        'https://contabo.com/en/vds/vds-xxl/',
    ];

    /** @var list<string> */
    private $urls;

    /**
     * @param list<string>|null $urls null = the built-in 16 URLs
     */
    public function __construct(?array $urls = null)
    {
        $this->urls = array_values($urls ?? self::DEFAULT_URLS);
    }

    /** @return list<string> */
    public function urls(): array
    {
        return $this->urls;
    }

    public static function slugFromUrl(string $url): string
    {
        $parts = explode('/', rtrim($url, '/'));
        return (string) end($parts);
    }

    /** Family display name for a slug (or URL); 'Unknown' when unrecognised. */
    public function familyOf(string $slugOrUrl): string
    {
        $slug = strpos($slugOrUrl, '/') !== false ? self::slugFromUrl($slugOrUrl) : $slugOrUrl;
        if (strpos($slug, 'cloud-vps-') === 0) {
            return self::FAMILY_VPS;
        }
        if (strpos($slug, 'storage-vps-') === 0) {
            return self::FAMILY_STORAGE;
        }
        if (strpos($slug, 'vds-') === 0) {
            return self::FAMILY_VDS;
        }
        return 'Unknown';
    }

    /** 1-based position in the full list, 0 when absent. */
    public function rankOf(string $url): int
    {
        $i = array_search($url, $this->urls, true);
        return $i === false ? 0 : ((int) $i) + 1;
    }

    /** 1-based position among URLs of the same family, 0 when absent. */
    public function familyRankOf(string $url): int
    {
        if ($this->rankOf($url) === 0) {
            return 0;
        }
        $family = $this->familyOf($url);
        $n = 0;
        foreach ($this->urls as $u) {
            if ($this->familyOf($u) !== $family) {
                continue;
            }
            $n++;
            if ($u === $url) {
                return $n;
            }
        }
        return 0;
    }

    /** @return array<string,string> family => first URL of that family */
    public function firstUrlPerFamily(): array
    {
        $out = [];
        foreach ($this->urls as $u) {
            $f = $this->familyOf($u);
            if (!isset($out[$f])) {
                $out[$f] = $u;
            }
        }
        return $out;
    }
}
