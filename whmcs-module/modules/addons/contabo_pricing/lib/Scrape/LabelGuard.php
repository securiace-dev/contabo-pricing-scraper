<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Forbidden-term scan for customer-facing labels. A term is split into tokens
 * on whitespace, '_' and '-'; tokens may be joined in the text by any run of
 * [\s_-]* (including none) and a match must start on a token boundary (the
 * preceding character is not [A-Za-z0-9]), case-insensitively. So "CloudVPS",
 * "Cloud-VPS" and "cloud_vps" hit "Cloud VPS" while "120i" does not hit "20i".
 *
 * A term that ends in a separator (e.g. "CB-") additionally requires one
 * separator character after its last token, so it matches the SKU prefix
 * "CB-10" but not an unrelated word such as "CBSE".
 */
final class LabelGuard
{
    /** @var list<string> */
    public const FORBIDDEN_TERMS = ['contabo', 'CB-', 'Cloud VPS', 'Cloud VDS', 'Storage VPS', '20i', 'vultr'];

    /** @var array<string,string> term => compiled regex */
    private $patterns = [];

    /** @param list<string>|null $terms null = FORBIDDEN_TERMS */
    public function __construct(?array $terms = null)
    {
        foreach ($terms ?? self::FORBIDDEN_TERMS as $term) {
            $term = (string) $term;
            $tokens = preg_split('/[\s_-]+/', $term, -1, PREG_SPLIT_NO_EMPTY);
            if (!is_array($tokens) || $tokens === []) {
                continue;
            }
            $quoted = array_map(static function (string $t): string {
                return preg_quote($t, '/');
            }, $tokens);
            $re = '/(?<![A-Za-z0-9])' . implode('[\s_-]*', $quoted);
            if (preg_match('/[\s_-]$/', $term) === 1) {
                $re .= '[\s_-]';
            }
            $this->patterns[$term] = $re . '/iu';
        }
    }

    /** @return list<string> forbidden terms found in the text */
    public function scan(string $text): array
    {
        $hits = [];
        foreach ($this->patterns as $term => $re) {
            if (preg_match($re, $text) === 1) {
                $hits[] = (string) $term;
            }
        }
        return $hits;
    }

    public function isClean(string $text): bool
    {
        return $this->scan($text) === [];
    }
}
