<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Reads the page's own visible plan title and 1-month EUR price straight from
 * the DOM (DOMDocument + XPath, libxml errors suppressed). Used only to
 * cross-check the structured extraction; its output is never importable.
 */
final class DomPlanProbe
{
    private const MAX_BYTES = 4194304;

    /** @return array{title:?string, monthly_eur:?float} */
    public function probe(string $html): array
    {
        $out = ['title' => null, 'monthly_eur' => null];
        if ($html === '') {
            return $out;
        }
        // The structured payload sits after the visible markup; the probe only needs the markup.
        $cut = strpos($html, '__SAPPER__=');
        $markup = $cut === false ? $html : substr($html, 0, $cut);
        $markup = substr($markup, 0, self::MAX_BYTES);

        $prev = libxml_use_internal_errors(true);
        try {
            $doc = new \DOMDocument();
            $doc->loadHTML('<?xml encoding="UTF-8">' . $markup, LIBXML_NONET | LIBXML_NOWARNING | LIBXML_NOERROR);
            $xp = new \DOMXPath($doc);

            $h1 = $xp->query('//h1');
            if ($h1 !== false && $h1->length > 0) {
                $text = trim((string) preg_replace('/\s+/', ' ', (string) $h1->item(0)->textContent));
                $text = (string) preg_replace('/^Configure your\s+/i', '', $text);
                $out['title'] = $text !== '' ? $text : null;
            }
            if ($out['title'] === null) {
                $t = $xp->query('//title');
                if ($t !== false && $t->length > 0) {
                    $parts = explode('|', (string) $t->item(0)->textContent);
                    $text = trim($parts[0]);
                    $out['title'] = $text !== '' ? $text : null;
                }
            }

            $price = $xp->query(
                "//*[contains(concat(' ', normalize-space(@class), ' '), ' discount-price ')]/strong"
            );
            if ($price !== false && $price->length > 0) {
                $out['monthly_eur'] = self::parseEuro((string) $price->item(0)->textContent);
            }
        } catch (\Throwable $e) {
            // best-effort probe: leave nulls
        } finally {
            libxml_clear_errors();
            libxml_use_internal_errors($prev);
        }
        return $out;
    }

    private static function parseEuro(string $text): ?float
    {
        if (preg_match('/€\s*([0-9]+(?:[.,][0-9]{1,2})?)/u', $text, $m) !== 1) {
            return null;
        }
        return (float) str_replace(',', '.', $m[1]);
    }
}
