<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Resolves a customer-facing image selection (the addon's `category:label`
 * value key, e.g. "OS:Ubuntu 24.04", or a bare label like "Ubuntu 24.04") to
 * the Contabo API imageId required by create/reinstall.
 *
 * FAIL-CLOSED by design: an unresolvable or ambiguous selection throws with
 * the candidate list in the message — the module must never provision a
 * guessed OS onto a customer's server. If the selection already IS an imageId
 * (UUID form) it passes straight through.
 */
final class ImageResolver
{
    private const MAX_PAGES = 10;
    private const PAGE_SIZE = 200;

    /** @var ContaboApiClient */
    private $client;

    /** @var array<string,string>|null normalized name → imageId (memoized per request) */
    private $catalog = null;

    public function __construct(ContaboApiClient $client)
    {
        $this->client = $client;
    }

    public function resolveImageId(string $selection): string
    {
        $selection = trim($selection);
        if ($selection === '') {
            throw new ContaboProvisioningException('Empty image selection');
        }
        // Already a Contabo imageId (UUID v4 form) — nothing to resolve.
        if (preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $selection) === 1) {
            return $selection;
        }

        $label = $this->labelFromSelection($selection);
        $needle = $this->normalize($label);
        if ($needle === '') {
            throw new ContaboProvisioningException('Image selection "' . $selection . '" has no usable label');
        }

        $catalog = $this->loadCatalog();

        if (isset($catalog[$needle])) {
            return $catalog[$needle];
        }

        // Prefix match tolerates version-suffix drift ("ubuntu 24.04" vs
        // "ubuntu 24.04 lts") — but ONLY when it is unambiguous.
        $candidates = [];
        foreach ($catalog as $name => $id) {
            if (strpos($name, $needle) === 0 || strpos($needle, $name) === 0) {
                $candidates[$name] = $id;
            }
        }
        if (count($candidates) === 1) {
            return (string) array_values($candidates)[0];
        }

        $hint = $candidates !== []
            ? ' Ambiguous candidates: ' . implode(', ', array_slice(array_keys($candidates), 0, 5))
            : ' No Contabo standard image matches.';
        throw new ContaboProvisioningException(
            'Cannot resolve image selection "' . $selection . '" to a Contabo imageId.' . $hint
            . ' Set the exact imageId on the product (config option 1) or fix the option label.'
        );
    }

    /** "OS:Ubuntu 24.04" → "Ubuntu 24.04"; "[Panel] cPanel" → "cPanel"; bare labels pass through. */
    private function labelFromSelection(string $selection): string
    {
        $label = $selection;
        $colon = strpos($label, ':');
        if ($colon !== false) {
            $label = substr($label, $colon + 1);
        }
        $label = preg_replace('/^\[[^\]]+\]\s*/', '', trim($label));
        return trim((string) $label);
    }

    private function normalize(string $name): string
    {
        $name = strtolower(trim($name));
        $name = (string) preg_replace('/\s+/', ' ', $name);
        return $name;
    }

    /** @return array<string,string> */
    private function loadCatalog(): array
    {
        if ($this->catalog !== null) {
            return $this->catalog;
        }
        $catalog = [];
        for ($page = 1; $page <= self::MAX_PAGES; $page++) {
            $resp = $this->client->get('/v1/compute/images?size=' . self::PAGE_SIZE . '&page=' . $page);
            $rows = isset($resp['data']) && is_array($resp['data']) ? $resp['data'] : [];
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $name = $this->normalize((string) ($row['name'] ?? ''));
                $id   = (string) ($row['imageId'] ?? '');
                if ($name !== '' && $id !== '' && !isset($catalog[$name])) {
                    $catalog[$name] = $id;
                }
            }
            if (count($rows) < self::PAGE_SIZE) {
                break;
            }
        }
        $this->catalog = $catalog;
        return $catalog;
    }
}
