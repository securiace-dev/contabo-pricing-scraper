<?php
declare(strict_types=1);

namespace SecuriAceVps;

final class CanonicalJson
{
    /** @param mixed $value */
    public static function encode($value): string
    {
        $encoded = json_encode(self::normalize($value), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if (!is_string($encoded)) {
            throw new ContaboProvisioningException('Unable to encode operation data');
        }
        return $encoded;
    }

    /** @param mixed $value @return mixed */
    private static function normalize($value)
    {
        if (!is_array($value)) {
            return $value;
        }
        if (self::isList($value)) {
            $out = [];
            foreach ($value as $item) {
                $out[] = self::normalize($item);
            }
            return $out;
        }
        ksort($value, SORT_STRING);
        foreach ($value as $key => $item) {
            $value[$key] = self::normalize($item);
        }
        return $value;
    }

    /** @param array<mixed> $value */
    private static function isList(array $value): bool
    {
        $expected = 0;
        foreach ($value as $key => $_item) {
            if ($key !== $expected) {
                return false;
            }
            $expected++;
        }
        return true;
    }
}
