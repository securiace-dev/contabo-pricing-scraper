<?php
declare(strict_types=1);

namespace SecuriAceVps;

final class ProviderAccount
{
    /** @param array<string,mixed> $params */
    public static function id(array $params): string
    {
        $serverId = (int) ($params['serverid'] ?? ($params['server'] ?? 0));
        $username = strtolower(trim((string) ($params['serverusername'] ?? '')));
        return hash('sha256', 'contabo|' . $serverId . '|' . $username);
    }
}
