<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class CapabilityRegistry
{
    /** @var list<string> */
    private const WRITE_STATES = ['supported', 'requires_polling'];

    public function assertWriteAllowed(string $providerAccountId, string $capability): void
    {
        SchemaGuard::assertProviderWriteEnabled($capability);
        $row = Capsule::table('mod_securiacevps_capabilities')
            ->where('provider_account_id', $providerAccountId)
            ->where('capability', $capability)
            ->first();
        $row = $row !== null ? (array) $row : [];
        $state = (string) ($row['state'] ?? 'not_certified');
        if (!in_array($state, self::WRITE_STATES, true)) {
            throw new ContaboProvisioningException(
                'Provider capability "' . $capability . '" is not certified for this Contabo account'
            );
        }
    }

    public function canRead(string $providerAccountId, string $capability): bool
    {
        $row = Capsule::table('mod_securiacevps_capabilities')
            ->where('provider_account_id', $providerAccountId)
            ->where('capability', $capability)
            ->first();
        $row = $row !== null ? (array) $row : [];
        return in_array(
            (string) ($row['state'] ?? 'not_certified'),
            ['supported', 'read_only', 'requires_polling'],
            true
        );
    }
}
