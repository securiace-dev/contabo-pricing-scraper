<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Raised for any provisioning failure (auth, API, mapping, misconfiguration).
 * The main module file catches it and returns its message to WHMCS as the
 * human-readable error string.
 */
class ContaboProvisioningException extends \RuntimeException
{
}
