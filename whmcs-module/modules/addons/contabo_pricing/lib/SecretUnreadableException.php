<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * A sealed (ENC:) secret could not be decrypted. Almost always the WHMCS
 * encryption key ($cc_encryption_hash) changed since the value was sealed, or
 * the stored value is corrupt. The remedy is to re-enter (re-seal) the key;
 * see docs/SECRET_RESEAL.md.
 */
final class SecretUnreadableException extends \RuntimeException
{
}
