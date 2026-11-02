<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Posted admin JSON is over JsonPayloadCap. Save paths catch this and render
 * an error before any profile or mapping write.
 */
final class JsonPayloadTooLarge extends \RuntimeException
{
}
