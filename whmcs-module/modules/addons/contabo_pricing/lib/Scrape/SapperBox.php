<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Identity-carrying holder for an object/array IIFE argument so that
 * `Param.key = value;` statements are visible through every reference to the
 * param (JS reference semantics). Internal to SapperLiteralDecoder.
 *
 * @internal
 */
final class SapperBox
{
    /** @var array<mixed> */
    public $data;
    /** @var array<mixed>|null memoised flattened value */
    public $flat = null;

    /** @param array<mixed> $data */
    public function __construct(array $data)
    {
        $this->data = $data;
    }
}
