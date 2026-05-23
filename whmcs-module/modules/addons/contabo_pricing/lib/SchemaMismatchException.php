<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Thrown when a row read from WHMCS lacks a column the addon treats as MANDATORY
 * WHMCS schema (e.g. `tblhosting.amount`). We fail loud rather than mask a missing
 * monetary value as 0.0 — the live-schema smoke (scripts/live-schema-smoke.php)
 * proves these columns should exist, so their absence is a real environment fault,
 * not a data condition to silently absorb.
 *
 * PHP 7.4 + 8.x polyglot.
 */
final class SchemaMismatchException extends \RuntimeException
{
}
