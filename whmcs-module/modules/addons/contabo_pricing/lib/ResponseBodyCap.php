<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Accumulates one HTTP response body up to a hard byte ceiling.
 *
 * Callers must leave CURLOPT_RETURNTRANSFER off and feed this from
 * CURLOPT_WRITEFUNCTION. Content-Length is ignored: only bytes actually
 * handed to write() count. The stored buffer never grows past MAX_BYTES.
 * On overflow the buffer is dropped and the transfer must be aborted.
 *
 * PHP 7.4 compatible.
 */
final class ResponseBodyCap
{
    /** Same ceiling SapperLiteralDecoder allows for one page. */
    public const MAX_BYTES = 4194304;

    /** Distinct from libcurl's own write-error text so callers can classify it. */
    public const ERROR = 'response body exceeds 4 MiB limit';

    /** CURLE_WRITE_ERROR. Non-zero so callers throw before json_decode. */
    public const ERRNO = 23;

    /** @var string */
    private $buffer = '';

    /** @var int */
    private $size = 0;

    /** @var int */
    private $peak = 0;

    /** @var bool */
    private $overflow = false;

    /**
     * Accept one curl write. Returns 0 when the chunk would pass the ceiling
     * so libcurl aborts the transfer. The overflowing chunk is not stored.
     */
    public function write(string $chunk): int
    {
        if ($this->overflow) {
            return 0;
        }
        $len = strlen($chunk);
        if ($len > self::MAX_BYTES - $this->size) {
            $this->overflow = true;
            $this->buffer = '';
            $this->size = 0;
            return 0;
        }
        $this->buffer .= $chunk;
        $this->size += $len;
        if ($this->size > $this->peak) {
            $this->peak = $this->size;
        }
        return $len;
    }

    public function overflowed(): bool
    {
        return $this->overflow;
    }

    public function body(): string
    {
        return $this->overflow ? '' : $this->buffer;
    }

    public function size(): int
    {
        return $this->size;
    }

    /** Highest stored length seen. Never greater than MAX_BYTES. */
    public function peak(): int
    {
        return $this->peak;
    }
}
