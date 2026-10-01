<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Decodes the `__SAPPER__={...}` payload of a Contabo page WITHOUT evaluating
 * any JavaScript. The payload is a devalue-style literal: an object literal
 * whose `preloaded` entries are IIFEs of the exact shape
 *
 *   (function(p1,p2,...){ P.key = <Value>; ... return <Value> }(<literal args>))
 *
 * A hand-written recursive-descent parser accepts ONLY this grammar:
 *
 *   Value   := Object | Array | String | Number | true | false | null | void 0
 *            | Ident (must be a bound IIFE parameter) | IIFE
 *   Object  := { (Key : Value) (, Key : Value)* }       Key := identifier | String | Number
 *   Array   := [ Value (, Value)* ]
 *   IIFE    := ( function ( params ) { (Ident . key = Value ;)* return Value ;? } ( literal-args ) )
 *   literal-args are Values parsed with identifiers (and IIFEs) DISALLOWED.
 *
 * Anything else (arrow functions, `new`, `this`, template literals, regex
 * literals, any other call, member access outside pre-return assignments,
 * comments, nested functions, unbound identifiers, arity mismatches) throws
 * DecoderException. String contents are inert data: `require('x')` inside a
 * string is just a string. Limits: 4 MB input, nesting depth 64, 20 M tokens.
 *
 * Reference semantics: an object/array ARGUMENT is shared by identity (JS
 * semantics), so `Va.metadata = ...` is visible wherever `Va` is used. This is
 * modelled with SapperBox handles and flattened after the IIFE returns.
 *
 * PHP 7.4 compatible.
 */
final class SapperLiteralDecoder
{
    public const MAX_INPUT_BYTES = 4194304;
    public const MAX_DEPTH = 64;
    public const MAX_TOKENS = 20000000;

    private const END_MARKERS = [
        '};(function(){try{eval("async function x(){}")',
        '};(function()',
    ];

    private const RESERVED = [
        'true', 'false', 'null', 'void', 'function', 'return', 'new', 'this', 'typeof', 'delete',
        'var', 'let', 'const', 'if', 'else', 'for', 'while', 'do', 'switch', 'case', 'class',
        'in', 'instanceof', 'throw', 'try', 'catch', 'finally', 'with', 'yield', 'await',
        'import', 'export', 'super', 'break', 'continue', 'default', 'debugger', 'enum',
    ];

    /** @var string */
    private $s = '';
    /** @var int */
    private $pos = 0;
    /** @var int */
    private $len = 0;
    /** @var int */
    private $tokens = 0;

    /**
     * Locates `__SAPPER__=` ... the closing brace of the object, exactly as
     * extract_sapper_snippet() in src/main.rs does (end markers, then a
     * `</script>` fallback), and decodes it.
     *
     * @return array<string,mixed>
     */
    public function decodeFromHtml(string $html): array
    {
        return $this->decodeSnippet(self::locate($html));
    }

    /** True when the page contains a `__SAPPER__=` assignment at all. */
    public static function present(string $html): bool
    {
        return strpos($html, '__SAPPER__=') !== false;
    }

    /** Returns the `__SAPPER__={...}` source text (including the closing brace). */
    public static function locate(string $html): string
    {
        $start = strpos($html, '__SAPPER__=');
        if ($start === false) {
            throw new DecoderException('__SAPPER__ payload not found in HTML');
        }

        $end = null;
        foreach (self::END_MARKERS as $marker) {
            $rel = strpos($html, $marker, $start);
            if ($rel !== false) {
                $end = $rel;
                break;
            }
        }
        if ($end === null) {
            $close = strpos($html, '</script>', $start);
            if ($close === false) {
                throw new DecoderException('Could not find end of __SAPPER__ payload');
            }
            $snippet = rtrim(substr($html, $start, $close - $start));
            $brace = strrpos($snippet, '}');
            if ($brace === false) {
                throw new DecoderException('Malformed __SAPPER__ block');
            }
            $end = $start + $brace;
        }

        return substr($html, $start, $end + 1 - $start);
    }

    /**
     * @param string $snippet text beginning with `__SAPPER__=`
     * @return array<string,mixed>
     */
    public function decodeSnippet(string $snippet): array
    {
        if (strlen($snippet) > self::MAX_INPUT_BYTES) {
            throw new DecoderException('__SAPPER__ payload exceeds the 4 MB limit');
        }
        $prefix = '__SAPPER__=';
        $trimmed = ltrim($snippet);
        if (strpos($trimmed, $prefix) !== 0) {
            throw new DecoderException('Payload does not start with __SAPPER__=');
        }

        $this->s = substr($trimmed, strlen($prefix));
        $this->pos = 0;
        $this->len = strlen($this->s);
        $this->tokens = 0;

        $value = $this->parseValue(null, true, 0);
        $this->skipWs();
        if ($this->pos < $this->len && $this->s[$this->pos] === ';') {
            $this->pos++;
            $this->skipWs();
        }
        if ($this->pos !== $this->len) {
            $this->fail('unexpected trailing content');
        }

        $flat = $this->flatten($value, 0, []);
        if (!is_array($flat)) {
            $this->fail('payload is not an object');
        }
        $this->s = '';
        return $flat;
    }

    // ── Value parsing ───────────────────────────────────────────────────────

    /**
     * @param array<string,mixed>|null $scope  bound parameters, null when identifiers are disallowed
     * @return mixed
     */
    private function parseValue(?array $scope, bool $allowIife, int $depth)
    {
        if (++$this->tokens > self::MAX_TOKENS) {
            throw new DecoderException('token budget exceeded');
        }
        $this->skipWs();
        if ($this->pos >= $this->len) {
            $this->fail('unexpected end of input');
        }
        $c = $this->s[$this->pos];

        switch (true) {
            case $c === '{':
                return $this->parseObject($scope, $allowIife, $depth);
            case $c === '[':
                return $this->parseArray($scope, $allowIife, $depth);
            case $c === '"' || $c === "'":
                return $this->parseString();
            case $c === '-' || $c === '.' || ($c >= '0' && $c <= '9'):
                return $this->parseNumber();
            case $c === '(':
                if (!$allowIife) {
                    $this->fail('function expression not allowed here');
                }
                return $this->parseIife($depth);
        }

        if ($this->isIdentStart($c)) {
            $name = $this->readIdent();
            if ($name === 'true') {
                return true;
            }
            if ($name === 'false') {
                return false;
            }
            if ($name === 'null') {
                return null;
            }
            if ($name === 'void') {
                $this->skipWs();
                if ($this->pos < $this->len && $this->s[$this->pos] === '0'
                    && !$this->isIdentPart($this->s[$this->pos + 1] ?? ' ')
                ) {
                    $this->pos++;
                    return null;
                }
                $this->fail('only `void 0` is allowed');
            }
            if ($scope === null) {
                $this->fail('identifier not allowed here');
            }
            if (!array_key_exists($name, $scope)) {
                $this->fail('unbound identifier');
            }
            return $scope[$name];
        }

        $this->fail('unexpected character');
    }

    /** @return array<mixed> */
    private function parseObject(?array $scope, bool $allowIife, int $depth): array
    {
        if ($depth >= self::MAX_DEPTH) {
            throw new DecoderException('nesting depth exceeded');
        }
        $this->pos++; // {
        $out = [];
        $hasIndexKey = false;
        $this->skipWs();
        if ($this->peek() === '}') {
            $this->pos++;
            return $out;
        }
        while (true) {
            $this->skipWs();
            $c = $this->peek();
            if ($c === '"' || $c === "'") {
                $key = $this->parseString();
            } elseif ($c === '-' || $c === '.' || ($c >= '0' && $c <= '9')) {
                $num = $this->parseNumber();
                $key = (string) $num;
            } elseif ($c !== '' && $this->isIdentStart($c)) {
                $key = $this->readIdent();
            } else {
                $this->fail('invalid object key');
            }
            $this->skipWs();
            if ($this->peek() !== ':') {
                $this->fail('expected ":" in object literal');
            }
            $this->pos++;
            $out[$key] = $this->parseValue($scope, $allowIife, $depth + 1);
            if (!$hasIndexKey && preg_match('/^(0|[1-9][0-9]{0,8})$/', (string) $key) === 1) {
                $hasIndexKey = true;
            }
            $this->skipWs();
            $n = $this->peek();
            if ($n === ',') {
                $this->pos++;
                continue;
            }
            if ($n === '}') {
                $this->pos++;
                break;
            }
            $this->fail('expected "," or "}" in object literal');
        }
        if ($hasIndexKey) {
            // JS orders integer-like keys ascending before string keys.
            $ints = [];
            $strs = [];
            foreach ($out as $k => $v) {
                if (is_int($k)) {
                    $ints[$k] = $v;
                } else {
                    $strs[$k] = $v;
                }
            }
            ksort($ints);
            $out = $ints + $strs;
        }
        return $out;
    }

    /** @return list<mixed> */
    private function parseArray(?array $scope, bool $allowIife, int $depth): array
    {
        if ($depth >= self::MAX_DEPTH) {
            throw new DecoderException('nesting depth exceeded');
        }
        $this->pos++; // [
        $out = [];
        $this->skipWs();
        if ($this->peek() === ']') {
            $this->pos++;
            return $out;
        }
        while (true) {
            $out[] = $this->parseValue($scope, $allowIife, $depth + 1);
            $this->skipWs();
            $n = $this->peek();
            if ($n === ',') {
                $this->pos++;
                continue;
            }
            if ($n === ']') {
                $this->pos++;
                break;
            }
            $this->fail('expected "," or "]" in array literal');
        }
        return $out;
    }

    private function parseString(): string
    {
        $quote = $this->s[$this->pos];
        $this->pos++;
        $re = $quote === '"' ? '/\G[^"\\\\\r\n]+/' : "/\\G[^'\\\\\r\n]+/";
        $out = '';
        while (true) {
            if ($this->pos >= $this->len) {
                $this->fail('unterminated string');
            }
            if (preg_match($re, $this->s, $m, 0, $this->pos) === 1) {
                $out .= $m[0];
                $this->pos += strlen($m[0]);
                continue;
            }
            $c = $this->s[$this->pos];
            if ($c === $quote) {
                $this->pos++;
                return $out;
            }
            if ($c !== '\\') {
                $this->fail('invalid character in string');
            }
            $this->pos++;
            if ($this->pos >= $this->len) {
                $this->fail('unterminated escape');
            }
            $e = $this->s[$this->pos++];
            switch ($e) {
                case 'n': $out .= "\n"; break;
                case 'r': $out .= "\r"; break;
                case 't': $out .= "\t"; break;
                case 'b': $out .= "\x08"; break;
                case 'f': $out .= "\x0c"; break;
                case 'v': $out .= "\x0b"; break;
                case '0':
                    $nx = $this->s[$this->pos] ?? '';
                    if ($nx !== '' && $nx >= '0' && $nx <= '9') {
                        $this->fail('octal escape not allowed');
                    }
                    $out .= "\0";
                    break;
                case '\\': $out .= '\\'; break;
                case '"': $out .= '"'; break;
                case "'": $out .= "'"; break;
                case '/': $out .= '/'; break;
                case 'x':
                    $hex = substr($this->s, $this->pos, 2);
                    if (strlen($hex) !== 2 || !ctype_xdigit($hex)) {
                        $this->fail('invalid \\x escape');
                    }
                    $this->pos += 2;
                    $out .= $this->utf8(hexdec($hex));
                    break;
                case 'u':
                    $cp = $this->readHex4();
                    if ($cp >= 0xD800 && $cp <= 0xDBFF) {
                        if (substr($this->s, $this->pos, 2) !== '\\u') {
                            $this->fail('lone surrogate');
                        }
                        $this->pos += 2;
                        $lo = $this->readHex4();
                        if ($lo < 0xDC00 || $lo > 0xDFFF) {
                            $this->fail('invalid surrogate pair');
                        }
                        $cp = 0x10000 + (($cp - 0xD800) << 10) + ($lo - 0xDC00);
                    } elseif ($cp >= 0xDC00 && $cp <= 0xDFFF) {
                        $this->fail('lone surrogate');
                    }
                    $out .= $this->utf8($cp);
                    break;
                default:
                    $this->fail('unsupported escape');
            }
        }
    }

    private function readHex4(): int
    {
        $hex = substr($this->s, $this->pos, 4);
        if (strlen($hex) !== 4 || !ctype_xdigit($hex)) {
            $this->fail('invalid \\u escape');
        }
        $this->pos += 4;
        return (int) hexdec($hex);
    }

    private function utf8(int $cp): string
    {
        if ($cp < 0x80) {
            return chr($cp);
        }
        if ($cp < 0x800) {
            return chr(0xC0 | ($cp >> 6)) . chr(0x80 | ($cp & 0x3F));
        }
        if ($cp < 0x10000) {
            return chr(0xE0 | ($cp >> 12)) . chr(0x80 | (($cp >> 6) & 0x3F)) . chr(0x80 | ($cp & 0x3F));
        }
        return chr(0xF0 | ($cp >> 18)) . chr(0x80 | (($cp >> 12) & 0x3F))
            . chr(0x80 | (($cp >> 6) & 0x3F)) . chr(0x80 | ($cp & 0x3F));
    }

    /** @return int|float */
    private function parseNumber()
    {
        if (preg_match('/\G-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/', $this->s, $m, 0, $this->pos) !== 1) {
            $this->fail('invalid number');
        }
        $text = $m[0];
        $this->pos += strlen($text);
        if ($this->pos < $this->len && $this->isIdentPart($this->s[$this->pos])) {
            $this->fail('invalid number suffix');
        }
        if (strpbrk($text, '.eE') === false && strlen($text) < 16) {
            return (int) $text;
        }
        return (float) $text;
    }

    // ── IIFE ────────────────────────────────────────────────────────────────

    /** @return mixed */
    private function parseIife(int $depth)
    {
        if ($depth >= self::MAX_DEPTH) {
            throw new DecoderException('nesting depth exceeded');
        }
        $this->pos++; // (
        $this->expectKeyword('function');
        $this->skipWs();
        $this->expectChar('(');

        $params = [];
        $this->skipWs();
        if ($this->peek() === ')') {
            $this->pos++;
        } else {
            while (true) {
                $this->skipWs();
                if ($this->pos >= $this->len || !$this->isIdentStart($this->s[$this->pos])) {
                    $this->fail('invalid parameter list');
                }
                $name = $this->readIdent();
                if (in_array($name, self::RESERVED, true) || isset($params[$name])) {
                    $this->fail('invalid or duplicate parameter name');
                }
                $params[$name] = true;
                $this->skipWs();
                $n = $this->peek();
                if ($n === ',') {
                    $this->pos++;
                    continue;
                }
                if ($n === ')') {
                    $this->pos++;
                    break;
                }
                $this->fail('invalid parameter list');
            }
        }
        $paramNames = array_keys($params);

        $this->skipWs();
        $this->expectChar('{');

        // The body can only be parsed after the arguments are known (identifiers
        // resolve to them), so remember where it starts, skip to the matching
        // call, parse the literal args, then come back. The body is scanned
        // twice at most; the first pass only finds its extent.
        $bodyStart = $this->pos;
        $bodyEnd = $this->skipBody();
        $this->pos = $bodyEnd;           // just after the closing "}"
        $this->skipWs();
        $this->expectChar('(');
        $args = $this->parseArgs($depth);
        $this->skipWs();
        $this->expectChar(')');           // closes the call "("
        $this->skipWs();
        $this->expectChar(')');           // closes the outer "("
        $after = $this->pos;

        if (count($args) !== count($paramNames)) {
            $this->fail('argument count does not match parameter count');
        }
        $scope = [];
        foreach ($paramNames as $i => $name) {
            $v = $args[$i];
            $scope[$name] = is_array($v) ? new SapperBox($v) : $v;
        }

        $this->pos = $bodyStart;
        $result = $this->parseBody($scope, $depth);
        $this->pos = $after;
        return $result;
    }

    /**
     * Finds the end of an IIFE body without evaluating it: scans tokens
     * tracking strings and brace depth only. Returns the offset just after
     * the closing brace.
     */
    private function skipBody(): int
    {
        $depth = 1;
        $i = $this->pos;
        $len = $this->len;
        $s = $this->s;
        while ($i < $len) {
            // fast-forward over characters that cannot change structure
            $skip = strcspn($s, "{}\"'", $i);
            $i += $skip;
            if ($i >= $len) {
                break;
            }
            $c = $s[$i];
            if ($c === '{') {
                $depth++;
                if ($depth > self::MAX_DEPTH + 2) {
                    throw new DecoderException('nesting depth exceeded');
                }
                $i++;
            } elseif ($c === '}') {
                $depth--;
                $i++;
                if ($depth === 0) {
                    return $i;
                }
            } else {
                $i++;
                while ($i < $len && $s[$i] !== $c) {
                    if ($s[$i] === '\\') {
                        $i++;
                    } elseif ($s[$i] === "\n" || $s[$i] === "\r") {
                        throw new DecoderException('unterminated string');
                    }
                    $i++;
                }
                if ($i >= $len) {
                    throw new DecoderException('unterminated string');
                }
                $i++;
            }
        }
        throw new DecoderException('unterminated function body');
    }

    /**
     * @param array<string,mixed> $scope
     * @return mixed
     */
    private function parseBody(array $scope, int $depth)
    {
        while (true) {
            $this->skipWs();
            if ($this->pos >= $this->len) {
                $this->fail('unterminated function body');
            }
            if ($this->isIdentStart($this->s[$this->pos])) {
                $save = $this->pos;
                $word = $this->readIdent();
                if ($word === 'return') {
                    if ($this->pos < $this->len && $this->isIdentPart($this->s[$this->pos])) {
                        $this->fail('unexpected token');
                    }
                    $value = $this->parseValue($scope, false, $depth + 1);
                    $this->skipWs();
                    if ($this->peek() === ';') {
                        $this->pos++;
                        $this->skipWs();
                    }
                    $this->expectChar('}');
                    return $value;
                }
                // assignment: Ident . key = Value ;
                if (!array_key_exists($word, $scope)) {
                    $this->pos = $save;
                    $this->fail('statement is not a parameter property assignment');
                }
                $this->skipWs();
                $this->expectChar('.');
                $this->skipWs();
                if ($this->pos >= $this->len || !$this->isIdentStart($this->s[$this->pos])) {
                    $this->fail('expected property name');
                }
                $key = $this->readIdent();
                $this->skipWs();
                if ($this->peek() !== '=' || ($this->s[$this->pos + 1] ?? '') === '=' || ($this->s[$this->pos + 1] ?? '') === '>') {
                    $this->fail('expected a plain "=" assignment');
                }
                $this->pos++;
                $target = $scope[$word];
                if (!($target instanceof SapperBox)) {
                    $this->fail('assignment target is not an object');
                }
                $value = $this->parseValue($scope, false, $depth + 1);
                $this->skipWs();
                $this->expectChar(';');
                $target->data[$key] = $value;
                continue;
            }
            $this->fail('unexpected token in function body');
        }
    }

    /** @return list<mixed> literal args (no identifiers, no function expressions) */
    private function parseArgs(int $depth): array
    {
        $args = [];
        $this->skipWs();
        if ($this->peek() === ')') {
            return $args;
        }
        $none = null;
        while (true) {
            $args[] = $this->parseValue($none, false, $depth + 1);
            $this->skipWs();
            if ($this->peek() === ',') {
                $this->pos++;
                continue;
            }
            return $args;
        }
    }

    // ── Flattening (resolve shared boxes) ───────────────────────────────────

    /**
     * @param mixed $v
     * @param array<int,true> $stack object ids currently being flattened (cycle guard)
     * @return mixed
     */
    private function flatten($v, int $depth, array $stack)
    {
        if ($v instanceof SapperBox) {
            $id = spl_object_id($v);
            if (isset($stack[$id])) {
                throw new DecoderException('cyclic reference');
            }
            if ($v->flat !== null) {
                return $v->flat;
            }
            $stack[$id] = true;
            $v->flat = $this->flatten($v->data, $depth, $stack);
            return $v->flat;
        }
        if (!is_array($v)) {
            return $v;
        }
        if ($depth > self::MAX_DEPTH * 2) {
            throw new DecoderException('nesting depth exceeded');
        }
        foreach ($v as $k => $item) {
            if ($item instanceof SapperBox || is_array($item)) {
                $v[$k] = $this->flatten($item, $depth + 1, $stack);
            }
        }
        return $v;
    }

    // ── Lexical helpers ─────────────────────────────────────────────────────

    private function skipWs(): void
    {
        $this->pos += strspn($this->s, " \t\r\n", $this->pos);
    }

    private function peek(): string
    {
        return $this->pos < $this->len ? $this->s[$this->pos] : '';
    }

    private function expectChar(string $c): void
    {
        if ($this->peek() !== $c) {
            $this->fail('expected "' . $c . '"');
        }
        $this->pos++;
    }

    private function expectKeyword(string $kw): void
    {
        $this->skipWs();
        if (substr($this->s, $this->pos, strlen($kw)) !== $kw
            || $this->isIdentPart($this->s[$this->pos + strlen($kw)] ?? ' ')
        ) {
            $this->fail('expected "' . $kw . '"');
        }
        $this->pos += strlen($kw);
    }

    private function isIdentStart(string $c): bool
    {
        return ($c >= 'a' && $c <= 'z') || ($c >= 'A' && $c <= 'Z') || $c === '_' || $c === '$';
    }

    private function isIdentPart(string $c): bool
    {
        return $this->isIdentStart($c) || ($c >= '0' && $c <= '9');
    }

    private function readIdent(): string
    {
        $n = strspn($this->s, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$', $this->pos);
        $name = substr($this->s, $this->pos, $n);
        $this->pos += $n;
        return $name;
    }

    /** @return never */
    private function fail(string $why): void
    {
        throw new DecoderException('__SAPPER__ literal rejected at offset ' . $this->pos . ': ' . $why);
    }
}
