<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\DecoderException;
use ContaboPricing\Scrape\PlanUrlList;
use ContaboPricing\Scrape\SapperLiteralDecoder;
use PHPUnit\Framework\TestCase;

final class SapperLiteralDecoderTest extends TestCase
{
    /** @return array<string,mixed> */
    private function decode(string $literal): array
    {
        return (new SapperLiteralDecoder())->decodeSnippet('__SAPPER__=' . $literal);
    }

    private function assertRejected(string $literal, string $why = ''): void
    {
        try {
            $this->decode($literal);
        } catch (DecoderException $e) {
            $this->addToAssertionCount(1);
            return;
        }
        $this->fail('expected DecoderException for: ' . $why . ' ' . substr($literal, 0, 80));
    }

    // ── Real page ───────────────────────────────────────────────────────────

    public function testFixtureDecodesTo154ProductsIncludingAll16Targets(): void
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        $payload = (new SapperLiteralDecoder())->decodeFromHtml($html);

        $this->assertArrayHasKey('preloaded', $payload);
        $products = $payload['preloaded'][0]['products'];
        $this->assertCount(154, $products);

        $slugs = [];
        foreach ($products as $p) {
            if (isset($p['slug']) && is_string($p['slug'])) {
                $slugs[$p['slug']] = true;
            }
        }
        foreach ([
            'cloud-vps-10', 'cloud-vps-20', 'cloud-vps-30', 'cloud-vps-40', 'cloud-vps-50', 'cloud-vps-60',
            'storage-vps-10', 'storage-vps-20', 'storage-vps-30', 'storage-vps-40', 'storage-vps-50',
            'vds-s', 'vds-m', 'vds-l', 'vds-xl', 'vds-xxl',
        ] as $slug) {
            $this->assertArrayHasKey($slug, $slugs);
        }
        $this->assertNull($payload['preloaded'][1]);
        $this->assertSame('EUR', $payload['session']['currency']);
    }

    public function testFixtureValuesOfInterestAreDecodedExactly(): void
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        $products = (new SapperLiteralDecoder())->decodeFromHtml($html)['preloaded'][0]['products'];
        $by = [];
        foreach ($products as $p) {
            if (isset($p['slug']) && is_string($p['slug'])) {
                $by[$p['slug']] = $p;
            }
        }
        $this->assertSame(4.5, $by['cloud-vps-10']['price']['EUR']);
        $this->assertSame('180 GB NVMe ', $this->specTitle($by['vds-s'], 'storage'));
        $this->assertSame('1.4 TB SSD', $this->specTitle($by['storage-vps-50'], 'storage'));
    }

    /** @param array<string,mixed> $product */
    private function specTitle(array $product, string $type): string
    {
        foreach ($product['specs'] as $s) {
            if ($s['type'] === $type) {
                return (string) $s['title'];
            }
        }
        return '';
    }

    // ── Locator ─────────────────────────────────────────────────────────────

    public function testLocateUsesEndMarkerAndKeepsClosingBrace(): void
    {
        $html = '<script>__SAPPER__={a:1};(function(){try{eval("async function x(){}")}catch(e){}})()</script>';
        $this->assertSame('__SAPPER__={a:1}', SapperLiteralDecoder::locate($html));
        $this->assertSame(['a' => 1], (new SapperLiteralDecoder())->decodeFromHtml($html));
    }

    public function testLocateFallsBackToScriptEnd(): void
    {
        $html = '<script>__SAPPER__={a:"x"}  </script><p>}</p>';
        $this->assertSame(['a' => 'x'], (new SapperLiteralDecoder())->decodeFromHtml($html));
    }

    public function testMissingPayloadIsRejected(): void
    {
        $this->expectException(DecoderException::class);
        (new SapperLiteralDecoder())->decodeFromHtml('<html>nothing</html>');
    }

    // ── Literal grammar ─────────────────────────────────────────────────────

    public function testLeadingDotNumbersVoidZeroAndKeyForms(): void
    {
        $d = $this->decode('{a:.99,b:-.5,c:4.,d:1e3,e:void 0,f:true,g:false,h:null,"q r":1,7:"n",i:-3}');
        $this->assertSame(0.99, $d['a']);
        $this->assertSame(-0.5, $d['b']);
        $this->assertSame(4.0, $d['c']);
        $this->assertSame(1000.0, $d['d']);
        $this->assertNull($d['e']);
        $this->assertTrue($d['f']);
        $this->assertFalse($d['g']);
        $this->assertNull($d['h']);
        $this->assertSame(1, $d['q r']);
        $this->assertSame('n', $d[7]);
        $this->assertSame(-3, $d['i']);
    }

    public function testStringEscapes(): void
    {
        $d = $this->decode('{a:"x\u00e9y",b:"\ud83d\ude00",c:\'it\\\'s\',d:"l1\nl2\t\\\\",e:"\x41\/"}');
        $this->assertSame("x\u{e9}y", $d['a']);
        $this->assertSame("\u{1F600}", $d['b']);
        $this->assertSame("it's", $d['c']);
        $this->assertSame("l1\nl2\t\\", $d['d']);
        $this->assertSame('A/', $d['e']);
    }

    public function testCodeInsideStringsIsInert(): void
    {
        $d = $this->decode('{a:"require(\'child_process\').execSync(\'id\')",b:(function(x){return {y:x}}("process.exit(1);//\"}"))}');
        $this->assertSame("require('child_process').execSync('id')", $d['a']);
        $this->assertSame('process.exit(1);//"}', $d['b']['y']);
    }

    public function testIifeBindsArgsAndPreReturnAssignmentsHaveReferenceSemantics(): void
    {
        $d = $this->decode('{r:(function(a,b,c){a.k=b;a.z=[1,c];b.late=2;return {first:a,second:a,raw:c}}({},{q:1},"s"))}');
        $expected = ['k' => ['q' => 1, 'late' => 2], 'z' => [1, 's']];
        $this->assertSame($expected, $d['r']['first']);
        $this->assertSame($expected, $d['r']['second']);
        $this->assertSame('s', $d['r']['raw']);
    }

    public function testEmptyParamIifeAndTopLevelArrayOfIifes(): void
    {
        $d = $this->decode('{p:[(function(){return {ok:true}}()),null,(function(a){return a}([1,2]))]}');
        $this->assertSame([['ok' => true], null, [1, 2]], $d['p']);
    }

    public function testIntegerLikeKeysFollowJsOrdering(): void
    {
        $d = $this->decode('{b:1,2:"two",a:2,1:"one"}');
        $this->assertSame([1, 2, 'b', 'a'], array_keys($d));
    }

    // ── Hostile inputs ──────────────────────────────────────────────────────

    /** @return array<string,array{0:string}> */
    public static function hostile(): array
    {
        return [
            'function body statement'     => ['{r:(function(a){alert(1);return {x:a}}("v"))}'],
            'call statement in body'      => ['{r:(function(a){a.x=1;f();return a}("v"))}'],
            'assign to unbound ident'     => ['{r:(function(a){b.x=1;return a}("v"))}'],
            'assign to primitive param'   => ['{r:(function(a){a.x=1;return a}("v"))}'],
            'nested property assignment'  => ['{r:(function(a){a.b.c=1;return a}({}))}'],
            'compound assignment'         => ['{r:(function(a){a.x+=1;return a}({}))}'],
            'equality instead of assign'  => ['{r:(function(a){a.x==1;return a}({}))}'],
            'arrow function'              => ['{x:(a)=>a}'],
            'arrow in return'             => ['{r:(function(a){return {f:a=>1}}("v"))}'],
            'new Date()'                  => ['{x:new Date()}'],
            'new in iife'                 => ['{r:(function(a){return {d:new Date()}}("v"))}'],
            'this'                        => ['{x:this}'],
            'unbound identifier'          => ['{r:(function(a){return {x:b}}("v"))}'],
            'identifier at top level'     => ['{x:foo}'],
            'identifier in args'          => ['{r:(function(a){return a}(foo))}'],
            'arity too few args'          => ['{r:(function(a,b){return a}("v"))}'],
            'arity too many args'         => ['{r:(function(a){return a}("v","w"))}'],
            'template literal'            => ['{x:`a${b}`}'],
            'regex literal'               => ['{x:/ab+c/g}'],
            'other call'                  => ['{x:eval("1")}'],
            'call on param'               => ['{r:(function(f){return {x:f(1)}}("s"))}'],
            'member access'               => ['{r:(function(a){return {x:a.b}}({}))}'],
            'member access top-level'     => ['{x:"a".length}'],
            'nested function in return'   => ['{r:(function(a){return {x:(function(){return 1}())}}("v"))}'],
            'function in args'            => ['{r:(function(a){return a}((function(){return 1}())))}'],
            'bare function expression'    => ['{x:function(){return 1}}'],
            'comment'                     => ['{x:1 /* c */}'],
            'line comment'                => ["{x:1 // c\n}"],
            'trailing comma'              => ['{x:1,}'],
            'array elision'               => ['{x:[1,,2]}'],
            'shorthand property'          => ['{x}'],
            'computed key'                => ['{[a]:1}'],
            'spread'                      => ['{...a}'],
            'getter'                      => ['{get x(){return 1}}'],
            'void non-zero'               => ['{x:void 1}'],
            'typeof'                      => ['{x:typeof 1}'],
            'undefined ident'             => ['{x:undefined}'],
            'NaN'                         => ['{x:NaN}'],
            'hex number'                  => ['{x:0x1f}'],
            'bigint'                      => ['{x:10n}'],
            'octal escape'                => ['{x:"\012"}'],
            'lone surrogate'              => ['{x:"\ud83d"}'],
            'unterminated string'         => ['{x:"abc}'],
            'raw newline in string'       => ["{x:\"a\nb\"}"],
            'trailing garbage'            => ['{x:1}garbage()'],
            'reserved param name'         => ['{r:(function(new){return 1}("v"))}'],
            'duplicate param'             => ['{r:(function(a,a){return a}(1,2))}'],
            'return missing'              => ['{r:(function(a){a.x=1}({}))}'],
            'top-level not object'        => ['"just a string"'],
        ];
    }

    /** @dataProvider hostile */
    public function testHostileInputIsRejected(string $literal): void
    {
        $this->assertRejected($literal);
    }

    public function testCyclicParamReferenceIsRejected(): void
    {
        $this->assertRejected('{r:(function(a,b){a.x=b;b.y=a;return a}({},{}))}', 'cycle');
    }

    // ── Limits ──────────────────────────────────────────────────────────────

    public function testInputOver4MbIsRejected(): void
    {
        $big = '{a:"' . str_repeat('x', SapperLiteralDecoder::MAX_INPUT_BYTES) . '"}';
        $this->assertRejected($big, 'size');
    }

    public function testInputJustUnderTheLimitIsAccepted(): void
    {
        $body = '{a:"' . str_repeat('x', SapperLiteralDecoder::MAX_INPUT_BYTES - 64) . '"}';
        $d = $this->decode($body);
        $this->assertSame(SapperLiteralDecoder::MAX_INPUT_BYTES - 64, strlen($d['a']));
    }

    public function testNestingDepthLimit(): void
    {
        $ok = '{a:' . str_repeat('[', 40) . '1' . str_repeat(']', 40) . '}';
        $this->assertSame('array', gettype($this->decode($ok)['a']));
        $deep = '{a:' . str_repeat('[', 70) . '1' . str_repeat(']', 70) . '}';
        $this->assertRejected($deep, 'depth');
        $deepObj = str_repeat('{a:', 70) . '1' . str_repeat('}', 70);
        $this->assertRejected($deepObj, 'object depth');
    }

    public function testDecoderIsReusableAfterFailure(): void
    {
        $dec = new SapperLiteralDecoder();
        try {
            $dec->decodeSnippet('__SAPPER__={x:foo}');
            $this->fail('expected rejection');
        } catch (DecoderException $e) {
            $this->addToAssertionCount(1);
        }
        $this->assertSame(['x' => 1], $dec->decodeSnippet('__SAPPER__={x:1}'));
    }
}
