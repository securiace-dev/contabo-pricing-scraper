<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\DriftHasher;
use PHPUnit\Framework\TestCase;

/**
 * Amendment #14 — DriftHasher (canonical hash of addon-controlled config fields).
 *
 * Pure / deterministic: no DB, no Capsule, no IO. These tests pin the canonical
 * form's guarantees the apply path relies on: order independence, sensitivity to
 * any controlled-field change, blindness to non-controlled columns, and the
 * no-collision rule for the tricky null / '' / '0' / 0 quartet.
 */
final class DriftHasherTest extends TestCase
{
    public function testHashIsAFortyCharHexSha1(): void
    {
        $h = DriftHasher::hash(['optionname' => 'IPv4', 'optiontype' => 1]);
        $this->assertMatchesRegularExpression('/^[0-9a-f]{40}$/', $h);
    }

    public function testKeyOrderIndependence(): void
    {
        $a = DriftHasher::hash([
            'optionname' => 'Networking:IPv4',
            'optiontype' => 4,
            'qtyminimum' => 0,
            'qtymaximum' => 5,
            'hidden'     => 0,
        ]);

        // Same logical map, keys supplied in a totally different order.
        $b = DriftHasher::hash([
            'hidden'     => 0,
            'qtymaximum' => 5,
            'optiontype' => 4,
            'qtyminimum' => 0,
            'optionname' => 'Networking:IPv4',
        ]);

        $this->assertSame($a, $b, 'reordering keys must not change the hash');
    }

    public function testAValueChangeChangesTheHash(): void
    {
        $base = ['optionname' => 'IPv4', 'optiontype' => 4, 'hidden' => 0];
        $changed = ['optionname' => 'IPv4', 'optiontype' => 4, 'hidden' => 1];

        $this->assertNotSame(
            DriftHasher::hash($base),
            DriftHasher::hash($changed),
            'flipping a controlled field must change the hash'
        );
    }

    public function testHashIsStableAcrossRepeatedCalls(): void
    {
        $fields = ['optionname' => 'Image', 'optiontype' => 1, 'qtyminimum' => 1];
        $this->assertSame(DriftHasher::hash($fields), DriftHasher::hash($fields));
    }

    public function testHashFieldsIgnoresNonListedColumns(): void
    {
        $controlled = ['optionname', 'optiontype', 'qtyminimum', 'qtymaximum', 'hidden'];

        $rowA = [
            'id'         => 9100,
            'optionname' => 'Networking:IPv4',
            'optiontype' => 4,
            'qtyminimum' => 0,
            'qtymaximum' => 5,
            'hidden'     => 0,
            'created_at' => '2026-01-01 00:00:00',
            'updated_at' => '2026-01-01 00:00:00',
        ];

        // Same controlled values, but WHMCS-managed columns differ.
        $rowB = [
            'id'         => 9999,
            'optionname' => 'Networking:IPv4',
            'optiontype' => 4,
            'qtyminimum' => 0,
            'qtymaximum' => 5,
            'hidden'     => 0,
            'created_at' => '2025-06-30 12:34:56',
            'updated_at' => '2026-05-23 09:00:00',
        ];

        $this->assertSame(
            DriftHasher::hashFields($rowA, $controlled),
            DriftHasher::hashFields($rowB, $controlled),
            'changing id / timestamps must not change the controlled-field hash'
        );
    }

    public function testHashFieldsTreatsMissingKeyAsNull(): void
    {
        $names = ['optionname', 'hidden'];

        // 'hidden' absent from the row → treated as null.
        $missing = DriftHasher::hashFields(['optionname' => 'IPv4'], $names);
        $explicitNull = DriftHasher::hashFields(['optionname' => 'IPv4', 'hidden' => null], $names);
        $this->assertSame($missing, $explicitNull, 'missing key === explicit null');

        // …and that is NOT the same as hidden = '' or hidden = 0.
        $emptyString = DriftHasher::hashFields(['optionname' => 'IPv4', 'hidden' => ''], $names);
        $zero = DriftHasher::hashFields(['optionname' => 'IPv4', 'hidden' => 0], $names);
        $this->assertNotSame($missing, $emptyString);
        $this->assertNotSame($missing, $zero);
    }

    public function testHashFieldsOnlyHashesListedColumns(): void
    {
        $row = ['optionname' => 'IPv4', 'optiontype' => 4, 'extra' => 'ignored'];

        // Hashing the same listed columns must equal hashing a map containing
        // ONLY those columns — the 'extra' key is invisible.
        $this->assertSame(
            DriftHasher::hashFields($row, ['optionname', 'optiontype']),
            DriftHasher::hash(['optionname' => 'IPv4', 'optiontype' => 4])
        );
    }

    public function testNullEmptyStringZeroStringAndZeroIntAreDistinct(): void
    {
        $h = static function ($v): string {
            return DriftHasher::hash(['f' => $v]);
        };

        // The three logically-distinct "absence / zero" buckets the apply path
        // must never confuse: a NULL column, an empty-string column, and a
        // literal zero. (int 0 and string '0' are the SAME bucket by design —
        // see testIntZeroAndStringZeroIntentionallyCollide — so '0' represents
        // the zero bucket here.)
        $hashes = [
            'null'        => $h(null),
            'emptyString' => $h(''),
            'zeroString'  => $h('0'),
            'zeroInt'     => $h(0),
        ];

        // null, '' and "0" must be pairwise distinct; the two zero forms collide.
        $this->assertNotSame($hashes['null'], $hashes['emptyString'], 'null vs ""');
        $this->assertNotSame($hashes['null'], $hashes['zeroString'], 'null vs "0"');
        $this->assertNotSame($hashes['emptyString'], $hashes['zeroString'], '"" vs "0"');
        $this->assertSame($hashes['zeroString'], $hashes['zeroInt'], '"0" === int 0');
        $this->assertSame(
            3,
            count(array_unique($hashes)),
            'null / "" / "0" must be 3 distinct buckets: ' . var_export($hashes, true)
        );
    }

    public function testIntZeroAndStringZeroIntentionallyCollide(): void
    {
        // As raw column data, int 0 and string '0' are indistinguishable and
        // SHOULD canonicalise the same; same for float 0.0.
        $this->assertSame(DriftHasher::hash(['f' => 0]), DriftHasher::hash(['f' => '0']));
        $this->assertSame(DriftHasher::hash(['f' => 0]), DriftHasher::hash(['f' => 0.0]));
    }

    public function testNumericStringAndFloatCanonicaliseConsistently(): void
    {
        // int 10, float 10.0 and string '10' all describe the same column value.
        $this->assertSame(DriftHasher::hash(['f' => 10]), DriftHasher::hash(['f' => 10.0]));
        $this->assertSame(DriftHasher::hash(['f' => 10]), DriftHasher::hash(['f' => '10']));
    }

    public function testBoolTrueCanonicalisesLikeOne(): void
    {
        // bool true → '1' so an exposure flag stored as true vs the int 1 it
        // becomes in the DB hash identically.
        $this->assertSame(DriftHasher::hash(['hidden' => true]), DriftHasher::hash(['hidden' => 1]));
        $this->assertSame(DriftHasher::hash(['hidden' => false]), DriftHasher::hash(['hidden' => 0]));
    }

    public function testSeparatorCannotBeForgedByValueContents(): void
    {
        // Two distinct maps that would collide under a naive "key=value" join
        // with no separator discipline must NOT collide here.
        $a = DriftHasher::hash(['a' => 'x', 'b' => 'y']);
        $b = DriftHasher::hash(['a' => 'x=y']);          // value contains '='
        $c = DriftHasher::hash(['a' => 'xy', 'b' => '']); // boundary shifted
        $this->assertNotSame($a, $b);
        $this->assertNotSame($a, $c);
    }

    public function testMatchesTrueWhenUnchanged(): void
    {
        $controlled = ['optionname', 'optiontype', 'hidden'];
        $row = ['id' => 9100, 'optionname' => 'IPv4', 'optiontype' => 4, 'hidden' => 0];

        $baseline = DriftHasher::hashFields($row, $controlled);

        // A later read with different WHMCS-managed columns still matches.
        $later = ['id' => 9100, 'optionname' => 'IPv4', 'optiontype' => 4, 'hidden' => 0, 'updated_at' => 'later'];
        $this->assertTrue(DriftHasher::matches($baseline, $later, $controlled));
    }

    public function testMatchesFalseWhenControlledFieldChanged(): void
    {
        $controlled = ['optionname', 'optiontype', 'hidden'];
        $baseline = DriftHasher::hashFields(
            ['optionname' => 'IPv4', 'optiontype' => 4, 'hidden' => 0],
            $controlled
        );

        // Admin manually hid the option → drift, must not match.
        $edited = ['optionname' => 'IPv4', 'optiontype' => 4, 'hidden' => 1];
        $this->assertFalse(DriftHasher::matches($baseline, $edited, $controlled));
    }

    public function testMatchesFalseOnEmptyExpectedHash(): void
    {
        $controlled = ['optionname'];
        $row = ['optionname' => 'IPv4'];
        // Unknown baseline → treat as not-matching regardless of row contents.
        $this->assertFalse(DriftHasher::matches('', $row, $controlled));
    }
}
