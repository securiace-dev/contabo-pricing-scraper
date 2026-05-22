<?php
declare(strict_types=1);

// Stub for Illuminate\Database\Schema\Blueprint so the typehints on the
// closures inside Installer::migrateTo2() / migrateTo3() resolve in the
// test environment (where the real Illuminate package isn't installed).
// FakeCapsule's CapsuleBlueprint extends this stub so it satisfies the
// typehint.
namespace Illuminate\Database\Schema {
    if (!class_exists(__NAMESPACE__ . '\\Blueprint', false)) {
        class Blueprint
        {
            public string $table = '';
            /** @var list<string> */
            public array $addedColumns = [];
            /** @var list<string> */
            public array $droppedColumns = [];

            public function bigIncrements(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function increments(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function string(string $name, int $_length = 0): self { $this->addedColumns[] = $name; return $this; }
            public function char(string $name, int $_length = 0): self { $this->addedColumns[] = $name; return $this; }
            public function text(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function longText(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function boolean(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function date(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function timestamp(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function integer(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function unsignedInteger(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function unsignedBigInteger(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function unsignedTinyInteger(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function decimal(string $name, int $_p = 0, int $_s = 0): self { $this->addedColumns[] = $name; return $this; }
            public function json(string $name): self { $this->addedColumns[] = $name; return $this; }
            public function enum(string $name, array $_vals): self { $this->addedColumns[] = $name; return $this; }
            public function timestamps(): self { $this->addedColumns[] = 'created_at'; $this->addedColumns[] = 'updated_at'; return $this; }

            /** @param string|list<string> $cols */
            public function dropColumn($cols): self
            {
                if (is_array($cols)) {
                    foreach ($cols as $c) { $this->droppedColumns[] = (string) $c; }
                } else {
                    $this->droppedColumns[] = (string) $cols;
                }
                return $this;
            }

            // Modifier no-ops: default(), nullable(), unique(), primary(),
            // useCurrent(), useCurrentOnUpdate(), index(), etc.
            public function __call(string $_name, array $_args): self
            {
                return $this;
            }
        }
    }
}

namespace WHMCS\Database {

if (!class_exists(__NAMESPACE__ . '\\Capsule', false)) {

    /**
     * Minimal stand-in for WHMCS's real Capsule facade.
     *
     * Originally only tracked where()+update() into a static $calls array
     * (preserved here for existing tests). Extended for the Phase A engine
     * tests to also track:
     *   - $inserts : every ->insert() / ->insertGetId() call
     *   - $tables  : the row store, keyed by table → list of associative rows
     *   - connection()->transaction(fn) : transparent passthrough
     *
     * Reset with Capsule::reset() between tests.
     */
    class Capsule
    {
        /** @var list<array{table:string,where:array<string,mixed>,update:array<string,mixed>}> */
        public static array $calls = [];

        /** @var list<array{table:string,values:array<string,mixed>}> */
        public static array $inserts = [];

        /** @var array<string,list<array<string,mixed>>> */
        public static array $tables = [];

        /** @var int auto-increment id seed */
        public static int $nextId = 1;

        /**
         * Per-table column registry. Tests that exercise migrations seed
         * this so hasTable()/hasColumn() can answer truthfully.
         *
         * @var array<string,list<string>>
         */
        public static array $columns = [];

        /**
         * Raw SQL statements captured via connection()->statement().
         * Test-only audit trail. Mostly used for migration tests that want
         * to assert "the UPDATE … bitwise-OR ran exactly once".
         *
         * @var list<string>
         */
        public static array $statements = [];

        /**
         * Fidelity switch: real WHMCS Capsule returns stdClass from first()/get();
         * FakeCapsule returns arrays by default. Flip this true to make the public
         * first()/get() return stdClass, so the integration-style tests can prove
         * the addon's `(array)` casts hold against the REAL return type (this is
         * exactly the class of bug FakeCapsule otherwise masks). Internal logic
         * keeps using arrays via firstRow(). Reset to false by reset().
         */
        public static bool $returnStdClass = false;

        public static function reset(): void
        {
            self::$calls = [];
            self::$inserts = [];
            self::$tables = [];
            self::$columns = [];
            self::$statements = [];
            self::$nextId = 1;
            self::$returnStdClass = false;
        }

        public static function table(string $table): CapsuleQuery
        {
            return new CapsuleQuery($table);
        }

        public static function connection(): CapsuleConnection
        {
            return new CapsuleConnection();
        }

        public static function schema(): CapsuleSchema
        {
            return new CapsuleSchema();
        }
    }

    final class CapsuleConnection
    {
        /**
         * Transparent passthrough of WHMCS\Database\Capsule::connection()->transaction(fn).
         * Closure is invoked immediately; any throw bubbles up.
         *
         * @template T
         * @param callable():T $fn
         * @return T
         */
        public function transaction(callable $fn)
        {
            return $fn();
        }

        /**
         * Records the raw SQL into Capsule::$statements. Best-effort hand-
         * parser for the small set of statements actually used by the test
         * suite (CREATE TABLE LIKE, INSERT … SELECT, UPDATE … bitwise-OR).
         */
        public function statement(string $sql): bool
        {
            Capsule::$statements[] = $sql;
            $trim = trim($sql);
            $upper = strtoupper($trim);

            // CREATE TABLE [IF NOT EXISTS] <backup> LIKE <source>
            if (preg_match('/^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)\s+LIKE\s+([A-Za-z0-9_]+)\b/i', $trim, $m)) {
                $dest = $m[1];
                $src  = $m[2];
                if (!isset(Capsule::$columns[$dest])) {
                    Capsule::$columns[$dest] = Capsule::$columns[$src] ?? [];
                }
                if (!isset(Capsule::$tables[$dest])) {
                    Capsule::$tables[$dest] = [];
                }
                return true;
            }

            // INSERT INTO <dest> SELECT * FROM <src>
            if (preg_match('/^INSERT\s+INTO\s+([A-Za-z0-9_]+)\s+SELECT\s+\*\s+FROM\s+([A-Za-z0-9_]+)\b/i', $trim, $m)) {
                $dest = $m[1];
                $src  = $m[2];
                $rows = Capsule::$tables[$src] ?? [];
                if (!isset(Capsule::$tables[$dest])) {
                    Capsule::$tables[$dest] = [];
                }
                foreach ($rows as $row) {
                    Capsule::$tables[$dest][] = $row;
                }
                return true;
            }

            // UPDATE <table> SET <col> = <col> | <int>, ... WHERE <col> = <val>
            if (preg_match('/^UPDATE\s+([A-Za-z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+([A-Za-z0-9_]+)\s*=\s*([0-9]+)$/is', $trim, $m)) {
                $table = $m[1];
                $setClause = $m[2];
                $whereCol = $m[3];
                $whereVal = (int) $m[4];
                if (!isset(Capsule::$tables[$table])) {
                    return true;
                }
                // Parse each "col = col | INT" assignment.
                $assignments = [];
                foreach (explode(',', $setClause) as $part) {
                    $part = trim($part);
                    if (preg_match('/^([A-Za-z0-9_]+)\s*=\s*([A-Za-z0-9_]+)\s*\|\s*([0-9]+)$/i', $part, $a)) {
                        $assignments[] = ['dst' => $a[1], 'src' => $a[2], 'bit' => (int) $a[3]];
                    }
                }
                foreach (Capsule::$tables[$table] as $idx => $row) {
                    if ((int) ($row[$whereCol] ?? 0) !== $whereVal) {
                        continue;
                    }
                    foreach ($assignments as $assign) {
                        $cur = (int) ($row[$assign['src']] ?? 0);
                        Capsule::$tables[$table][$idx][$assign['dst']] = $cur | $assign['bit'];
                    }
                }
                return true;
            }

            return true;
        }
    }

    /**
     * Minimal Capsule::schema() stand-in. Tracks tables + columns in the
     * shared Capsule::$columns registry so hasTable()/hasColumn() answer
     * consistently with whatever the migration just did.
     */
    final class CapsuleSchema
    {
        public function hasTable(string $name): bool
        {
            return array_key_exists($name, Capsule::$columns)
                || array_key_exists($name, Capsule::$tables);
        }

        public function hasColumn(string $table, string $column): bool
        {
            $cols = Capsule::$columns[$table] ?? null;
            if ($cols === null) {
                return false;
            }
            return in_array($column, $cols, true);
        }

        /** @param callable(\Illuminate\Database\Schema\Blueprint):void $cb */
        public function create(string $table, callable $cb): void
        {
            if (!isset(Capsule::$columns[$table])) {
                Capsule::$columns[$table] = [];
            }
            if (!isset(Capsule::$tables[$table])) {
                Capsule::$tables[$table] = [];
            }
            $bp = new \Illuminate\Database\Schema\Blueprint();
            $bp->table = $table;
            $cb($bp);
            Capsule::$columns[$table] = array_values(array_unique(array_merge(
                Capsule::$columns[$table],
                $bp->addedColumns
            )));
        }

        /** @param callable(\Illuminate\Database\Schema\Blueprint):void $cb */
        public function table(string $table, callable $cb): void
        {
            $bp = new \Illuminate\Database\Schema\Blueprint();
            $bp->table = $table;
            $cb($bp);
            if (!isset(Capsule::$columns[$table])) {
                Capsule::$columns[$table] = [];
            }
            Capsule::$columns[$table] = array_values(array_unique(array_merge(
                Capsule::$columns[$table],
                $bp->addedColumns
            )));
            foreach ($bp->droppedColumns as $col) {
                Capsule::$columns[$table] = array_values(array_filter(
                    Capsule::$columns[$table],
                    static function ($c) use ($col) { return $c !== $col; }
                ));
                // Also strip the column from every existing row so that
                // subsequent reads behave like a real DROP COLUMN.
                if (isset(Capsule::$tables[$table])) {
                    foreach (Capsule::$tables[$table] as $idx => $row) {
                        if (array_key_exists($col, $row)) {
                            unset(Capsule::$tables[$table][$idx][$col]);
                        }
                    }
                }
            }
        }
    }

    final class CapsuleQuery
    {
        /** @var array<int,array{col:string,op:string,val:mixed}> */
        private array $where = [];

        /** @var array<string,string> */
        private array $orderBy = [];

        /** @var int|null */
        private $limit = null;

        private string $table;

        public function __construct(string $table)
        {
            $this->table = $table;
        }

        /**
         * @param array<string,mixed>|string $column
         */
        public function where($column, $opOrValue = null, $value = null): self
        {
            if (is_array($column)) {
                foreach ($column as $k => $v) {
                    $this->where[] = ['col' => (string) $k, 'op' => '=', 'val' => $v];
                }
                return $this;
            }
            if ($value === null && func_num_args() === 2) {
                // where('col', 'value')
                $this->where[] = ['col' => (string) $column, 'op' => '=', 'val' => $opOrValue];
                return $this;
            }
            // where('col', 'op', 'value')
            $this->where[] = ['col' => (string) $column, 'op' => (string) $opOrValue, 'val' => $value];
            return $this;
        }

        public function orderByDesc(string $col): self
        {
            $this->orderBy[$col] = 'desc';
            return $this;
        }

        public function orderBy(string $col, string $dir = 'asc'): self
        {
            $this->orderBy[$col] = strtolower($dir);
            return $this;
        }

        public function limit(int $n): self
        {
            $this->limit = $n;
            return $this;
        }

        /**
         * @param array<string,mixed> $values
         */
        public function update(array $values): int
        {
            // Preserve legacy $calls shape: flatten the first where for the
            // simple equality form so existing assertions keep working.
            $legacyWhere = [];
            foreach ($this->where as $w) {
                if ($w['op'] === '=') {
                    $legacyWhere[$w['col']] = $w['val'];
                }
            }
            Capsule::$calls[] = [
                'table'  => $this->table,
                'where'  => $legacyWhere,
                'update' => $values,
            ];
            // Mirror the update into the in-memory store, too.
            if (isset(Capsule::$tables[$this->table])) {
                foreach (Capsule::$tables[$this->table] as $idx => $row) {
                    if ($this->rowMatches($row)) {
                        Capsule::$tables[$this->table][$idx] = array_merge($row, $values);
                    }
                }
            }
            return 1;
        }

        /**
         * @param array<string,mixed> $values
         */
        public function insert(array $values): bool
        {
            $this->recordInsert($values);
            return true;
        }

        /**
         * @param array<string,mixed> $values
         */
        public function insertGetId(array $values): int
        {
            $id = Capsule::$nextId++;
            $values['id'] = $id;
            $this->recordInsert($values);
            return $id;
        }

        /**
         * @param array<string,mixed> $values
         */
        private function recordInsert(array $values): void
        {
            // Mimic an auto-increment `id` column: if the caller didn't supply
            // one (the common ::insert() / ::updateOrInsert() insert path),
            // synthesize a fresh id so a follow-up ->value('id') lookup behaves
            // like the real schema. Existing inserts that pass an explicit id
            // (or use insertGetId) keep theirs untouched.
            if (!array_key_exists('id', $values)) {
                $values['id'] = Capsule::$nextId++;
            }
            Capsule::$inserts[] = [
                'table'  => $this->table,
                'values' => $values,
            ];
            if (!isset(Capsule::$tables[$this->table])) {
                Capsule::$tables[$this->table] = [];
            }
            Capsule::$tables[$this->table][] = $values;
        }

        /** @return array<string,mixed>|null */
        /** Internal: always returns the raw array row (FakeCapsule logic uses this). */
        private function firstRow(): ?array
        {
            $rows = $this->collect();
            return $rows[0] ?? null;
        }

        /**
         * Public read: array by default, stdClass when $returnStdClass is on
         * (mimics real WHMCS Capsule). No return type so it can return either
         * (PHP 7.4 has no union types).
         *
         * @return array<string,mixed>|object|null
         */
        public function first()
        {
            $row = $this->firstRow();
            if ($row === null) {
                return null;
            }
            return Capsule::$returnStdClass ? (object) $row : $row;
        }

        /** @return list<array<string,mixed>|object> */
        public function get(): array
        {
            $rows = $this->collect();
            if (Capsule::$returnStdClass) {
                return array_map(static function (array $r) { return (object) $r; }, $rows);
            }
            return $rows;
        }

        public function exists(): bool
        {
            return $this->first() !== null;
        }

        public function count(): int
        {
            return count($this->collect());
        }

        /**
         * Eloquent ::value() — return the named column from the first matching
         * row, or null if no rows match.
         *
         * @return mixed
         */
        public function value(string $column)
        {
            $first = $this->firstRow();
            if ($first === null) return null;
            return array_key_exists($column, $first) ? $first[$column] : null;
        }

        /**
         * Eloquent ::updateOrInsert($attributes, $values). Finds the first row
         * matching $attributes; if present, ->update($values); otherwise
         * ->insert($attributes + $values). The where-clause builder is reset
         * to only the attribute filters so subsequent count()/get() reflect
         * exactly the lookup key.
         *
         * @param array<string,mixed> $attributes
         * @param array<string,mixed> $values
         */
        public function updateOrInsert(array $attributes, array $values = []): bool
        {
            $this->where = [];
            foreach ($attributes as $k => $v) {
                $this->where[] = ['col' => (string) $k, 'op' => '=', 'val' => $v];
            }
            $existing = $this->first();
            if ($existing !== null) {
                $this->update($values);
                return true;
            }
            $this->insert(array_merge($attributes, $values));
            return true;
        }

        /** @return list<array<string,mixed>> */
        private function collect(): array
        {
            $rows = Capsule::$tables[$this->table] ?? [];
            $matched = [];
            foreach ($rows as $row) {
                if ($this->rowMatches($row)) {
                    $matched[] = $row;
                }
            }
            foreach ($this->orderBy as $col => $dir) {
                usort($matched, static function ($a, $b) use ($col, $dir): int {
                    $av = $a[$col] ?? null;
                    $bv = $b[$col] ?? null;
                    if ($av == $bv) return 0;
                    $cmp = ($av < $bv) ? -1 : 1;
                    return $dir === 'desc' ? -$cmp : $cmp;
                });
            }
            if ($this->limit !== null) {
                $matched = array_slice($matched, 0, $this->limit);
            }
            return $matched;
        }

        /** @param array<string,mixed> $row */
        private function rowMatches(array $row): bool
        {
            foreach ($this->where as $w) {
                $col = $w['col'];
                $op  = $w['op'];
                $val = $w['val'];
                $rowVal = $row[$col] ?? null;
                switch ($op) {
                    case '=':
                        if ($rowVal != $val) return false;
                        break;
                    case '<':
                        if (!($rowVal < $val)) return false;
                        break;
                    case '<=':
                        if (!($rowVal <= $val)) return false;
                        break;
                    case '>':
                        if (!($rowVal > $val)) return false;
                        break;
                    case '>=':
                        if (!($rowVal >= $val)) return false;
                        break;
                    default:
                        return false;
                }
            }
            return true;
        }
    }
}
} // namespace WHMCS\Database
