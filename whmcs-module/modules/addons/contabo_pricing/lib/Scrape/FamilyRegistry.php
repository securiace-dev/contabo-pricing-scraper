<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use WHMCS\Database\Capsule;

/**
 * Self-learning registry of product families (mod_contabo_scrape_families).
 *
 * Nothing about the lineup is configured: every run, reconcile() compares what
 * the site itself reports (CatalogStructure::discover(): categories, nav links,
 * membership) with what was learned before, and
 *
 *  - learns:        upserts every category by its stable upstream id, keeps a
 *                   title history, the member slugs and a count history;
 *  - adapts:        nav-linked plan categories become `active`, categories in
 *                   the blob but not in the nav become `hidden`, categories
 *                   that vanish become `retired` (rows are never deleted);
 *  - calibrates:    `typical_plan_count` = median of the last 5 accepted counts;
 *  - heals:         a vanished id whose slug (or >= 80 % of whose member slugs)
 *                   reappears under a NEW id is linked as its successor, with
 *                   approval, display name and history carried forward;
 *  - governs:       new / renamed / retired / delisted / reappeared families,
 *                   plan moves between families and count deviations are
 *                   returned as a diff that RunValidator turns into
 *                   needs_review; `approved`, `admin_hidden` and `display_name`
 *                   are operator-owned and never overwritten by a run.
 *
 * reconcile(..., $persist = false) is a pure preview. ScrapeRunService only
 * persists for runs that end succeeded / needs_review, so a rejected, failed
 * or dry run never "uses up" a new-family flag.
 *
 * PHP 7.4 compatible.
 */
final class FamilyRegistry
{
    public const TABLE = 'mod_contabo_scrape_families';

    public const STATUS_ACTIVE = 'active';
    public const STATUS_HIDDEN = 'hidden';
    public const STATUS_RETIRED = 'retired';
    public const STATUS_NEW = 'new';

    /** Accepted counts kept for the median. */
    public const HISTORY_LIMIT = 5;
    /** Minimum member-slug overlap for a successor match. */
    public const SUCCESSOR_OVERLAP = 0.8;

    /** @return list<array<string,mixed>> decoded rows, nav order first (hidden/retired last) */
    public function all(): array
    {
        $out = [];
        foreach (Capsule::table(self::TABLE)->orderBy('id')->get() as $r) {
            $out[] = $this->decode((array) $r);
        }
        usort($out, static function (array $a, array $b): int {
            $pa = $a['nav_position'] === null ? PHP_INT_MAX : (int) $a['nav_position'];
            $pb = $b['nav_position'] === null ? PHP_INT_MAX : (int) $b['nav_position'];
            return $pa !== $pb ? $pa <=> $pb : (int) $a['id'] <=> (int) $b['id'];
        });
        return $out;
    }

    /** @return array<string,mixed>|null */
    public function find(int $id): ?array
    {
        $r = Capsule::table(self::TABLE)->where('id', $id)->first();
        return $r === null ? null : $this->decode((array) $r);
    }

    /** @return array<string,mixed>|null */
    public function findByCategory(string $categoryId): ?array
    {
        $r = Capsule::table(self::TABLE)->where('category_id', $categoryId)->first();
        return $r === null ? null : $this->decode((array) $r);
    }

    /** Families a run imports: active or new, not hidden by an admin, nav order. @return list<array<string,mixed>> */
    public function active(): array
    {
        $out = [];
        foreach ($this->all() as $r) {
            if (in_array($r['status'], [self::STATUS_ACTIVE, self::STATUS_NEW], true) && (int) $r['admin_hidden'] === 0) {
                $out[] = $r;
            }
        }
        return $out;
    }

    /** Public display name: admin override, else the site's nav title, else its category title. @param array<string,mixed> $row */
    public static function effectiveName(array $row): string
    {
        foreach (['display_name', 'nav_title', 'title'] as $k) {
            if (isset($row[$k]) && is_string($row[$k]) && trim($row[$k]) !== '') {
                return trim($row[$k]);
            }
        }
        return (string) ($row['slug'] ?? '');
    }

    // ── governance (operator actions) ───────────────────────────────────────

    public function approve(int $id): bool
    {
        return $this->touch($id, ['approved' => 1]);
    }

    /** Hiding keeps the row and its history; the family just stops importing. */
    public function setHidden(int $id, bool $hidden): bool
    {
        $r = $this->find($id);
        if ($r === null) {
            return false;
        }
        $upd = ['admin_hidden' => $hidden ? 1 : 0];
        if ($hidden) {
            $upd['status'] = self::STATUS_HIDDEN;
        } elseif ($r['status'] === self::STATUS_HIDDEN && $r['nav_position'] !== null) {
            $upd['status'] = self::STATUS_ACTIVE; // still nav-linked: it imports again
        }
        return $this->touch($id, $upd);
    }

    /** An empty name clears the override. */
    public function renameDisplay(int $id, string $name): bool
    {
        $name = trim((string) preg_replace('/\s+/u', ' ', $name));
        if (strlen($name) > 160) {
            $name = substr($name, 0, 160);
        }
        return $this->touch($id, ['display_name' => $name === '' ? null : $name]);
    }

    /** @param array<string,mixed> $fields */
    private function touch(int $id, array $fields): bool
    {
        if ($this->find($id) === null) {
            return false;
        }
        $fields['updated_at'] = date('Y-m-d H:i:s');
        Capsule::table(self::TABLE)->where('id', $id)->update($fields);
        return true;
    }

    // ── reconcile ───────────────────────────────────────────────────────────

    /**
     * @param CatalogStructure|array<string,mixed> $structure a CatalogStructure built on the page's
     *        preloaded[0], or its discover() result
     * @param bool $persist false = compute the diff only, write nothing
     * @param float $maxDeviationPct count deviation (vs typical_plan_count) that becomes a flag
     * @return array<string,mixed> diff: new, renamed, retired, delisted, reappeared, count_changes,
     *         plan_moves, unapproved (each a list) and families (importable-family snapshot)
     */
    public function reconcile($structure, int $runId, bool $persist = true, float $maxDeviationPct = 20.0): array
    {
        $d = $structure instanceof CatalogStructure ? $structure->discover() : (array) $structure;
        $cats = isset($d['categories']) && is_array($d['categories']) ? $d['categories'] : [];
        $now = date('Y-m-d H:i:s');

        $rows = [];
        foreach ($this->all() as $r) {
            $rows[(string) $r['category_id']] = $r;
        }

        $diff = [
            'new' => [], 'renamed' => [], 'retired' => [], 'delisted' => [], 'reappeared' => [],
            'count_changes' => [], 'plan_moves' => [], 'unapproved' => [], 'families' => [],
            'persisted' => $persist,
        ];

        // ── self-healing: a vanished id that comes back under a new id ──────
        $vanished = [];
        foreach ($rows as $cid => $r) {
            if (!isset($cats[$cid]) && $r['status'] !== self::STATUS_RETIRED) {
                $vanished[$cid] = $r;
            }
        }
        $successor = []; // new category id => predecessor category id
        $taken = [];
        foreach ($cats as $cid => $c) {
            if (isset($rows[$cid])) {
                continue;
            }
            $match = null;
            foreach ($vanished as $vid => $v) {
                if (!isset($taken[$vid]) && $v['slug'] === $c['slug']) {
                    $match = $vid;
                    break;
                }
            }
            if ($match === null) {
                $slugs = !empty($c['plan_family']) ? $c['plans'] : $c['members'];
                foreach ($vanished as $vid => $v) {
                    if (!isset($taken[$vid]) && self::overlap($v['plan_slugs'], $slugs) >= self::SUCCESSOR_OVERLAP) {
                        $match = $vid;
                        break;
                    }
                }
            }
            if ($match !== null) {
                $successor[(string) $cid] = (string) $match;
                $taken[$match] = true;
            }
        }
        $replacedBy = array_flip($successor); // predecessor id => new id

        // ── previous plan ownership (for plan_moves) ────────────────────────
        $prevOwner = [];
        foreach ($rows as $cid => $r) {
            if (in_array($r['status'], [self::STATUS_ACTIVE, self::STATUS_NEW], true)) {
                foreach ($r['plan_slugs'] as $slug) {
                    $prevOwner[(string) $slug] = isset($replacedBy[$cid]) ? $replacedBy[$cid] : $cid;
                }
            }
        }
        $labelOf = static function (string $cid) use ($rows, $cats): string {
            if (isset($cats[$cid])) {
                return self::catLabel($cats[$cid]);
            }
            return isset($rows[$cid]) ? self::effectiveName($rows[$cid]) : $cid;
        };

        $writes = [];
        $curOwner = [];
        foreach ($cats as $cid => $c) {
            $cid = (string) $cid;
            $linked = !empty($c['plan_family']);
            $label = self::catLabel($c);
            $count = $linked ? count($c['plans']) : count($c['members']);
            $slugs = $linked ? array_values($c['plans']) : array_values($c['members']);
            if ($linked) {
                foreach ($c['plans'] as $slug) {
                    $curOwner[(string) $slug] = $cid;
                }
            }

            $base = isset($rows[$cid]) ? $rows[$cid] : null;
            $pred = isset($successor[$cid]) ? $rows[$successor[$cid]] : null;
            $src = $base ?? $pred; // history source

            $titleHistory = $src !== null ? $src['title_history'] : [];
            $countHistory = $src !== null ? $src['count_history'] : [];
            $typical = $src !== null ? (int) $src['typical_plan_count'] : 0;
            $approved = $src !== null ? (int) $src['approved'] : 0;
            $adminHidden = $src !== null ? (int) $src['admin_hidden'] : 0;
            $display = $src !== null ? $src['display_name'] : null;
            $prevStatus = $base !== null ? (string) $base['status'] : null;

            // ── status ──────────────────────────────────────────────────────
            if ($adminHidden === 1 || !$linked) {
                $status = self::STATUS_HIDDEN;
            } elseif ($base === null && $pred === null) {
                $status = self::STATUS_NEW;
            } elseif ($prevStatus === self::STATUS_NEW) {
                $status = self::STATUS_ACTIVE; // seen again by an accepted run
            } else {
                $status = self::STATUS_ACTIVE;
            }

            // ── diff entries ────────────────────────────────────────────────
            $info = ['category_id' => $cid, 'slug' => (string) $c['slug'], 'label' => $label];
            $lastLabel = $titleHistory !== [] ? (string) $titleHistory[count($titleHistory) - 1]['title'] : '';

            if ($base === null && $pred === null) {
                $diff['new'][] = $info + ['status' => $status, 'plans' => $count, 'reason' => 'first_seen'];
            } elseif ($base === null && $pred !== null) {
                $diff['renamed'][] = $info + [
                    'kind' => 'id_change', 'from' => self::effectiveName($pred), 'to' => $label,
                    'from_category_id' => $pred['category_id'], 'successor_of' => $pred['category_id'],
                ];
            } else {
                if ($prevStatus === self::STATUS_RETIRED) {
                    $diff['reappeared'][] = $info + ['plans' => $count];
                } elseif ($linked && $adminHidden === 0 && $prevStatus === self::STATUS_HIDDEN) {
                    $diff['new'][] = $info + ['status' => $status, 'plans' => $count, 'reason' => 'listed_in_nav'];
                } elseif (!$linked && in_array($prevStatus, [self::STATUS_ACTIVE, self::STATUS_NEW], true)) {
                    $diff['delisted'][] = $info + ['plans' => $count];
                }
            }
            if ($base !== null && $lastLabel !== '' && $lastLabel !== $label) {
                $diff['renamed'][] = $info + ['kind' => 'title', 'from' => $lastLabel, 'to' => $label];
            }
            if ($lastLabel !== $label) {
                $titleHistory[] = ['title' => $label, 'seen_at' => $now];
            }

            if ($linked && $adminHidden === 0 && $typical > 0 && $countHistory !== []) {
                $pct = abs($count - $typical) / $typical * 100.0;
                if ((int) round($pct * 100) > (int) round($maxDeviationPct * 100)) {
                    $diff['count_changes'][] = $info + [
                        'typical' => $typical, 'current' => $count, 'pct' => round($pct, 2), 'max_pct' => $maxDeviationPct,
                    ];
                }
            }
            if ($linked && $adminHidden === 0) {
                if ($approved === 0) {
                    $diff['unapproved'][] = $info;
                }
                $diff['families'][] = $info + [
                    'status' => $status, 'approved' => $approved, 'count' => $count, 'typical_plan_count' => $typical,
                    'nav_href' => $c['nav_href'], 'nav_position' => $c['nav_position'],
                    'sample_product_url' => $c['sample_product_url'], 'display_name' => $display,
                    'name' => $display !== null && $display !== '' ? $display : $label,
                ];
            }

            // ── calibration (accepted runs only; preview never reaches here) ──
            if ($linked) {
                $countHistory[] = $count;
                $countHistory = array_slice($countHistory, -self::HISTORY_LIMIT);
                $typical = self::median($countHistory);
            }

            $writes[] = [
                'base' => $base,
                'fields' => [
                    'category_id' => $cid,
                    'slug' => substr((string) $c['slug'], 0, 80),
                    'title' => substr((string) $c['title'], 0, 160),
                    'nav_title' => $c['nav_title'] === null ? null : substr((string) $c['nav_title'], 0, 160),
                    'nav_href' => $c['nav_href'] === null ? null : substr((string) $c['nav_href'], 0, 255),
                    'nav_position' => $c['nav_position'] === null ? null : (int) $c['nav_position'],
                    'status' => $status,
                    'last_seen_at' => $now,
                    'last_plan_count' => $count,
                    'typical_plan_count' => $typical,
                    'plan_count_history_json' => json_encode(array_values($countHistory)),
                    'title_history_json' => json_encode(array_values($titleHistory), JSON_UNESCAPED_UNICODE),
                    'plan_slugs_json' => json_encode($slugs, JSON_UNESCAPED_SLASHES),
                    'sample_product_url' => $c['sample_product_url'] !== null
                        ? substr((string) $c['sample_product_url'], 0, 255)
                        : ($src !== null ? $src['sample_product_url'] : null),
                    'approved' => $approved,
                    'admin_hidden' => $adminHidden,
                    'display_name' => $display,
                    'successor_of' => $base !== null ? $base['successor_of'] : ($pred !== null ? $pred['category_id'] : null),
                    'last_run_id' => $runId > 0 ? $runId : null,
                    'notes' => $base !== null ? $base['notes'] : ($pred !== null ? 'Successor of ' . $pred['category_id'] . ' (' . $pred['slug'] . ')' : null),
                    'first_seen_at' => $base !== null ? $base['first_seen_at'] : ($pred !== null ? $pred['first_seen_at'] : $now),
                    'updated_at' => $now,
                ],
            ];
        }

        // ── vanished ────────────────────────────────────────────────────────
        $retire = [];
        foreach ($vanished as $vid => $v) {
            $retire[] = $vid;
            if (!isset($replacedBy[$vid]) && in_array($v['status'], [self::STATUS_ACTIVE, self::STATUS_NEW], true)) {
                $diff['retired'][] = [
                    'category_id' => $vid, 'slug' => $v['slug'], 'label' => self::effectiveName($v), 'was' => $v['status'],
                    'plans' => count($v['plan_slugs']),
                ];
            }
        }

        // ── plan moves ──────────────────────────────────────────────────────
        foreach ($curOwner as $slug => $to) {
            if (isset($prevOwner[$slug]) && $prevOwner[$slug] !== $to) {
                $diff['plan_moves'][] = [
                    'slug' => $slug, 'from' => $prevOwner[$slug], 'from_label' => $labelOf($prevOwner[$slug]),
                    'to' => $to, 'to_label' => $labelOf($to),
                ];
            }
        }
        usort($diff['plan_moves'], static function (array $a, array $b): int {
            return strcmp($a['slug'], $b['slug']);
        });
        usort($diff['families'], static function (array $a, array $b): int {
            return (int) $a['nav_position'] <=> (int) $b['nav_position'];
        });

        if ($persist) {
            foreach ($writes as $w) {
                $f = $w['fields'];
                if ($w['base'] === null) {
                    $f['created_at'] = $now;
                    Capsule::table(self::TABLE)->insert($f);
                } else {
                    unset($f['category_id']);
                    Capsule::table(self::TABLE)->where('id', $w['base']['id'])->update($f);
                }
            }
            foreach ($retire as $vid) {
                $note = isset($replacedBy[$vid]) ? 'Superseded by ' . $replacedBy[$vid] : 'Vanished from the catalogue';
                Capsule::table(self::TABLE)->where('category_id', $vid)->update([
                    'status' => self::STATUS_RETIRED, 'notes' => $note, 'last_run_id' => $runId > 0 ? $runId : null, 'updated_at' => $now,
                ]);
            }
        }
        return $diff;
    }

    /** Median of the last counts, rounded to a whole number of plans. @param list<int> $counts */
    public static function median(array $counts): int
    {
        if ($counts === []) {
            return 0;
        }
        sort($counts);
        $n = count($counts);
        $mid = intdiv($n, 2);
        return $n % 2 === 1 ? (int) $counts[$mid] : (int) round(($counts[$mid - 1] + $counts[$mid]) / 2);
    }

    /** |A ∩ B| / max(|A|, |B|); 0 when either side is empty. @param list<string> $a @param list<string> $b */
    public static function overlap(array $a, array $b): float
    {
        if ($a === [] || $b === []) {
            return 0.0;
        }
        return count(array_intersect($a, $b)) / max(count($a), count($b));
    }

    /** @param array<string,mixed> $cat */
    private static function catLabel(array $cat): string
    {
        $n = isset($cat['nav_title']) && is_string($cat['nav_title']) ? trim($cat['nav_title']) : '';
        return $n !== '' ? $n : (string) $cat['title'];
    }

    /**
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    private function decode(array $row): array
    {
        $row['plan_slugs'] = self::list($row['plan_slugs_json'] ?? null);
        $row['title_history'] = self::list($row['title_history_json'] ?? null);
        $row['count_history'] = array_map('intval', self::list($row['plan_count_history_json'] ?? null));
        $row['nav_position'] = isset($row['nav_position']) && $row['nav_position'] !== null ? (int) $row['nav_position'] : null;
        foreach (['typical_plan_count', 'last_plan_count', 'approved', 'admin_hidden'] as $k) {
            $row[$k] = (int) ($row[$k] ?? 0);
        }
        foreach (['display_name', 'nav_title', 'nav_href', 'successor_of', 'sample_product_url', 'notes'] as $k) {
            $row[$k] = isset($row[$k]) && $row[$k] !== '' ? $row[$k] : null;
        }
        return $row;
    }

    /** @param mixed $json @return list<mixed> */
    private static function list($json): array
    {
        if (!is_string($json) || $json === '') {
            return [];
        }
        $d = json_decode($json, true);
        return is_array($d) ? array_values($d) : [];
    }
}
