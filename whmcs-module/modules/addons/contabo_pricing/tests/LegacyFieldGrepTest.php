<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Phase A.5.1 regression gate for the whole round.
 *
 * Schema v3 dropped the legacy mapping columns (the three per-cycle booleans).
 * They may legitimately appear ONLY in Installer.php (the migrateTo3() legacy
 * backfill) and in tests/ + docs/. Anywhere else in the executable runtime
 * tree (lib/, templates/, contabo_pricing.php, hooks.php, ajax.php) is a
 * regression that crashes with SQLSTATE[42S22] (Unknown column) on the
 * migrated schema.
 *
 * The scan strips PHP comments before matching, so a file that merely
 * *documents* the historical columns in a docblock (e.g. CycleSet.php, whose
 * fromLegacyBooleans() helper is a migration-support utility) is not flagged —
 * only executable references count.
 *
 * NOTE: until the orchestrator removes the legacy keys from
 * lib/AdminController.php this test FAILS by design — that failure is the
 * signal that the controller integration is still pending.
 */
final class LegacyFieldGrepTest extends TestCase
{
    private const PATTERN = '/apply_to_monthly|apply_to_semiannually|apply_to_annually/';

    /** Files allowed to mention the legacy fields (basename match). */
    private const ALLOWED_BASENAMES = ['Installer.php'];

    public function testNoLegacyApplyToInRuntime(): void
    {
        $root = dirname(__DIR__);

        $offenders = [];
        foreach ($this->runtimeFiles($root) as $file) {
            $contents = file_get_contents($file);
            if ($contents === false) {
                continue;
            }
            if (in_array(basename($file), self::ALLOWED_BASENAMES, true)) {
                continue;
            }
            $code = $this->stripComments($file, $contents);
            if (!preg_match(self::PATTERN, $code)) {
                continue;
            }
            $offenders[] = $this->relative($root, $file);
        }

        $this->assertSame(
            [],
            $offenders,
            "Legacy apply_to_* fields found outside Installer.php (runtime regression): \n  "
            . implode("\n  ", $offenders)
        );
    }

    /**
     * The runtime PHP/TPL surface: lib/, templates/, and the three entry files.
     * vendor/ and tests/ are excluded (tests legitimately reference the legacy
     * fields; vendor is third-party).
     *
     * @return list<string>
     */
    private function runtimeFiles(string $root): array
    {
        $files = [];

        foreach (['lib', 'templates'] as $dir) {
            $base = $root . DIRECTORY_SEPARATOR . $dir;
            if (!is_dir($base)) {
                continue;
            }
            $it = new \RecursiveIteratorIterator(
                new \RecursiveDirectoryIterator($base, \FilesystemIterator::SKIP_DOTS)
            );
            foreach ($it as $entry) {
                /** @var \SplFileInfo $entry */
                if (!$entry->isFile()) {
                    continue;
                }
                $ext = strtolower($entry->getExtension());
                if ($ext === 'php' || $ext === 'tpl') {
                    $files[] = $entry->getPathname();
                }
            }
        }

        foreach (['contabo_pricing.php', 'hooks.php', 'ajax.php'] as $entryFile) {
            $path = $root . DIRECTORY_SEPARATOR . $entryFile;
            if (is_file($path)) {
                $files[] = $path;
            }
        }

        sort($files);
        return $files;
    }

    /**
     * Return the file's source with PHP comments removed, so a docblock or
     * inline comment that merely names the historical columns does not count
     * as a runtime reference. Inline HTML in .tpl files is preserved (it can
     * contain executable-equivalent output), but PHP comments inside it are
     * stripped via token_get_all.
     */
    private function stripComments(string $file, string $contents): string
    {
        $ext = strtolower(pathinfo($file, PATHINFO_EXTENSION));
        // token_get_all needs a PHP open tag to enter PHP mode. .tpl files
        // already contain one; bare PHP files do too. If a file has no PHP at
        // all, fall back to the raw contents.
        if (strpos($contents, '<?php') === false && strpos($contents, '<?=') === false) {
            return $contents;
        }

        $out = '';
        $tokens = @token_get_all($contents);
        foreach ($tokens as $token) {
            if (is_array($token)) {
                if ($token[0] === T_COMMENT || $token[0] === T_DOC_COMMENT) {
                    // Replace with a newline to preserve line structure.
                    $out .= "\n";
                    continue;
                }
                $out .= $token[1];
            } else {
                $out .= $token;
            }
        }
        unset($ext);
        return $out;
    }

    private function relative(string $root, string $file): string
    {
        if (strpos($file, $root) === 0) {
            return ltrim(substr($file, strlen($root)), DIRECTORY_SEPARATOR);
        }
        return $file;
    }
}
