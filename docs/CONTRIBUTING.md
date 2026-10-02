# Contributing — branches, checkouts, PRs

## Local layout (two checkouts, one git dir)

| Path | Purpose | Rule |
|---|---|---|
| `~/Projects/contabo-pricing-scraper` | main checkout, always on `main` | never commit here; `git pull --ff-only` only |
| `~/Projects/contabo-pricing-scraper-whmcs-native` | standing feature worktree | one feature branch at a time, created off `main` |

Create a feature branch in the standing worktree: `git -C ~/Projects/contabo-pricing-scraper-whmcs-native switch -c feat/<scope> main`.
Do not add further long-lived worktrees; a temporary one for a hotfix is fine if it is removed (`git worktree remove`) when its PR merges.
The WHMCS devbox project `contabo-pricing` mounts the standing worktree, so whatever branch it has checked out is what the devbox runs.

## One PR per feature

- Open the PR early as a draft; keep it rebased on `main` (`git rebase origin/main`, then push with a lease: verify `origin/<branch>` still equals the tip you rebased from, then `push origin +<branch>`).
- Before marking ready: `vendor/bin/phpunit` in the addon, `php -l` under PHP 7.4 and 8.2 containers, `scripts/local-whmcs.sh integration` (devbox smoke).
- Merge with a merge commit (keeps the feature's history), delete the remote branch, fast-forward `main` in the main checkout.
- Never leave uncommitted work in a worktree overnight: commit it as `wip(...)` on the feature branch and push.

## Supersession

When a PR is replaced by another, close it with a comment naming the commits that carry its work (`harvested from PR #N`), and delete its branch after the replacement merges. The `docs/round2-2026-10-01/harvest-2026-10-02.md` table records what was ported, dropped and why.

## Automation on `main`

No workflow commits to `main` any more (the scrape cron was removed with the Rust scraper). Catalog data lives in the addon's database, not in the repo.
