# Self-hosted runner operations contract

This repository has a Linux-only build and release contract. GitHub Actions
jobs are routed to repository-scoped runners on the disposable
`securiace-zoss` staging host.

## Routing and trust

| Label | Unix identity | Docker | Allowed workload |
|---|---|---:|---|
| `contabo-ci-pr` | `contabo-ci-pr` | No | Same-repository pull-request and read-only contract validation |
| `contabo-ci-release` | `contabo-ci-release` | Yes | Main-branch packaging, tagged releases, GHCR publication, and scheduled scraping |

The runners were registered with `--no-default-labels`. Generic
`self-hosted`, `Linux`, or `X64` jobs cannot claim them.

Pull-request jobs contain an explicit same-repository guard. In addition,
`fork-policy.yml` runs from the trusted base branch through
`pull_request_target` and deliberately fails for an external fork without
checking out, sourcing, or executing its head. External fork code must not run
on either self-hosted identity. Same-repository branches are a
trusted-maintainer boundary because a maintainer who can edit workflow YAML can
request either repository runner label.

The two identities have separate homes, work roots, credentials, systemd
private temporary directories, and file permissions. The PR identity is not a
member of the `docker` group. The release identity has Docker access because it
publishes multi-architecture Linux images.

These are persistent runners on one host, not ephemeral virtual machines.
GitHub recommends ephemeral runners for untrusted workloads. If contributors
outside the trusted-maintainer boundary need CI, keep their jobs off this pool
until one-job ephemeral runners and external log retention are available.

## Installed contract

- Runner: `actions/runner` 2.336.0, Linux x64
- Runner archive SHA-256:
  `04cf0be1aff4c3ec3554466c39124ca250e3effd8873bb7e8d68535aa9505d5d`
- PHP: `/usr/bin/php7.4` and `/usr/bin/php8.2`
- Composer: `/usr/local/bin/composer`
- Go and ShellCheck: distro-managed
- Node and Rust: exact workflow versions installed into the owning runner's
  tool cache by commit-pinned actions
- Docker: available only to `contabo-ci-release`

Workflow jobs must not use `sudo`, mutate apt repositories, or install system
packages. Third-party actions must use full commit SHAs. Release permissions
belong only to the publish job that needs them.

## Release contract

Supported executable builds are Linux only:

- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- multi-architecture `linux/amd64,linux/arm64` OCI images

`release-validation.yml` exercises these paths without publishing. It also
validates the current scraper asset checksum and both WHMCS package streams.
Tagged releases publish immutable `:vX.Y.Z` and `:X.Y.Z` image aliases first,
smoke-test the immutable digest, promote `:latest` only after success, and create
the GitHub Release only after the Docker and binary jobs pass.

The optional CloakBrowser binary is downloaded by the runtime user on first use
and is not embedded in the image. Persist `/home/contabo/.cloakbrowser` only when
that cache is required.

## Services and paths

```text
actions.runner.securiace-dev-contabo-pricing-scraper.contabo-pr-linux-x64.service
actions.runner.securiace-dev-contabo-pricing-scraper.contabo-release-linux-x64.service

/opt/contabo-runner-pr
/opt/contabo-runner-release
/var/lib/contabo-ci-pr
/var/lib/contabo-ci-release
```

Runner program files are root-owned. Registration files are mode `0600`, and
each `_work` directory and home is mode `0700`.

## Verification

On GitHub, confirm both runners are online, idle when no job is active, and
have exactly one custom label each. On the host:

```bash
systemctl is-active \
  actions.runner.securiace-dev-contabo-pricing-scraper.contabo-pr-linux-x64.service
systemctl is-active \
  actions.runner.securiace-dev-contabo-pricing-scraper.contabo-release-linux-x64.service

id -nG contabo-ci-pr
id -nG contabo-ci-release
```

The first identity must not list `docker`; the second must list it.

## Incident handling

If a job is routed to the wrong identity:

1. Cancel it before secret-bearing or write steps.
2. Stop the affected runner service.
3. Purge that runner's exact `_work` directory and recreate it mode `0700`.
4. Remove and re-register only that repository runner.
5. Verify the custom labels before restarting workflows.

Do not copy runner credential files, log their contents, or reuse a compromised
work directory.

## Rollback

Reverting workflow routing is a repository change and must use a reviewed PR.
Runner removal is separate: stop and uninstall the exact service, delete its
repository runner registration, then remove only its dedicated directory and
Unix identity. Existing organization-level runners on the host are outside this
repository contract and must not be stopped as part of this rollback.
