#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repository_root
readonly validator="$repository_root/scripts/check-workflow-trust.rb"
test_root="$(mktemp -d)"
readonly test_root
trap 'rm -rf -- "$test_root"' EXIT

new_case() {
  local name="$1"
  local root="$test_root/$name"
  mkdir -p "$root/.github/workflows"
  printf '%s\n' "$root"
}

expect_pass() {
  local root="$1"
  ruby "$validator" "$root" >/dev/null
}

expect_fail() {
  local root="$1"
  if ruby "$validator" "$root" >/dev/null 2>&1; then
    echo "Expected workflow trust validation to fail for $root" >&2
    exit 1
  fi
}

root="$(new_case dockerless-block)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on:
  pull_request:
jobs:
  test:
    runs-on: contabo-ci-pr
    steps:
      - run: "true"
EOF
expect_pass "$root"

root="$(new_case trusted-release)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on:
  push:
    branches: [main]
jobs:
  test:
    runs-on: contabo-ci-release
    steps:
      - run: "true"
EOF
expect_pass "$root"

root="$(new_case direct-block)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on:
  pull_request:
jobs:
  unsafe:
    runs-on: contabo-ci-release
    steps:
      - run: "true"
EOF
expect_fail "$root"

root="$(new_case inline-trigger)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on: [pull_request]
jobs:
  unsafe:
    runs-on: contabo-ci-release
    steps:
      - run: "true"
EOF
expect_fail "$root"

root="$(new_case scalar-target-trigger)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on: pull_request_target
jobs:
  unsafe:
    runs-on: [self-hosted, contabo-ci-release]
    steps:
      - run: "true"
EOF
expect_fail "$root"

root="$(new_case matrix-runner)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on: pull_request
jobs:
  unsafe:
    strategy:
      matrix:
        runner: [contabo-ci-release]
    runs-on: ${{ matrix.runner }}
    steps:
      - run: "true"
EOF
expect_fail "$root"

root="$(new_case local-reusable)"
cat >"$root/.github/workflows/caller.yml" <<'EOF'
on: pull_request
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
EOF
cat >"$root/.github/workflows/reusable.yml" <<'EOF'
on: workflow_call
jobs:
  unsafe:
    runs-on: contabo-ci-release
    steps:
      - run: "true"
EOF
expect_fail "$root"

root="$(new_case external-reusable)"
cat >"$root/.github/workflows/check.yml" <<'EOF'
on: pull_request
jobs:
  call:
    uses: example/repository/.github/workflows/check.yml@0123456789abcdef
EOF
expect_fail "$root"

ruby "$validator" "$repository_root"
echo "Workflow trust regression fixtures: PASS"
