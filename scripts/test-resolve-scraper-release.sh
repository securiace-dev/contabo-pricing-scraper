#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repository_root
readonly resolver="$repository_root/scripts/resolve-scraper-release.sh"
test_root="$(mktemp -d)"
readonly test_root
trap 'rm -rf -- "$test_root"' EXIT

mkdir -p "$test_root/bin"
cat >"$test_root/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1 $2" == "release list" ]]; then
  case "${FAKE_SCENARIO:-default}" in
    default)
      printf '%s\n' \
        contabo_pricing-v9.9.9 \
        v2.4.0 \
        securiacevps-v9.9.9 \
        v2.3.2
      ;;
    empty)
      printf '%s\n' contabo_pricing-v9.9.9 securiacevps-v9.9.9
      ;;
  esac
  exit 0
fi

if [[ "$1 $2" == "release view" ]]; then
  case "$3" in
    v2.4.0)
      printf '%s\n' contabo-scraper-linux-x86_64
      ;;
    v2.3.2)
      printf '%s\n' contabo-scraper-linux-x86_64 SHA256SUMS.txt
      ;;
    *)
      printf '%s\n' unrelated.zip
      ;;
  esac
  exit 0
fi

echo "Unexpected gh invocation: $*" >&2
exit 2
EOF
chmod 0700 "$test_root/bin/gh"

run_resolver() {
  PATH="$test_root/bin:$PATH" GH_REPO=example/repository "$resolver" "$@"
}

actual="$(run_resolver latest)"
test "$actual" = "v2.3.2"

actual="$(run_resolver v2.3.2)"
test "$actual" = "v2.3.2"

if run_resolver contabo_pricing-v9.9.9 >/dev/null 2>&1; then
  echo "Expected explicit non-scraper release to fail" >&2
  exit 1
fi

if FAKE_SCENARIO=empty run_resolver latest >/dev/null 2>&1; then
  echo "Expected missing scraper release to fail" >&2
  exit 1
fi

echo "Scraper release resolver contract: PASS"
