#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_dir="${1:-${repo_root}/target/branch-page-focused-benchmark}"
cpu="${REDB_BENCHMARK_CPU:-0}"
mkdir -p "${result_dir}"

run_variant() {
    local name="$1"
    local round="$2"
    shift 2
    echo "Running ${name}, round ${round}"
    taskset -c "${cpu}" env \
        -u REDB_BRANCH_VARIABLE_WIDTH_OFFSETS \
        -u REDB_BRANCH_CHECKSUM_PAGES \
        "$@" \
        cargo bench --frozen -p redb-bench --bench branch_page_benchmark \
        2>&1 | tee "${result_dir}/${name}-${round}.log"
}

cd "${repo_root}"
run_variant baseline 1
run_variant variable-offsets 1 REDB_BRANCH_VARIABLE_WIDTH_OFFSETS=1
run_variant checksum-pages 1 REDB_BRANCH_CHECKSUM_PAGES=1
run_variant combined 1 \
    REDB_BRANCH_VARIABLE_WIDTH_OFFSETS=1 \
    REDB_BRANCH_CHECKSUM_PAGES=1
run_variant combined 2 \
    REDB_BRANCH_VARIABLE_WIDTH_OFFSETS=1 \
    REDB_BRANCH_CHECKSUM_PAGES=1
run_variant checksum-pages 2 REDB_BRANCH_CHECKSUM_PAGES=1
run_variant variable-offsets 2 REDB_BRANCH_VARIABLE_WIDTH_OFFSETS=1
run_variant baseline 2
