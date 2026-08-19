# Branch-page density prototype

## Decision

Checksum companion pages are promising for read-heavy, concurrent workloads. Variable-width key
offsets are not. On the required `redb_benchmark.rs` workload, checksum pages improved point lookup
by 21.11% and 4-32 thread lookup by 11.13-30.12%, while reducing branch-page count by 12.57%. The
tradeoff was 9.08% slower no-sync transactions, 21.79% slower small batches, and 20.31% longer
compaction. Bulk loading was neutral in the final run (+0.10%), but was 4.34% slower in the focused
benchmark.

Combining both layouts reduced branch-page count by 18.22%, but was worse than checksum pages alone:
point lookup improved by 19.00%, while bulk loading fell by 7.24%. The variable offset decode
cost and slightly larger fanout erased the benefit of its 1.45% branch-page reduction.

These are format-breaking prototypes, enabled only by build-time environment variables. A normal
build has the existing stable format, including under `--all-features`. No format-version bump or
migration path is included.

## Implemented layouts

Variable-width offsets derive the width from the allocated branch-page size, so no per-page width
field is needed:

- pages through 65,535 bytes use 2-byte little-endian offsets;
- pages through 16,777,215 bytes use 3-byte little-endian offsets;
- larger pages use 4-byte little-endian offsets.

Required-size calculation accounts for allocator order rounding before choosing the width. The hot
branch accessor caches section boundaries and offset width, and decodes adjacent key offsets in one
specialized 2-, 3-, or 4-byte path.

Checksum pages replace the inline array of 16-byte child checksums with an 8-byte `PageNumber` in
the branch header. That number points to a companion page containing a 4-byte header followed by
the checksum array. Normal lookup constructs only `BranchAccessor` and never resolves that pointer.
Write, verification, and repair paths explicitly request a `ChecksummedBranchAccessor`.

The prototype handles companion pages in copy-on-write cloning, allocation/free tracking, repair
page visits, checksum verification/finalization, table deletion, statistics, and regular and
multimap compaction relocation. Deferred child-checksum writes do not dirty the companion page;
commit finalization discovers dirty children through the allocator and writes each final checksum
once. Compaction preserves copy-on-write isolation by cloning every relocated branch companion. It
detects repeated or reversed relocation sets and stops before committing a branch/companion
oscillation; normal compaction still reaches the same 1.29 GiB result.

## Benchmark environment and method

Measurements were taken on 2026-08-19 from base revision `e433e521e27b` plus this working-tree
prototype, using `rustc 1.90.0` and the release benchmark profile.

- CPU: AMD EPYC-Milan, 4 cores / 8 logical CPUs
- Cache: 128 KiB L1d, 128 KiB L1i, 2 MiB L2, 32 MiB L3 (aggregate as reported by `lscpu`)
- Architecture and page size: x86-64, 4 KiB
- No new dependencies

`scripts/benchmark_branch_pages.sh` runs the repository's required
`crates/redb-bench/benches/redb_benchmark.rs` twice in counter-order:

1. baseline, variable offsets, checksum pages, combined;
2. combined, checksum pages, variable offsets, baseline.

Point and range rows are already medians of three samples inside `redb_benchmark.rs`. Rates below
are computed from the summed elapsed milliseconds across both process runs, not from rounded rates
printed by the benchmark. Positive rate deltas are faster than baseline. The compaction
parenthetical is an elapsed-time delta, so negative is faster.

### `redb_benchmark.rs`: lookup results

| Workload | Baseline | Variable offsets | Checksum pages | Combined |
| --- | ---: | ---: | ---: | ---: |
| Point lookup | 513,347 key/s | 531,350 (+3.51%) | 621,697 (+21.11%) | 610,874 (+19.00%) |
| Range lookup | 278,087 scan/s | 284,414 (+2.28%) | 299,850 (+7.83%) | 309,598 (+11.33%) |
| 4-thread lookup | 2,277,188 key/s | 2,160,654 (-5.12%) | 2,564,600 (+12.62%) | 2,517,595 (+10.56%) |
| 8-thread lookup | 3,585,799 key/s | 3,574,601 (-0.31%) | 4,665,761 (+30.12%) | 4,508,534 (+25.73%) |
| 16-thread lookup | 3,711,095 key/s | 3,838,301 (+3.43%) | 4,508,534 (+21.49%) | 4,448,187 (+19.86%) |
| 32-thread lookup | 4,044,759 key/s | 3,974,537 (-1.74%) | 4,494,764 (+11.13%) | 4,092,968 (+1.19%) |

The host has eight logical CPUs, so the 16- and 32-thread rows measure oversubscribed behavior. The
4- and 8-thread rows are the relevant scaling results for this machine.

### `redb_benchmark.rs`: write and maintenance results

| Workload | Baseline | Variable offsets | Checksum pages | Combined |
| --- | ---: | ---: | ---: | ---: |
| Bulk load | 301,623 key/s | 309,847 (+2.73%) | 301,932 (+0.10%) | 279,799 (-7.24%) |
| Individual writes | 3,295 txn/s | 3,170 (-3.80%) | 3,236 (-1.78%) | 3,155 (-4.26%) |
| 1,000-key batches | 26,929 key/s | 27,307 (+1.41%) | 21,062 (-21.79%) | 21,142 (-21.49%) |
| No-sync writes | 46,642 txn/s | 46,970 (+0.71%) | 42,409 (-9.08%) | 41,876 (-10.22%) |
| Removal | 198,988 key/s | 200,132 (+0.58%) | 183,525 (-7.77%) | 177,946 (-10.57%) |
| Retain | 1,002,921 key/s | 1,038,508 (+3.55%) | 999,806 (-0.31%) | 1,049,939 (+4.69%) |
| `extract_if` | 787,626 key/s | 827,083 (+5.01%) | 824,699 (+4.71%) | 850,012 (+7.92%) |
| Pop | 1,165,501 key/s | 1,187,648 (+1.90%) | 917,431 (-21.28%) | 866,551 (-25.65%) |
| Sorted insert | 2,111,932 key/s | 2,044,990 (-3.17%) | 2,053,388 (-2.77%) | 2,096,436 (-0.73%) |
| Compaction time | 5.640 s | 5.724 s (+1.50%) | 6.785 s (+20.31%) | 6.176 s (+9.50%) |
| Compacted size | 1.28 GiB | 1.28 GiB | 1.29 GiB | 1.29 GiB |

Individual-write results are more variable than bulk and batch results, but every sidecar workload
that repeatedly updates branch metadata shows the expected extra cost in the larger batch and
no-sync samples. The sorted-insert and `extract_if` improvements are small enough to treat as noise
rather than a design benefit. Compaction pays for full companion-page copy-on-write, which is
necessary to preserve the previous durable root if an I/O error interrupts the commit.

The final shared-host matrix had unusually slow closing baseline concurrency samples. A separate
full-COW matrix on the same host measured checksum-page gains of 5.72% for point lookup and 9.10% at
8 threads, while the CPU-pinned focused benchmark measured +6.28% for point lookup. The direction is
repeatable; the practical magnitude observed here is about 6-21% for point lookup and 9-30% at eight
threads, rather than a universal 21% or 30% claim.

## Focused branch-page benchmark

`scripts/benchmark_branch_pages_focused.sh` runs
`crates/redb-bench/benches/branch_page_benchmark.rs`, pinned to one logical CPU and in the same
counter-order. Each process bulk-loads two million random 24-byte keys with 150-byte values, performs
seven two-million-key random-read samples, and performs five 250,000-key no-durability update
samples. The table reports the combined rate from two process medians.

| Variant | Bulk load | Random lookup | Updates |
| --- | ---: | ---: | ---: |
| Baseline | 354,903 key/s | 688,440 key/s | 172,819 key/s |
| Variable offsets | 347,813 (-2.00%) | 694,860 (+0.93%) | 178,255 (+3.15%) |
| Checksum pages | 339,516 (-4.34%) | 731,681 (+6.28%) | 161,364 (-6.63%) |
| Combined | 317,664 (-10.49%) | 714,946 (+3.85%) | 165,021 (-4.51%) |

The focused benchmark confirms the sidecar direction. It also shows that the variable-offset lookup
effect is near the noise floor, while the broader benchmark finds a regression once range and
concurrent access are included.

### Density and space accounting

These statistics are deterministic for the focused benchmark's generated data. Metadata includes
the used bytes in checksum companion pages; fragmentation includes their allocator slack.

| Variant | Tree height | Branch pages | Metadata bytes | Fragmented bytes |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 4 | 2,140 | 23,329,444 | 168,527,452 |
| Variable offsets | 4 | 2,109 (-1.45%) | 23,069,132 (-1.12%) | 168,660,788 (+0.08%) |
| Checksum pages | 4 | 1,871 (-12.57%) | 23,335,804 (+0.03%) | 175,082,884 (+3.89%) |
| Combined | 4 | 1,750 (-18.22%) | 23,071,644 (-1.11%) | 174,355,812 (+3.46%) |

The companion layout moves bytes out of the lookup page, but does not reduce total metadata: each
companion allocation introduces a 4-byte header and buddy-allocation slack. Its benefit comes from
keeping the pages traversed by lookup denser, not from reducing the whole database footprint.

## Validation

- `just test` passes, including formatting, license checks, Clippy with all targets and features,
  the full unit and integration suite, and documentation tests.
- The combined prototype passes all 113 library tests with all features enabled.
- A 60-second combined-layout fuzz run completed 32,721 executions with no failure. Three minimized
  regression inputs found while developing companion-page copy-on-write and compaction also replay
  successfully against the final implementation.
- The default build and `--all-features` continue to use the stable branch-page format; no third-party
  dependency was added.

## Reproduction

On this branch, the standard benchmark automatically enables both prototypes:

```sh
just bench
```

Run the broad required benchmark matrix:

```sh
scripts/benchmark_branch_pages.sh
```

Run the focused, CPU-pinned benchmark matrix (CPU 0 by default):

```sh
scripts/benchmark_branch_pages_focused.sh
```

Set `REDB_BENCHMARK_CPU` to choose another CPU. Both scripts accept an output directory as their
first argument. A single prototype can also be selected for any Cargo command by setting one or
both of:

```sh
REDB_BRANCH_VARIABLE_WIDTH_OFFSETS=1
REDB_BRANCH_CHECKSUM_PAGES=1
```

Databases created with a prototype switch must be opened with the same switch. They are not
compatible with the stable layout.
