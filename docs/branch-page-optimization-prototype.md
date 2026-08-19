# Branch checksum-page prototype

## Decision

Checksum companion pages are promising for read-heavy, concurrent workloads. On the required
`redb_benchmark.rs` workload, they improved point lookup by 21.11% and 4-32 thread lookup by
11.13-30.12%, while reducing branch-page count by 12.57%. The tradeoff was 9.08% slower no-sync
transactions, 21.79% slower small batches, and 20.31% longer compaction. Bulk loading was neutral in
the final broad run (+0.10%), but was 4.34% slower in the focused benchmark.

This is a format-breaking prototype enabled only by a build-time environment variable. A normal
build has the existing stable format, including under `--all-features`. No format-version bump or
migration path is included.

## Implemented layout

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

Measurements were taken on 2026-08-19 from base revision `e433e521e27b` plus this prototype, using
`rustc 1.90.0` and the release benchmark profile.

- CPU: AMD EPYC-Milan, 4 cores / 8 logical CPUs
- Cache: 128 KiB L1d, 128 KiB L1i, 2 MiB L2, 32 MiB L3 (aggregate as reported by `lscpu`)
- Architecture and page size: x86-64, 4 KiB
- No new dependencies

`scripts/benchmark_branch_pages.sh` runs the repository's required
`crates/redb-bench/benches/redb_benchmark.rs` twice in counter-order: baseline then checksum pages,
followed by checksum pages then baseline. Point and range rows are already medians of three samples
inside `redb_benchmark.rs`. Rates below are computed from the summed elapsed milliseconds across
both process runs, not from rounded rates printed by the benchmark. Positive rate deltas are faster
than baseline. The compaction parenthetical is an elapsed-time delta, so positive is slower.

### `redb_benchmark.rs`: lookup results

| Workload | Baseline | Checksum pages |
| --- | ---: | ---: |
| Point lookup | 513,347 key/s | 621,697 (+21.11%) |
| Range lookup | 278,087 scan/s | 299,850 (+7.83%) |
| 4-thread lookup | 2,277,188 key/s | 2,564,600 (+12.62%) |
| 8-thread lookup | 3,585,799 key/s | 4,665,761 (+30.12%) |
| 16-thread lookup | 3,711,095 key/s | 4,508,534 (+21.49%) |
| 32-thread lookup | 4,044,759 key/s | 4,494,764 (+11.13%) |

The host has eight logical CPUs, so the 16- and 32-thread rows measure oversubscribed behavior. The
4- and 8-thread rows are the relevant scaling results for this machine.

### `redb_benchmark.rs`: write and maintenance results

| Workload | Baseline | Checksum pages |
| --- | ---: | ---: |
| Bulk load | 301,623 key/s | 301,932 (+0.10%) |
| Individual writes | 3,295 txn/s | 3,236 (-1.78%) |
| 1,000-key batches | 26,929 key/s | 21,062 (-21.79%) |
| No-sync writes | 46,642 txn/s | 42,409 (-9.08%) |
| Removal | 198,988 key/s | 183,525 (-7.77%) |
| Retain | 1,002,921 key/s | 999,806 (-0.31%) |
| `extract_if` | 787,626 key/s | 824,699 (+4.71%) |
| Pop | 1,165,501 key/s | 917,431 (-21.28%) |
| Sorted insert | 2,111,932 key/s | 2,053,388 (-2.77%) |
| Compaction time | 5.640 s | 6.785 s (+20.31%) |
| Compacted size | 1.28 GiB | 1.29 GiB |

Individual-write results are more variable than bulk and batch results, but workloads that
repeatedly update branch metadata show the expected extra cost in the larger batch and no-sync
samples. The sorted-insert and `extract_if` changes are small enough to treat as noise rather than a
design benefit. Compaction pays for full companion-page copy-on-write, which is necessary to
preserve the previous durable root if an I/O error interrupts the commit.

The final shared-host matrix had unusually slow closing baseline concurrency samples. A separate
full-copy-on-write matrix on the same host measured checksum-page gains of 5.72% for point lookup
and 9.10% at eight threads, while the CPU-pinned focused benchmark measured +6.28% for point lookup.
The direction is repeatable; the practical magnitude observed here is about 6-21% for point lookup
and 9-30% at eight threads, rather than a universal 21% or 30% claim.

## Focused branch-page benchmark

`scripts/benchmark_branch_pages_focused.sh` runs
`crates/redb-bench/benches/branch_page_benchmark.rs`, pinned to one logical CPU and in the same
counter-order. Each process bulk-loads two million random 24-byte keys with 150-byte values,
performs seven two-million-key random-read samples, and performs five 250,000-key no-durability
update samples. The table reports the combined rate from two process medians.

| Variant | Bulk load | Random lookup | Updates |
| --- | ---: | ---: | ---: |
| Baseline | 354,903 key/s | 688,440 key/s | 172,819 key/s |
| Checksum pages | 339,516 (-4.34%) | 731,681 (+6.28%) | 161,364 (-6.63%) |

### Density and space accounting

These statistics are deterministic for the focused benchmark's generated data. Metadata includes
the used bytes in checksum companion pages; fragmentation includes their allocator slack.

| Variant | Tree height | Branch pages | Metadata bytes | Fragmented bytes |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 4 | 2,140 | 23,329,444 | 168,527,452 |
| Checksum pages | 4 | 1,871 (-12.57%) | 23,335,804 (+0.03%) | 175,082,884 (+3.89%) |

The companion layout moves bytes out of the lookup page, but does not reduce total metadata: each
companion allocation introduces a 4-byte header and buddy-allocation slack. Its benefit comes from
keeping the pages traversed by lookup denser, not from reducing the whole database footprint.

## Validation

- `just test` passes, including formatting, license checks, Clippy with all targets and features,
  the full unit and integration suite, and documentation tests.
- The prototype passes the library suite with all features enabled.
- A 60-second fuzz run covering the checksum-page layout completed 32,721 executions with no
  failure. Minimized regression inputs found while developing companion-page copy-on-write and
  compaction replay successfully against the final implementation.
- The default build and `--all-features` continue to use the stable branch-page format; no
  third-party dependency was added.

## Reproduction

On this branch, the standard benchmark automatically enables checksum companion pages:

```sh
just bench
```

Run the broad required benchmark comparison:

```sh
scripts/benchmark_branch_pages.sh
```

Run the focused, CPU-pinned comparison (CPU 0 by default):

```sh
scripts/benchmark_branch_pages_focused.sh
```

Set `REDB_BENCHMARK_CPU` to choose another CPU. Both scripts accept an output directory as their
first argument. Enable the prototype for any Cargo command with:

```sh
REDB_BRANCH_CHECKSUM_PAGES=1
```

Databases created with the prototype switch must be opened with the same switch. They are not
compatible with the stable layout.
