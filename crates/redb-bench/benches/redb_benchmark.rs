use std::{fs, process};
use tempfile::NamedTempFile;

mod benchmark_dir;
use benchmark_dir::benchmark_dir;

#[expect(dead_code)]
mod common;
use common::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = benchmark_dir().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
    let mut db = redb::Database::builder()
        .set_cache_size(CACHE_SIZE)
        .create(tmpfile.path())
        .unwrap();
    let table = RedbBenchDatabase::new(&mut db);
    let results = benchmark(table, tmpfile.path());

    let offsets = std::env::var_os("REDB_BRANCH_VARIABLE_WIDTH_OFFSETS").is_some();
    let checksums = std::env::var_os("REDB_BRANCH_CHECKSUM_PAGES").is_some();
    let variant = match (offsets, checksums) {
        (false, false) => "baseline",
        (true, false) => "variable offsets",
        (false, true) => "checksum pages",
        (true, true) => "combined",
    };
    print_results_table(&[(variant, results)]);

    fs::remove_dir_all(&tmpdir).unwrap();
}
