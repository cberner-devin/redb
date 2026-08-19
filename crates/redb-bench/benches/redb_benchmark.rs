use redb::{Durability, TableDefinition};
use std::time::Instant;
use std::{fs, process};
use tempfile::NamedTempFile;

mod benchmark_dir;
use benchmark_dir::benchmark_dir;

#[expect(dead_code)]
mod common;
use common::*;

const MUTATION_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("mutation");
const MUTATION_ENTRIES: usize = 10_000;
const MUTATION_ITERATIONS: usize = 500_000;
const MUTATION_VALUE_SIZE: usize = 128;

fn mutation_benchmark(tmpdir: &std::path::Path) -> (String, ResultType) {
    let tmpfile = NamedTempFile::new_in(tmpdir).unwrap();
    let db = redb::Database::builder()
        .set_cache_size(CACHE_SIZE)
        .create(tmpfile.path())
        .unwrap();
    let value = [42u8; MUTATION_VALUE_SIZE];
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(MUTATION_TABLE).unwrap();
        for i in 0..MUTATION_ENTRIES {
            table
                .insert((i as u64).to_be_bytes().as_slice(), value.as_slice())
                .unwrap();
        }
    }
    write_txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.set_durability(Durability::None).unwrap();
    let mut table = txn.open_table(MUTATION_TABLE).unwrap();
    let start = Instant::now();
    for i in 0..MUTATION_ITERATIONS {
        let key = ((i % MUTATION_ENTRIES) as u64).to_be_bytes();
        table.remove(key.as_slice()).unwrap().unwrap();
        table.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    let duration = start.elapsed();
    drop(table);
    txn.abort().unwrap();
    (
        "full-leaf remove + insert".to_string(),
        ResultType::keys(2 * MUTATION_ITERATIONS, duration),
    )
}

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
    let common_results = benchmark(table, tmpfile.path());
    let mut results = vec![mutation_benchmark(&tmpdir)];
    results.extend(common_results);

    print_results_table(&[("redb", results)]);

    fs::remove_dir_all(&tmpdir).unwrap();
}
