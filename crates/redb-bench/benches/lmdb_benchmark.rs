use std::env::current_dir;
use std::{fs, process};
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let rocksdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(CACHE_SIZE));
        bb.set_bloom_filter(10.0, false);

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);
        opts.increase_parallelism(
            std::thread::available_parallelism().map_or(1, |n| n.get()) as i32
        );

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table, tmpfile.path())
    };
}
