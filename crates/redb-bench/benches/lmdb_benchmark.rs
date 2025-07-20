use std::env::current_dir;
use std::{fs, process};
use tempfile::TempDir;

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

    let _rocksdb_results = {
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

        opts.set_write_buffer_size(16 * 1024 * 1024); // 16 MiB (further reduced)
        opts.set_max_write_buffer_number(2); // Limit concurrent memtables
        opts.set_target_file_size_base(16 * 1024 * 1024); // 16 MiB
        opts.set_max_bytes_for_level_base(64 * 1024 * 1024); // 64 MiB
        opts.set_db_write_buffer_size(32 * 1024 * 1024); // Global write buffer limit (reduced)
        opts.set_max_total_wal_size(16 * 1024 * 1024); // Limit WAL memory usage (reduced)
        opts.set_arena_block_size(2 * 1024 * 1024); // 2 MiB arena blocks (reduced)
        opts.set_allow_mmap_reads(true); // Use memory mapping for reads
        opts.set_allow_mmap_writes(false); // Disable mmap writes to save memory
        opts.set_max_open_files(100); // Limit open file descriptors
        opts.set_keep_log_file_num(2); // Limit number of log files

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table, tmpfile.path())
    };
}
