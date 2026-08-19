use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use std::hint::black_box;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("branch-density");
const ELEMENTS: usize = 2_000_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const READS: usize = 2_000_000;
const READ_RUNS: usize = 7;
const UPDATES: usize = 250_000;
const UPDATE_RUNS: usize = 5;

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn variant() -> &'static str {
    match std::env::var_os("REDB_BRANCH_CHECKSUM_PAGES").is_some() {
        false => "baseline",
        true => "checksum-pages",
    }
}

fn rate(operations: usize, duration: Duration) -> f64 {
    operations as f64 / duration.as_secs_f64()
}

fn main() {
    let mut rng = fastrand::Rng::with_seed(3);
    let mut keys = Vec::with_capacity(ELEMENTS);
    for _ in 0..ELEMENTS {
        let mut key = [0; KEY_SIZE];
        rng.fill(&mut key);
        keys.push(key);
    }
    let mut read_indexes = Vec::with_capacity(READS);
    for _ in 0..READS {
        read_indexes.push(rng.usize(0..ELEMENTS));
    }
    let mut update_indexes = Vec::with_capacity(UPDATES);
    for _ in 0..UPDATES {
        update_indexes.push(rng.usize(0..ELEMENTS));
    }
    let value = [7; VALUE_SIZE];

    let file = NamedTempFile::new().unwrap();
    let db = Database::builder()
        .set_cache_size(1024 * 1024 * 1024)
        .create(file.path())
        .unwrap();

    let start = Instant::now();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(TABLE).unwrap();
        for key in &keys {
            table.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    let bulk = start.elapsed();

    let mut read_samples = [Duration::ZERO; READ_RUNS];
    let mut expected = 0u64;
    for sample in &mut read_samples {
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        let start = Instant::now();
        let mut checksum = 0u64;
        for &index in &read_indexes {
            let result = table.get(keys[index].as_slice()).unwrap().unwrap();
            checksum += u64::from(result.value()[0]);
        }
        *sample = start.elapsed();
        expected = black_box(checksum);
    }
    assert_eq!(expected, (READS * usize::from(value[0])) as u64);
    let reads = median(&mut read_samples);

    let mut update_samples = [Duration::ZERO; UPDATE_RUNS];
    for (run, sample) in update_samples.iter_mut().enumerate() {
        let mut write = db.begin_write().unwrap();
        write.set_durability(Durability::None).unwrap();
        let start = Instant::now();
        {
            let mut table = write.open_table(TABLE).unwrap();
            let mut replacement = value;
            replacement[0] = u8::try_from(run + 8).unwrap();
            for &index in &update_indexes {
                table
                    .insert(keys[index].as_slice(), replacement.as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        *sample = start.elapsed();
    }
    let updates = median(&mut update_samples);

    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    let stats = table.stats().unwrap();
    println!("variant={}", variant());
    println!("bulk_ns={}", bulk.as_nanos());
    println!("bulk_keys_per_second={:.3}", rate(ELEMENTS, bulk));
    println!("read_median_ns={}", reads.as_nanos());
    println!("read_keys_per_second={:.3}", rate(READS, reads));
    println!("update_median_ns={}", updates.as_nanos());
    println!("update_keys_per_second={:.3}", rate(UPDATES, updates));
    println!("tree_height={}", stats.tree_height());
    println!("branch_pages={}", stats.branch_pages());
    println!("leaf_pages={}", stats.leaf_pages());
    println!("metadata_bytes={}", stats.metadata_bytes());
    println!("fragmented_bytes={}", stats.fragmented_bytes());
    println!("file_bytes={}", file.as_file().metadata().unwrap().len());
}
