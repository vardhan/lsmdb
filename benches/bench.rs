use std::path::PathBuf;
use std::time::Instant;
use std::{fs, io};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lsmdb::db::{self, DB};
use lsmdb::db_config::DBConfig;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use tempdir::{self, TempDir};

fn gen_bytes(num_bytes: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(Uniform::from(0..250))
        .take(num_bytes)
        .collect::<Vec<u8>>()
}

fn dir_size(path: impl Into<PathBuf>) -> io::Result<u64> {
    fn dir_size(mut dir: fs::ReadDir) -> io::Result<u64> {
        dir.try_fold(0, |acc, file| {
            let file = file?;
            let size = match file.metadata()? {
                data if data.is_dir() => dir_size(fs::read_dir(file.path())?)?,
                data => data.len(),
            };
            Ok(acc + size)
        })
    }

    dir_size(fs::read_dir(path.into())?)
}

fn create_db() -> (TempDir, DB) {
    let tempdir = tempdir::TempDir::new("lsmdb").unwrap();
    let db = db::DB::open_with_config(
        tempdir.path(),
        DBConfig {
            max_active_memtable_size: 1024 * 100, // 100KB
            max_frozen_memtables: 2,              // 200KB before flushing to level 0,
            level_base_max_size: 1024 * 1024,     // 1MB level 0. level 10 = 1 MB.
            ..Default::default()
        },
    )
    .unwrap();

    (tempdir, db)
}

fn gen_key() -> String {
    format!("/key/{:011}", thread_rng().gen_range(0..99_999_999_999u64))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut g = c.benchmark_group("db");
    g.sample_size(1_000);

    fillseq(&mut g);
    fillrandom(&mut g);
    readseq(&mut g);
    readrandom(&mut g);
}

fn fillseq(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let (tempdir, mut db) = create_db();

    let mut i: u64 = 0; // this should be outside of `bench_function`, so that its shared between each batch.
    g.bench_function("fillseq", |b| {
        b.iter_batched(
            || {
                i += 1;
                // make the key 16 bytes
                (format!("/key/{:011}", i), gen_bytes(100))
            },
            |(key, val)| db.put(key, val).unwrap(),
            criterion::BatchSize::SmallInput,
        );
    });
    let db_size = dir_size(tempdir.path()).unwrap();
    println!("db size: {} mb", db_size / (1024 * 1024));
}

fn fillrandom(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let (tempdir, mut db) = create_db();

    g.bench_function("fillrandom", |b| {
        b.iter_batched(
            || (gen_key(), gen_bytes(100)),
            |(key, val)| db.put(key, val).unwrap(),
            criterion::BatchSize::SmallInput,
        );
    });

    let db_size = dir_size(tempdir.path()).unwrap();
    println!("db size: {} mb", db_size / (1024 * 1024));
}

fn readseq(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let (tempdir, mut db) = create_db();

    for _ in 0..400_000 {
        db.put(gen_key(), gen_bytes(100)).unwrap();
    }

    let db_size = dir_size(tempdir.path()).unwrap();
    println!("db size: {} mb", db_size / (1024 * 1024));

    g.bench_function("readseq", |b| {
        b.iter_custom(|num_iterations| {
            let start = Instant::now();
            let mut iter = db.scan("").unwrap();
            for _i in 0..num_iterations {
                match iter.next() {
                    Some((_key, _value)) => {
                        1;
                    }
                    None => {
                        std::mem::drop(iter);
                        iter = db.scan("").unwrap();
                    }
                };
                black_box(());
            }
            start.elapsed()
        });
    });
}

fn readrandom(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let (tempdir, mut db) = create_db();

    let mut keys = vec![];
    // 1 entry = 116 bytes.
    // 10,000 entries = 1.16 MB
    // 200,000 entries ~= 20.32 MB
    for _ in 0..400_000 {
        let key = gen_key();
        keys.push(key.clone());
        db.put(key, gen_bytes(100)).unwrap();
    }

    let db_size = dir_size(tempdir.path()).unwrap();
    println!("db size: {} mb", db_size / (1024 * 1024));

    let mut rng: rand::prelude::ThreadRng = thread_rng();
    g.bench_function("readrandom", |b| {
        // let (tempdir, mut db) = create_db();
        b.iter_custom(|num_iters| {
            let start = Instant::now();
            for _ in 0..num_iters {
                black_box(db.get(&keys[rng.gen_range(0..keys.len())]).unwrap());
            }
            start.elapsed()
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
