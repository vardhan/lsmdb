# lsmdb
[![lsmdb](https://github.com/adamlesinski/lsmdb/actions/workflows/rust.yml/badge.svg)](https://github.com/adamlesinski/lsmdb/actions/workflows/rust.yml) [![Coverage Status](https://coveralls.io/repos/github/vardhan/lsmdb/badge.svg?branch=main&kill_cache=1)](https://coveralls.io/github/vardhan/lsmdb?branch=main)

A toy log structured merge tree database.

The design is based off
[LevelDB](https://github.com/google/leveldb/blob/main/doc/impl.md) and [RocksDB](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/).

### API

See db.rs for the public interface.  Roughly:

- `DB::open(root_dir)` to open a new (or reopen an existing) database, persisting the data under `root_dir`.
- `DB::put(key, value)` to write a `key` and associated `value` to the database.
- `DB::get(key)` to get the value associated with the `key`
- `DB::scan(key_prefix)` to get an iterator which returns all keys which begin
   with `key_prefix`, in sorted key order.
- `DB::delete(key)` to delete the `key` and its value.

### TODO

In **rough** order:

- Performance
  * mmap sstable files instead of doing file I/O
- Concurrency
  * SSTableReader can have multiple reads and scans in-flight
  * Do compaction in the background
- Transactions
  * MVCC?
- Benchmarks
  * Benchmarks for consecutive read-heavy, write-heavy,
    mixed, and with some temporal workloads.
- Crash Recovery (WAL)