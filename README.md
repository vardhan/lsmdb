# lsmdb
[![lsmdb](https://github.com/adamlesinski/lsmdb/actions/workflows/rust.yml/badge.svg)](https://github.com/adamlesinski/lsmdb/actions/workflows/rust.yml) [![Coverage Status](https://coveralls.io/repos/github/vardhan/lsmdb/badge.svg?branch=main)](https://coveralls.io/github/vardhan/lsmdb?branch=main)

A toy log structured merge tree database.

The design is based off
[LevelDB](https://github.com/google/leveldb/blob/main/doc/impl.md) and [RocksDB](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/).

# TODO:
- Compaction
  * DB::get() and DB::seek() across level 0 and 1
  * compact level N to N+1. sstables in level >=1 do not have overlapping key ranges.
- Performance
  * mmap sstable files, instead of doing file I/O.
- Crash Recovery (WAL)
- Benchmarks
  * Benchmarks for consecutive read-heavy, write-heavy,
    mixed, and with some temporal workloads.
- Concurrency
  * SSTableReader can have multiple reads and scans in-flight. 
  * do compaction in the background
- Transactions
  * MVCC or reader-writer?
