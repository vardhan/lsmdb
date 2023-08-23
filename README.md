[![lsmdb](https://github.com/adamlesinski/lsmdb/actions/workflows/rust.yml/badge.svg)](https://github.com/adamlesinski/lsmdb/actions/workflows/rust.yml) [![codecov](https://codecov.io/github/vardhan/lsmdb/graph/badge.svg?token=Dtb9glS5dO)](https://codecov.io/github/vardhan/lsmdb)

# lsmdb
A toy log structured merge tree database.

The design is based off:
- [LevelDB](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/)

# TODO:
- Compaction
  * compact level 0 to level 1. sstables in level 0 is have overlapping key ranges.
  * DB::get() and DB::seek() across level 0 and 1
  * compact level N to N+1. sstables in level >=1 do not have overlapping key ranges.
- Crash Recovery (WAL)
- Benchmarks
  * Benchmarks for consecutive read-heavy, write-heavy,
    mixed, and with some temporal workloads.
- Concurrency
  * do compaction in the background
  * read-writer locks? or multi-version?
