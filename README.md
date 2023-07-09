# lsmdb
A toy log structured merge tree database.

The design is based off:
- [LevelDB](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/)
- [Original LSM-Tree paper](http://paperhub.s3.amazonaws.com/18e91eb4db2114a06ea614f0384f2784.pdf)

# TODO:
- DB::seek() seeks across all memtables and sstables
- log files for crash recovery
- MANIFEST files for tracking sstable files and their key ranges
- compact level 0 to level 1 (level 0 is special, because it has overlapping keys
  in its sstables), but level 1 does not
- DB::get() and DB::seek() across level 0 and 1
- compact level N to N+1
- do compaction in the background
