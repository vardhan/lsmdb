# lsmdb
A toy log structured merge tree database.

The design is based off of [LevelDB](https://github.com/google/leveldb/blob/main/doc/impl.md) and [RocksDB](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/):

# TODO:
- get() looks up memtables *and* sstables
- seek() as well ^
- MANIFEST files for tracking sstable files and their key ranges
- logs for crash recovery
- compact level 0 to level 1 (level 0 is special, because it has overlapping keys
  in its sstables), but level 1 does not
- get() and seek() across level 0 and 1
- compact level N to N+1
- do compaction in the background
