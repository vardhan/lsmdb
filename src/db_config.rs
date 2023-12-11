/// The configuration used when opening a database with [DB::open_with_config].
pub struct DBConfig {
    /// Size threshold for a memtable (counts key & value sizes)
    pub max_active_memtable_size: usize,
    /// Max number of frozen memtables before they are force-flushed to sstable
    pub max_frozen_memtables: usize,
    /// Size threshold for a block (counts all bytes in a block)
    pub block_max_size: usize,
    /// The size (total bytes) level N can hold is [level_base_max_size_bytes]*10^(N)
    /// Once level N gets to this size, level N is compacted into level N+1.
    pub level_base_max_size: u64,
    /// The max # of sstable files for level N is [level_base_max_shards]*2^(N)
    /// This is used to compute the size of each sstable shard during compaction.
    pub level_base_max_sstables: u64,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            max_active_memtable_size: 1024 * 1024, // 1 MB
            max_frozen_memtables: 1,
            block_max_size: 1024 * 4,
            // L0 = 10MB, L1 = 100mb, L2 = 1GB, L3 = 10GB ..
            level_base_max_size: 1024 * 1024 * 10,
            // L1 = 4 shards, L2 = 8, L3 = 16, ..
            level_base_max_sstables: 2,
        }
    }
}

impl DBConfig {
    pub fn level_max_size(&self, level: u32) -> usize {
        (self.level_base_max_size * 10u64.pow(level)) as usize
    }

    pub fn level_max_sstables(&self, level: u32) -> usize {
        (self.level_base_max_sstables * 2_u64.pow(level)) as usize
    }
}
