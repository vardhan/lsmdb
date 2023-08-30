struct Compactor {
    level: u32,
    sstables: Vec<SSTableLazyReader>,
}

impl Compactor {
    fn compact() {}
}
