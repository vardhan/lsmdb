use crate::db_iter::{DBIterator, DBIteratorItem, DBIteratorItemPeekable};

mod compaction;
mod manifest;
mod reader_ext;
pub(crate) mod sstable;

use sstable::SSTableError;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, VecDeque},
    path::{Path, PathBuf},
};
use thiserror::Error;

use self::{
    compaction::Compactor,
    manifest::{KeyRange, Manifest},
    sstable::{SSTableReader, SSTableWriter},
};

#[derive(Error, Debug)]
pub enum DBError {
    #[error("SSTableError: path={sstable_path}, err={err}")]
    SSTableOpen { sstable_path: PathBuf, err: String },

    #[error("SSTableError: {0}")]
    SSTable(String),

    #[error("Root path is not valid directory: {0}")]
    InvalidRootPath(String),

    #[error("SSTable file path error: {0}")]
    SSTableFilePath(String),

    #[error("IO Error: {0}")]
    Io(String),

    #[error("ManifestError: {0}")]
    ManifestError(String),
}

pub type Key = String;
pub type Value = Vec<u8>;

#[derive(Clone, PartialEq, Debug)]
pub enum EntryValue {
    Present(Value),
    Deleted,
}

impl EntryValue {
    /// Returns the entry's size on disk.
    pub fn len(&self) -> usize {
        match self {
            EntryValue::Present(value) => value.len(),
            EntryValue::Deleted => 0,
        }
    }
}

pub(crate) type Memtable = BTreeMap<Key, EntryValue>;

pub struct DB {
    // Database files (SSTables, manifest) are stored under the directory `root_dir`
    root_dir: PathBuf,

    // Manifest describing where all the persistent data is.
    pub(crate) manifest: Manifest,

    // Opened SSTable readers.
    // For now:
    // - All known sstables are opened.
    //
    // level 0 is 0th element, etc.
    // - level 0 is special, since sstables have overlapping keys.
    // - First element is the newest sstable, the last is the oldest.
    // level >= 1 have non-overlapping keys.
    sstables: Vec<VecDeque<(KeyRange, SSTableReader)>>,

    // Active memtable, the latest source of data mutations
    active_memtable: Memtable,

    // Number of bytes that the active memtable has taken up so far.
    // Accounts for key and value size.
    active_memtable_size: usize,

    // Frozen memtables are former active memtables which got too big
    // (DBConfig::memtable_max_size_bytes) were snapshotted and saved. A frozen
    // memtable is not mutable, and will be flushed to an SSTable file
    // and removed from this list.
    //
    // The first element is the oldest memtable, the last is the newest.
    frozen_memtables: VecDeque<Memtable>,

    config: DBConfig,
}

/// The configuration used when opening a database with [DB::open_with_config].
pub struct DBConfig {
    /// Size threshold for a memtable (counts key & value sizes)
    pub memtable_max_size_bytes: usize,
    /// Max number of frozen memtables before they are force-flushed to sstable
    pub max_frozen_memtables: usize,
    /// Size threshold for a block (counts all bytes in a block)
    pub block_max_size_bytes: usize,
    /// The size (total bytes) level N can hold is [level_base_max_size_bytes]*10^(N)
    /// Once level N gets to this size, level N is compacted into level N+1.
    pub level_base_max_size_bytes: u64,
    /// The max # of sstable files for level N is [level_base_max_shards]*2^(N)
    /// This is used to compute the size of each sstable shard during compaction.
    pub level_base_max_shards: u64,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            memtable_max_size_bytes: 1024 * 1024, // 1 MB
            max_frozen_memtables: 1,
            block_max_size_bytes: 1024 * 4,
            // L0 = 10MB, L1 = 100mb, L2 = 1GB, L3 = 10GB ..
            level_base_max_size_bytes: 1024 * 1024 * 10,
            // L1 = 4 shards, L2 = 8, L3 = 16, ..
            level_base_max_shards: 2,
        }
    }
}

impl DB {
    /// Opens the database at `root_path` using default configuration, and creates one if it doesn't exist.
    ///
    /// `root_dir` is the directory where data files will live.
    pub fn open(root_dir: &Path) -> Result<DB, DBError> {
        DB::open_with_config(root_dir, DBConfig::default())
    }

    /// Opens the database at `root_path` using configuration provided by `config`, and creates one if it doesn't exists.
    ///
    /// `root_dir` is the directory where data files will live.
    pub fn open_with_config(root_dir: &Path, config: DBConfig) -> Result<DB, DBError> {
        let manifest = Manifest::open(&root_dir.to_path_buf())
            .map_err(|e| DBError::ManifestError(format!("manifest err: {}", e)))?;
        let sstables = Self::open_all_sstables(&manifest)?;
        Ok(DB {
            root_dir: root_dir.into(),
            manifest,
            sstables,
            active_memtable: BTreeMap::new(),
            active_memtable_size: 0,
            frozen_memtables: VecDeque::<Memtable>::new(),
            config,
        })
    }

    // Opens all SSTable files stored under given the `root_dir` directory.
    //
    // SSTable filenames are formatted as <age>.sstable, where <age> is a number used
    // to signify the precedence order of the sstables.
    // - The oldest SSTable is `0.sst`, the 2nd oldest is `1.sst`, and so on.
    // - The newest SSTable has the highest number.
    // - New SSTables are stored using the filename `<highest age so far + 1>.sst`.
    fn open_all_sstables(
        manifest: &Manifest,
    ) -> Result<Vec<VecDeque<(KeyRange, SSTableReader)>>, DBError> {
        let mut levels = vec![];
        for level in manifest.iter_levels() {
            let mut readers = VecDeque::new();
            for (key_range, sstable_path) in level {
                let sstable_path = manifest.root_dir().join(sstable_path);
                readers.push_front((
                    key_range.clone(),
                    SSTableReader::new(&sstable_path).map_err(|err| DBError::SSTableOpen {
                        sstable_path,
                        err: err.to_string(),
                    })?,
                ));
            }
            levels.push(readers);
        }
        Ok(levels)
    }

    /// get() looks up the value of the given `key`.
    ///
    /// Returns `Some(value)` if the given `key` is found, or `None` if `key` does not exist.
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, DBError> {
        // first check the active memtable
        // if not in the active memtable, check the frozen memtables (newest first, and oldest last)
        // we have to check the most recently frozen memtable first (the last element)
        for memtable in [&self.active_memtable]
            .into_iter()
            .chain(self.frozen_memtables.iter())
        {
            match self.get_from_memtable(key, memtable)? {
                Some(EntryValue::Present(value)) => return Ok(Some(value)),
                Some(EntryValue::Deleted) => return Ok(None),
                _ => continue,
            };
        }

        // Not in the memtables?  Lets try each sstable level.
        // try level 0, then 1, etc. For each level, look up the sstable shard it could be on.
        for level_sstables in self.sstables.iter_mut() {
            // TODO: look up the right sstable instead of checking every one.
            for (range, sstable) in level_sstables.iter_mut() {
                if key < &range.smallest || key > &range.largest {
                    continue; // skip this sstable
                }
                match sstable
                    .get(key)
                    .map_err(|err| DBError::SSTable(err.to_string()))?
                {
                    Some(EntryValue::Present(value)) => return Ok(Some(value)),
                    Some(EntryValue::Deleted) => return Ok(None),
                    None => continue, // try the next level
                };
            }
        }

        Ok(None)
    }

    /// put() inserts the given `key`-`value` pair, replacing the previous value if it exists.
    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), DBError> {
        self.put_entry(key.into(), EntryValue::Present(value.into()))
    }

    /// delete() removes the key-value pair given by `key` if it exists.
    pub fn delete(&mut self, key: impl Into<Key>) -> Result<(), DBError> {
        self.put_entry(key.into(), EntryValue::Deleted)
    }

    /// scan() returns an iterator of all key-value pairs whose key begins with given `key_prefix`.
    ///
    /// The iterator items are in ordered by key, in ascending order.
    pub fn scan(&mut self, key_prefix: &str) -> Result<DBIterator, DBError> {
        // make a min-heap of peekable iterators, where the heap key is:
        // (peekable iterator, precedent)
        Ok(DBIterator {
            // most recent memtable is first
            iterators: {
                // (next element, rest of iterator)
                let mut db_iters_peekable = Vec::<DBIteratorItemPeekable>::new();

                // add the active memtable, and then the frozen memtables
                for memtable in [&self.active_memtable]
                    .into_iter()
                    .chain(self.frozen_memtables.iter())
                {
                    db_iters_peekable
                        .push(DBIteratorItemPeekable::from_memtable(memtable, key_prefix));
                }

                // add the sstables
                if !self.sstables.is_empty() {
                    // scan through sstables in each level
                    for level in &mut self.sstables {
                        db_iters_peekable.append(
                            &mut level
                                .iter_mut()
                                // for sstable_reader in self.sstables {}
                                .map(|sstable_reader| {
                                    DBIteratorItemPeekable::from_sstable(
                                        &sstable_reader.1,
                                        key_prefix,
                                    )
                                    .unwrap()
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                }

                let mut heap = BinaryHeap::new();
                let mut iter_precedence = 0; // 0 means newest
                for db_iter in db_iters_peekable.into_iter() {
                    heap.push(Reverse(DBIteratorItem(db_iter, iter_precedence)));
                    iter_precedence += 1;
                }
                heap
            },
            key_prefix: key_prefix.to_string(),
        })
    }

    fn get_from_memtable(
        &self,
        key: &str,
        memtable: &BTreeMap<Key, EntryValue>,
    ) -> Result<Option<EntryValue>, DBError> {
        Ok(memtable.get(key).cloned())
    }

    fn put_entry(&mut self, key: Key, entry: EntryValue) -> Result<(), DBError> {
        let key_len = key.as_bytes().len();
        let value_len = entry.len();
        match self.active_memtable.insert(key, entry) {
            Some(old_value) => {
                self.active_memtable_size += value_len;
                self.active_memtable_size -= old_value.len();
            }
            None => {
                self.active_memtable_size += key_len;
                self.active_memtable_size += value_len;
            }
        }
        if self.active_memtable_size >= self.config.memtable_max_size_bytes {
            self.freeze_active_memtable()
                .map_err(|sstable_err| DBError::SSTable(sstable_err.to_string()))?;
        }
        // Is it time to compact level 0 -> level 1?
        if self.frozen_memtables.len() > self.config.max_frozen_memtables {
            self.flush_frozen_memtables_to_sstable()
                .map_err(|sstable_err| DBError::SSTable(sstable_err.to_string()))?;

            let l0_size = self.compute_level_size(0).map_err(|ss_err| {
                DBError::SSTable(format!(
                    "Could not compute size of level 0 sstables: {}",
                    ss_err
                ))
            })?;
            if l0_size > self.config.level_base_max_size_bytes {
                // Initialize L1 if it doesn't exist.
                if self.sstables.len() == 1 {
                    self.sstables.push(VecDeque::new());
                }
                let sstables_out = Compactor {
                    root_dir: &self.root_dir,
                    db_config: &self.config,
                }
                .compact(
                    self.sstables[0]
                        .iter()
                        .chain(self.sstables[1].iter())
                        .map(|(_range, reader)| reader.try_clone().unwrap()),
                    0,
                )
                .unwrap(); // panic if compaction fails; FIXME: fail more gracefully?

                // save the new merged files
                self.manifest
                    .merge_level(sstables_out, 1)
                    .map_err(|manifest_err| DBError::ManifestError(manifest_err.to_string()))?;

                self.open_level_from_manifest(0)
                    .map_err(|err| DBError::SSTable(err.to_string()))?;
                self.open_level_from_manifest(1)
                    .map_err(|err| DBError::SSTable(err.to_string()))?;
            }
        }
        Ok(())
    }

    fn open_level_from_manifest(&mut self, level: usize) -> Result<(), SSTableError> {
        debug_assert!(level <= self.sstables.len());

        let sstables = self
            .manifest
            .iter_level_files(level)
            .map(
                |(key_range, path)| -> Result<(KeyRange, SSTableReader), SSTableError> {
                    Ok((key_range.clone(), SSTableReader::new(&path.clone().into())?))
                },
            )
            .collect::<Result<VecDeque<_>, _>>()?;

        if level == self.sstables.len() {
            self.sstables.push(sstables);
        } else {
            self.sstables[level] = sstables;
        }

        Ok(())
    }

    fn compute_level_size(&self, level: u32) -> Result<u64, SSTableError> {
        assert!(level == 0, "only level 0 compaction is supported");
        self.sstables[level as usize]
            .iter()
            .map(|(_range, reader)| reader.size())
            .sum()
    }

    pub(crate) fn freeze_active_memtable(&mut self) -> Result<(), SSTableError> {
        self.frozen_memtables
            .push_front(std::mem::take(&mut self.active_memtable));
        self.active_memtable_size = 0;
        Ok(())
    }

    /// Flushes the set of frozen memtables into a set of L0 sstables.
    ///
    /// Does not do any compaction.
    pub(crate) fn flush_frozen_memtables_to_sstable(&mut self) -> Result<(), SSTableError> {
        if self.sstables.is_empty() {
            self.sstables.push(VecDeque::new());
        }
        // flush the oldest frozen memtable first, and the newest one last.
        for frozen_memtable in self.frozen_memtables.iter().rev() {
            let sstable_filename: String = format!("l0-{}.sst", self.sstables[0].len());
            let sstable_path = self.root_dir.join(&sstable_filename);

            // flush the frozen memtable to sstable
            let mut sstable_file =
                std::fs::File::create(sstable_path.clone()).expect("could not create sstable file");
            let mut sstable_writer = SSTableWriter::new(&mut sstable_file, &self.config);
            for (key, value) in frozen_memtable.iter() {
                sstable_writer.push(key, value)?;
            }
            sstable_writer.flush()?;
            sstable_file.sync_all()?;
            std::mem::drop(sstable_file);

            match (
                frozen_memtable.first_key_value(),
                frozen_memtable.last_key_value(),
            ) {
                (Some((smallest, _)), Some((largest, _))) => {
                    let (range, reader) = (
                        KeyRange {
                            smallest: smallest.to_string(),
                            largest: largest.to_string(),
                        },
                        SSTableReader::new(&sstable_path)?,
                    );
                    self.sstables[0].push_front((range, reader));
                    self.manifest
                        .add_sstable_l0(
                            sstable_filename,
                            KeyRange {
                                smallest: smallest.clone(),
                                largest: largest.clone(),
                            },
                        )
                        .map_err(|e| SSTableError::ManifestError(e.to_string()))?;
                }
                _ => {
                    return Err(SSTableError::Custom(
                        "Frozen memtable has no elements to flush",
                    ))
                }
            };
        }

        // remove all frozen memtables; From now on, DB::get() will query the sstable instead.
        self.frozen_memtables.clear();

        Ok(())
    }

    // for testing.
    pub(crate) fn get_sstables_mut(&mut self) -> &mut VecDeque<(KeyRange, SSTableReader)> {
        &mut self.sstables[0]
    }
}

#[cfg(test)]
mod test {

    use std::{clone, collections::HashMap};

    use super::*;
    use anyhow;
    use pretty_assertions::assert_eq;
    use rand::Rng;

    #[test]
    fn basic_put_get() {
        let mut db = DB::open(Path::new("/tmp/hello")).expect("failed to open");

        db.put("1", "hello").expect("cant put 1");
        db.put("2", "world").expect("cant put 2");

        assert_eq!(db.get("1").unwrap(), Some("hello".as_bytes().to_vec()));
        assert_eq!(db.get("2").unwrap(), Some("world".as_bytes().to_vec()));
        assert_eq!(db.get("3").unwrap(), None);
    }

    #[test]
    fn basic_delete() {
        let mut db = DB::open(Path::new("/tmp/hello")).expect("failed to open");

        db.put("1", "hello").expect("cant put 1");
        db.put("2", "world").expect("cant put 2");

        assert_eq!(db.get("2").unwrap().unwrap(), b"world".to_vec());

        db.delete("2").expect("couldnt delete 2");
        assert_eq!(db.get("2").expect("cant put 2"), None);
    }

    #[test]
    fn basic_scan() {
        let mut db = DB::open(Path::new("/tmp/hello")).expect("failed to open");

        db.put("/user/name/adam", "adam")
            .expect("cant put /user/adam");
        db.put("/user/name/vardhan", "vardhan")
            .expect("cant put /user/vardhan");
        db.put("/abc", "abc").expect("cant put /abc");
        db.put("/xyz", "xyz").expect("cant put /xyz");

        assert_eq!(
            db.get("/user/name/vardhan").unwrap().unwrap(),
            b"vardhan".to_vec()
        );

        assert_eq!(
            db.scan("/user/")
                .expect("couldnt scan /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"adam".to_vec()),
                ("/user/name/vardhan".to_string(), b"vardhan".to_vec())
            ]
        );

        assert_eq!(
            db.scan("/user/vardhan_")
                .expect("couldn't seen /user/vardhan_")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );

        assert_eq!(
            db.scan("/items/")
                .expect("couldnt scan /items")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );
    }

    #[test]
    fn scan_with_active_and_frozen_memtable() {
        let tmpdir = tempdir::TempDir::new("lsmdb").expect("tmpdir");
        let mut db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                // don't trigger writing to sstables
                max_frozen_memtables: 100,
                ..DBConfig::default()
            },
        )
        .expect("couldnt make db");

        db.put("/user/name/adam", "adam")
            .expect("cant put /user/adam");
        db.put("/user/name/vardhan", "vardhan")
            .expect("cant put /user/vardhan");
        db.put("/user/name/catherine", "catherine")
            .expect("cant put /user/catherine");
        db.put("/abc", "abc").expect("cant put /abc");
        db.put("/xyz", "xyz").expect("cant put /xyz");

        db.freeze_active_memtable()
            .expect("could not freeze active memtable");

        // There should be two memtables now; active and 1 frozen.

        assert_eq!(
            db.scan("/user/")
                .expect("couldnt scan /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"adam".to_vec()),
                ("/user/name/catherine".to_string(), b"catherine".to_vec()),
                ("/user/name/vardhan".to_string(), b"vardhan".to_vec())
            ]
        );

        // put() and delete() below go in the active memtable, replacing the frozen entries.
        db.delete("/user/name/catherine")
            .expect("couldnt delete /user/catherine");

        db.put("/user/name/adam", "adam2")
            .expect("cant put /user/name/adam");

        assert_eq!(db.get("/user/name/adam").unwrap(), Some(b"adam2".to_vec()));
        assert_eq!(db.get("/user/name/catherine").unwrap(), None);
        assert_eq!(
            db.get("/user/name/vardhan").unwrap(),
            Some(b"vardhan".to_vec())
        );

        assert_eq!(
            db.scan("/user/")
                .expect("couldnt scan /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"adam2".to_vec()),
                ("/user/name/vardhan".to_string(), b"vardhan".to_vec())
            ]
        );

        db.freeze_active_memtable()
            .expect("could not freeze active memtable");

        // Now we have 3 memtables:  1 active and 2 frozen.
        db.put("/user/name/adam", "adam3")
            .expect("cant put /user/name/adam");
        db.put("/user/name/catherine", "catherine3")
            .expect("cant put /user/name/catherine");
        db.delete("/user/name/vardhan")
            .expect("cant put /user/name/vardhan");
        assert_eq!(
            db.scan("/user/")
                .expect("couldnt scan /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"adam3".to_vec()),
                ("/user/name/catherine".to_string(), b"catherine3".to_vec())
            ]
        );

        assert_eq!(
            db.scan("/user/vardhan_")
                .expect("couldn't see /user/vardhan_")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );

        assert_eq!(
            db.scan("/items/")
                .expect("couldnt scan /items")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );
    }

    #[test]
    fn flush_memtable_to_sstable() {
        let tmpdir = tempdir::TempDir::new("lsmdb").expect("tmpdir");
        let mut db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                // No automatic flushing; all manual for now
                max_frozen_memtables: 100,
                ..DBConfig::default()
            },
        )
        .expect("couldnt make db");

        // one key per frozen memtable:
        let keys = vec!["a", "b", "c", "d", "e", "f"];
        for key in &keys {
            db.put(format!("/key/{}", key), format!("val {}", key))
                .expect("couldnt put");
            db.freeze_active_memtable()
                .expect("couldnt freeze active memtables");
        }
        assert_eq!(db.frozen_memtables.len(), keys.len());

        // every frozen memtable turns into an sstable
        db.flush_frozen_memtables_to_sstable()
            .expect("couldnt flush frozen memtables");

        assert_eq!(db.sstables[0].len(), keys.len());

        // sstables should now be persisted -- test that they are accessible when db is re-opened
        std::mem::drop(db);
        let mut db: DB = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                // No automatic flushing; all manual for now
                max_frozen_memtables: 100,
                ..DBConfig::default()
            },
        )
        .expect("couldnt reopen db");
        assert_eq!(db.sstables[0].len(), keys.len());

        // check that each sstable has the correct key, and doesn't have any of the other keys.
        for (i, key) in keys.iter().rev().enumerate() {
            assert_eq!(
                db.sstables[0][i]
                    .1
                    .get(format!("/key/{}", key).as_str())
                    .unwrap_or_else(|_| panic!("couldnt get /key/{}", key)),
                Some(EntryValue::Present(format!("val {}", key).into_bytes()))
            );
            for non_present_key in keys.iter().rev() {
                if non_present_key == key {
                    continue;
                }
                assert_eq!(
                    db.sstables[0][i]
                        .1
                        .get(format!("/key/{}", non_present_key).as_str())
                        .unwrap_or_else(|_| panic!("couldnt get /key/{}", non_present_key)),
                    None
                );
            }
        }
    }

    #[test]
    fn basic_across_memtables_and_sstables() -> anyhow::Result<()> {
        // zig-zag keys across active, frozen, and an sstable.
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let mut db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                // No automatic flushing; all manual for now
                max_frozen_memtables: 100,
                ..DBConfig::default()
            },
        )?;

        db.put("/aa/insstable", "garbage".as_bytes())?;
        db.put("/zz/insstable", "garbage".as_bytes())?;
        db.put("/key/1", "sstable0".as_bytes())?; // should be replaced by sstable1 and then frozen memtable
        db.put("/key/3", "sstable0".as_bytes())?;
        db.put("/key/4", "sstable0".as_bytes())?; // should be replaced by sstable1
        db.freeze_active_memtable()?;
        assert_eq!(db.active_memtable_size, 0);
        assert_eq!(db.frozen_memtables.len(), 1);
        db.flush_frozen_memtables_to_sstable()?;
        assert_eq!(db.frozen_memtables.len(), 0);
        assert_eq!(db.sstables.len(), 1); // just L0
        assert_eq!(db.sstables[0].len(), 1); // just 1 sstable in L0

        db.delete("/key/1")?; // should replace the version in sstable-L0-0, and be replaced by active memtable
        db.put("/key/2", "sstable1".as_bytes())?;
        db.put("/key/4", "sstable1".as_bytes())?; // should replace sstable-L0-0
        db.put("/aa/insstable2", "garbage".as_bytes())?;
        db.put("/zz/insstable2", "garbage".as_bytes())?;
        db.freeze_active_memtable()?;
        assert_eq!(db.active_memtable_size, 0);
        assert_eq!(db.frozen_memtables.len(), 1);
        db.flush_frozen_memtables_to_sstable()?;
        assert_eq!(db.frozen_memtables.len(), 0);
        assert_eq!(db.sstables.len(), 1); // just L0
        assert_eq!(db.sstables[0].len(), 2); // now we have 2 sstables in L0

        db.put("/aa/infrozen", "garbage".as_bytes())?;
        db.put("/zz/infrozen", "garbage".as_bytes())?;
        db.put("/key/1", "frozen".as_bytes())?;
        db.put("/key/5", "frozen".as_bytes())?;
        db.freeze_active_memtable()?;
        assert_eq!(db.active_memtable_size, 0);
        assert_eq!(db.frozen_memtables.len(), 1);

        db.put("/key/0", "active".as_bytes())?;
        db.put("/key/6", "active".as_bytes())?;
        db.put("/aa/active", "garbage".as_bytes())?;
        db.put("/zz/nactive", "garbage".as_bytes())?;
        assert_eq!(db.active_memtable.len(), 4);

        assert_eq!(db.frozen_memtables.len(), 1);
        assert_eq!(db.frozen_memtables[0].len(), 4);
        assert_eq!(db.sstables[0].len(), 2);

        assert_eq!(db.get("/key/3")?, Some("sstable0".into()));
        assert_eq!(db.get("/key/2")?, Some("sstable1".into()));
        assert_eq!(db.get("/key/4")?, Some("sstable1".into()));
        assert_eq!(db.get("/key/1")?, Some("frozen".into()));
        assert_eq!(db.get("/key/5")?, Some("frozen".into()));
        assert_eq!(db.get("/key/0")?, Some("active".into()));
        assert_eq!(db.get("/key/6")?, Some("active".into()));

        // Ensure that scan("/key") doesn't pick up on surrounding keys:

        db.put("/z", "garbage".as_bytes())?;

        let actual = db.scan("/key/")?.collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                ("/key/0".to_string(), b"active".to_vec()),
                ("/key/1".to_string(), b"frozen".to_vec()),
                ("/key/2".to_string(), b"sstable1".to_vec()),
                ("/key/3".to_string(), b"sstable0".to_vec()),
                ("/key/4".to_string(), b"sstable1".to_vec()),
                ("/key/5".to_string(), b"frozen".to_vec()),
                ("/key/6".to_string(), b"active".to_vec()),
            ]
        );

        Ok(())
    }

    // Test get(), put(), delete() and seek().
    #[test]
    fn basic_across_levels() -> anyhow::Result<()> {
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let mut db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                memtable_max_size_bytes: 1024 * 1, // 1KB per memtable / L0 sstable
                max_frozen_memtables: 2, // 2 frozen memtables before flushing to 1 sstable
                level_base_max_shards: 2, // 2**2 = 4 shards on level 1
                level_base_max_size_bytes: 1024 * 6, // 6KB for level 0, 60KB for level 1.
                ..DBConfig::default()
            },
        )?;

        // put enough keys to generate 2 levels, where the 2nd level has atleast 3 shards.
        // Keep putting in random KVs until we have 3 levels:
        let mut rng = rand::thread_rng();
        let mut expected = HashMap::new();
        while db.sstables.len() < 2 || db.sstables[1].len() < 3 {
            let num = rng.gen::<u32>().to_string();
            let key = format!("/key/{}", num);
            let val = format!("val {}", num).as_bytes().to_vec();
            db.put(key.clone(), val.clone())?;
            expected.insert(key, val);
        }

        // Now delete 30% of the keys (IDK why 50%)
        let keys_to_delete = expected
            .iter()
            .filter(|_| rng.gen_ratio(5, 10))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in &keys_to_delete {
            db.delete(key)?;
            expected.remove(key);
        }

        // Now ensure scan() is able to accurately scan() the expected keys.
        let actual = db
            .scan("")?
            .into_iter()
            .collect::<HashMap<String, Vec<u8>>>();
        assert_eq!(expected, actual);

        // Make sure we can get() the existing keys.
        assert_eq!(
            expected
                .iter()
                .map(|(key, val)| db.get(key).unwrap() == Some(val.clone()))
                .filter(|found| *found)
                .count(),
            expected.len()
        );

        // Make sure we cannot get() the deleted keys.
        assert_eq!(
            keys_to_delete
                .iter()
                .map(|key| db.get(key).unwrap().is_some())
                .filter(|found| *found)
                .count(),
            0
        );

        Ok(())
    }
}
