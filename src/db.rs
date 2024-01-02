use crate::db_config::*;
use crate::db_iter::{
    KeyEValueIterator, KeyEValueIteratorItem, KeyEValueIteratorItemPeekable, KeyValueIterator,
};

mod compaction;
mod manifest;
mod reader_ext;
pub(crate) mod sstable;
pub(crate) mod types;
mod wal;

use sstable::SSTableError;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, VecDeque},
    path::{Path, PathBuf},
};
use thiserror::Error;

use self::types::EntryValue;
use self::wal::WALEntry;
use self::{
    compaction::Compactor,
    manifest::{KeyRange, Manifest},
    sstable::{SSTableReader, SSTableWriter},
    wal::{WALIterator, WALWriter},
};
pub type Key = String;
pub type Value = Vec<u8>;

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

    #[error("write-ahead log Error: {0}")]
    WALError(String),
}

pub(crate) type Memtable = BTreeMap<Key, EntryValue>;

pub struct DB {
    // Database files (SSTables, manifest) are stored under the directory `root_dir`
    root_dir: PathBuf,

    // Manifest describes where all the persisted data files live.
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

    // Write ahead log.
    wal: WALWriter,

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
    // The first element is the newest memtable, the last is the oldest.
    frozen_memtables: VecDeque<Memtable>,

    config: DBConfig,
}

impl DB {
    /// Opens the database at `root_path` using default configuration, and creates one if it doesn't exist.
    ///
    /// `root_dir` is the directory where data files will live.
    pub fn open(root_dir: &Path) -> Result<DB, DBError> {
        DB::open_with_config(root_dir, DBConfig::default())
    }

    /// Close the database.
    pub fn close(self) {}

    /// Opens the database at `root_path` using configuration provided by `config`, and creates one if it doesn't exists.
    ///
    /// `root_dir` is the directory where data files will live.
    pub fn open_with_config(root_dir: &Path, config: DBConfig) -> Result<DB, DBError> {
        let mut manifest = Manifest::open(root_dir)
            .map_err(|e| DBError::ManifestError(format!("manifest err: {}", e)))?;
        let sstables = Self::open_all_sstables(&manifest)?;
        let wal = Self::init_wal(&root_dir, &mut manifest)?;

        let mut db = DB {
            root_dir: root_dir.into(),
            manifest,
            sstables,
            wal,
            active_memtable: BTreeMap::new(),
            active_memtable_size: 0,
            frozen_memtables: VecDeque::<Memtable>::new(),
            config,
        };
        db.init_memtables_from_logs()?;

        Ok(db)
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

    // Initializes the writer for the current log, or creates one if it doesn't exist.
    fn init_wal(root_dir: &Path, manifest: &mut Manifest) -> Result<WALWriter, DBError> {
        let current_log_filename = match manifest.current_log() {
            Some(log_filename) => log_filename.clone(),
            None => {
                // This is the first time opening this database.
                // Make a new log and record it in the manifest.
                let (_, log_filename) = WALWriter::make_new_log(root_dir)
                    .map_err(|e| DBError::WALError(e.to_string()))?;
                manifest
                    .new_log(log_filename.clone())
                    .map_err(|e| DBError::ManifestError(e.to_string()))?;
                log_filename
            }
        };

        Ok(WALWriter::new(&root_dir, &current_log_filename)
            .map_err(|e| DBError::WALError(e.to_string()))?)
    }

    // Initializes the frozen memtables and active memtable from the WAL logs.
    fn init_memtables_from_logs(&mut self) -> Result<(), DBError> {
        for (log_no, log_filename) in self.manifest.logs.clone().iter().enumerate() {
            let wal_iter =
                WALIterator::open(&self.root_dir.join(&log_filename)).map_err(|err| {
                    DBError::WALError(format!(
                        "Could not iterate log file {:?}: {}",
                        log_filename,
                        err.to_string()
                    ))
                })?;
            for entry in wal_iter {
                match entry {
                    WALEntry::Put(key, value) => {
                        self.put_entry_into_active_memtable(key, value);
                    }
                }
            }
            // Freeze the active memtable if there are more logs to process.
            if log_no < self.manifest.logs.len() - 1 {
                self.move_active_memtable_to_frozen();
            }
        }
        Ok(())
    }

    /// get() looks up the value of the given `key`.
    ///
    /// Returns `Some(value)` if the given `key` is found, or `None` if `key` does not exist.
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, DBError> {
        // first check the active memtable
        // if not in the active memtable, check the frozen memtables (newest first, and oldest last)
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
        let candidate_sstables = self.sstables.iter_mut().flat_map(|level_sstables| {
            level_sstables
                .iter_mut()
                .filter(|(range, _sstable)| key >= &range.smallest && key <= &range.largest)
                .map(|(_, sstable)| sstable)
        });
        for sstable_reader in candidate_sstables {
            match sstable_reader
                .get(key)
                .map_err(|err| DBError::SSTable(err.to_string()))?
            {
                Some(EntryValue::Present(value)) => return Ok(Some(value)),
                Some(EntryValue::Deleted) => return Ok(None),
                None => continue, // try the next level
            };
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
    pub fn scan(&mut self, key_prefix: &str) -> Result<KeyValueIterator, DBError> {
        // make a min-heap of peekable iterators, where the heap key is:
        // (peekable iterator, precedent)
        let iter = KeyEValueIterator {
            key_prefix: key_prefix.to_string(),
            should_skip_deleted: true,
            heap_of_iterators: {
                // (next element, rest of iterator)
                let mut db_iters_peekable = Vec::<KeyEValueIteratorItemPeekable>::new();

                // add the active memtable, and then the frozen memtables
                for memtable in [&self.active_memtable]
                    .into_iter()
                    .chain(self.frozen_memtables.iter())
                {
                    db_iters_peekable.push(KeyEValueIteratorItemPeekable::from_memtable(
                        memtable, key_prefix,
                    ));
                }

                // Go through each level, adding every sstable which contains key prefix.
                db_iters_peekable.extend(
                    self.sstables
                        .iter()
                        .flat_map(|level_sstables| {
                            level_sstables.iter().filter(|(range, _sstable)| {
                                (range.smallest.starts_with(key_prefix)
                                    || key_prefix >= &range.smallest)
                                    && (range.largest.starts_with(key_prefix)
                                        || key_prefix <= &range.largest)
                            })
                        })
                        .map(|(_, sstable_reader)| {
                            KeyEValueIteratorItemPeekable::from_sstable(sstable_reader, key_prefix)
                                .unwrap()
                        }),
                );

                let mut heap = BinaryHeap::new();
                for (iter_precedence, db_iter) in db_iters_peekable.into_iter().enumerate() {
                    heap.push(Reverse(KeyEValueIteratorItem(
                        db_iter,
                        iter_precedence as u32,
                    )));
                }
                heap
            },
        };

        Ok(KeyValueIterator { iter })
    }

    fn get_from_memtable(
        &self,
        key: &str,
        memtable: &BTreeMap<Key, EntryValue>,
    ) -> Result<Option<EntryValue>, DBError> {
        Ok(memtable.get(key).cloned())
    }

    fn put_entry(&mut self, key: Key, entry: EntryValue) -> Result<(), DBError> {
        self.wal
            .put(&key, &entry)
            .map_err(|err| DBError::WALError(err.to_string()))?;

        self.put_entry_into_active_memtable(key, entry);
        if self.active_memtable_size >= self.config.max_active_memtable_size {
            self.freeze_active_memtable()
                .map_err(|sstable_err| DBError::SSTable(sstable_err.to_string()))?;
        }
        // Is it time to compact memtables into L0?
        if self.frozen_memtables.len() > self.config.max_frozen_memtables {
            self.flush_frozen_memtables_to_sstable()
                .map_err(|sstable_err| DBError::SSTable(sstable_err.to_string()))?;

            // is L0 too big?
            self.compact_if_l0_full()?;
        }
        Ok(())
    }

    fn put_entry_into_active_memtable(&mut self, key: String, entry: EntryValue) {
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
    }

    // If L0 is full, starts the compaction process which compacts each level into the next if its too big.
    fn compact_if_l0_full(&mut self) -> Result<(), DBError> {
        for level in 0..self.sstables.len() {
            let level_size = self.compute_level_size(level as u32).map_err(|ss_err| {
                DBError::SSTable(format!(
                    "Could not compute size of level {} sstables: {}",
                    level, ss_err
                ))
            })?;
            if level_size <= self.config.level_max_size(level as u32) {
                return Ok(());
            }
            // 1. Initialize the next level if it doesn't exist.
            if self.sstables.len() == level + 1 {
                self.sstables.push(VecDeque::new());
            }

            // 2. Compact level into level+1
            let sstables_out = Compactor {
                root_dir: &self.root_dir,
                db_config: &self.config,
            }
            .compact(
                self.sstables[level]
                    .iter()
                    .chain(self.sstables[level + 1].iter())
                    .map(|(_range, reader)| reader),
                level as u32,
                level == self.sstables.len(),
            )
            .unwrap(); // panic if compaction fails; TODO: should we fail more gracefully?

            // 3. Save the new level+1's files and empty out the previous level.
            let files_to_gc = self.sstables[level]
                .iter()
                .chain(self.sstables[level + 1].iter())
                .map(|(_range, reader)| reader.get_path().clone())
                .collect::<Vec<_>>();

            self.manifest
                .merge_level(sstables_out, level + 1)
                .map_err(|manifest_err| DBError::ManifestError(manifest_err.to_string()))?;

            self.open_level_from_manifest(level)
                .map_err(|err| DBError::SSTable(err.to_string()))?;
            self.open_level_from_manifest(level + 1)
                .map_err(|err| DBError::SSTable(err.to_string()))?;

            // 4. Delete the previous sstables.
            for path in files_to_gc {
                std::fs::remove_file(path)
                    .or_else(|err| Ok(println!("couldn't remove file {}", err)))?;
            }
        }
        Ok(())
    }

    fn open_level_from_manifest(&mut self, level: usize) -> Result<(), SSTableError> {
        debug_assert!(level < self.sstables.len());

        let sstables = self
            .manifest
            .iter_level_files(level)
            .map(
                |(key_range, path)| -> Result<(KeyRange, SSTableReader), SSTableError> {
                    Ok((key_range.clone(), SSTableReader::new(Path::new(path))?))
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

    fn compute_level_size(&self, level: u32) -> Result<usize, SSTableError> {
        self.sstables[level as usize]
            .iter()
            .map(|(_range, reader)| reader.size())
            .sum()
    }

    /// Freezes active memtable and rotates to a new WAL log.
    pub(crate) fn freeze_active_memtable(&mut self) -> Result<(), DBError> {
        self.move_active_memtable_to_frozen();

        // rotate log and record it in the manifest.
        let new_log_filename = self
            .wal
            .rotate_log()
            .map_err(|err| DBError::WALError(format!("Could not rotate log: {}", err)))?;

        self.manifest.new_log(new_log_filename).map_err(|err| {
            DBError::ManifestError(format!("Could not record new log in manifest: {}", err))
        })?;

        Ok(())
    }

    pub fn move_active_memtable_to_frozen(&mut self) {
        self.frozen_memtables
            .push_front(std::mem::take(&mut self.active_memtable));
        self.active_memtable_size = 0;
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

        // remove the frozen memtable logs;  leave the active memtable log, though.
        while self.manifest.logs.len() > 1 {
            let filename = self.manifest.logs.pop_front().unwrap();
            std::fs::remove_file(self.root_dir.join(filename))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use super::*;
    use anyhow;
    use pretty_assertions::assert_eq;
    use rand::Rng;

    #[test]
    fn basic_put_get() {
        let tmpdir = tempdir::TempDir::new("lsmdb").unwrap();
        let mut db = DB::open(tmpdir.path()).expect("failed to open");

        db.put("1", "hello").expect("cant put 1");
        db.put("2", "world").expect("cant put 2");

        assert_eq!(db.get("1").unwrap(), Some("hello".as_bytes().to_vec()));
        assert_eq!(db.get("2").unwrap(), Some("world".as_bytes().to_vec()));
        assert_eq!(db.get("3").unwrap(), None);
    }

    #[test]
    fn basic_delete() {
        let tmpdir = tempdir::TempDir::new("lsmdb").unwrap();
        let mut db = DB::open(tmpdir.path()).expect("failed to open");

        db.put("1", "hello").expect("cant put 1");
        db.put("2", "world").expect("cant put 2");

        assert_eq!(db.get("2").unwrap().unwrap(), b"world".to_vec());

        db.delete("2").expect("couldnt delete 2");
        assert_eq!(db.get("2").expect("cant put 2"), None);
    }

    #[test]
    fn basic_scan() {
        let tmpdir = tempdir::TempDir::new("lsmdb").unwrap();
        let mut db = DB::open(tmpdir.path()).expect("failed to open");

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

        assert_eq!(db.manifest.logs.len(), 1);

        // one key per frozen memtable:
        let keys = vec!["a", "b", "c", "d", "e", "f"];
        for key in &keys {
            db.put(format!("/key/{}", key), format!("val {}", key))
                .expect("couldnt put");
            db.freeze_active_memtable()
                .expect("couldnt freeze active memtables");
        }
        assert_eq!(db.frozen_memtables.len(), keys.len());
        assert_eq!(db.manifest.logs.len(), keys.len() + 1); // `keys.len()` frozen memtables and an active memtable

        // every frozen memtable turns into an sstable
        db.flush_frozen_memtables_to_sstable()
            .expect("couldnt flush frozen memtables");

        assert_eq!(db.sstables[0].len(), keys.len());
        assert_eq!(db.manifest.logs.len(), 1); // only for the active memtable

        // sstables should now be persisted -- test that they are accessible when db is re-opened
        db.close();
        db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                // No automatic flushing; all manual for now
                max_frozen_memtables: 100,
                ..DBConfig::default()
            },
        )
        .expect("couldnt reopen db");
        assert_eq!(db.sstables[0].len(), keys.len());
        assert_eq!(db.manifest.logs.len(), 1); // only for the active memtable

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

    // Test get(), put(), delete() and scan() across levels.
    #[test]
    fn basic_across_levels() -> anyhow::Result<()> {
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let mut db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                max_active_memtable_size: 512, // 512 bytes per memtable / L0 sstable
                max_frozen_memtables: 2,       // 2 frozen memtables before flushing sstable
                level_base_max_sstables: 1,    // 2**1 = 2 shards on level 0
                level_base_max_size: 1024, // 1KB for level 0, 10KB for level 1, 100KB level 2, ~1MB level 3, ..
                ..DBConfig::default()
            },
        )?;

        // put() enough keys to generate 3 levels (L0, L1, L2)
        // The keys are of the format /key/<bucket>/<random number>.
        // The bucket is used to test scanning.
        let mut rng = rand::thread_rng();
        let mut expected = HashMap::new();
        let mut all_buckets = ["a", "b", "c", "d", "e", "f"];
        all_buckets.sort();
        while db.sstables.len() < 4 || db.sstables[2].len() < 2 {
            let num = rng.gen::<u32>().to_string();
            let bucket = all_buckets[rng.gen_range(0..all_buckets.len())];
            let key = format!("/key/{}/{}", bucket, num);
            let val = format!("val {}", num).as_bytes().to_vec();
            db.put(key.clone(), val.clone())?;
            expected.insert(key, val);
        }

        // Now delete ~50% of the keys
        let keys_to_delete = expected
            .iter()
            .filter(|_| rng.gen_ratio(5, 10))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in &keys_to_delete {
            db.delete(key)?;
            expected.remove(key);
        }

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
                .filter(|key| db.get(key).unwrap().is_some())
                .count(),
            0
        );

        // Make sure scan() is able to accurately scan() the expected keys.
        let actual = db.scan("")?.collect::<HashMap<String, Vec<u8>>>();
        assert_eq!(expected, actual);

        // Make sure scanning with several non-empty prefixs yields the same stuff as an empty-prefix scan.
        let mut all_keys = vec![];
        all_buckets.sort();
        for bucket in all_buckets {
            all_keys.extend(db.scan(&format!("/key/{}/", bucket))?);
        }

        assert_eq!(db.scan("")?.collect::<Vec<_>>(), all_keys);

        Ok(())
    }

    #[test]
    fn test_resume_open_close() -> anyhow::Result<()> {
        // Test that closing a database and opening it up again retains keys and values:
        // - test that active memtable contains the same keys as before closing
        // - test that all frozen memtables exists.
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let mut db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                max_active_memtable_size: 1024 * 4, // 4KB per memtable / L0 sstable
                max_frozen_memtables: 2,            // 2 frozen memtables before flushing sstable
                ..DBConfig::default()
            },
        )?;

        // keep filling keys until we have an 2 frozen memtables, and a non-empty active memtable.
        let mut num_keys = 0i32;
        while db.frozen_memtables.len() < 2 || db.active_memtable.is_empty() {
            db.put(format!("/key/{}", num_keys), vec![(num_keys % 255) as u8])?;
            num_keys += 1;
        }

        assert_eq!(db.sstables.len(), 0); // there are no sstables

        // now close the database and open it up again -- test that all the data is there.
        let expected_active_memtable_len = db.active_memtable.len();
        let expected_active_memtable_size = db.active_memtable_size;
        let expected_frozen_memtable_lens = db
            .frozen_memtables
            .iter()
            .map(|memtable| memtable.len())
            .collect::<Vec<_>>();
        db.close();
        db = DB::open_with_config(
            tmpdir.path(),
            DBConfig {
                max_active_memtable_size: 1024 * 4, // 4KB per memtable / L0 sstable
                max_frozen_memtables: 2,            // 2 frozen memtables before flushing sstable
                ..DBConfig::default()
            },
        )?;

        assert_eq!(db.active_memtable.len(), expected_active_memtable_len);
        assert_eq!(db.active_memtable_size, expected_active_memtable_size);
        assert_eq!(
            db.frozen_memtables
                .iter()
                .map(|memtable| memtable.len())
                .collect::<Vec<_>>(),
            expected_frozen_memtable_lens
        );
        assert_eq!(db.sstables.len(), 0);

        // check that the keys we made earlier still exist:
        for key in 0..num_keys {
            assert!(matches!(db.get(&format!("/key/{}", key)), Ok(Some(_))));
        }

        Ok(())
    }
}
