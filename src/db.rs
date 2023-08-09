use sstable::SSTableError;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, VecDeque},
    fs::DirBuilder,
    ops::Bound,
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::sstable::{self, write_memtable_to_sstable, SSTableReader};

#[derive(Error, Debug, Eq, PartialEq)]
pub enum DBError {
    #[error("SSTableError: {0}")]
    SSTable(String),

    #[error("Root path is not valid directory: {0}")]
    InvalidRootPath(String),

    #[error("SSTable file path error: {0}")]
    SSTableFilePath(String),

    #[error("IO Error: {0}")]
    Io(String),
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
    // SSTable files are stored under the root_path
    root_path: PathBuf,

    // Opened SSTables readers.
    // For now:
    // - All known sstables are opened.
    // - All SSTables are level-0 (they have overlapping keys)
    //
    // First element is the oldest sstable, the last is the newest.
    //
    // SSTables are stored
    sstables: Vec<SSTableReader>,

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

pub struct DBConfig {
    // Size threshold for a memtable (counts key & value)
    pub memtable_max_size_bytes: usize,
    // Max number of frozen memtables before they are force-flushed to sstable
    pub max_frozen_memtables: usize,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            memtable_max_size_bytes: 1024 * 1024 * 1, // 1 MB
            max_frozen_memtables: 1,
        }
    }
}

// This is a container for a peekable iterator for iterating over memtable or sstable.
struct DBIteratorPeekable<'a> {
    next: Option<(Key, EntryValue)>, // this is the item next() and peek() will return
    iter: Box<dyn Iterator<Item = (Key, EntryValue)> + 'a>, //
}

impl<'a> DBIteratorPeekable<'a> {
    pub(crate) fn new(
        mut iter: Box<dyn Iterator<Item = (Key, EntryValue)> + 'a>,
    ) -> DBIteratorPeekable {
        let next = iter.next();
        DBIteratorPeekable { next, iter }
    }

    pub(crate) fn next(&mut self) -> Option<(Key, EntryValue)> {
        let next = std::mem::take(&mut self.next);
        self.next = self.iter.next();
        next
    }

    pub(crate) fn peek(&self) -> Option<&(Key, EntryValue)> {
        self.next.as_ref()
    }
}

impl DB {
    // `root_path` is the directory where data files will live.
    pub fn open(root_path: &Path) -> Result<DB, DBError> {
        DB::open_with_config(root_path, DBConfig::default())
    }

    // `root_path` is the directory where data files will live.
    pub fn open_with_config(root_path: &Path, config: DBConfig) -> Result<DB, DBError> {
        Ok(DB {
            root_path: root_path.into(),
            sstables: Self::open_all_sstables(root_path)?,
            active_memtable: BTreeMap::new(),
            active_memtable_size: 0,
            frozen_memtables: VecDeque::<Memtable>::new(),
            config,
        })
    }

    // Opens all SSTable files stored under given the `root_path` directory.
    //
    // SSTable filenames are formatted as <age>.sstable, where <age> is a number used
    // to signify the precedence order of the sstables.
    // - The oldest SSTable is `0.sst`, the 2nd oldest is `1.sst`, and so on.
    // - The newest SSTable has the highest number.
    // - New SSTables are stored using the filename `<highest age so far + 1>.sst`.
    fn open_all_sstables(root_path: &Path) -> Result<Vec<SSTableReader>, DBError> {
        if !root_path
            .try_exists()
            .map_err(|io_err| DBError::InvalidRootPath(io_err.to_string()))?
        {
            // create dir and exit
            DirBuilder::new()
                .recursive(true)
                .create(root_path)
                .map_err(|io_err| DBError::Io(io_err.to_string()))?;
            return Ok(Vec::new());
        } else if !root_path.is_dir() {
            return Err(DBError::InvalidRootPath(
                root_path.to_str().unwrap().to_string(),
            ));
        }

        // Grab all the .sst files, which are formatted as `<age>.sst`
        // sort them by their age (ascending), and open/store them in this sorted order.
        let mut path_bufs = Vec::new();
        for dirent in root_path
            .read_dir()
            .map_err(|io_err: std::io::Error| DBError::Io(io_err.to_string()))?
        {
            let path_buf = dirent
                .map_err(|io_err: std::io::Error| DBError::Io(io_err.to_string()))?
                .path();
            let sst_num = i32::from_str_radix(
                path_buf
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .split(".")
                    .next()
                    .unwrap(),
                10,
            )
            .map_err(|err| DBError::SSTableFilePath(err.to_string()))?;
            path_bufs.push((sst_num, path_buf.clone()));
        }
        path_bufs.sort_by_key(|(num, _)| *num);

        let mut readers = Vec::new();
        for (_, path_buf) in path_bufs {
            readers.push(
                SSTableReader::from_path(&path_buf)
                    .map_err(|err| DBError::SSTable(err.to_string()))?,
            );
        }
        Ok(readers)
    }

    // Looks up the given `key`.
    //
    // Returns `Some(value)` if the given `key` is found.
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, DBError> {
        // first check the active memtable
        // if not in the active memtable, check the frozen memtables
        // we have to check the most recently frozen memtable first (the last element)
        for memtable in self
            .frozen_memtables
            .iter()
            .chain([&self.active_memtable])
            .rev()
        {
            match self.get_from_memtable(key, memtable)? {
                Some(EntryValue::Present(value)) => return Ok(Some(value)),
                Some(EntryValue::Deleted) => return Ok(None),
                _ => continue,
            };
        }

        // Not in the memtables?  Lets try the sstables
        // Newest one first
        for sstable in self.sstables.iter_mut().rev() {
            match sstable.get(key) {
                Ok(Some(EntryValue::Present(value))) => return Ok(Some(value)),
                Ok(Some(EntryValue::Deleted)) => return Ok(None),
                _ => continue,
            }
        }

        Ok(None)
    }

    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), DBError> {
        self.put_entry(key.into(), EntryValue::Present(value.into()))
    }

    pub fn delete(&mut self, key: impl Into<Key>) -> Result<(), DBError> {
        self.put_entry(key.into(), EntryValue::Deleted)
    }

    pub fn scan(&mut self, key_prefix: &str) -> Result<DBIterator, DBError> {
        // make a min-heap of peekable iterators, where the heap key is:
        // (peekable iterator, precedent)
        Ok(DBIterator {
            // most recent memtable is first
            iterators: {
                // (next element, rest of iterator)
                let mut db_iters_peekable = Vec::<DBIteratorPeekable>::new();

                // add the sstables
                db_iters_peekable.append(
                    &mut self
                        .sstables
                        .iter_mut()
                        // for sstable_reader in self.sstables {}
                        .map(|sstable_reader| -> DBIteratorPeekable {
                            DBIteratorPeekable::new(Box::new(
                                sstable_reader.scan(key_prefix).unwrap(),
                            ))
                        })
                        .collect::<Vec<_>>(),
                );

                // add the memtables
                for memtable in self.frozen_memtables.iter().chain([&self.active_memtable]) {
                    db_iters_peekable.push(DBIteratorPeekable::new(Box::new(
                        memtable
                            .range((Bound::Included(key_prefix.to_string()), Bound::Unbounded))
                            .map(|(&ref key, &ref entry_val)| (key.clone(), entry_val.clone())),
                    )));
                }

                let mut heap = BinaryHeap::new();
                let mut iter_precedence = 0; // 0 means newest
                for db_iter in db_iters_peekable.into_iter().rev() {
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
        if self.frozen_memtables.len() > self.config.max_frozen_memtables {
            self.flush_frozen_memtables()
                .map_err(|sstable_err| DBError::SSTable(sstable_err.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn freeze_active_memtable(&mut self) -> Result<(), SSTableError> {
        self.frozen_memtables
            .push_back(std::mem::take(&mut self.active_memtable));
        Ok(())
    }

    pub(crate) fn flush_frozen_memtables(&mut self) -> Result<(), SSTableError> {
        for frozen_memtable in self.frozen_memtables.iter() {
            let i = self.sstables.len();
            let sstable_path = self.root_path.join(format!("{}.sst", i));

            // flush the frozen memtable to sstable
            let mut sstable_file =
                std::fs::File::create(sstable_path.clone()).expect("could not create sstable file");
            write_memtable_to_sstable(&frozen_memtable, &mut sstable_file)?;
            sstable_file.sync_all()?;
            std::mem::drop(sstable_file);

            self.sstables.push(SSTableReader::from_path(&sstable_path)?);
        }

        // remove all frozen memtables; From now on, DB::get() will query the sstable instead.
        self.frozen_memtables.clear();

        Ok(())
    }
}

// DBIteratorItem is an element in priority queue used for DB::scan().
struct DBIteratorItem<'a>(DBIteratorPeekable<'a>, DBIteratorPrecedence);
type DBIteratorPrecedence = u32; // smaller is newer.  active memtable has 0, frozen memtables have 1..N, and sstables have N+1..

impl<'a> Ord for DBIteratorItem<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0.peek(), &other.0.peek()) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((self_key, _)), Some((ref other_key, _))) => {
                (self_key, self.1).cmp(&(&&other_key, other.1))
            }
            (None, None) => Ordering::Equal,
        }
    }
}
impl<'a> PartialOrd for DBIteratorItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}
impl<'a> PartialEq for DBIteratorItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl<'a> Eq for DBIteratorItem<'a> {}

// An iterator used to scan over many memtables.
pub struct DBIterator<'a> {
    // BinaryHeap is a max-heap, so items (memtable & sstable iterators) are placed with
    // Reverse() to make it a min-heap.
    iterators: BinaryHeap<Reverse<DBIteratorItem<'a>>>,

    // the prefix to scan
    key_prefix: Key,
}

impl<'a> Iterator for DBIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        'pop_key_val: loop {
            if self.iterators.peek().is_none() {
                return None;
            }
            // 1. Take out the smallest iterator (we have to put it back at the end)
            let mut top_memtable = self.iterators.pop();
            let top_kv = match top_memtable {
                None => return None, // There are no more memtables.
                Some(Reverse(DBIteratorItem(ref mut db_iter_peekable, _))) => {
                    db_iter_peekable.next()
                }
            };

            match top_kv {
                Some((key, entry_value)) => {
                    if !key.starts_with(self.key_prefix.as_str()) {
                        return None;
                    }
                    // 2. Skip any duplicates of this key -- we already have the newest one.
                    self.skip_entries_with_key(&key);

                    // 3. Put the memtable iterator back into the heap and return the entry
                    self.iterators.push(top_memtable.unwrap());

                    match entry_value {
                        EntryValue::Present(value) => {
                            return Some((key, value));
                        }
                        EntryValue::Deleted => {
                            continue 'pop_key_val;
                        } // deleted -- try the next key value.
                    }
                }
                // If we hit an memtable iterator that's empty, it implies that all iterators are empty,
                // so we can early-exit this iterator.
                None => return None,
            };
        }
    }
}

impl<'a> DBIterator<'a> {
    fn peek_next_key(&mut self) -> Option<&Key> {
        let next_memtable = self.iterators.peek();
        if next_memtable.is_none() {
            return None;
        }
        let DBIteratorItem(ref db_peekable_iter_ref, _) = next_memtable.as_ref().unwrap().0;
        // let mut db_peekable_iter = db_peekable_iter_ref.borrow_mut();
        let next_kv = db_peekable_iter_ref.peek();
        match next_kv {
            Some((key, _)) => Some(key),
            _ => None,
        }
    }

    // Must call peek_next_key() first -- panics if there is no next key.
    fn skip_next_key(&mut self) {
        let next_memtable = self.iterators.pop().unwrap();
        let DBIteratorItem(mut db_peekable_iter_ref, next_kv_order) = next_memtable.0;
        db_peekable_iter_ref.next();
        self.iterators
            .push(Reverse(DBIteratorItem(db_peekable_iter_ref, next_kv_order)));
    }

    fn skip_entries_with_key(&mut self, key: &String) {
        loop {
            match self.peek_next_key() {
                Some(next_key) if key == next_key => self.skip_next_key(),
                _ => return,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use anyhow;
    use tempdir::TempDir;

    fn make_db_for_test(config: DBConfig) -> DB {
        let tmpdir = tempdir::TempDir::new("lsmdb").expect("tmpdir");
        let mut db = DB::open_with_config(tmpdir.path(), config).expect("couldnt make db");
        db
    }

    #[test]
    fn basic_put_get() {
        let mut db = DB::open(Path::new("/tmp/hello")).expect("failed to open");

        db.put("1", "hello").expect("cant put 1");
        db.put("2", "world").expect("cant put 2");

        assert_eq!(db.get("1"), Ok(Some("hello".as_bytes().to_vec())));
        assert_eq!(db.get("2"), Ok(Some("world".as_bytes().to_vec())));
        assert_eq!(db.get("3"), Ok(None));
    }

    #[test]
    fn basic_delete() {
        let mut db = DB::open(Path::new("/tmp/hello")).expect("failed to open");

        db.put("1", "hello").expect("cant put 1");
        db.put("2", "world").expect("cant put 2");

        assert_eq!(db.get("2"), Ok(Some(b"world".to_vec())));

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

        assert_eq!(db.get("/user/name/vardhan"), Ok(Some(b"vardhan".to_vec())));

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

        assert_eq!(db.get("/user/name/adam"), Ok(Some(b"adam2".to_vec())));
        assert_eq!(db.get("/user/name/catherine"), Ok(None));
        assert_eq!(db.get("/user/name/vardhan"), Ok(Some(b"vardhan".to_vec())));

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

        let keys = vec!["a", "b", "c", "d", "e", "f"];
        for key in &keys {
            db.put(format!("/key/{}", key), format!("val {}", key))
                .expect("couldnt put");
            db.freeze_active_memtable()
                .expect("couldnt freeze active memtables");
        }
        assert_eq!(db.frozen_memtables.len(), keys.len());

        db.flush_frozen_memtables()
            .expect("couldnt flush frozen memtables");

        assert_eq!(db.sstables.len(), keys.len());

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
        assert_eq!(db.sstables.len(), keys.len());

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                db.sstables[i]
                    .get(format!("/key/{}", key).as_str())
                    .expect(format!("couldnt get /key/{}", key).as_str()),
                Some(EntryValue::Present(format!("val {}", key).into_bytes()))
            );
            for non_present_key in keys.iter() {
                if non_present_key == key {
                    continue;
                }
                assert_eq!(
                    db.sstables[i]
                        .get(format!("/key/{}", non_present_key).as_str())
                        .expect(format!("couldnt get /key/{}", non_present_key).as_str()),
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

        db.put("/key/1", "sstable0".as_bytes())?; // should be replaced by sstable1 and then frozen memtable
        db.put("/key/3", "sstable0".as_bytes())?;
        db.put("/key/4", "sstable0".as_bytes())?; // should be replaced by sstable1
        db.freeze_active_memtable()?;
        db.flush_frozen_memtables()?;

        db.delete("/key/1")?; // should replace sstable0, and be replaced by frozen memtable
        db.put("/key/2", "sstable1".as_bytes())?;
        db.put("/key/4", "sstable1".as_bytes())?; // should replace sstable0
        db.freeze_active_memtable()?;
        db.flush_frozen_memtables()?;

        db.put("/key/1", "frozen".as_bytes())?;
        db.put("/key/5", "frozen".as_bytes())?;
        db.freeze_active_memtable()?;

        db.put("/key/0", "active".as_bytes())?;
        db.put("/key/6", "active".as_bytes())?;

        assert_eq!(db.active_memtable.len(), 2);
        assert_eq!(db.frozen_memtables.len(), 1);
        assert_eq!(db.frozen_memtables[0].len(), 2);
        assert_eq!(db.sstables.len(), 2);

        assert_eq!(db.get("/key/3")?, Some("sstable0".into()));
        assert_eq!(db.get("/key/2")?, Some("sstable1".into()));
        assert_eq!(db.get("/key/4")?, Some("sstable1".into()));
        assert_eq!(db.get("/key/1")?, Some("frozen".into()));
        assert_eq!(db.get("/key/5")?, Some("frozen".into()));
        assert_eq!(db.get("/key/0")?, Some("active".into()));
        assert_eq!(db.get("/key/6")?, Some("active".into()));

        // Ensure that scan("/key") doesn't pick up on surrounding keys:
        db.put("/a", "garbage".as_bytes())?;
        db.put("/z", "garbage".as_bytes())?;

        assert_eq!(
            db.scan("/key/")?.collect::<Vec<_>>(),
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
}
