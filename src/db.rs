use sstable::SSTableError;
use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{btree_map::Range, BTreeMap, BinaryHeap, VecDeque},
    iter::Peekable,
    ops::Bound,
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::sstable::{self, write_memtable_to_sstable};

#[derive(Error, Debug, PartialEq)]
pub enum DBError {
    #[error("SSTableError: {0}")]
    SSTableError(String),
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
            EntryValue::Deleted => 1,
        }
    }
}

pub(crate) type Memtable = BTreeMap<Key, EntryValue>;

pub struct DB {
    // SSTable files are stored under the root_path
    root_path: PathBuf,

    // Active memtable, the latest source of data mutations
    active_memtable: Memtable,

    // Number of bytes that the active memtable has taken up so far.
    // Accounts for key and value size.
    active_memtable_size: usize,

    // Frozen memtables are former active memtables which got too big
    // (MEMTABLE_MAX_SIZE_BYTES) were snapshotted and saved. A frozen
    // memtable is not mutable, and will be flushed to an SSTable file
    // and removed from this list.
    //
    // The first element is the oldest memtable, the last is the newest.
    frozen_memtables: VecDeque<Memtable>,

    config: DBConfig,
}

pub struct DBConfig {
    // Size threshold for a memtable (counts key & value)
    memtable_max_size_bytes: usize,
    // Max number of frozen memtables before they are force-flushed to sstable
    max_frozen_memtables: usize,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            memtable_max_size_bytes: 1024 * 1024 * 1, // 1 MB
            max_frozen_memtables: 1,
        }
    }
}

impl DB {
    // `root_path` is the directory where data files will live.
    pub fn open(root_path: &Path) -> Result<DB, DBError> {
        Ok(DB {
            root_path: root_path.into(),
            active_memtable: BTreeMap::new(),
            active_memtable_size: 0,
            frozen_memtables: VecDeque::<Memtable>::new(),
            config: DBConfig::default(),
        })
    }

    pub fn open_with_config(root_path: &Path, config: DBConfig) -> Result<DB, DBError> {
        Ok(DB {
            root_path: root_path.into(),
            active_memtable: BTreeMap::new(),
            active_memtable_size: 0,
            frozen_memtables: VecDeque::<Memtable>::new(),
            config,
        })
    }

    // Looks up the given `key`.
    //
    // Returns `Some(value)` if the given `key` is found.
    pub fn get(&self, key: &str) -> Result<Option<Value>, DBError> {
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

        Ok(None)
    }

    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), DBError> {
        self.put_entry(key.into(), EntryValue::Present(value.into()))
    }

    pub fn delete(&mut self, key: impl Into<Key>) -> Result<(), DBError> {
        self.put_entry(key.into(), EntryValue::Deleted)
    }

    pub fn seek(&self, key_prefix: &str) -> Result<DBIterator, DBError> {
        // make a min-heap of peekable iterators, where the heap key is:
        // (peekable iterator, precedent)
        Ok(DBIterator {
            // most recent memtable is first
            memtables: {
                let memtable_iters = self
                    .frozen_memtables
                    .iter()
                    .chain([&self.active_memtable])
                    .map(|memtable| {
                        memtable
                            .range((Bound::Included(key_prefix.to_string()), Bound::Unbounded))
                            .peekable()
                    })
                    .rev();
                let mut heap = BinaryHeap::new();
                let mut memtable_order = 0; // 0 means newest
                for memtable_iter in memtable_iters {
                    heap.push(Reverse(DBIteratorItem(
                        RefCell::new(memtable_iter),
                        memtable_order,
                    )));
                    memtable_order += 1;
                }
                heap
            },
            prefix: key_prefix.to_string(),
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
                .map_err(|sstable_err| DBError::SSTableError(sstable_err.to_string()))?;
        }
        if self.frozen_memtables.len() > self.config.max_frozen_memtables {
            self.flush_frozen_memtables()
                .map_err(|sstable_err| DBError::SSTableError(sstable_err.to_string()))?;
        }
        Ok(())
    }

    fn freeze_active_memtable(&mut self) -> Result<(), SSTableError> {
        self.frozen_memtables
            .push_back(std::mem::take(&mut self.active_memtable));
        Ok(())
    }

    fn flush_frozen_memtables(&mut self) -> Result<(), SSTableError> {
        // TODO: write ALL frozen memtables to sstables
        assert!(self.frozen_memtables.len() == 1);

        // flush the frozen memtable to sstable
        let mut sstable_file = std::fs::File::create(self.root_path.join("sstable"))
            .expect("could not create sstable file");

        write_memtable_to_sstable(
            self.frozen_memtables
                .front()
                .ok_or_else(|| SSTableError::Custom("No frozen memtables to flush"))?,
            &mut sstable_file,
        )?;
        sstable_file.sync_all()?;
        std::mem::drop(sstable_file); // close the new sstable file

        // remove the frozen memtable; From now on, DB::get() will query the sstable instead.
        self.frozen_memtables.clear();

        Ok(())
    }
}

type MemtablePeekableIter<'a> = Peekable<Range<'a, Key, EntryValue>>;
type MemtableOrder = u32; // smaller is newer

// TODO: Avoid using a RefCell<> by caching the saving the next key in DBIteratorItem.
// e.g., struct DBIteratorItem(next_key, MemTableIter<>, MemtableOrder);
// TODO: Instead of just iterating over Memtable, also iterate over SSTables.
struct DBIteratorItem<'a>(RefCell<MemtablePeekableIter<'a>>, MemtableOrder);

impl<'a> Ord for DBIteratorItem<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0.borrow_mut().peek(), &other.0.borrow_mut().peek()) {
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
    // BinaryHeap is a max-heap, so items (memtable iterators) are placed with
    // Reverse() to make it a min-heap.
    memtables: BinaryHeap<Reverse<DBIteratorItem<'a>>>,

    // the prefix to scan
    prefix: Key,
}

impl<'a> Iterator for DBIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        'pop_key_val: loop {
            if self.memtables.peek().is_none() {
                return None;
            }
            // 1. Take out the smallest iterator (we have to put it back at the end)
            let top_memtable = self.memtables.pop();
            let top_kv = match &top_memtable {
                None => return None, // There are no more memtables.
                Some(Reverse(DBIteratorItem(kv_iter, _))) => kv_iter.borrow_mut().next(),
            };

            match top_kv {
                Some((key, entry_value)) => {
                    if !key.starts_with(self.prefix.as_str()) {
                        return None;
                    }
                    // 2. Skip any duplicates of this key -- we already have the newest one.
                    self.skip_entries_with_key(key);

                    // 3. Put the memtable iterator back into the heap
                    self.memtables.push(top_memtable.unwrap());

                    match entry_value {
                        EntryValue::Present(value) => {
                            return Some((key.to_string(), value.clone()))
                        }
                        EntryValue::Deleted => continue 'pop_key_val, // deleted -- try the next key value.
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
        let next_memtable = self.memtables.peek();
        if next_memtable.is_none() {
            return None;
        }
        let DBIteratorItem(ref next_kv_iter_ref, _) = next_memtable.as_ref().unwrap().0;
        let mut next_kv_iter = next_kv_iter_ref.borrow_mut();
        let next_kv = next_kv_iter.peek();
        match next_kv {
            Some((key, _)) => Some(key),
            _ => None,
        }
    }
    // Must call peek_next_key() first -- panics if there is no next key.
    fn skip_next_key(&mut self) {
        let next_memtable = self.memtables.pop().unwrap();
        let DBIteratorItem(next_kv_iter_ref, next_kv_order) = next_memtable.0;
        next_kv_iter_ref.borrow_mut().next();
        self.memtables
            .push(Reverse(DBIteratorItem(next_kv_iter_ref, next_kv_order)));
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
    use super::*;
    use tempdir::TempDir;

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
    fn basic_seek() {
        let mut db = DB::open(Path::new("/tmp/hello")).expect("failed to open");

        db.put("/user/name/adam", "adam")
            .expect("cant put /user/adam");
        db.put("/user/name/vardhan", "vardhan")
            .expect("cant put /user/vardhan");
        db.put("/abc", "abc").expect("cant put /abc");
        db.put("/xyz", "xyz").expect("cant put /xyz");

        assert_eq!(db.get("/user/name/vardhan"), Ok(Some(b"vardhan".to_vec())));

        assert_eq!(
            db.seek("/user/")
                .expect("couldnt seek /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"adam".to_vec()),
                ("/user/name/vardhan".to_string(), b"vardhan".to_vec())
            ]
        );

        assert_eq!(
            db.seek("/user/vardhan_")
                .expect("couldn't seen /user/vardhan_")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );

        assert_eq!(
            db.seek("/items/")
                .expect("couldnt seek /items")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );
    }

    #[test]
    fn seek_with_active_and_frozen_memtable() {
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
            db.seek("/user/")
                .expect("couldnt seek /user")
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
            db.seek("/user/")
                .expect("couldnt seek /user")
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
            db.seek("/user/")
                .expect("couldnt seek /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"adam3".to_vec()),
                ("/user/name/catherine".to_string(), b"catherine3".to_vec())
            ]
        );

        assert_eq!(
            db.seek("/user/vardhan_")
                .expect("couldn't see /user/vardhan_")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );

        assert_eq!(
            db.seek("/items/")
                .expect("couldnt seek /items")
                .collect::<Vec<(Key, Value)>>(),
            vec![]
        );
    }

    #[test]
    fn write_to_sstable() {
        let tempdir = TempDir::new("lsmdb_test").expect("couldnt make a temp dir");
        let mut db = DB::open_with_config(
            tempdir.path(),
            DBConfig {
                // don't auto-write to sstable; this test triggers that manually
                max_frozen_memtables: 4,
                ..DBConfig::default()
            },
        )
        .expect("couldnt make db");

        // generates the value for the key. the value is vector of u8s: (0 .. key%255)
        let fn_generate_val_for_key = |key| {
            (0..(key % (u8::MAX as u32)))
                .map(|num| num as u8)
                .collect::<Vec<u8>>()
        };

        // generate <1 MB of key/value pairs.
        let num_keys_to_generate = 7000u32; // from experimenting, this generates < 1MB
        for i in 0..num_keys_to_generate {
            db.put(format!("/user/b_{i}", i = i), fn_generate_val_for_key(i))
                .expect("could not put");
        }

        db.freeze_active_memtable()
            .expect("could not freeze active memtable");
        db.flush_frozen_memtables()
            .expect("could not flush frozen memtable");
        let all_sstable_paths: Vec<PathBuf> = tempdir
            .path()
            .read_dir()
            .expect("couldnt read temp dir")
            .map(|dirent| dirent.unwrap().path())
            .into_iter()
            .collect();
        assert!(all_sstable_paths.len() == 1);
        for path in all_sstable_paths {
            let mut file = std::fs::File::open(path.clone()).expect("couldnt open file");
            let mut sstable =
                sstable::SSTableReader::from_reader(&mut file).expect("couldnt make sstable");
            for i in 0..num_keys_to_generate {
                // this key should exist
                assert_eq!(
                    sstable
                        .get(format!("/user/b_{i}", i = i).as_str())
                        .expect("couldnt get"),
                    Some(EntryValue::Present(fn_generate_val_for_key(i)))
                );

                // append a `_` to the end of the key, so that the same block is (most likely) going to be queried.
                assert_eq!(
                    sstable
                        .get(format!("/user/b_{i}_", i = i + 1).as_str())
                        .expect("couldnt get unknown"),
                    None,
                );
            }

            // Try to get a missing key which would be past the last key in the sstable
            assert_eq!(sstable.get("/user/c").expect("couldnt get unknown"), None);
            // Try to get a missing key which would be before the first key in the sstable
            assert_eq!(sstable.get("/user/a").expect("couldnt get unknown"), None);
        }
    }
}
