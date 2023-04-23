use std::{
    collections::{
        btree_map::{Entry as BEntry, Range},
        BTreeMap,
    },
    ops::Bound,
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq)]
pub struct DBError;

pub type Key = String;
pub type Value = Vec<u8>;

enum Entry {
    Present(Value),
    Deleted,
}

pub struct DB {
    // path: PathBuf,
    memtable: BTreeMap<Key, Entry>,
}

impl DB {
    // `path` is a directory
    pub fn open(_path: &Path) -> Result<DB, DBError> {
        Ok(DB {
            memtable: BTreeMap::new(),
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, DBError> {
        Ok(self.memtable.get(key).and_then(|entry| match entry {
            Entry::Present(data) => Some(data.clone()),
            Entry::Deleted => None,
        }))
    }

    fn _put_entry(&mut self, key: Key, entry: Entry) -> Result<(), DBError> {
        self.memtable.insert(key, entry);
        Ok(())
    }

    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), DBError> {
        self._put_entry(key.into(), Entry::Present(value.into()))
    }

    pub fn delete(&mut self, key: impl Into<Key>) -> Result<(), DBError> {
        match self.memtable.entry(key.into()) {
            BEntry::Vacant(_) => return Err(DBError),
            BEntry::Occupied(mut o) => {
                o.insert(Entry::Deleted);
            }
        }
        Ok(())
    }

    pub fn seek(&self, prefix: &str) -> Result<DBIterator, DBError> {
        Ok(DBIterator {
            iter: self
                .memtable
                .range((Bound::Included(prefix.to_string()), Bound::Unbounded)),
            prefix: prefix.to_string(),
        })
    }
}

pub struct DBIterator<'a> {
    iter: Range<'a, Key, Entry>,
    prefix: Key,
}

impl<'a> Iterator for DBIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        // We may need to skip deleted items, so iterate the inner iterator in a loop.
        loop {
            // Pull from the underlying iterator.
            let (key, value) = self.iter.next()?;
            // The underlying iterator iterates over a range that is unbounded, so we need to
            // check when the keys stop matching the desired prefix.
            if !key.starts_with(&self.prefix) {
                // Terminate iteration. This is enough to satisfy the iterator protocol; we don't
                // need to mark any internal state that iteration is ended.
                return None;
            }
            match value {
                Entry::Present(data) => return Some((key.clone(), data.clone())),
                Entry::Deleted => {
                    // The key was deleted, so skip it and fetch the next value.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
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
}
