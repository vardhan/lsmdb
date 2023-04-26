use std::{
    cmp::Ordering,
    collections::{btree_map::Range, BTreeMap},
    io::{Read, Seek, Write},
    iter::Peekable,
    mem::size_of,
    ops::Bound,
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq)]
pub struct DBError;

pub type Key = String;
pub type Value = Vec<u8>;

#[derive(Clone)]
enum Entry {
    Present(Value),
    Deleted,
}

impl Entry {
    /// Returns the entry's size on disk.
    pub fn len(&self) -> usize {
        match self {
            Entry::Present(value) => value.len(),
            Entry::Deleted => 1,
        }
    }
}

const MEMTABLE_MAX_SIZE_BYTES: usize = 1024 * 1024 * 1; // 1 MB size threshold

type Memtable = BTreeMap<Key, Entry>;

pub struct DB {
    root_path: PathBuf,

    memtable: Memtable,
    memtable_frozen: Option<Memtable>,

    // number of bytes that memtable has taken up so far
    // accounts for key and value size.
    memtable_size: usize,
}

impl DB {
    // `path` is a directory
    pub fn open(path: &Path) -> Result<DB, DBError> {
        Ok(DB {
            root_path: path.into(),
            memtable: BTreeMap::new(),
            memtable_frozen: None,
            memtable_size: 0,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, DBError> {
        let mut result: Option<Entry> = self._get_from_memtable(key, &self.memtable)?;
        if result.is_none() {
            if let Some(snapshot) = self.memtable_frozen.as_ref() {
                result = self._get_from_memtable(key, snapshot)?;
            }
        }

        Ok(match result {
            Some(Entry::Present(data)) => Some(data.clone()),
            Some(Entry::Deleted) | None => None,
        })
    }

    fn _get_from_memtable(
        &self,
        key: &str,
        memtable: &BTreeMap<Key, Entry>,
    ) -> Result<Option<Entry>, DBError> {
        Ok(memtable.get(key).cloned())
    }

    fn _put_entry(&mut self, key: Key, entry: Entry) -> Result<(), DBError> {
        let key_len = key.as_bytes().len();
        let value_len = entry.len();
        self.memtable_size += value_len;
        match self.memtable.insert(key, entry) {
            Some(old_value) => {
                self.memtable_size -= old_value.len();
            }
            None => {
                self.memtable_size += key_len;
            }
        }
        if self.memtable_size >= MEMTABLE_MAX_SIZE_BYTES {
            self._swap_and_compact();
        }
        Ok(())
    }

    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), DBError> {
        self._put_entry(key.into(), Entry::Present(value.into()))
    }

    pub fn delete(&mut self, key: impl Into<Key>) -> Result<(), DBError> {
        self._put_entry(key.into(), Entry::Deleted)
    }

    pub fn seek(&self, prefix: &str) -> Result<DBIterator, DBError> {
        Ok(DBIterator {
            iter_memtable_mut: self
                .memtable
                .range((Bound::Included(prefix.to_string()), Bound::Unbounded))
                .peekable(),
            iter_memtable_immut: self.memtable_frozen.as_ref().map(|memtable| {
                memtable
                    .range((Bound::Included(prefix.to_string()), Bound::Unbounded))
                    .peekable()
            }),
            prefix: prefix.to_string(),
        })
    }

    fn _swap_and_compact(&mut self) {
        assert!(self.memtable_frozen.is_none());
        self.memtable_frozen = Some(std::mem::take(&mut self.memtable));

        // flush memtable to sstable
        let mut file = std::fs::File::create(self.root_path.join("sstable")).expect("create file");
        self.flush_memtable(self.memtable_frozen.as_ref().unwrap(), &mut file)
            .expect("flush table");
    }

    fn flush_memtable(
        &self,
        memtable: &Memtable,
        writer: &mut dyn Write,
    ) -> Result<(), std::io::Error> {
        let mut block_writer = BlockWriter::new();
        let mut block_indices: Vec<(usize, String)> = Vec::new();

        // write out all the keys
        for (key, entry) in memtable {
            match block_writer.add_to_block(key, entry) {
                Ok(()) => {}
                Err(BlockWriterError::BlockSizeOverflow) => {
                    block_indices.push(block_writer.write(writer)?);
                    block_writer = BlockWriter::new();
                    block_writer
                        .add_to_block(key, entry)
                        .expect("single key/value won't fit in block");
                }
                Err(BlockWriterError::Io(err)) => return Err(err),
            }
        }

        block_indices.push(block_writer.write(writer)?);

        // write out the sstable index:
        // - block #1 byte offset (4 bytes), key length (4 bytes), key (variable length)
        // - block #2 ..
        // - ..
        // - byte offset of sstable index
        let mut block_offset = 0u32; // sstable index offset by the time the following loop is done.
        for (block_size, last_key) in block_indices {
            writer.write(&block_offset.to_le_bytes())?;
            writer.write(&(last_key.len() as u32).to_le_bytes())?;
            writer.write(last_key.as_bytes())?;
            block_offset += block_size as u32;
        }
        writer.write(&block_offset.to_be_bytes())?;

        Ok(())
    }
}

struct SSTableReader<T: Read + Seek> {
    reader: T,

    // (last_key, block byte offset, block size), sorted by last_key.
    index: Vec<(String, u32, u32)>,
}
type BlockIndex = u32;
impl<T: Read + Seek> SSTableReader<T> {
    fn from_reader(reader: T) -> Result<Self, std::io::Error> {
        // parse sstable index into `index`
        Ok(SSTableReader {
            reader,
            index: Vec::new(),
        })
    }

    fn get(&self, key: String) -> Result<Entry> {
        let block_index = self.get_candidate_block(key);
        let block_reader = self.read_block(block_index);
        block_reader.get(key)
    }

    // given a key, returns which block # might contain the key value pair
    fn get_candidate_block(&self, key: String) -> BlockIndex {
        match self
            .index
            .binary_search_by_key(&&key, |(last_key, _, _)| last_key)
        {
            // FIXME:  should fail if key is past the last block.
            Ok(idx) | Err(idx) => {
                // Found in this block.
                self.index[idx].1
            }
        }
    }

    fn read_block(&self, block_index: BlockIndex) -> BlockReader {
        let (_, offset, size) = self.index[block_index];
        BlockReader::new(&self.reader, offset, size)
    }
}

struct BlockReader {}
impl BlockReader {
    fn new() {}
    fn get(&self, key: String) -> Result<Entry> {}
}

pub struct DBIterator<'a> {
    iter_memtable_mut: Peekable<Range<'a, Key, Entry>>,
    iter_memtable_immut: Option<Peekable<Range<'a, Key, Entry>>>,
    prefix: Key,
}

impl<'a> Iterator for DBIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        // We may need to skip deleted items, so iterate the inner iterator in a loop.
        loop {
            // Peek at both iterators and see which comes next.
            let (key, value) = match (
                self.iter_memtable_mut.peek(),
                self.iter_memtable_immut
                    .as_mut()
                    .map(|i| i.peek())
                    .flatten(),
            ) {
                // Both iterators have a value, check which takes precedence.
                (Some((key_mut, _value_mut)), Some((key_immut, _value_immut))) => {
                    match key_mut.cmp(key_immut) {
                        Ordering::Equal => {
                            // The left (mutable) key takes precedence over the right (immutable).
                            // Skip the stale value in the immutable iterator.
                            let _ = self.iter_memtable_immut.as_mut().unwrap().next();
                            self.iter_memtable_mut.next().unwrap()
                        }
                        Ordering::Less => {
                            // Consume the left (mutable) value first
                            self.iter_memtable_mut.next().unwrap()
                        }
                        Ordering::Greater => {
                            // Consume the right (immutable) value first
                            self.iter_memtable_immut.as_mut().unwrap().next().unwrap()
                        }
                    }
                }
                // Only the left iterator (mutable) has a value, take it as-is.
                (Some((_key, _value)), None) => self.iter_memtable_mut.next().unwrap(),
                // Only the right iterator (immutable) has a value, take it as-is.
                (None, Some((_key, _value))) => {
                    self.iter_memtable_immut.as_mut().unwrap().next().unwrap()
                }
                // Both iterators are exhausted, terminate.
                (None, None) => return None,
            };
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

#[derive(Debug)]
enum BlockWriterError {
    BlockSizeOverflow,
    Io(std::io::Error),
}

impl From<std::io::Error> for BlockWriterError {
    fn from(value: std::io::Error) -> Self {
        BlockWriterError::Io(value)
    }
}

const BLOCKS_SIZE_KB: usize = 4 * 1024;
const BLOCK_NUM_ENTRIES_BYTES: usize = 4;
struct BlockWriter {
    // number of bytes written so far in block_bytes
    block_data: Vec<u8>,
    block_footer: Vec<u8>,
    last_key: Option<String>,
}

impl BlockWriter {
    pub fn new() -> Self {
        BlockWriter {
            block_data: Vec::new(),
            block_footer: Vec::new(),
            last_key: None,
        }
    }

    pub fn add_to_block(&mut self, key: &str, entry: &Entry) -> Result<(), BlockWriterError> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len();
        let value_len = entry.len();

        let entry_offset = self.block_data.len();

        let entry_size = key_len + value_len + (3 * (size_of::<u32>())) + 1; // 3 u32s + 1 u8: entry_offset, key_len, value_len, present byte// num keys
        if self.bytes_written() + entry_size > BLOCKS_SIZE_KB {
            return Err(BlockWriterError::BlockSizeOverflow);
        }
        self.block_data.write(&(key_len as u32).to_le_bytes())?;
        self.block_data.write(&(value_len as u32).to_le_bytes())?;
        self.block_data.write(key_bytes)?;
        match entry {
            Entry::Present(value) => {
                self.block_data.write(&1u8.to_le_bytes())?;
                self.block_data.write(&value)?;
            }
            Entry::Deleted => {
                self.block_data.write(&0u8.to_le_bytes())?;
            }
        }

        self.block_footer
            .write(&(entry_offset as u32).to_be_bytes())?;
        self.last_key = Some(key.to_string());
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        self.block_data.len() + self.block_footer.len() + BLOCK_NUM_ENTRIES_BYTES
    }

    // Encoding:
    //   - entry #1
    //     - key length (4 bytes)
    //     - value length (4 bytes)
    //     - key (variable length)
    //     - indicator for Present (1) or Deleted (0).  (1 byte)
    //     - value (variable length)
    //   - entry #2
    //     ..
    //   - byte offset of entry #1 (4 bytes)
    //   - byte offset of entry #2 (4 bytes)
    //     ..
    //   - number of entries (4 bytes)
    pub fn write(self, writer: &mut dyn std::io::Write) -> Result<(usize, String), std::io::Error> {
        writer.write_all(&self.block_data)?;
        writer.write_all(&self.block_footer)?;
        let num_entries = (&self.block_footer).len() / size_of::<u32>();
        writer.write(&(num_entries as u32).to_le_bytes())?;
        let bytes_written = self.bytes_written();
        Ok((bytes_written, self.last_key.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_writer_one_entry() {
        let mut buffer = Vec::new();
        let mut writer = BlockWriter::new();
        writer
            .add_to_block("/user/vardhan", &Entry::Present(b"vardhan".to_vec()))
            .expect("write to block");
        let (bytes_written, last_key) = writer.write(&mut buffer).expect("write to buffer");
        assert_eq!(
            buffer,
            b"\x0d\x00\x00\x00\x07\x00\x00\x00/user/vardhan\x01vardhan\x00\x00\x00\x00\x01\x00\x00\x00"
        );
        assert_eq!(last_key, "/user/vardhan");
        assert_eq!(bytes_written, buffer.len());
    }

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
    fn seek_with_frozen_memtable() {
        let tmpdir = tempdir::TempDir::new("lsmdb").expect("tmpdir");
        let mut db = DB::open(tmpdir.path()).expect("failed to open");

        db.put("/user/name/adam", "adam")
            .expect("cant put /user/adam");
        db.put("/user/name/vardhan", "vardhan")
            .expect("cant put /user/vardhan");
        db.put("/user/name/catherine", "catherine")
            .expect("cant put /user/catherine");
        db.put("/abc", "abc").expect("cant put /abc");
        db.put("/xyz", "xyz").expect("cant put /xyz");

        db._swap_and_compact();

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

        db.delete("/user/name/catherine")
            .expect("couldnt delete /user/catherine");

        db.put("/user/name/adam", "vardhan")
            .expect("cant put /user/name/adam");

        assert_eq!(db.get("/user/name/vardhan"), Ok(Some(b"vardhan".to_vec())));

        assert_eq!(
            db.seek("/user/")
                .expect("couldnt seek /user")
                .collect::<Vec<(Key, Value)>>(),
            vec![
                ("/user/name/adam".to_string(), b"vardhan".to_vec()),
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
