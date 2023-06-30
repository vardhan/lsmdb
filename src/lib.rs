use std::{
    char::from_u32,
    cmp::Ordering,
    collections::{btree_map::Range, BTreeMap},
    error::Error,
    io::{IoSliceMut, Read, Seek, SeekFrom, Write},
    iter::Peekable,
    mem::size_of,
    ops::{Bound, Neg},
    path::{Path, PathBuf},
    str::{FromStr, Utf8Error},
    string::FromUtf8Error,
};
use thiserror::Error;

// SSTable file format
// ===================
//
// - All numbers are encoded in little-endian
// - string lengths are encoded as # of bytes, and strings are utf-8
// - Entries in a block are sorted in ascending order by key
//
// Encoding:
// ---------------------------------------
//
// - <Block> #1
// - <Block> #2
// - ..
// - <SSTable index>
//
// Block format:
//   - entry #1
//     - key length (4 bytes)
//     - value length (4 bytes)
//     - key (variable length)
//     - indicator for isPresent (1) or deleted (0).  (1 byte)
//     - value (val_len length)
//   - entry #2
//     ...
//   - block footer:
//     * byte offset inside block of entry #1 (4 bytes)
//     * byte offset inside block of entry #2 (4 bytes)
//       ...
//     * number of entries (4 bytes)
//
// SSTable index format (i.e., the footer of an sstable file):
// - block #1 byte size (4 bytes), last_key_length (4 bytes), last key (last_key_length bytes)
// - block #2 byte size ..
// - ..
//
// - size of sstable index in bytes (4 bytes)

#[derive(Debug, PartialEq)]
pub struct DBError;

pub type Key = String;
pub type Value = Vec<u8>;

#[derive(Clone, PartialEq, Debug)]
enum EntryValue {
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

const MEMTABLE_MAX_SIZE_BYTES: usize = 1024 * 1024 * 1; // 1 MB size threshold

type Memtable = BTreeMap<Key, EntryValue>;

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
        // first check the active memtable
        let mut result: Option<EntryValue> = self._get_from_memtable(key, &self.memtable)?;

        // if not in the active memtable, check the frozen memtable
        if result.is_none() {
            if let Some(snapshot) = self.memtable_frozen.as_ref() {
                result = self._get_from_memtable(key, snapshot)?;
            }
        }

        Ok(match result {
            Some(EntryValue::Present(data)) => Some(data.clone()),
            Some(EntryValue::Deleted) | None => None,
        })
    }

    fn _get_from_memtable(
        &self,
        key: &str,
        memtable: &BTreeMap<Key, EntryValue>,
    ) -> Result<Option<EntryValue>, DBError> {
        Ok(memtable.get(key).cloned())
    }

    fn _put_entry(&mut self, key: Key, entry: EntryValue) -> Result<(), DBError> {
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
        self._put_entry(key.into(), EntryValue::Present(value.into()))
    }

    pub fn delete(&mut self, key: impl Into<Key>) -> Result<(), DBError> {
        self._put_entry(key.into(), EntryValue::Deleted)
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

    fn _swap_and_compact(&mut self) -> Result<(), SSTableError> {
        assert!(self.memtable_frozen.is_none());
        self.memtable_frozen = Some(std::mem::take(&mut self.memtable));

        // flush memtable to sstable
        let mut file = std::fs::File::create(self.root_path.join("sstable"))
            .expect("could not create sstable file");
        self.write_memtable_to_sstable(self.memtable_frozen.as_ref().unwrap(), &mut file)
    }

    // Flushes the given `memtable` to an sstable file using `writer`.
    fn write_memtable_to_sstable(
        &self,
        memtable: &Memtable,
        writer: &mut impl Write,
    ) -> Result<(), SSTableError> {
        let mut block_writer = BlockWriter::new();
        // `block_sizes` is a list of block size entries.
        // each entry is:  # of bytes in the block, last key in the block.
        let mut block_sizes: Vec<(usize, String)> = Vec::new();

        // write out all the keys
        for (key, entry) in memtable {
            match block_writer.add_to_block(key, entry) {
                Ok(()) => {}
                Err(SSTableError::BlockSizeOverflow) => {
                    // flush the current block to the `writer`, make a new block and add entry to it.
                    block_sizes.push(block_writer.flush(writer)?);
                    block_writer = BlockWriter::new();
                    block_writer
                        .add_to_block(key, entry)
                        .expect("single key/value won't fit in block");
                }
                Err(err) => return Err(err),
            }
        }

        // flush the last block.
        block_sizes.push(block_writer.flush(writer)?);

        // write out the sstable index (i.e., the footer):
        // - block #1 size in bytes (4 bytes), last key length (4 bytes), last key (variable length)
        // - block #2 ..
        // - ..
        // - sstable index size (4 bytes)
        let mut index_size = 0u32;
        for (block_size, last_key) in block_sizes {
            index_size += size_of::<u32>() as u32;
            writer.write_all(&(block_size as u32).to_le_bytes())?;

            index_size += size_of::<u32>() as u32;
            writer.write_all(&(last_key.len() as u32).to_le_bytes())?;

            let last_key_bytes = last_key.as_bytes();
            index_size += last_key_bytes.len() as u32;
            writer.write_all(last_key_bytes)?;
        }
        writer.write_all(&index_size.to_le_bytes())?;

        Ok(())
    }
}

pub struct DBIterator<'a> {
    iter_memtable_mut: Peekable<Range<'a, Key, EntryValue>>,
    iter_memtable_immut: Option<Peekable<Range<'a, Key, EntryValue>>>,
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
                EntryValue::Present(data) => return Some((key.clone(), data.clone())),
                EntryValue::Deleted => {
                    // The key was deleted, so skip it and fetch the next value.
                }
            }
        }
    }
}

#[derive(Error, Debug)]
enum SSTableError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("block is too big. make a new block")]
    BlockSizeOverflow,
    #[error("SSTableError: {0}")]
    Custom(&'static str),
}

const BLOCK_SIZE_MAX_KB: usize = 4 * 1024;
const BLOCK_NUM_ENTRIES_SIZEOF: usize = size_of::<u32>();
struct BlockWriter {
    // Encoding of a single block:
    //   - entry #1
    //     - key length (4 bytes)
    //     - value length (4 bytes)
    //     - key (variable length)
    //     - indicator for Present (1) or Deleted (0).  (1 byte)
    //     - value (variable length)
    //   - entry #2
    //     ...
    //   - byte offset of entry #1 (4 bytes)
    //   - byte offset of entry #2 (4 bytes)
    //     ...
    //   - number of entries (4 bytes)
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

    // Appends the given `entry` to the current block. Returns an error if there is not enough space for the entry
    pub fn add_to_block(&mut self, key: &str, entry: &EntryValue) -> Result<(), SSTableError> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len();
        let value_len = entry.len();

        let entry_offset = self.block_data.len();
        let entry_size = key_len + value_len + (3 * (size_of::<u32>())) + 1;
        if self.block_size() + entry_size > BLOCK_SIZE_MAX_KB {
            return Err(SSTableError::BlockSizeOverflow);
        }
        self.block_data.write_all(&(key_len as u32).to_le_bytes())?;
        self.block_data
            .write_all(&(value_len as u32).to_le_bytes())?;
        self.block_data.write_all(key_bytes)?;
        match entry {
            EntryValue::Present(value_bytes) => {
                self.block_data.write_all(&1u8.to_le_bytes())?;
                self.block_data.write_all(&value_bytes)?;
            }
            EntryValue::Deleted => {
                self.block_data.write_all(&0u8.to_le_bytes())?;
            }
        }

        self.block_footer
            .write_all(&(entry_offset as u32).to_le_bytes())?;
        self.last_key = Some(key.to_string());
        Ok(())
    }

    fn block_size(&self) -> usize {
        self.block_data.len() + self.block_footer.len() + BLOCK_NUM_ENTRIES_SIZEOF
    }

    // Flushes the entire block using the given `writer`.
    //
    // Returns:
    //  - number of bytes in the block
    //  - the last key in the block
    pub fn flush(self, writer: &mut dyn Write) -> Result<(usize, String), std::io::Error> {
        writer.write_all(&self.block_data)?;
        writer.write_all(&self.block_footer)?;
        let num_entries = (&self.block_footer).len() / size_of::<u32>();
        writer.write(&(num_entries as u32).to_le_bytes())?;
        let block_size = self.block_size();
        Ok((block_size, self.last_key.unwrap()))
    }
}

struct BlockReader<'r, T: Read + Seek + 'r> {
    reader: &'r mut T,
    block_offset: u32,
    num_entries: u32,
    entry_offsets: Vec<u32>,
}

trait ReaderExt {
    // Read a little-endian-encoded u32
    fn read_u32_le(&mut self) -> Result<u32, std::io::Error>;
    fn read_u8(&mut self) -> Result<u8, std::io::Error>;
    // Allocates a new vector of size `length` and reads into it.
    fn read_u8s(&mut self, length: usize) -> Result<Vec<u8>, std::io::Error>;
}

impl<T: Read> ReaderExt for T {
    fn read_u32_le(&mut self) -> Result<u32, std::io::Error> {
        let mut encoded_num: [u8; 4] = Default::default();
        self.read_exact(&mut encoded_num)?;
        Ok(u32::from_le_bytes(encoded_num))
    }

    fn read_u8(&mut self) -> Result<u8, std::io::Error> {
        let mut encoded_num: [u8; 1] = Default::default();
        self.read_exact(&mut encoded_num)?;
        Ok(encoded_num[0])
    }

    fn read_u8s(&mut self, length: usize) -> Result<Vec<u8>, std::io::Error> {
        let mut bytes = Vec::<u8>::new();
        bytes.resize(length, 0);
        self.read(bytes.as_mut_slice())?;
        Ok(bytes)
    }
}

impl<'r, T: Read + Seek + 'r> BlockReader<'r, T> {
    fn new(
        reader: &'r mut T,
        block_offset: u32,
        block_size: u32,
    ) -> Result<BlockReader<T>, std::io::Error> {
        reader.seek(SeekFrom::Start(
            (block_offset + block_size - (BLOCK_NUM_ENTRIES_SIZEOF as u32)).into(),
        ))?;
        let num_entries = reader.read_u32_le()?;

        let mut entry_offsets = Vec::<u32>::with_capacity(num_entries as usize);
        reader.seek(SeekFrom::Start(block_offset as u64))?;
        // TODO:  reduce to just 1 read() using read_vectored() or something custom
        for _ in 0..num_entries {
            let entry_offset = reader.read_u32_le()?;
            entry_offsets.push(entry_offset);
        }

        return Ok(Self {
            reader,
            block_offset,
            num_entries,
            entry_offsets,
        });
    }

    fn _read_entry(&mut self) -> Result<(Key, EntryValue), SSTableError> {
        let key_len = self.reader.read_u32_le()? as usize;
        let val_len = self.reader.read_u32_le()? as usize;
        let key = self.reader.read_u8s(key_len)?;
        let is_present = self.reader.read_u8()?;
        let val = self.reader.read_u8s(val_len)?;
        Ok((
            String::from_utf8(key)?,
            match is_present {
                0 => EntryValue::Deleted,
                1 => EntryValue::Present(val),
                _ => {
                    return Err(SSTableError::Custom("invalid isPresent"));
                }
            },
        ))
    }

    fn get(&mut self, key: &str) -> Result<Option<EntryValue>, SSTableError> {
        self.reader
            .seek(SeekFrom::Start(self.block_offset as u64))?;

        // TODO: do a binary search instead
        for _ in 0..self.num_entries {
            let (entry_key, entry_val) = self._read_entry()?;
            if key == entry_key {
                return Ok(Some(entry_val));
            }
        }

        Ok(None)
    }
}

struct SSTableReader<'r, R: Read + Seek + 'r> {
    reader: &'r mut R,

    // (last_key, block byte offset, block size), sorted by last_key.
    index: Vec<(String, u32, u32)>,
}

impl<'r, R: Read + Seek + 'r> SSTableReader<'r, R> {
    fn from_reader(reader: &'r mut R) -> Result<Self, SSTableError> {
        let index = Self::parse_index(reader)?;
        Ok(SSTableReader { reader, index })
    }

    fn parse_index(reader: &mut R) -> Result<Vec<(String, u32, u32)>, SSTableError> {
        // Parse the sstable index size (last 4 bytes)
        reader.seek(SeekFrom::End(-1 * (size_of::<u32>() as i64)))?;

        let index_size = reader.read_u32_le()?;
        // Go to the beginning of the index
        reader.seek(SeekFrom::End(
            -1 * ((index_size + size_of::<u32>() as u32) as i64),
        ))?;

        // Parse the index;  a list of metadata about where each block is and its last key.
        let mut index = Vec::<(String, u32, u32)>::new();
        let mut block_offset = 0u32;
        let mut index_pos = 0;
        while index_pos < index_size {
            let block_size = reader.read_u32_le()?;
            index_pos += 4;

            let key_len = reader.read_u32_le()?;
            index_pos += 4;

            let key_encoded = reader.read_u8s(key_len as usize)?;
            index_pos += key_len;

            let key = String::from_utf8(key_encoded)?;

            index.push((key, block_offset, block_size));
            block_offset += block_size;
        }

        Ok(index)
    }

    fn get(&mut self, key: &str) -> Result<Option<EntryValue>, SSTableError> {
        Ok(match self.get_candidate_block(key) {
            None => None,
            Some((offset, size)) => {
                // TODO: cache the BlockReader
                let mut block_reader = BlockReader::new(self.reader, offset, size)?;
                block_reader.get(key)?
            }
        })
    }

    // given a key, returns which block # might contain the key value pair
    fn get_candidate_block(&self, key: &str) -> Option<(u32, u32)> {
        if let Some(&ref last_entry) = self.index.last() {
            if key > last_entry.0.as_str() {
                return None;
            }
        } else {
            return None;
        }

        match self
            .index
            .binary_search_by_key(&key, |(last_key, _, _)| last_key)
        {
            Ok(idx) | Err(idx) => {
                // Found in this block.
                let (_, offset, size) = self.index[idx];
                Some((offset, size))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tempdir::TempDir;

    use super::*;

    #[test]
    fn block_writer_one_entry() {
        let mut buffer = Vec::new();
        let mut writer = BlockWriter::new();
        writer
            .add_to_block("/user/vardhan", &EntryValue::Present(b"vardhan".to_vec()))
            .expect("write to block");
        let (bytes_written, last_key) = writer.flush(&mut buffer).expect("write to buffer");
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

        db._swap_and_compact().expect("could not flush memtable");

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
    fn block_read_write() {
        let mut mem = Cursor::new(Vec::<u8>::new());
        let mut writer = BlockWriter::new();
        writer
            .add_to_block("/key1", &EntryValue::Present(vec![1, 2, 3]))
            .expect("cant put /key1");
        writer
            .add_to_block("/key2", &EntryValue::Present(vec![4, 5, 6]))
            .expect("cant put /key2");
        writer
            .add_to_block("/key3", &EntryValue::Deleted)
            .expect("cant delete /key3");

        let (size, last_key) = writer.flush(&mut mem).expect("could not flush");
        assert_eq!(last_key, "/key3");

        let mut reader = BlockReader::new(&mut mem, 0, size as u32).expect("couldnt make reader");
        assert_eq!(
            reader.get("/key1").expect("cant find /key1"),
            Some(EntryValue::Present(vec![1, 2, 3]))
        );
        assert_eq!(
            reader.get("/key2").expect("cant find /key2"),
            Some(EntryValue::Present(vec![4, 5, 6]))
        );
        assert_eq!(
            reader.get("/key3").expect("cant find deleted /key3"),
            Some(EntryValue::Deleted)
        );
        assert_eq!(reader.get("/key4").expect("found unknown key /key4"), None);
    }

    #[test]
    fn block_overflow() {
        let mut writer = BlockWriter::new();
        for i in 0..68 {
            assert_eq!(
                writer
                    .add_to_block(
                        format!("/user/username_{0}", i).as_str(),
                        &EntryValue::Present((0..30).collect()),
                    )
                    .expect("failed to put"),
                ()
            );
        }
        assert!(writer.block_size() < BLOCK_SIZE_MAX_KB);
        assert!(matches!(
            writer.add_to_block(
                "/user/username_30495",
                &EntryValue::Present((0..30).collect())
            ),
            Err(SSTableError::BlockSizeOverflow)
        ));
    }

    #[test]
    fn write_to_sstable() {
        let tempdir = TempDir::new("lsmdb_test").expect("couldnt make a temp dir");
        let mut db = DB::open(tempdir.path()).expect("couldnt make db");

        // generates the value for the key. the value is vector of u8s: (0 .. key%255)
        let fn_generate_val_for_key = |key| {
            (0..(key % (u8::MAX as u32)))
                .map(|num| num as u8)
                .collect::<Vec<u8>>()
        };

        // generate 1 MB of key/value pairs.
        let num_keys_to_generate = 7000u32; // from experimenting, this generates 1MB.
        for i in 0..num_keys_to_generate {
            db.put(format!("/user/b_{i}", i = i), fn_generate_val_for_key(i))
                .expect("could not put");
        }

        db._swap_and_compact()
            .expect("could not flush memtable to sstable");
        let all_sstable_paths = tempdir
            .path()
            .read_dir()
            .expect("couldnt read temp dir")
            .map(|dirent| dirent.unwrap().path())
            .into_iter();
        for path in all_sstable_paths {
            let mut file = std::fs::File::open(path.clone()).expect("couldnt open file");
            let mut sstable = SSTableReader::from_reader(&mut file).expect("couldnt make sstable");
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
