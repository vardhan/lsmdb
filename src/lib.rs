use std::{
    cell::RefCell,
    char::from_u32,
    cmp::{Ordering, Reverse},
    collections::{btree_map::Range, BTreeMap, BinaryHeap, VecDeque},
    error::Error,
    fmt::Binary,
    io::{IoSliceMut, Read, Seek, SeekFrom, Write},
    iter::Peekable,
    mem::size_of,
    ops::{Bound, Deref, Neg},
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

#[derive(Error, Debug, PartialEq)]
pub enum DBError {
    #[error("SSTableError: {0}")]
    SSTableError(String),
}

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
// Size threshold for a memtable (counts key & value)
const MEMTABLE_MAX_SIZE_BYTES: usize = 1024 * 1024 * 1;
// Max number of frozen memtables before they are force-flushed to sstable
const MAX_NUM_FROZEN_MEMTABLES: usize = 4;

type Memtable = BTreeMap<Key, EntryValue>;

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
}

impl DB {
    // `root_path` is the directory where data files will live.
    pub fn open(root_path: &Path) -> Result<DB, DBError> {
        Ok(DB {
            root_path: root_path.into(),
            active_memtable: BTreeMap::new(),
            active_memtable_size: 0,
            frozen_memtables: VecDeque::<Memtable>::new(),
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
        if self.active_memtable_size >= MEMTABLE_MAX_SIZE_BYTES {
            self.freeze_active_memtable()
                .map_err(|sstable_err| DBError::SSTableError(sstable_err.to_string()))?;
        }
        if self.frozen_memtables.len() >= MAX_NUM_FROZEN_MEMTABLES {
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

        self.write_memtable_to_sstable(
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

#[derive(Error, Debug)]
pub enum SSTableError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("block is too big. make a new block")]
    BlockSizeOverflow,
    // TODO:  Replace `Custom` with specific error codes
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
    fn seek_with_active_and_frozen_memtable() {
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

        db.freeze_active_memtable()
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
