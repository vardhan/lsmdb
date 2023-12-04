use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::PathBuf,
    str::Utf8Error,
    string::FromUtf8Error
};


use thiserror::Error;

use crate::db::{EntryValue, Key, Value, DBConfig};

use super::reader_ext::ReaderExt;

// This file provides functionality to read from, write to, and iterator over SSTable files.
//
// SSTable file format
// ===================
//
// Some additional context:
// - All numbers are encoded in little-endian (LE)
// - Strings (keys) are encoded as # of bytes (LE), followed by bytes.
// - Blocks are sorted by their key range.
// - Entries within a block are sorted by key, in ascending order.
//
// SSTable Encoding:
// ---------------------------------------
//
// - <Block> #1
// - <Block> #2
// - ..
// - <SSTable index>
//
// Block format:
// ---------------------------------------
//   - entry #1
//     - key length (u32; LE)
//     - key (variable length)
//     - indicator for isPresent (1) or deleted (0).  (u8)
//     - value length (u32; LE; only if isPresent)
//     - value (value length bytes; only if isPresent)
//   - entry #2
//     ...
//   - block footer:
//     * byte offset inside block of entry #1 (u32; LE)
//     * byte offset inside block of entry #2 (u32; LE)
//       ...
//     * number of entries (u32; little-endian)
//
// SSTable index format (i.e., the footer of an sstable file):
// ---------------------------------------
// - <block #1>
//   byte size (u32; LE),
//   last_key_length (u32; LE),
//   last key in the block (last_key_length bytes)
// - <block #2>
//   ..
// - ..
// - size of sstable index in bytes (u32; LE)
pub(crate) struct SSTableReader {
    path: PathBuf,

    file: File,

    // (last_key, block byte offset, block size), sorted by last_key ascending.
    block_index: Vec<(String, u32, u32)>,
}
// TODO: mmap an sstable files into memory instead of doing File I/O directly.
impl SSTableReader {
    /// Reads the SSTable file located at `path`.
    ///
    /// Returns an error if the SSTable's index is corrupt or malformed.
    pub(crate) fn new(path: &PathBuf) -> Result<Self, SSTableError> {
        let mut file: File = std::fs::File::open(path.clone())?;
        let block_index: Vec<(String, u32, u32)> = Self::parse_index(&mut file)?;
        Ok(SSTableReader { path: path.clone(), file, block_index })
    }

    /// Returns the try clone of this [`SSTableReader`].
    pub(crate) fn try_clone(&self) -> Result<Self, SSTableError> {
        SSTableReader::new(&self.path)
    }

    /// The byte size of the file backing this SSTable.
    pub(crate) fn size(&self) -> Result<u64, SSTableError> {
        Ok(self.file.metadata()?.len())
    }

    fn parse_index(reader: &mut File) -> Result<Vec<(String, u32, u32)>, SSTableError> {
        // Parse the sstable index size (last 4 bytes)
        reader.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;
        let index_size = reader.read_u32_le()?;

        // Go to the beginning of the index
        reader.seek(SeekFrom::End(
            -((index_size + size_of::<u32>() as u32) as i64),
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

    pub fn get(&mut self, key: &str) -> Result<Option<EntryValue>, SSTableError> {
        Ok(match self.get_candidate_block(key) {
            None => None,
            Some(idx) => {
                let (_, block_offset, block_size) = self.block_index[idx];
                // TODO: cache the BlockReader
                let mut block_reader = BlockReader::new(&mut self.file, block_offset, block_size)?;
                block_reader.get(key)?
            }
        })
    }

    // given a key, returns which block # (into self.index) might contain the key value pair
    fn get_candidate_block(&self, key: &str) -> Option<usize> {
        if let Some(last_entry) = self.block_index.last() {
            if key > last_entry.0.as_str() {
                return None;
            }
        } else {
            return None;
        }

        match self
            .block_index
            .binary_search_by_key(&key, |(last_key, _, _)| last_key)
        {
            Ok(idx) | Err(idx) => Some(idx),
        }
    }

    /// Iterates the SSTable for keys beginning with `key_prefix`.
    /// Iterator yields keys in ascending sorted order. Deleted entries are skipped.
    pub(crate) fn scan(&self, key_prefix: &str, skip_deleted: bool) -> Result<SSTableIterator, SSTableError> {
        let (block_idx, mut block_reader) = match self.get_candidate_block(key_prefix) {
            Some(idx) => {
                let (_, block_offset, block_size) = self.block_index[idx];
                (idx, BlockReader::new(self.file.try_clone()?, block_offset, block_size)
                    .unwrap())
            }
            None => {
                return Ok(SSTableIterator::empty());
            }
        };
        match block_reader.seek(key_prefix) {
            Ok(_) =>  {
                Ok(SSTableIterator {
                        key_prefix: key_prefix.to_string(),
                        sst_reader: Some(self.try_clone().unwrap()),
                        skip_deleted,
                        // (block index, block reader)
                        cur_block: Some((block_idx, block_reader)),
                })
            },
            Err(SSTableError::KeyPrefixNotFound) => Ok(SSTableIterator::empty()),
            Err(err) => Err(err)
        }
    }
}

/// This iterator is constructed using [`SSTableReader::scan`].
pub(crate) struct SSTableIterator {
    key_prefix: String,
    sst_reader: Option<SSTableReader>,
    skip_deleted: bool,
    // (block index, block reader)
    cur_block: Option<(usize, BlockReader<File>)>,
}
impl SSTableIterator {
    fn empty() -> SSTableIterator {
        SSTableIterator { key_prefix: "".to_string(), sst_reader: None, skip_deleted: true, cur_block: None }
    }
}
impl<'a> Iterator for SSTableIterator {
    type Item = (Key, EntryValue);

    /// next() scans through blocks looking for the `key_prefix` used to construct this Iterator.
    fn next(&mut self) -> Option<Self::Item> {
        // Scan by repeatedly calling [`BlockReader::read_next_entry`] on a block
        // until we see [`SSTableError::EndOfBlock`], and then go to next block, and so on.
        //
        // Stop when there are no more blocks, or if we see SSTableError::KeyPrefixNotFound.
        self.sst_reader.as_ref()?;
        let sst_reader = self.sst_reader.as_mut().unwrap();
        'read_block_entry: loop {
            self.cur_block.as_ref()?;
            let (_cur_block_idx, ref mut cur_block_reader) = self.cur_block.as_mut().unwrap();
            match cur_block_reader.read_next_entry() {
                Ok((key, EntryValue::Present(value))) if key.starts_with(&self.key_prefix) => return Some((key, EntryValue::Present(value))),
                Ok((key, EntryValue::Deleted)) if key.starts_with(&self.key_prefix) => {
                    if self.skip_deleted {
                        continue 'read_block_entry;
                    }
                    return Some((key, EntryValue::Deleted));
                },
                Ok(_) => return None,
                Err(SSTableError::KeyPrefixNotFound) => return None,
                Err(SSTableError::EndOfBlock) => {
                    // move to next block
                    let (mut block_idx, _) = self.cur_block.as_ref().unwrap();
                    block_idx += 1;
                    if block_idx < sst_reader.block_index.len() {
                        // (last_key, block byte offset, block size)
                        let (_, block_offset, block_size) = sst_reader.block_index[block_idx];
                        let sst_file = sst_reader.file.try_clone();
                        if sst_file.is_err() {
                            return None;
                        }
                        self.cur_block =
                            BlockReader::new(sst_file.unwrap(), block_offset, block_size)
                            .ok()
                            .map(|block_reader| (block_idx, block_reader));
                        continue 'read_block_entry;
                    } else {
                        self.cur_block = None;
                    }
                    return None;
                }
                Err(_) => return None,
            }
        }
    }
}
pub(crate) struct SSTableWriter<'w,'c, W> {
    /// Underlying writer to write the sstable to
    writer: &'w mut W,

    /// Number of bytes written so far to the writer.
    bytes_written_so_far: usize,

    /// A running count of the sstable index byte size, used for computing the size() of the sstable so far.
    sstable_index_byte_size: usize,

    /// the byte size of the most recently written key, used for computing the size() of the sstable so far.
    prev_key_byte_size: usize,

    db_config: &'c DBConfig,

    /// current block to write to.
    cur_block_writer: BlockWriter<'c>,

    /// `block_sizes` is a list of block size entries.
    /// each entry is:  # of bytes in the block, last key in the block.
    /// As blocks. This gets appended to the writer when the SSTable is finalized
    block_sizes: Vec<(usize, String)>,
}

impl<'w, 'c, W: Write> SSTableWriter<'w, 'c, W> {
    pub(crate) fn new(writer: &'w mut W, db_config: &'c DBConfig) -> SSTableWriter<'w, 'c, W> {
        let sstable_index_byte_size =
            size_of::<u32>(); // byte size of the sstable index (u32)
        SSTableWriter{
            writer,
            bytes_written_so_far: 0,
            sstable_index_byte_size,
            prev_key_byte_size: 0,
            db_config,
            cur_block_writer: BlockWriter::new(db_config),
            block_sizes: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, key: &Key, entry: &EntryValue) -> Result<(), SSTableError>{
        match self.cur_block_writer.push(key, entry) {
            Ok(bytes_written) => {
                self.bytes_written_so_far += bytes_written;
            }
            Err(SSTableError::BlockSizeOverflow) => {
                // flush the current block to the `writer`, make a new block and add entry to it.
                let prev_block_writer = std::mem::replace(&mut self.cur_block_writer, BlockWriter::new(self.db_config));
                let (block_size, last_key) = prev_block_writer.flush(self.writer)?;
                self.sstable_index_byte_size +=
                    size_of::<u32>() // block size (u32)
                    + size_of::<u32>() // to store the byte size of the last key
                    + last_key.as_bytes().len() // # of bytes in the last key
                ;
                self.block_sizes.push((block_size, last_key));
                self.bytes_written_so_far += block_size;
                self.cur_block_writer = BlockWriter::new(self.db_config);
                self.bytes_written_so_far += self.cur_block_writer
                    .push(key, entry)
                    .expect("single key/value won't fit in block");
            }
            Err(err) => return Err(err),
        }
        self.prev_key_byte_size = key.as_bytes().len();
        Ok(())
    }

    /// Computed size of the sstable if it were to be flush()'d.
    pub(crate) fn size(&self) -> usize {
        // written bytes so far + size of the sstable index so far + pending final block entry sstable
        self.bytes_written_so_far + self.sstable_index_byte_size
         + (size_of::<u32>() + self.prev_key_byte_size) // (last key byte size, the last key)
    }

    /// Flushes the sstable index.
    /// After this call, the sstable file is ready to be used with SSTableReader.
    pub(crate) fn flush(mut self) -> Result<(), SSTableError> {
        // flush the last block.
        self.block_sizes.push(self.cur_block_writer.flush(self.writer)?);

        // write out the sstable index (i.e., the footer):
        // - block #1 size in bytes (4 bytes), last key length (4 bytes), last key (variable length)
        // - block #2 ..
        // - ..
        // - sstable index size (4 bytes)
        //
        // NOTE: the above might be out-of-sync -- see top of this file for source of truth.
        let mut index_size = 0u32;
        for (block_size, last_key) in self.block_sizes {
            index_size += size_of::<u32>() as u32;
            self.writer.write_all(&(block_size as u32).to_le_bytes())?;

            index_size += size_of::<u32>() as u32;
            self.writer.write_all(&(last_key.len() as u32).to_le_bytes())?;

            let last_key_bytes = last_key.as_bytes();
            index_size += last_key_bytes.len() as u32;
            self.writer.write_all(last_key_bytes)?;
        }
        self.writer.write_all(&index_size.to_le_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn write_to_sstable<'kv>(
    kv_iter: impl Iterator<Item=&'kv (Key, EntryValue)>,
    writer: &mut impl Write,
    db_config: &DBConfig,
) -> Result<(), SSTableError> {
    let mut ss_writer = SSTableWriter::new(writer, db_config);
    for (ref key, ref value) in kv_iter {
        ss_writer.push(key, value)?;
    }
    ss_writer.flush()?;
    Ok(())
}

#[derive(Error, Debug)]
pub(crate) enum SSTableError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("block is too big. make a new block")]
    BlockSizeOverflow,
    #[error("no keys with prefix")]
    KeyPrefixNotFound,
    #[error("Reached the end of block")]
    EndOfBlock,
    // TODO:  Replace `Custom` with specific error codes
    #[error("SSTableError: {0}")]
    Custom(&'static str),
    #[error("ManifestError: {0}")]
    ManifestError(String),
}

const BLOCK_NUM_ENTRIES_SIZEOF: usize = size_of::<u32>();
pub(crate) struct BlockWriter<'c> {
    db_config: &'c DBConfig,
    block_data: Vec<u8>,
    block_footer: Vec<u8>,
    last_key: Option<String>,
}

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
impl<'c> BlockWriter<'c> {
    pub fn new(db_config: &'c DBConfig) -> Self {
        BlockWriter {
            db_config,
            block_data: Vec::new(),
            block_footer: Vec::new(),
            last_key: None,
        }
    }

    /// Appends the given `entry` to the current block.
    ///
    /// Returns the number of bytes the (key,entry) took up.
    /// Returns an error if there is not enough space in the block for the (key,entry).
    pub fn push(&mut self, key: &str, entry: &EntryValue) -> Result<usize, SSTableError> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len();
        let value_len = entry.len();
        let entry_offset = self.block_data.len();
        let entry_size =
            size_of::<u32>() // key length
            + key_len
            + 1 // presence bit (present or deleted)
            + match entry {
                &EntryValue::Present(_) => 
                    size_of::<u32>() // value length
                    + value_len,
                &EntryValue::Deleted => 0
            }
            + size_of::<u32>() // byte offset for block footer
            ;
        if self.size() + entry_size > self.db_config.block_max_size {
            return Err(SSTableError::BlockSizeOverflow);
        }
        self.block_data.write_all(&(key_len as u32).to_le_bytes())?;
        self.block_data.write_all(key_bytes)?;
        match entry {
            EntryValue::Present(value_bytes) => {
                self.block_data.write_all(&1u8.to_le_bytes())?;
                self.block_data.write_all(&(value_len as u32).to_le_bytes())?;
                self.block_data.write_all(value_bytes)?;
            }
            EntryValue::Deleted => {
                self.block_data.write_all(&0u8.to_le_bytes())?;
            }
        }

        self.block_footer
            .write_all(&(entry_offset as u32).to_le_bytes())?;
        self.last_key = Some(key.to_string());
        Ok(entry_size)
    }

    /// An estimated size of the block so far.
    fn size(&self) -> usize {
        self.block_data.len() + self.block_footer.len() + BLOCK_NUM_ENTRIES_SIZEOF
    }

    /// Flushes the entire block using the given `writer`.
    ///
    /// Returns:
    ///  - number of bytes in the block
    ///  - the last key in the block
    pub fn flush(self, writer: &mut dyn Write) -> Result<(usize, String), std::io::Error> {
        writer.write_all(&self.block_data)?;
        writer.write_all(&self.block_footer)?;
        let num_entries = self.block_footer.len() / size_of::<u32>();
        writer.write(&(num_entries as u32).to_le_bytes())?;
        let block_size = self.size();
        Ok((block_size, self.last_key.unwrap()))
    }
}

// BlockReader provides a way to get() keys from it
// seek(), along with multiple calls to read_next_entry(), allows a client to
// iterator across the block too; SSTable::scan() uses it.
pub(crate) struct BlockReader<R: Read + Seek> {
    reader: R,
    block_offset: u32,
    num_entries: u32,
    // entry_offsets: Vec<u32>,
    entry_cursor: u32 // cursor for the next entry
}

impl<R: Read + Seek> BlockReader<R> {
    pub fn new(
        mut reader: R,
        block_offset: u32,
        block_size: u32,
    ) -> Result<BlockReader<R>, std::io::Error> {
        reader.seek(SeekFrom::Start(
            (block_offset + block_size - (BLOCK_NUM_ENTRIES_SIZEOF as u32)).into(),
        ))?;
        let num_entries = reader.read_u32_le()?;

        let mut entry_offsets = Vec::<u32>::with_capacity(num_entries as usize);
        reader.seek(SeekFrom::Start(
            (block_offset + block_size) as u64
            -
            (
              BLOCK_NUM_ENTRIES_SIZEOF // rewind the num-of-entries field
            + size_of::<u32>()*(num_entries as usize) // rewind the entry offset fields
            ) as u64
        ))?;
        // TODO:  reduce to just 1 read() using read_vectored(), or something custom
        for _ in 0..num_entries {
            let entry_offset = reader.read_u32_le()?;
            entry_offsets.push(entry_offset);
        }

        reader.seek(SeekFrom::Start(block_offset as u64))?;
        Ok(Self {
            reader,
            block_offset,
            num_entries,
            // entry_offsets,
            entry_cursor: 0
        })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<EntryValue>, SSTableError> {
        self.reader
            .seek(SeekFrom::Start(self.block_offset as u64))?;

        // TODO: do a binary search instead
        for i in 0..self.num_entries {
            self.entry_cursor = i;
            let ((entry_key, _), entry_val) = (self.read_next_key()?, self.read_next_value()?);
            if key == entry_key {
                return Ok(Some(entry_val));
            }
        }

        Ok(None)
    }

    // reads and returns the next key, along with total # of bytes read.
    // NOTE: does not update the cursor
    fn read_next_key(&mut self) -> Result<(Key, usize), SSTableError> {
        let key_len: usize = self.reader.read_u32_le()? as usize;
        Ok((String::from_utf8(self.reader.read_u8s(key_len)?)?, key_len + size_of::<u32>()))
    }

    // reads and returns the next value.
    // NOTE: does not update the cursor
    fn read_next_value(&mut self) -> Result<EntryValue, SSTableError> {
        let is_present = self.reader.read_u8()?;
        Ok(match is_present {
            0 => EntryValue::Deleted,
            1 => {
                let val_len = self.reader.read_u32_le()? as usize;
                let val = self.reader.read_u8s(val_len)?;
                EntryValue::Present(val)
            },
            _ => {
                return Err(SSTableError::Custom("invalid isPresent"));
            }
        })
    }

    // To be used after seek().
    //
    // Reads the next entry and returns it, or returns SSTableError::EndOfBlock
    pub fn read_next_entry(&mut self) -> Result<(Key,EntryValue), SSTableError> {
        if self.entry_cursor >= self.num_entries {
            return Err(SSTableError::EndOfBlock);
        }
        self.entry_cursor += 1;
        let (key, _) = self.read_next_key()?;
        Ok((key, self.read_next_value()?))
    }

    // Seeks the BlockReader's cursor to be at the first entry with prefix `key_prefix`
    // Returns the key-value entry's index if found.
    //
    // If no such entry is found, returns an error of SSTableError::NotFound
    pub fn seek(&mut self, key_prefix: &str) -> Result<u32, SSTableError> {
        self.reader
            .seek(SeekFrom::Start(self.block_offset as u64))?;

        // TODO: do a binary search instead
        for idx in 0..self.num_entries {
            self.entry_cursor = idx;
            let (entry_key, bytes_read_for_key) = self.read_next_key()?;
            if entry_key.starts_with(key_prefix) {
                // rewind key we just read
                self.reader.seek(SeekFrom::Current(-(bytes_read_for_key as i64)))?;
                return Ok(idx);
            }
            let is_present = self.reader.read_u8()?;
            if is_present == 1 {
                let val_len = self.reader.read_u32_le()?;
                self.reader.seek(SeekFrom::Current(val_len as i64))?; // seek past the value
            }
        }
        Err(SSTableError::KeyPrefixNotFound)
    }
}

struct BlockIterator<'k, 'r, R: Read + Seek + 'r> {
    key_prefix: &'k String,
    block_reader: &'r mut BlockReader<R>,
    is_empty: bool
}
impl<'k, 'r, R: Read + Seek + 'r> Iterator for BlockIterator<'k, 'r, R> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_empty {
            return None;
        }
        'next_present_key: loop {
            match self.block_reader.read_next_key() {
                Ok((next_key, _)) if next_key.starts_with(self.key_prefix) => {
                    match self.block_reader.read_next_value() {
                        Ok(EntryValue::Present(next_val)) => return Some((next_key, next_val)),
                        Ok(EntryValue::Deleted) => continue 'next_present_key,
                        _ => return None
                    }
                },
                _ => break
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::db::{DBConfig, DB, sstable::{SSTableError, SSTableReader, BlockReader, BlockWriter}, EntryValue};
    use std::{io::Cursor, fs::File, collections::HashMap};
    use rand::Rng;
    use tempdir::TempDir;

    use super::SSTableWriter;

    #[test]
    fn block_writer_one_entry() -> anyhow::Result<()> {
        let db_config = DBConfig::default();
        let mut buffer = Vec::new();
        let mut writer = BlockWriter::new(&db_config);
        writer
            .push("/keyspace/keyname", &EntryValue::Present(b"value of the key".to_vec()))
            .expect("write to block");
        let (bytes_written, last_key) = writer.flush(&mut buffer).expect("write to buffer");

        let mut reader = BlockReader::new(Cursor::new(&buffer), 0, bytes_written.try_into()?)?;
        assert_eq!(
            reader.read_next_entry()?,
            ("/keyspace/keyname".to_string(), EntryValue::Present(b"value of the key".to_vec()))
        );
        assert_eq!(last_key, "/keyspace/keyname");
        assert_eq!(bytes_written, buffer.len());

        Ok(())
    }

    #[test]
    fn block_read_write() {
        let db_config = DBConfig::default();
        let mut mem = Cursor::new(Vec::<u8>::new());
        let mut writer = BlockWriter::new(&db_config);
        writer
            .push("/key1", &EntryValue::Present(vec![1, 2, 3]))
            .expect("cant put /key1");
        writer
            .push("/key2", &EntryValue::Present(vec![4, 5, 6]))
            .expect("cant put /key2");
        writer
            .push("/key3", &EntryValue::Deleted)
            .expect("cant delete /key3");
        writer
            .push("/key4", &EntryValue::Present(vec![7, 8, 9]))
            .expect("cant put /key4");

        let (size, last_key) = writer.flush(&mut mem).expect("could not flush");
        assert_eq!(last_key, "/key4");

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
        assert_eq!(
            reader.get("/key4").expect("cant find deleted /key4"),
            Some(EntryValue::Present(vec![7, 8, 9]))
        );
        assert_eq!(reader.get("/key5").expect("found unknown key /key5"), None);
    }

    #[test]
    fn block_overflow() {
        let db_config = DBConfig::default();
        let mut writer = BlockWriter::new(&db_config);
        for i in 0..68 {
            writer
                .push(
                    format!("/user/username_{0}", i).as_str(),
                    &EntryValue::Present((0..30).collect()),
                )
                .expect("failed to put");
        }
        assert!(writer.size() < db_config.block_max_size);
        assert!(matches!(
            writer.push(
                "/user/username_30495",
                &EntryValue::Present((0..30).collect())
            ),
            Err(SSTableError::BlockSizeOverflow)
        ));
    }

    #[test]
    fn sstable_seek_across_all_blocks() {
        let tempdir = TempDir::new("lsmdb_test").expect("couldnt make a temp dir");
        let num_entries = 300;
        let mut db = DB::open_with_config(
            tempdir.path(),
            DBConfig {
                // don't auto-write to sstable; this test triggers that manually
                max_frozen_memtables: 4,
                block_max_size: num_entries, // this should force multiple blocks to be written
                ..DBConfig::default()
            },
        )
        .expect("couldnt make db");

        // pad the beginning of the memtable a key, which should not show up in our SSTable scan().
        db.put("/aaa", "a val").unwrap();

        // pad the end of the memtable with a key, which should not show up in our SSTable scan().
        db.put("/zzz", "a val").unwrap();

        // this should make enough keys
        let mut expected_keys: Vec<String> = (0..100).map(|k| format!("/key/{}",k)).collect();
        expected_keys.sort();
        for key in &expected_keys {
            db.put(key, format!("v:{}", key)).unwrap();
        }

        db.freeze_active_memtable().unwrap();
        db.flush_frozen_memtables_to_sstable().unwrap();

        let sstables = &db.sstables[0];
        assert!(sstables.len() == 1);
        assert!(sstables[0].1.block_index.len() > 1); // there should be multiple blocks

        let actual_kvs: Vec<_> = sstables[0].1.scan("/key/", true).unwrap().collect();

        assert_eq!(expected_keys.len(), actual_kvs.len());

        // assert that we can scan all of the keys
        for (expected_key, (actual_key,actual_val)) in expected_keys.iter().zip(actual_kvs.iter()) {
            assert_eq!(expected_key, actual_key);
            assert_eq!(&EntryValue::Present(format!("v:{}", expected_key).to_string().into_bytes()), actual_val);
        }
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
        db.flush_frozen_memtables_to_sstable()
            .expect("could not flush frozen memtable");
        let all_levels = db.manifest.iter_levels().collect::<Vec<_>>();
        assert!(all_levels.len() == 1);
        assert!(all_levels[0].len() == 1);
        for (_key_range, path) in all_levels[0] {
            // let mut file = std::fs::File::open(path.clone()).expect("couldnt open file");
            let mut sstable =
                SSTableReader::new(&tempdir.path().join(path))
                    .unwrap_or_else(|_| panic!("couldnt open sstable {}", path));
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

    #[test]
    fn large_sstable_read_write() -> anyhow::Result<()> {
        let tempdir = TempDir::new("lsmdb_test").expect("couldnt make a temp dir");
        let db_config = DBConfig {
            // don't auto-write to sstable; this test triggers that manually
            max_frozen_memtables: 4,
            ..DBConfig::default()
        };

        let sstable_path = tempdir.path().join("dummy_sstable");
        let mut sstable_file = File::create(sstable_path.clone())?;

        let mut ss_writer = SSTableWriter::new(&mut sstable_file, &db_config);

        // write 20k keys into sstable, half of them deleted.
        let mut expected = HashMap::new();
        let mut rng = rand::thread_rng();
        let key_range = 0..20000;
        for i in key_range.clone() {
            let key = format!("/key/{:0>5}", i);
            let val = if rng.gen_ratio(5, 10) {
                EntryValue::Present(format!("val {}", i).as_bytes().to_vec())
            } else {
                EntryValue::Deleted
            };
            ss_writer.push(&key, &val)?;
            expected.insert(key, val);
        };

        ss_writer.flush()?;

        // read it back
        let mut ss_reader = SSTableReader::new(&sstable_path).unwrap();
        assert_eq!(expected, ss_reader.scan("", false)?.collect());

        let deleted_actual: HashMap<&String,&EntryValue> = expected.iter().filter(|&(_key,val)| val != &EntryValue::Deleted).collect();
        assert_eq!(deleted_actual, ss_reader.scan("", true)?.collect::<HashMap<String,EntryValue>>().iter().collect());

        // pick 500 random keys to read. some should've been deleted, some should exist.
        for _i in 0..500 {
            let key = format!("/key/{:0>5}", rng.gen_range(key_range.clone()));
            assert_eq!(expected[&key], ss_reader.get(&key).unwrap().unwrap());
        }

        Ok(())
    }
}
