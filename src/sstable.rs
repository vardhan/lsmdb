use std::{
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    str::Utf8Error,
    string::FromUtf8Error,
};

use thiserror::Error;

use crate::db::{EntryValue, Key, Memtable};

// SSTable file format
// ===================
//
// Some additional context:
// - All numbers are encoded in little-endian (LE)
// - Strings (keys) are encoded as # of bytes (LE), followed by bytes.
// - Blocks are sorted by their keyspan.
// - Entries in a block are sorted in ascending order, by key.
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
//     - value length (u32; LE)
//     - key (variable length)
//     - indicator for isPresent (1) or deleted (0).  (u8)
//     - value (val_len bytes)
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

pub struct SSTableReader<'r, R: Read + Seek + 'r> {
    reader: &'r mut R,

    // (last_key, block byte offset, block size), sorted by last_key.
    index: Vec<(String, u32, u32)>,
}

impl<'r, R: Read + Seek + 'r> SSTableReader<'r, R> {
    pub fn from_reader(reader: &'r mut R) -> Result<Self, SSTableError> {
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

    pub fn get(&mut self, key: &str) -> Result<Option<EntryValue>, SSTableError> {
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

pub(crate) fn write_memtable_to_sstable(
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
pub struct BlockWriter {
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

pub struct BlockReader<'r, T: Read + Seek + 'r> {
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
    pub fn new(
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

    pub fn get(&mut self, key: &str) -> Result<Option<EntryValue>, SSTableError> {
        self.reader
            .seek(SeekFrom::Start(self.block_offset as u64))?;

        // TODO: do a binary search instead
        for _ in 0..self.num_entries {
            let (entry_key, entry_val) = self.read_entry()?;
            if key == entry_key {
                return Ok(Some(entry_val));
            }
        }

        Ok(None)
    }

    fn read_entry(&mut self) -> Result<(Key, EntryValue), SSTableError> {
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
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::sstable::*;

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
}
