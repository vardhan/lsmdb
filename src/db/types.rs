use std::{
    io::{Read, Write},
    mem::size_of,
    string::FromUtf8Error,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{reader_ext::ReaderExt, Key, Value};

/// Types for representing keys and values in-memory and on-disk.

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum EntryValue {
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

#[derive(Error, Debug)]
pub(crate) enum SerializableEntryError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializeError(String),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
}

/// This type provides an API to read and write key-value entries on disk.
#[derive(Debug)]
pub(crate) struct SerializableEntry {
    pub key: Key,
    pub value: EntryValue,
}

impl SerializableEntry {
    /// Writes this entry's key and value into `dest`.
    ///
    /// This method first serializes into a memory buffer before writing the buffer into `dest`.
    /// This is done in order to save on the write() calls to `dest`, which may be expensive if backed by disk.
    ///
    /// If you want to serialize without buffering, use [serialize_unbuffered].
    pub fn serialize_buffered(&self, dest: &mut impl Write) -> Result<(), SerializableEntryError> {
        // Serialize into a buffer first, instead of directly into `dest`. Useful
        // in case `dest`` is a File, which would incur lots of write() syscalls.
        let mut buf = Vec::<u8>::with_capacity(Self::entry_size(self.key.as_bytes(), &self.value));
        Self::serialize_unbuffered(&mut buf, self.key.as_bytes(), &self.value)
            .map_err(|err| SerializableEntryError::SerializeError(err.to_string()))?;

        dest.write_all(buf.as_slice())?;
        Ok(())
    }

    /// Writes the given key and value into `dest` without buffering into memory first.
    pub fn serialize_unbuffered(
        dest: &mut impl Write,
        key: &[u8],
        value: &EntryValue,
    ) -> Result<(), SerializableEntryError> {
        dest.write_all(&(key.len() as u32).to_le_bytes())?;
        dest.write_all(key)?;
        match value {
            EntryValue::Present(value_bytes) => {
                dest.write_all(&1u8.to_le_bytes())?;
                dest.write_all(&(value_bytes.len() as u32).to_le_bytes())?;
                dest.write_all(value_bytes)?;
            }
            EntryValue::Deleted => {
                dest.write_all(&0u8.to_le_bytes())?;
            }
        };
        Ok(())
    }

    /// Deserialize key-value entry from `src`.
    pub fn deserialize(
        mut src: &mut impl Read,
    ) -> Result<SerializableEntry, SerializableEntryError> {
        let (key, _) = Self::read_next_key(&mut src)
            .map_err(|err| SerializableEntryError::SerializeError(err.to_string()))?;

        let value = Self::read_next_value(src)
            .map_err(|err| SerializableEntryError::SerializeError(err.to_string()))?;

        Ok(SerializableEntry { key, value })
    }

    /// Compute the number of bytes `key` and `value` take up on-disk.
    pub fn entry_size(key: &[u8], value: &EntryValue) -> usize {
        size_of::<u32>() // key length
        + key.len()
        + 1 // presence bit (present or deleted)
        + value.len()
        + size_of::<u32>() // byte offset for block footer
    }

    /// Reads and returns the next key, along with total # of bytes read.
    /// It is the caller's responsibility to ensure that the next thing in `reader` is a key.
    /// Call [read_next_value] after calling this function.
    pub fn read_next_key(reader: &mut impl Read) -> Result<(Key, usize), SerializableEntryError> {
        let key_len: usize = reader.read_u32_le()? as usize;
        Ok((
            String::from_utf8(reader.read_u8s(key_len)?)?,
            key_len + size_of::<u32>(),
        ))
    }

    /// Reads and returns the next value.
    /// It is the caller's responsibility to ensure that the next thing in `reader` is a value.
    /// Call [read_next_key] before calling this function.
    pub(crate) fn read_next_value(
        reader: &mut impl Read,
    ) -> Result<EntryValue, SerializableEntryError> {
        let is_present = reader.read_u8()?;
        Ok(match is_present {
            0 => EntryValue::Deleted,
            1 => {
                let val_len = reader.read_u32_le()? as usize;
                let val = reader.read_u8s(val_len)?;
                EntryValue::Present(val)
            }
            _ => {
                return Err(SerializableEntryError::SerializeError(
                    "invalid isPresent".to_string(),
                ));
            }
        })
    }
}
