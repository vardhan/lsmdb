use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use thiserror::Error;

use super::{
    types::{EntryValue, SerializableEntry, SerializableEntryError},
    Key,
};

/// This file contains write-ahead-log functionality used by the database.
/// Use [WALWriter] to append key-value changes to the log.
/// Use [WALIterator] to iterate through all the key-values.

#[derive(Error, Debug)]
pub(crate) enum WALError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SerializableEntryError(#[from] SerializableEntryError),
    #[error("WALError: {0}")]
    Custom(String),
}

#[derive(PartialEq, Debug)]
pub(crate) enum WALEntry {
    Put(Key, EntryValue),
}
impl WALEntry {
    pub(crate) fn serialize(self, dest: &mut impl Write) -> Result<(), WALError> {
        match &self {
            WALEntry::Put(key, value) => {
                let mut buf: Vec<u8> = Vec::<u8>::with_capacity(
                    1 /* op code */
                        + SerializableEntry::entry_size(key.as_bytes(), value),
                );
                buf.write_all(&[self.to_code()])?;
                SerializableEntry::serialize(&mut buf, key.as_bytes(), value)?;

                dest.write_all(&buf)?;
            }
        };

        Ok(())
    }

    pub(crate) fn deserialize(mut src: &mut impl Read) -> Result<WALEntry, WALError> {
        let mut op = vec![0u8; 1];
        src.read_exact(&mut op)?;

        match op[0] {
            0 => {
                let SerializableEntry { key, value } = SerializableEntry::deserialize(&mut src)?;
                Ok(WALEntry::Put(key, value))
            }
            x => Err(WALError::Custom(format!("Invalid op {}", x))),
        }
    }

    fn to_code(&self) -> u8 {
        match self {
            WALEntry::Put(..) => 0u8,
        }
    }
}

/// WALWriter appends key-value entries to a log file. This is used for implementing a write-ahead-log.
/// To read out all the key-values in a log, use [WALIterator].
pub(crate) struct WALWriter {
    root_dir: PathBuf,
    current_writer: File,
}

impl WALWriter {
    /// Creates or re-opens log file.
    pub(crate) fn new(
        root_dir: &Path,
        current_log_filename: &String,
    ) -> Result<WALWriter, WALError> {
        Ok(WALWriter {
            root_dir: root_dir.to_path_buf(),
            current_writer: OpenOptions::new()
                .create(true)
                .append(true)
                .open(root_dir.join(current_log_filename))?,
        })
    }

    /// Appends the (key, value) to the end of the log
    pub(crate) fn put(&mut self, key: &Key, value: &EntryValue) -> Result<(), WALError> {
        WALEntry::Put(key.to_string(), value.clone()).serialize(&mut self.current_writer)?;
        Ok(())
    }

    /// Rotates to using a new log file and returns its file name.
    pub(crate) fn rotate_log(&mut self) -> Result<String, WALError> {
        let (log_writer, log_filename) = Self::make_new_log(&self.root_dir)?;
        let mut new_self = WALWriter {
            root_dir: self.root_dir.clone(),
            current_writer: log_writer,
        };
        std::mem::swap(self, &mut new_self);
        Ok(log_filename)
    }

    pub(crate) fn make_new_log(root_dir: &Path) -> std::io::Result<(std::fs::File, String)> {
        let mut i = 0;
        loop {
            let candidate_filename = format!("LOG.{i}");
            let candidate_path = root_dir.join(format!("LOG.{i}"));
            if !candidate_path.exists() {
                let file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(candidate_path)?;
                return Ok((file, candidate_filename));
            }
            i += 1;
        }
    }
}

/// [WALIterator] iterates over a single memtable's key values recorded in the WAL.
/// The call must use WALIterator on the same File multiple times until it sees an end of log.
pub(crate) struct WALIterator {
    reader: File,
}

impl WALIterator {
    /// After calling open(), WALIterator can be used as an iterator to read each key-value entry.
    pub(crate) fn open(path: &Path) -> Result<WALIterator, WALError> {
        let reader = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(WALIterator { reader })
    }
}

impl Iterator for WALIterator {
    type Item = WALEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match WALEntry::deserialize(&mut self.reader) {
            Ok(entry) => Some(entry),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::db::{types::EntryValue, wal::WALEntry};

    use super::{WALIterator, WALWriter};

    #[test]
    fn test_put_iterate_rotate() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new("db1")?;
        let (_tmp_log_file, tmp_log_filename) = WALWriter::make_new_log(tmp_dir.path())?;
        let mut wal = WALWriter::new(tmp_dir.path(), &tmp_log_filename)?;

        wal.put(
            &"key1".to_string(),
            &EntryValue::Present("val1".as_bytes().to_vec()),
        )?;
        wal.put(
            &"key2".to_string(),
            &EntryValue::Present("val2".as_bytes().to_vec()),
        )?;
        wal.put(&"key3".to_string(), &EntryValue::Deleted)?;

        let entries =
            WALIterator::open(&tmp_dir.path().join(tmp_log_filename))?.collect::<Vec<WALEntry>>();
        assert_eq!(
            entries,
            vec![
                WALEntry::Put(
                    "key1".to_string(),
                    EntryValue::Present("val1".as_bytes().to_vec())
                ),
                WALEntry::Put(
                    "key2".to_string(),
                    EntryValue::Present("val2".as_bytes().to_vec())
                ),
                WALEntry::Put("key3".to_string(), EntryValue::Deleted)
            ]
        );

        let new_log_file = wal.rotate_log()?;
        wal.put(
            &"key4".to_string(),
            &EntryValue::Present("val4".as_bytes().to_vec()),
        )?;

        let entries =
            WALIterator::open(&tmp_dir.path().join(new_log_file))?.collect::<Vec<WALEntry>>();
        assert_eq!(
            entries,
            vec![WALEntry::Put(
                "key4".to_string(),
                EntryValue::Present("val4".as_bytes().to_vec())
            )]
        );

        Ok(())
    }

    #[test]
    fn test_open_close() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new("db1")?;
        let tmp_log_name = &"LOG".to_string();
        let tmp_log_path = tmp_dir.path().join(tmp_log_name);

        let mut logger = WALWriter::new(tmp_dir.path(), tmp_log_name)?;
        logger.put(
            &"key1".to_string(),
            &EntryValue::Present("val1".as_bytes().to_vec()),
        )?;
        logger.put(
            &"key2".to_string(),
            &EntryValue::Present("val2".as_bytes().to_vec()),
        )?;

        std::mem::drop(logger);
        let mut logger = WALWriter::new(tmp_dir.path(), tmp_log_name)?;
        logger.put(&"key3".to_string(), &EntryValue::Deleted)?;

        std::mem::drop(logger);

        let entries = WALIterator::open(&tmp_log_path)?.collect::<Vec<WALEntry>>();
        assert_eq!(
            entries,
            vec![
                WALEntry::Put(
                    "key1".to_string(),
                    EntryValue::Present("val1".as_bytes().to_vec())
                ),
                WALEntry::Put(
                    "key2".to_string(),
                    EntryValue::Present("val2".as_bytes().to_vec())
                ),
                WALEntry::Put("key3".to_string(), EntryValue::Deleted)
            ]
        );

        Ok(())
    }
}
