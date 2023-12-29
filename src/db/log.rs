use std::{
    fs::{File, OpenOptions},
    io::Read,
    io::Write,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use thiserror::Error;

use super::{
    sstable::{self, BlockWriter},
    EntryValue, Key,
};

#[derive(Error, Debug)]
pub(crate) enum LogError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializeError(String),
}

#[derive(Deserialize, Serialize, Debug)]
struct LogEntry {
    key: Key,
    value: EntryValue,
}

impl LogEntry {
    fn serialize(&self, dest: &mut impl Write) -> Result<(), LogError> {
        let mut buf =
            Vec::<u8>::with_capacity(BlockWriter::entry_size(self.key.as_bytes(), &self.value));
        BlockWriter::write_entry(
            &mut buf,
            self.key.len(),
            self.key.as_bytes(),
            &self.value,
            self.value.len(),
        )
        .map_err(|err| LogError::SerializeError(err.to_string()))?;

        dest.write_all(buf.as_slice())?;
        Ok(())
    }

    fn deserialize(mut src: &mut impl Read) -> Result<LogEntry, LogError> {
        let (key, _) = sstable::read_next_key(&mut src)
            .map_err(|err| LogError::SerializeError(err.to_string()))?;

        let value = sstable::read_next_value(src)
            .map_err(|err| LogError::SerializeError(err.to_string()))?;

        Ok(LogEntry { key, value })
    }
}

pub(crate) struct LogWriter {
    path: PathBuf,
    writer: File,
}

/// LogWriter appends key-value to the end of a log file.  This is used for implementing write-ahead-logging.
/// To read out all the key-values in a log, use [LogIterator].
impl LogWriter {
    /// Creates or re-opens log file.
    pub(crate) fn new(path: &PathBuf) -> Result<LogWriter, LogError> {
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.clone())?;
        Ok(LogWriter {
            path: path.clone(),
            writer,
        })
    }

    /// Appends the (key, value) to the end of the log
    pub(crate) fn put(&mut self, key: &Key, value: &EntryValue) -> Result<(), LogError> {
        let entry = LogEntry {
            key: key.to_string(),
            value: value.clone(),
        };
        entry
            .serialize(&mut self.writer)
            .map_err(|err| LogError::SerializeError(err.to_string()))?;
        Ok(())
    }

    /// Clears the contents of the log file -- there will be no entries after this.
    pub(crate) fn clear(&mut self) -> Result<(), LogError> {
        let writer = OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(&self.path)?;
        let mut new_self = LogWriter {
            path: self.path.clone(),
            writer,
        };
        std::mem::swap(self, &mut new_self);
        Ok(())
    }
}

pub(crate) struct LogIterator {
    reader: File,
}

/// LogIterator provides an API to read all the key-value entries in a log.
impl LogIterator {
    // After calling open(), LogIterator can be used as an iterator to read each key-value entry.
    pub(crate) fn open(path: &PathBuf) -> Result<LogIterator, LogError> {
        Ok(LogIterator {
            reader: OpenOptions::new().read(true).open(path)?,
        })
    }
}

impl Iterator for LogIterator {
    type Item = (Key, EntryValue);

    fn next(&mut self) -> Option<Self::Item> {
        match LogEntry::deserialize(&mut self.reader)
            .map_err(|err| LogError::SerializeError(err.to_string()))
        {
            Ok(entry) => Some((entry.key, entry.value)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::db::{EntryValue, Key};

    use super::{LogIterator, LogWriter};

    #[test]
    fn test_put_iterate_clear() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new("db1")?;
        let tmp_log = tmp_dir.path().join("LOG");
        let mut logger = LogWriter::new(&tmp_log)?;

        logger.put(
            &"key1".to_string(),
            &EntryValue::Present("val1".as_bytes().to_vec()),
        )?;
        logger.put(
            &"key2".to_string(),
            &EntryValue::Present("val2".as_bytes().to_vec()),
        )?;
        logger.put(&"key3".to_string(), &EntryValue::Deleted)?;

        let entries = LogIterator::open(&tmp_log)?.collect::<Vec<(Key, EntryValue)>>();
        assert_eq!(
            entries,
            vec![
                (
                    "key1".to_string(),
                    EntryValue::Present("val1".as_bytes().to_vec())
                ),
                (
                    "key2".to_string(),
                    EntryValue::Present("val2".as_bytes().to_vec())
                ),
                ("key3".to_string(), EntryValue::Deleted)
            ]
        );

        logger.clear()?;
        logger.put(
            &"key4".to_string(),
            &EntryValue::Present("val4".as_bytes().to_vec()),
        )?;

        let entries = LogIterator::open(&tmp_log)?.collect::<Vec<(Key, EntryValue)>>();
        assert_eq!(
            entries,
            vec![(
                "key4".to_string(),
                EntryValue::Present("val4".as_bytes().to_vec())
            )]
        );

        Ok(())
    }

    #[test]
    fn test_open_close() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new("db1")?;
        let tmp_log = tmp_dir.path().join("LOG");

        let mut logger = LogWriter::new(&tmp_log)?;
        logger.put(
            &"key1".to_string(),
            &EntryValue::Present("val1".as_bytes().to_vec()),
        )?;
        logger.put(
            &"key2".to_string(),
            &EntryValue::Present("val2".as_bytes().to_vec()),
        )?;

        std::mem::drop(logger);
        let mut logger = LogWriter::new(&tmp_log)?;
        logger.put(&"key3".to_string(), &EntryValue::Deleted)?;

        std::mem::drop(logger);

        let entries = LogIterator::open(&tmp_log)?.collect::<Vec<(Key, EntryValue)>>();
        assert_eq!(
            entries,
            vec![
                (
                    "key1".to_string(),
                    EntryValue::Present("val1".as_bytes().to_vec())
                ),
                (
                    "key2".to_string(),
                    EntryValue::Present("val2".as_bytes().to_vec())
                ),
                ("key3".to_string(), EntryValue::Deleted)
            ]
        );

        Ok(())
    }
}
