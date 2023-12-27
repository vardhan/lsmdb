use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};

use thiserror::Error;

use super::{EntryValue, Key};

#[derive(Error, Debug)]
pub(crate) enum LogError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    // #[error(transparent)]
    // Utf8Error(#[from] Utf8Error),
    // #[error(transparent)]
    // FromUtf8Error(#[from] FromUtf8Error),
    // #[error("block is too big. make a new block")]
    // BlockSizeOverflow,
    // #[error("no keys with prefix")]
    // KeyPrefixNotFound,
    // #[error("Reached the end of block")]
    // EndOfBlock,
    // // TODO:  Replace `Custom` with specific error codes
    #[error("Serialization error: {0}")]
    SerializeError(String),
    // #[error("ManifestError: {0}")]
    // ManifestError(String),
}

pub(crate) struct LogWriter {
    path: PathBuf,
    serializer: Serializer<File>,
}

#[derive(Deserialize, Serialize, Debug)]
struct LogEntry {
    key: Key,
    value: EntryValue,
}

impl LogWriter {
    pub(crate) fn new(path: &PathBuf) -> Result<LogWriter, LogError> {
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.clone())?;
        let serializer = rmp_serde::Serializer::new(writer);
        // .map_err(|err| DBError::LogError(err.to_string()))?;
        Ok(LogWriter {
            path: path.clone(),
            serializer,
        })
    }

    pub(crate) fn put(&mut self, key: &Key, value: &EntryValue) -> Result<(), LogError> {
        let entry = LogEntry {
            key: key.to_string(),
            value: value.clone(),
        };
        entry
            .serialize(&mut self.serializer)
            .map_err(|err| LogError::SerializeError(err.to_string()))?;
        Ok(())
    }

    pub(crate) fn clear(&mut self) -> Result<(), LogError> {
        let writer = OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(&self.path)?;
        let mut new_self = LogWriter {
            path: self.path.clone(),
            serializer: rmp_serde::Serializer::new(writer),
        };
        std::mem::swap(self, &mut new_self);
        Ok(())
    }
}

pub(crate) struct LogIterator {
    reader: File,
}

impl LogIterator {
    pub(crate) fn open(path: &PathBuf) -> Result<LogIterator, LogError> {
        Ok(LogIterator {
            reader: OpenOptions::new().read(true).open(path)?,
        })
    }
}

impl Iterator for LogIterator {
    type Item = (Key, EntryValue);

    fn next(&mut self) -> Option<Self::Item> {
        match rmp_serde::from_read::<&File, LogEntry>(&self.reader)
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
