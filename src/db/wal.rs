use std::{
    fs::{File, OpenOptions},
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
}

/// WALWriter appends key-value entries to a log file. This is used for implementing a write-ahead-log.
/// To read out all the key-values in a log, use [WALIterator].
pub(crate) struct WALWriter {
    path: PathBuf,
    writer: File,
}

impl WALWriter {
    /// Creates or re-opens log file.
    pub(crate) fn new(path: &Path) -> Result<WALWriter, WALError> {
        Ok(WALWriter {
            path: path.to_path_buf(),
            writer: OpenOptions::new().create(true).append(true).open(path)?,
        })
    }

    /// Appends the (key, value) to the end of the log
    pub(crate) fn put(&mut self, key: &Key, value: &EntryValue) -> Result<(), WALError> {
        Ok(SerializableEntry {
            key: key.to_string(),
            value: value.clone(),
        }
        .serialize_buffered(&mut self.writer)?)
    }

    /// Clears the contents of the log file -- there will be no entries after this.
    pub(crate) fn clear(&mut self) -> Result<(), WALError> {
        let writer = OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(&self.path)?;
        let mut new_self = WALWriter {
            path: self.path.clone(),
            writer,
        };
        std::mem::swap(self, &mut new_self);
        Ok(())
    }
}

/// WALIterator provides an API to read all the key-value entries in a log.
pub(crate) struct WALIterator {
    reader: File,
}

impl WALIterator {
    /// After calling open(), WALIterator can be used as an iterator to read each key-value entry.
    pub(crate) fn open(path: &PathBuf) -> Result<WALIterator, WALError> {
        Ok(WALIterator {
            reader: OpenOptions::new().read(true).open(path)?,
        })
    }
}

impl Iterator for WALIterator {
    type Item = (Key, EntryValue);

    fn next(&mut self) -> Option<Self::Item> {
        match SerializableEntry::deserialize(&mut self.reader) {
            Ok(entry) => Some((entry.key, entry.value)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::db::types::EntryValue;
    use crate::db::Key;

    use super::{WALIterator, WALWriter};

    #[test]
    fn test_put_iterate_clear() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new("db1")?;
        let tmp_log = tmp_dir.path().join("LOG");
        let mut logger = WALWriter::new(&tmp_log)?;

        logger.put(
            &"key1".to_string(),
            &EntryValue::Present("val1".as_bytes().to_vec()),
        )?;
        logger.put(
            &"key2".to_string(),
            &EntryValue::Present("val2".as_bytes().to_vec()),
        )?;
        logger.put(&"key3".to_string(), &EntryValue::Deleted)?;

        let entries = WALIterator::open(&tmp_log)?.collect::<Vec<(Key, EntryValue)>>();
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

        let entries = WALIterator::open(&tmp_log)?.collect::<Vec<(Key, EntryValue)>>();
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

        let mut logger = WALWriter::new(&tmp_log)?;
        logger.put(
            &"key1".to_string(),
            &EntryValue::Present("val1".as_bytes().to_vec()),
        )?;
        logger.put(
            &"key2".to_string(),
            &EntryValue::Present("val2".as_bytes().to_vec()),
        )?;

        std::mem::drop(logger);
        let mut logger = WALWriter::new(&tmp_log)?;
        logger.put(&"key3".to_string(), &EntryValue::Deleted)?;

        std::mem::drop(logger);

        let entries = WALIterator::open(&tmp_log)?.collect::<Vec<(Key, EntryValue)>>();
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
