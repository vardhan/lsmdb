use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom, Write},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use serde_json;
use thiserror::Error;

/// Manifest file
///
/// The manifest file is a log file which describes changes to the database file structure.
///
/// When the database is opened, it consults the manifest file to figure out the list of all sstable
/// files in each level, along with their key ranges. By replaying all of the manifest log, we can
/// construct the latest snapshot of the database file structure. Any further changes to the database
/// file structure are appended as a log entry to the manifest file.
///
/// Each log entry describes one of these events:
/// - New SSTable files are recorded in the manifest.
/// - A compaction event is recorded in the manifest, describing the level which was compacted, and
///   what the new sstables files are for it.
///
/// The latest manifest filename is recorded in a file called `CURRENT`.
/// TODO: As the current manifest file gets large, a new manifest file is created containing the latest snapshot.
///
/// See [`ManifestOp`] for the various kinds of log records in the manifest.

/// Used to express the key-range of an SSTable.
#[derive(Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub(crate) struct KeyRange {
    pub smallest: String, // smallest key in the sstable
    pub largest: String,  // largest key in the sstable
}

/// See [`Manifest::open`] for how to create one.
pub(crate) struct Manifest {
    root_dir: PathBuf,

    manifest_file: File,
    // Each entry is a level. The index of `levels` corresponds to the level #.
    //
    // Each level has a list of sstables file names and the key-range each file contains.
    // The file names are relative to the `root_dir`.
    //
    // The list of sstables are sorted by key-range in ascending order.
    // - level 0 sstables have overlapping keys
    // - level 1 has non-overlapping keys
    levels: Vec<Vec<(KeyRange, String)>>, // the String is the filename
}

#[derive(Error, Debug)]
pub(crate) enum ManifestError {
    #[error("Root dir does not exist")]
    InvalidRootDir,

    #[error("I/O")]
    Io(#[from] io::Error),

    #[error("invalid value in op: {op:?}")]
    OpParseError { op: ManifestOp },

    #[error("could not deserialize op: err={serde_err:?} op={input:?}")]
    OpDeserializeError {
        serde_err: serde_json::Error,
        input: String,
    },

    #[error("SerdeError")]
    SerdeError(#[from] serde_json::Error),

    #[error("trying to merge into an invalid level")]
    MergeLevelInvalid { to_level: usize },
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum ManifestOp {
    // add a new sstable to level 0
    AddSSTable {
        key_range: KeyRange,
        file: String,
    },
    // compact level n-1 into level n.
    // This means level n-1 is now empty, and level n's files are replaced with the new, given files.
    MergeLevel {
        to_level: usize,
        new_level_files: Vec<(KeyRange, String)>,
    },
}

static CURRENT_FILENAME: &str = "CURRENT";
impl Manifest {
    pub(crate) fn open(root_dir: &PathBuf) -> Result<Manifest, ManifestError> {
        if !root_dir.exists() {
            std::fs::create_dir(root_dir.as_path())?;
        } else if !root_dir.is_dir() {
            return Err(ManifestError::InvalidRootDir);
        }

        let current_path = root_dir.join(CURRENT_FILENAME);
        if !current_path.is_file() {
            // create the CURRENT file
            let mut current = File::create(&current_path)?;
            current.write_all("MANIFEST-0".as_bytes())?;
            // current.sync_all()?;
        }

        let manifest_path: PathBuf = root_dir.join(std::fs::read_to_string(&current_path)?);
        let mut manifest_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(manifest_path)?;

        let mut levels: Vec<Vec<(KeyRange, String)>> = vec![];

        for line in BufReader::new(&manifest_file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            Self::parse_op(
                &mut levels,
                serde_json::from_str(&line).map_err(|serde_err| {
                    ManifestError::OpDeserializeError {
                        serde_err,
                        input: line.clone(),
                    }
                })?,
            )?;
        }
        // keep all the key ranges in every level in sorted order
        for level in levels.iter_mut() {
            level.sort();
        }

        manifest_file.seek(SeekFrom::End(0))?;
        Ok(Manifest {
            root_dir: root_dir.clone(),
            manifest_file,
            levels,
        })
    }

    /// The same `root_dir` which was passed into Manifest::open()
    pub(crate) fn root_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    fn parse_op(
        levels: &mut Vec<Vec<(KeyRange, String)>>,
        op: ManifestOp,
    ) -> Result<(), ManifestError> {
        match op {
            ManifestOp::AddSSTable { key_range, file } => {
                if levels.len() == 0 {
                    levels.push(vec![]);
                }
                levels[0].push((key_range, file));
            }
            ManifestOp::MergeLevel {
                to_level,
                new_level_files,
            } if to_level > 0 => {
                // uhoh: we are trying to merge into a level which is higher than a new level.
                if to_level > levels.len() {
                    return Err(ManifestError::MergeLevelInvalid { to_level });
                }
                levels[to_level - 1].clear();
                levels[to_level] = new_level_files;
            }
            _ => return Err(ManifestError::OpParseError { op }),
        }
        Ok(())
    }

    pub(crate) fn add_sstable(
        &mut self,
        file_name: String,
        key_range: KeyRange,
    ) -> Result<(), ManifestError> {
        let op = ManifestOp::AddSSTable {
            key_range,
            file: file_name,
        };
        let log_record = format!("\n{}", serde_json::to_string::<ManifestOp>(&op)?);
        self.manifest_file.write_all(log_record.as_bytes())?;
        Self::parse_op(&mut self.levels, op)?;
        self.levels[0].sort(); // keep level 0 sorted
        Ok(())
    }

    pub(crate) fn levels(&self) -> impl Iterator<Item = &Vec<(KeyRange, std::string::String)>> {
        self.levels.iter()
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use assert_unordered::assert_eq_unordered;
    use tempdir::TempDir;

    use super::{KeyRange, Manifest};

    #[test]
    fn empty() -> anyhow::Result<()> {
        let tmp = TempDir::new("db")?;
        let _manifest = Manifest::open(&tmp.path().to_path_buf())?;
        let expect = vec!["MANIFEST-0".to_string(), "CURRENT".to_string()];
        let actual = tmp
            .path()
            .read_dir()?
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .as_os_str()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq_unordered!(expect, actual);
        Ok(())
    }

    #[test]
    fn add_stable() -> anyhow::Result<()> {
        let tmp = TempDir::new("db")?;
        fs::write(tmp.path().join("CURRENT"), "MANIFEST-42")?;
        fs::write(
            tmp.path().join("MANIFEST-42"),
            [r#"{"AddSSTable":{"key_range":{"smallest":"/ghi","largest":"/jkl"},"file":"1.sst"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/mno","largest":"/pqr"},"file":"2.sst"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/abc","largest":"/def"},"file":"0.sst"}}"#]
                .join("\n"),
        )?;
        let manifest = Manifest::open(&tmp.path().to_path_buf())?;
        assert_eq!(
            vec![vec![
                (
                    KeyRange {
                        smallest: "/abc".to_string(),
                        largest: "/def".to_string(),
                    },
                    "0.sst".to_string()
                ),
                (
                    KeyRange {
                        smallest: "/ghi".to_string(),
                        largest: "/jkl".to_string(),
                    },
                    "1.sst".to_string()
                ),
                (
                    KeyRange {
                        smallest: "/mno".to_string(),
                        largest: "/pqr".to_string(),
                    },
                    "2.sst".to_string()
                ),
            ]],
            manifest.levels
        );
        Ok(())
    }

    #[test]
    fn merge_level() -> anyhow::Result<()> {
        let tmp = TempDir::new("db")?;
        fs::write(tmp.path().join("CURRENT"), "MANIFEST-42")?;
        fs::write(
            tmp.path().join("MANIFEST-42"),
            [r#"{"AddSSTable":{"key_range":{"smallest":"/ghi","largest":"/jkl"},"file":"1.sst"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/mno","largest":"/pqr"},"file":"2.sst"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/abc","largest":"/def"},"file":"0.sst"}}"#]
                .join("\n"),
        )?;
        let manifest = Manifest::open(&tmp.path().to_path_buf())?;
        assert_eq!(
            vec![vec![
                (
                    KeyRange {
                        smallest: "/abc".to_string(),
                        largest: "/def".to_string(),
                    },
                    "0.sst".to_string()
                ),
                (
                    KeyRange {
                        smallest: "/ghi".to_string(),
                        largest: "/jkl".to_string(),
                    },
                    "1.sst".to_string()
                ),
                (
                    KeyRange {
                        smallest: "/mno".to_string(),
                        largest: "/pqr".to_string(),
                    },
                    "2.sst".to_string()
                ),
            ]],
            manifest.levels
        );
        Ok(())
    }
}
