use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use thiserror::Error;
/// Manifest file
///
/// The manifest file is a log file which describes changes to the database file structure.
/// It is different from the log file which describes changes to the database key-value space.
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
#[derive(Clone, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Debug)]
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

    /// Each entry is a log file. The first entry is the oldest log file, and the last is the newest / current.
    pub(crate) logs: VecDeque<String>,
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
    // add a new sstable to level 0, and mark the log file as removed.
    AddSSTable {
        key_range: KeyRange,
        sstable: String,
    },
    // compact level n-1 into level n.
    // This means level n-1 is now empty, and level n's files are replaced with the new, given files.
    MergeLevel {
        to_level: usize,
        sstables: Vec<(KeyRange, String)>,
    },
    // introduce new log file
    NewLog {
        filename: String,
    },
}

static CURRENT_FILENAME: &str = "CURRENT";
impl Manifest {
    pub(crate) fn open(root_dir: &Path) -> Result<Manifest, ManifestError> {
        if !root_dir.exists() {
            std::fs::create_dir(root_dir)?;
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
        let mut logs = VecDeque::<String>::new();

        for line in BufReader::new(&manifest_file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            Self::parse_op(
                &mut levels,
                &mut logs,
                serde_json::from_str(&line).map_err(|serde_err| {
                    ManifestError::OpDeserializeError {
                        serde_err,
                        input: line.clone(),
                    }
                })?,
            )?;
        }
        // keep all the key ranges in every level in sorted order,
        // except for level 0, which is ordered by time range.
        for (levelno, level) in levels.iter_mut().enumerate() {
            if levelno == 0 {
                continue;
            }
            level.sort();
        }

        manifest_file.seek(SeekFrom::End(0))?;
        Ok(Manifest {
            root_dir: root_dir.to_path_buf(),
            manifest_file,
            levels,
            logs,
        })
    }

    /// The same `root_dir` which was passed into Manifest::open()
    pub(crate) fn root_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    /// Parses the operation and mutates the given `levels`.
    fn parse_op(
        levels: &mut Vec<Vec<(KeyRange, String)>>,
        logs: &mut VecDeque<String>,
        op: ManifestOp,
    ) -> Result<(), ManifestError> {
        match op {
            ManifestOp::AddSSTable { key_range, sstable } => {
                if levels.is_empty() {
                    levels.push(vec![]);
                }
                levels[0].push((key_range, sstable));
                // we expect that the oldest log (logs[0]) is compacted into this sstable,
                // so we can stop tracking it.
                logs.pop_front().expect("log expected, but missing"); // unwrap() to make sure we popped something, or
            }
            ManifestOp::MergeLevel { to_level, sstables } if to_level > 0 => {
                if to_level > levels.len() {
                    // uhoh: we are trying to merge into a level which is higher than a new level.
                    return Err(ManifestError::MergeLevelInvalid { to_level });
                }
                levels[to_level - 1].clear();
                if to_level == levels.len() {
                    levels.push(sstables);
                } else {
                    levels[to_level] = sstables;
                }
            }
            ManifestOp::NewLog { filename: file } => {
                logs.push_back(file);
            }
            _ => return Err(ManifestError::OpParseError { op }),
        }
        Ok(())
    }

    /// Add the given SSTable to level 0
    /// Note that the oldest log file is associated with this sstable, and will be removed from being tracked.
    pub(crate) fn add_sstable_l0(
        &mut self,
        sstable_filename: String,
        key_range: KeyRange,
    ) -> Result<(), ManifestError> {
        let op = ManifestOp::AddSSTable {
            key_range,
            sstable: sstable_filename,
        };
        let log_record = format!("\n{}", serde_json::to_string::<ManifestOp>(&op)?);
        self.manifest_file.write_all(log_record.as_bytes())?;
        self.manifest_file.flush()?;
        Self::parse_op(&mut self.levels, &mut self.logs, op)?;
        Ok(())
    }

    /// Record a new set of SSTables for level `to_level`, and clear out the sstables
    /// for level `level`.
    pub(crate) fn merge_level(
        &mut self,
        sstables: Vec<(KeyRange, String)>,
        to_level: usize,
    ) -> Result<(), ManifestError> {
        let op = ManifestOp::MergeLevel { to_level, sstables };
        let log_record = format!("\n{}", serde_json::to_string::<ManifestOp>(&op)?);
        self.manifest_file.write_all(log_record.as_bytes())?;
        self.manifest_file.flush()?;
        Self::parse_op(&mut self.levels, &mut self.logs, op)?;
        Ok(())
    }

    pub(crate) fn new_log(&mut self, file_name: String) -> Result<(), ManifestError> {
        let op = ManifestOp::NewLog {
            filename: file_name,
        };
        let log_record = format!("\n{}", serde_json::to_string::<ManifestOp>(&op)?);
        self.manifest_file.write_all(log_record.as_bytes())?;
        self.manifest_file.flush()?;
        Self::parse_op(&mut self.levels, &mut self.logs, op)?;
        Ok(())
    }

    pub(crate) fn current_log(&self) -> Option<&String> {
        self.logs.back()
    }

    pub(crate) fn iter_levels(
        &self,
    ) -> impl Iterator<Item = &Vec<(KeyRange, std::string::String)>> {
        self.levels.iter()
    }

    pub(crate) fn iter_level_files(
        &self,
        level: usize,
    ) -> impl Iterator<Item = &(KeyRange, String)> {
        self.levels[level].iter()
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
    fn new_log_current_log() -> anyhow::Result<()> {
        let tmp = TempDir::new("db")?;
        fs::write(tmp.path().join("CURRENT"), "MANIFEST-42")?;
        fs::write(
            tmp.path().join("MANIFEST-42"),
            [r#"{"NewLog":{"filename":"LOG.0"}}"#].join("\n"),
        )?;
        let mut manifest = Manifest::open(&tmp.path().to_path_buf())?;
        assert_eq!(Some(&"LOG.0".to_string()), manifest.current_log());

        manifest.new_log("LOG.1".to_string())?;
        assert_eq!(Some(&"LOG.1".to_string()), manifest.current_log());

        manifest.new_log("LOG.2".to_string())?;
        assert_eq!(Some(&"LOG.2".to_string()), manifest.current_log());

        // assert the logs are added in order.
        assert_eq!(
            vec![
                &"LOG.0".to_string(),
                &"LOG.1".to_string(),
                &"LOG.2".to_string()
            ],
            manifest.logs.iter().collect::<Vec<_>>()
        );

        // AddTable should consume/erase the oldest log.
        manifest.add_sstable_l0(
            "l0-0.sst".to_string(),
            KeyRange {
                smallest: "".to_string(),
                largest: "".to_string(),
            },
        )?;
        assert_eq!(Some(&"LOG.2".to_string()), manifest.current_log());

        // assert the logs are added in order.
        assert_eq!(
            vec![&"LOG.1".to_string(), &"LOG.2".to_string()],
            manifest.logs.iter().collect::<Vec<_>>()
        );

        // but merge operations should not affect the logs
        manifest.merge_level(
            vec![(
                KeyRange {
                    smallest: "".to_string(),
                    largest: "".to_string(),
                },
                "l1-0.sst".to_string(),
            )],
            1,
        )?;
        assert_eq!(
            vec![&"LOG.1".to_string(), &"LOG.2".to_string()],
            manifest.logs.iter().collect::<Vec<_>>()
        );
        assert_eq!(Some(&"LOG.2".to_string()), manifest.current_log());

        // empty out all the logs by convering them to SSTables
        manifest.add_sstable_l0(
            "l0-1.sst".to_string(),
            KeyRange {
                smallest: "".to_string(),
                largest: "".to_string(),
            },
        )?;
        manifest.add_sstable_l0(
            "l0-2.sst".to_string(),
            KeyRange {
                smallest: "".to_string(),
                largest: "".to_string(),
            },
        )?;
        assert_eq!(None, manifest.current_log());

        manifest.new_log("LOG.0".to_string())?;
        assert_eq!(Some(&"LOG.0".to_string()), manifest.current_log());

        Ok(())
    }

    #[test]
    fn add_stable() -> anyhow::Result<()> {
        let tmp = TempDir::new("db")?;
        fs::write(tmp.path().join("CURRENT"), "MANIFEST-42")?;
        fs::write(
            tmp.path().join("MANIFEST-42"),
            [
                    r#"{"NewLog":{"filename":"LOG.0"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/ghi","largest":"/jkl"},"sstable":"1.sst"}}"#,
                    r#"{"NewLog":{"filename":"LOG.1"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/mno","largest":"/pqr"},"sstable":"2.sst"}}"#,
                    r#"{"NewLog":{"filename":"LOG.2"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/abc","largest":"/def"},"sstable":"0.sst"}}"#]
                .join("\n"),
        )?;
        let manifest = Manifest::open(&tmp.path().to_path_buf())?;
        // assert that the sstables in level 0 are not re-ordered.
        assert_eq!(
            vec![vec![
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
                (
                    KeyRange {
                        smallest: "/abc".to_string(),
                        largest: "/def".to_string(),
                    },
                    "0.sst".to_string()
                ),
            ]],
            manifest.levels
        );
        Ok(())
    }

    #[test]
    fn merge_level() -> anyhow::Result<()> {
        let tmp1 = TempDir::new("db1")?;
        let tmp2 = TempDir::new("db2")?;
        fs::write(tmp1.path().join("CURRENT"), "MANIFEST-42")?;
        fs::write(
            tmp1.path().join("MANIFEST-42"),
            [
                    r#"{"NewLog":{"filename":"LOG.0"}}"#,
                    r#"{"NewLog":{"filename":"LOG.1"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/ghi","largest":"/jkl"},"sstable":"1.sst"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/mno","largest":"/pqr"},"sstable":"2.sst"}}"#,
                    r#"{"NewLog":{"filename":"LOG.2"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/abc","largest":"/def"},"sstable":"0.sst"}}"#,
                    r#"{"MergeLevel":{"to_level":1,"sstables":[[{"smallest":"/ghi","largest":"/jkl"}, "4.sst"],[{"smallest":"/abc","largest":"/def"}, "3.sst"]]}}"#,
                    r#"{"NewLog":{"filename":"LOG.3"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/xyx","largest":"/xzz"},"sstable":"5.sst"}}"#,
                    r#"{"MergeLevel":{"to_level":1,"sstables":[[{"smallest":"/abc","largest":"/jkl"}, "6.sst"], [{"smallest":"/mno","largest":"/xzz"}, "7.sst"]]}}"#,
                    r#"{"NewLog":{"filename":"LOG.4"}}"#,
                    r#"{"AddSSTable":{"key_range":{"smallest":"/abc","largest":"/def"},"sstable":"8.sst"}}"#,
                    ]
                .join("\n"),
        )?;
        // `manifest` is used to test that the JSON input was parsed out correctly
        let manifest = Manifest::open(&tmp1.path().to_path_buf())?;
        // `manifest2` is populated to be the same as `manifest`, but using the `Manifest` API.
        let mut manifest2 = Manifest::open(&tmp2.path().to_path_buf())?;
        manifest2.new_log("LOG.0".to_string())?;
        manifest2.new_log("LOG.1".to_string())?;
        manifest2.add_sstable_l0(
            "1.sst".to_string(),
            KeyRange {
                smallest: "/ghi".to_string(),
                largest: "/jkl".to_string(),
            },
        )?;
        manifest2.add_sstable_l0(
            "2.sst".to_string(),
            KeyRange {
                smallest: "/mno".to_string(),
                largest: "/pqr".to_string(),
            },
        )?;
        manifest2.new_log("LOG.2".to_string())?;
        manifest2.add_sstable_l0(
            "0.sst".to_string(),
            KeyRange {
                smallest: "/abc".to_string(),
                largest: "/def".to_string(),
            },
        )?;
        manifest2.merge_level(
            vec![
                (
                    KeyRange {
                        smallest: "/ghi".to_string(),
                        largest: "/jkl".to_string(),
                    },
                    "3.sst".to_string(),
                ),
                (
                    KeyRange {
                        smallest: "/abc".to_string(),
                        largest: "/def".to_string(),
                    },
                    "4.sst".to_string(),
                ),
            ],
            1,
        )?;
        manifest2.new_log("LOG.3".to_string())?;
        manifest2.add_sstable_l0(
            "5.sst".to_string(),
            KeyRange {
                smallest: "/xyx".to_string(),
                largest: "/xzz".to_string(),
            },
        )?;
        manifest2.merge_level(
            vec![
                (
                    KeyRange {
                        smallest: "/abc".to_string(),
                        largest: "/jkl".to_string(),
                    },
                    "6.sst".to_string(),
                ),
                (
                    KeyRange {
                        smallest: "/mno".to_string(),
                        largest: "/xzz".to_string(),
                    },
                    "7.sst".to_string(),
                ),
            ],
            1,
        )?;
        manifest2.new_log("LOG.4".to_string())?;
        manifest2.add_sstable_l0(
            "8.sst".to_string(),
            KeyRange {
                smallest: "/abc".to_string(),
                largest: "/def".to_string(),
            },
        )?;
        assert_eq!(
            vec![
                // level 0 should have /abc -> /def
                vec![(
                    KeyRange {
                        smallest: "/abc".to_string(),
                        largest: "/def".to_string(),
                    },
                    "8.sst".to_string()
                )],
                // level 1 should merge all the files (and keyspace) in level 0
                vec![
                    (
                        KeyRange {
                            smallest: "/abc".to_string(),
                            largest: "/jkl".to_string(),
                        },
                        "6.sst".to_string()
                    ),
                    (
                        KeyRange {
                            smallest: "/mno".to_string(),
                            largest: "/xzz".to_string(),
                        },
                        "7.sst".to_string()
                    ),
                ]
            ],
            manifest.levels
        );
        assert_eq!(manifest.levels, manifest2.levels);
        Ok(())
    }
}
