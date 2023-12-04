use std::{cmp::Reverse, path::PathBuf};

use crate::{
    db::manifest::KeyRange,
    db::sstable::SSTableReader,
    db_iter::{
        DBIteratorItemPrecedence, KeyEValueIterator, KeyEValueIteratorItem,
        KeyEValueIteratorItemPeekable,
    },
};

use super::{
    sstable::{SSTableError, SSTableWriter},
    DBConfig,
};

pub(crate) struct Compactor<'p, 'c> {
    // the directory where we will store the new sstable files
    pub root_dir: &'p PathBuf,
    pub db_config: &'c DBConfig,
}

impl<'p, 'c> Compactor<'p, 'c> {
    /// Merges all the provided sstables in `sstables_in`, and writes a new set of sstables.
    ///
    /// - `sstables_in`: the set of sstables to merge.
    ///   The first sstable has precedence over the rest (and so on).
    /// - `level`: the level which the supplied `sstables_in` files are in
    ///
    /// Returns a merged list of sstables, where each sstable is denoted by the key range it
    /// contains, and its filename.
    ///
    /// # Notes
    /// - Caller must garbage collect the files in `sstables_in`
    /// - Caller must record the new files to the `Manifest` and `DB`
    pub(crate) fn compact<'r, I: Iterator<Item = SSTableReader>>(
        &self,
        sstables_in: I,
        level_in: u32,
    ) -> Result<Vec<(KeyRange, String)>, SSTableError> {
        // make a DBIterator out of `sstables_in`.
        let iterators = sstables_in
            .map(|reader| KeyEValueIteratorItemPeekable::from_sstable(&reader, "").unwrap())
            .enumerate()
            .map(|(precedence, iter)| {
                Reverse(KeyEValueIteratorItem(
                    iter,
                    precedence as DBIteratorItemPrecedence,
                ))
            })
            .collect();

        // compute the max size an sstable for (level+1) can be
        let sstable_max_bytes = self.db_config.level_max_size(level_in + 1)
            / self.db_config.level_max_sstables(level_in + 1);

        let mut sstables_out: Vec<(KeyRange, String)> = Vec::new();

        let (mut cur_sstable_file, mut cur_sstable_pathbuf) =
            make_new_sstable_file(self.root_dir, level_in)?;
        let mut cur_sstable_writer: SSTableWriter<'_, '_, std::fs::File> =
            SSTableWriter::new(&mut cur_sstable_file, self.db_config);

        let (mut first_key, mut last_key) = (None, None);
        let iter = KeyEValueIterator {
            heap_of_iterators: iterators,
            key_prefix: "".to_string(),
            should_skip_deleted: false,
        };
        for (key, value) in iter {
            cur_sstable_writer.push(&key, &value)?;
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            last_key = Some(key);
            if (cur_sstable_writer.size() as u64) > sstable_max_bytes {
                // flush current sstable and rotate a new one
                cur_sstable_writer.flush()?;
                sstables_out.push((
                    KeyRange {
                        smallest: first_key.unwrap(),
                        largest: last_key.unwrap(),
                    },
                    cur_sstable_pathbuf.to_str().unwrap().to_string(),
                ));
                // Rotate a new sstable
                (first_key, last_key) = (None, None);
                (cur_sstable_file, cur_sstable_pathbuf) =
                    make_new_sstable_file(self.root_dir, level_in)?;
                cur_sstable_writer = SSTableWriter::new(&mut cur_sstable_file, self.db_config);
            }
        }

        // flush final sstable if it isn't empty
        if last_key.is_some() {
            cur_sstable_writer.flush()?;
            sstables_out.push((
                KeyRange {
                    smallest: first_key.unwrap(),
                    largest: last_key.unwrap(),
                },
                // TODO: replace unwrap() with an error.
                cur_sstable_pathbuf.to_str().unwrap().to_string(),
            ));
        }

        Ok(sstables_out)
    }
}

pub fn make_new_sstable_file(
    root_dir: &PathBuf,
    level: u32,
) -> std::io::Result<(std::fs::File, PathBuf)> {
    let mut i = 0;
    loop {
        let candidate = root_dir.join(format!("l{level}-{i}.sst"));
        if !candidate.exists() {
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(candidate.clone())?;
            return Ok((file, candidate));
        }
        i += 1;
    }
}

#[cfg(test)]
mod test {

    use std::fs::OpenOptions;

    use crate::db::{sstable::write_to_sstable, EntryValue};

    use super::*;
    use anyhow;
    use pretty_assertions::assert_eq;
    use tempdir::TempDir;

    // Generates a list of (sstable file, number of keys in the file),
    // where each sstable file has almost a max amount of keys. Keys in
    // each sstable are sorted and non-overlapping with other sstables.
    fn generate_sstables(
        root_dir: &PathBuf,
        db_config: &DBConfig,
        num_files: i32,
        max_bytes_per_file: usize,
    ) -> anyhow::Result<Vec<(PathBuf, i32)>> {
        let mut sstables_in = Vec::new();
        for file_no in 0..num_files {
            // 2 KB each.
            let (mut file, pathbuf) = make_new_sstable_file(root_dir, 0)?;
            let mut writer = SSTableWriter::new(&mut file, db_config);
            let mut key_no = 0;
            while writer.size() < max_bytes_per_file {
                writer.push(
                    &format!("/f/{:0>5}/{:0>5}", file_no, key_no),
                    &EntryValue::Present(format!("val {}-{}", file_no, key_no).into_bytes()),
                )?;
                key_no += 1;
            }
            writer.flush()?;
            sstables_in.push((pathbuf, key_no));
        }
        Ok(sstables_in)
    }

    #[test]
    fn compact_l0_to_l1_basic() -> anyhow::Result<()> {
        let tmpdir = TempDir::new("db")?;

        let in_num_files = 3;
        let in_shard_size = 1024 * 2; // 2 KB per shard, 6 KB in total
        let out_num_files = 2; // attempt to make 4 files across 10KB for level 1
        let out_level_base_size = 1024; // 10 KB total size for level 1 (2.5KB per shard)

        let root_dir: PathBuf = tmpdir.path().into();
        let db_config = DBConfig {
            level_base_max_sstables: out_num_files, // L1 = OUT_NUM_FILES*2^1
            level_base_max_size: out_level_base_size, // OUT_SHARD_SIZE shards for level 0, OUT_SHARD_SIZE*10 shards for level 1
            ..DBConfig::default()
        };

        let compactor = Compactor {
            db_config: &db_config,
            root_dir: &root_dir,
        };

        let sstables_in = generate_sstables(&root_dir, &db_config, in_num_files, in_shard_size)?;

        let sstables_out = compactor.compact(
            sstables_in
                .iter()
                .map(|(pathbuf, _)| SSTableReader::new(pathbuf).unwrap()),
            0,
        )?;

        // we should see 6 KB input / 2.5KB per output shard = 3 files
        assert_eq!(sstables_out.len(), 3);

        // we should see consecutive keys for each file, and the correct key range.
        for (key_range, sstable_filename) in sstables_out {
            let reader = SSTableReader::new(&root_dir.join(sstable_filename))?;
            let (mut first_key, mut last_key) = (None, None);
            for (key, _) in reader.scan("", true)? {
                if first_key.is_none() {
                    first_key = Some(key.clone());
                }
                // assert that keys are consecutive
                if let Some(previous_key) = last_key {
                    assert!(key > previous_key);
                }
                last_key = Some(key);
            }
            assert_eq!(key_range.smallest, first_key.unwrap());
            assert_eq!(key_range.largest, last_key.unwrap());
        }

        Ok(())
    }

    #[test]
    fn compact_with_deletions() -> anyhow::Result<()> {
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let db_config = DBConfig {
            ..Default::default()
        };

        let mut ss1 = make_new_file(&tmpdir, "ss1")?;
        let mut ss2 = make_new_file(&tmpdir, "ss2")?;

        // put key values.
        write_to_sstable(
            (0..10)
                .map(|num| {
                    (
                        format!("/key/{}", num),
                        EntryValue::Present(vec![num as u8]),
                    )
                })
                .collect::<Vec<_>>()
                .iter(),
            &mut ss1,
            &db_config,
        )?;

        // delete alternate key values from ss2
        write_to_sstable(
            [
                ("/key/0".to_string(), EntryValue::Deleted),
                ("/key/2".to_string(), EntryValue::Deleted),
                ("/key/4".to_string(), EntryValue::Deleted),
                ("/key/6".to_string(), EntryValue::Deleted),
                ("/key/8".to_string(), EntryValue::Deleted),
            ]
            .iter(),
            &mut ss2,
            &db_config,
        )?;

        let ss1_reader = SSTableReader::new(&tmpdir.path().join("ss1")).unwrap();
        let ss2_reader = SSTableReader::new(&tmpdir.path().join("ss2")).unwrap();
        assert_eq!(ss2_reader.scan("", true)?.count(), 0); // only deleted entries, so should be empty.

        let root_dir = &tmpdir.path().to_path_buf();
        let compactor = Compactor {
            db_config: &db_config,
            root_dir,
        };
        // compacting [ss1, ss2] should result in all numbers, since ss2 (with deleted entries) has lower precedence
        let out_vec = compactor.compact(
            vec![ss1_reader.try_clone()?, ss2_reader.try_clone()?].into_iter(),
            0,
        )?;
        // small data size, so should be 1 sstable file
        assert_eq!(out_vec.len(), 1);
        let sstable_filename = &out_vec[0].1;
        assert_eq!(
            SSTableReader::new(&root_dir.join(sstable_filename))?
                .scan("", true)?
                .map(|(k, _v)| k)
                .collect::<Vec<_>>(),
            (0..10)
                .map(|num| format!("/key/{}", num))
                .collect::<Vec<_>>()
        );

        // but compacting the reverse, [ss2, ss1], should omit the deleted entries in ss2 since ss2 has precedence
        let out_vec =
            compactor.compact(vec![ss2_reader.try_clone()?, ss1_reader].into_iter(), 0)?;
        // small data size, so should be 1 sstable file
        assert_eq!(out_vec.len(), 1);
        let sstable_filename = &out_vec[0].1;
        assert_eq!(
            SSTableReader::new(&root_dir.join(sstable_filename))?
                .scan("", true)?
                .map(|(k, _v)| k)
                .collect::<Vec<_>>(),
            vec!["/key/1", "/key/3", "/key/5", "/key/7", "/key/9"]
        );

        assert_eq!(ss2_reader.scan("", true)?.count(), 0); // only deleted entries, so should be empty.

        Ok(())
    }

    #[test]
    fn compaction_3_levels() -> anyhow::Result<()> {
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let db_config = DBConfig {
            ..Default::default()
        };

        // Compact across 3 levels, where each level has 1 sstable.
        let mut l0_sst = make_new_file(&tmpdir, "l0")?;
        let mut l1_sst = make_new_file(&tmpdir, "l1")?;
        let mut l2_sst = make_new_file(&tmpdir, "l2")?;

        // L2 puts 3 keys
        // L1 deletes 1 key from L2, and puts 2 keys.
        // L0 deletes 1 from L1, 1 from L2, and puts 1 key.
        write_to_sstable(
            [
                ("/key/1".to_string(), EntryValue::Present(vec![0, 1])),
                ("/key/2".to_string(), EntryValue::Present(vec![0, 2])),
                ("/key/3".to_string(), EntryValue::Present(vec![0, 3])),
            ]
            .iter(),
            &mut l2_sst,
            &db_config,
        )?;

        write_to_sstable(
            [
                ("/key/1".to_string(), EntryValue::Deleted),
                ("/key/4".to_string(), EntryValue::Present(vec![0, 4])),
                ("/key/5".to_string(), EntryValue::Present(vec![0, 5])),
            ]
            .iter(),
            &mut l1_sst,
            &db_config,
        )?;

        write_to_sstable(
            [
                ("/key/3".to_string(), EntryValue::Deleted),
                ("/key/4".to_string(), EntryValue::Deleted),
                ("/key/6".to_string(), EntryValue::Present(vec![0, 6])),
            ]
            .iter(),
            &mut l0_sst,
            &db_config,
        )?;

        // compact L0 -> L1, and then compact again -> L2.
        // The output from  L0 -> L1 should have these keys:
        // - Present: /key/5, /key/6
        // - Deleted: /key/1, /key/3, /key/4
        // The output from L1 -> L2 should have these keys:
        // - Present: /key/2, /key/5, /key/6
        // - Deleted: /key/1, /key/3, /key/4
        let root_dir = &tmpdir.path().to_path_buf();
        let compactor = Compactor {
            db_config: &db_config,
            root_dir,
        };
        let new_l1_sst = compactor.compact(
            [
                SSTableReader::new(&root_dir.join("l0"))?,
                SSTableReader::new(&root_dir.join("l1"))?,
            ]
            .into_iter(),
            0,
        )?;
        assert_eq!(new_l1_sst.len(), 1);
        let new_l1_sst_reader = SSTableReader::new(&root_dir.join(new_l1_sst[0].1.clone()))?;
        assert_eq!(
            new_l1_sst_reader.scan("", false)?.collect::<Vec<_>>(),
            vec![
                ("/key/1".to_string(), EntryValue::Deleted),
                ("/key/3".to_string(), EntryValue::Deleted),
                ("/key/4".to_string(), EntryValue::Deleted),
                ("/key/5".to_string(), EntryValue::Present(vec![0, 5])),
                ("/key/6".to_string(), EntryValue::Present(vec![0, 6])),
            ]
        );

        let new_l2_sst = compactor.compact(
            [
                SSTableReader::new(&root_dir.join(new_l1_sst[0].1.clone()))?,
                SSTableReader::new(&root_dir.join("l2"))?,
            ]
            .into_iter(),
            0,
        )?;
        assert_eq!(new_l2_sst.len(), 1);
        let new_l2_sst_reader = SSTableReader::new(&root_dir.join(new_l2_sst[0].1.clone()))?;
        assert_eq!(
            new_l2_sst_reader.scan("", false)?.collect::<Vec<_>>(),
            vec![
                ("/key/1".to_string(), EntryValue::Deleted),
                ("/key/2".to_string(), EntryValue::Present(vec![0, 2])),
                ("/key/3".to_string(), EntryValue::Deleted),
                ("/key/4".to_string(), EntryValue::Deleted),
                ("/key/5".to_string(), EntryValue::Present(vec![0, 5])),
                ("/key/6".to_string(), EntryValue::Present(vec![0, 6])),
            ]
        );

        Ok(())
    }

    fn make_new_file(tmpdir: &TempDir, filename: &str) -> Result<std::fs::File, anyhow::Error> {
        Ok(OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(tmpdir.path().join(filename))?)
    }
}
