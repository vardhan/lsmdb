use std::{cmp::Reverse, collections::VecDeque, path::PathBuf};

use crate::{
    db::manifest::KeyRange,
    db::sstable::SSTableReader,
    db_iter::{DBIterator, DBIteratorItem, DBIteratorItemPeekable, DBIteratorItemPrecedence},
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
    /// # Notes
    /// - Caller must garbage collect the files in `sstables_in`
    /// - Caller must record the new files to the `Manifest`
    pub(crate) fn compact(
        &self,
        sstables_in: &mut VecDeque<SSTableReader>,
        level: u32,
    ) -> Result<Vec<(KeyRange, PathBuf)>, SSTableError> {
        // make a DBIterator out of `sstables_in`.
        let iterators = sstables_in
            .iter_mut()
            .map(|reader| DBIteratorItemPeekable::from_sstable(reader, "").unwrap())
            .enumerate()
            .map(|(precedence, iter)| {
                Reverse(DBIteratorItem(iter, precedence as DBIteratorItemPrecedence))
            })
            .collect();
        let iter = DBIterator {
            iterators,
            key_prefix: "".to_string(),
        };

        // compute the max size an sstable shard for (level+1) can be
        let level_max_bytes =
            self.db_config.sstable_level_base_max_size_bytes * 10_u64.pow(level + 1);
        let level_max_shards =
            self.db_config.compaction_level_base_max_shards * 2_u64.pow(level + 1);
        let shard_max_bytes = level_max_bytes / level_max_shards;

        let mut sstables_out = Vec::new();

        let (mut cur_sstable_file, mut cur_sstable_pathbuf) =
            make_new_sstable_file(self.root_dir, level)?;
        let mut cur_sstable_writer: SSTableWriter<'_, '_, std::fs::File> =
            SSTableWriter::new(&mut cur_sstable_file, self.db_config);
        let (mut first_key, mut last_key) = (None, None);

        for (key, value) in iter {
            if (cur_sstable_writer.size() as u64) > shard_max_bytes {
                // flush current sstable and rotate a new one
                cur_sstable_writer.flush()?;
                sstables_out.push((
                    KeyRange {
                        smallest: first_key.unwrap(),
                        largest: last_key.unwrap(),
                    },
                    cur_sstable_pathbuf,
                ));
                // Rotate a new sstable
                (first_key, last_key) = (None, None);
                (cur_sstable_file, cur_sstable_pathbuf) =
                    make_new_sstable_file(self.root_dir, level)?;
                cur_sstable_writer = SSTableWriter::new(&mut cur_sstable_file, self.db_config);
            }
            cur_sstable_writer.push(&key, &super::EntryValue::Present(value))?;
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            last_key = Some(key);
        }

        // flush final sstable if it isn't empty
        if last_key.is_some() {
            cur_sstable_writer.flush()?;
            sstables_out.push((
                KeyRange {
                    smallest: first_key.unwrap(),
                    largest: last_key.unwrap(),
                },
                cur_sstable_pathbuf,
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
    let mut candidate = root_dir.join(format!("l{level}-{i}.sst"));
    while candidate.exists() {
        i += 1;
        candidate = root_dir.join(format!("l{level}-{i}.sst"));
    }
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(candidate.clone())?;
    Ok((file, candidate))
}

#[cfg(test)]
mod test {

    use std::fs::OpenOptions;

    use crate::db::{sstable::write_to_sstable, EntryValue};

    use super::*;
    use anyhow;
    use pretty_assertions::assert_eq;
    use tempdir::TempDir;

    // Generates a list of (sstable file, number of keys in the file), where each sstable file has almost a max amount of keys.
    fn generate_sstables(
        root_dir: &PathBuf,
        db_config: &DBConfig,
        num_files: i32,
        max_per_file_size: usize,
    ) -> anyhow::Result<Vec<(PathBuf, i32)>> {
        let mut sstables_in = Vec::new();
        for file_no in 0..num_files {
            // 2 KB each.
            let (mut file, pathbuf) = make_new_sstable_file(root_dir, 0)?;
            let mut writer = SSTableWriter::new(&mut file, db_config);
            let mut key_no = 0;
            while writer.size() < max_per_file_size {
                writer.push(
                    &format!("/f/{}/{}", file_no, key_no),
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
            compaction_level_base_max_shards: out_num_files, // L1 = OUT_NUM_FILES*2^1
            sstable_level_base_max_size_bytes: out_level_base_size, // OUT_SHARD_SIZE shards for level 0, OUT_SHARD_SIZE*10 shards for level 1
            ..DBConfig::default()
        };

        let compactor = Compactor {
            db_config: &db_config,
            root_dir: &root_dir,
        };

        let sstables_in = generate_sstables(&root_dir, &db_config, in_num_files, in_shard_size)?;

        let sstables_out = compactor.compact(
            &mut sstables_in
                .iter()
                .map(|(pathbuf, _)| SSTableReader::new(pathbuf).unwrap())
                .collect(),
            0,
        )?;

        // we should see 6 KB input / 2.5KB per output shard = 3 files
        assert_eq!(sstables_out.len(), 3);

        // we should see consecutive keys for each file, and the correct key range.
        for (key_range, sstable_pathbuf) in sstables_out {
            let mut reader = SSTableReader::new(&sstable_pathbuf)?;
            let (mut first_key, mut last_key) = (None, None);
            for (key, _) in reader.scan("", true)? {
                if first_key.is_none() {
                    first_key = Some(key.clone());
                }
                last_key = Some(key);
            }
            assert_eq!(key_range.smallest, first_key.unwrap());
            assert_eq!(key_range.largest, last_key.unwrap());
        }

        Ok(())
    }

    #[test]
    fn compact_l0_to_l1_with_deletions() -> anyhow::Result<()> {
        let tmpdir = tempdir::TempDir::new("lsmdb")?;
        let db_config = DBConfig {
            ..Default::default()
        };

        let mut ss1 = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(tmpdir.path().join("ss1"))?;

        // delete the even numbered keys
        let mut ss2 = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(tmpdir.path().join("ss2"))?;

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
        let mut ss2_reader = SSTableReader::new(&tmpdir.path().join("ss2")).unwrap();
        assert_eq!(ss2_reader.scan("", true)?.count(), 0); // only deleted entries, so should be empty.

        let compactor = Compactor {
            db_config: &db_config,
            root_dir: &tmpdir.path().to_path_buf(),
        };
        // compacting [ss1, ss2] should result in all numbers, since ss2 (with deleted entries) has lower precedence
        let mut out_vec = compactor.compact(
            &mut VecDeque::from([ss1_reader.try_clone()?, ss2_reader.try_clone()?]),
            0,
        )?;
        // small data size, so should be 1 sstable file
        assert_eq!(out_vec.len(), 1);
        assert_eq!(
            SSTableReader::new(&out_vec[0].1)?
                .scan("", true)?
                .map(|(k, _v)| k)
                .collect::<Vec<_>>(),
            (0..10)
                .map(|num| format!("/key/{}", num))
                .collect::<Vec<_>>()
        );
        // but compacting the reverse, [ss2, ss1], should omit the deleted entries in ss2 since ss2 has precedence
        out_vec = compactor.compact(&mut VecDeque::from([ss2_reader, ss1_reader]), 0)?;
        // small data size, so should be 1 sstable file
        assert_eq!(out_vec.len(), 1);
        assert_eq!(
            SSTableReader::new(&out_vec[0].1)?
                .scan("", true)?
                .map(|(k, _v)| k)
                .collect::<Vec<_>>(),
            vec!["/key/1", "/key/3", "/key/5", "/key/7", "/key/9"]
        );
        Ok(())
    }
}
