use std::{cmp::Reverse, path::PathBuf};

use crate::{
    db_iter::{DBIterator, DBIteratorItem, DBIteratorItemPeekable, DBIteratorPrecedence},
    db::manifest::KeyRange,
    db::sstable::SSTableReader,
};

struct Compactor {
    // the set of sstables to merge. the first sstable has precedence over the rest (and so on).
    sstables_in: Vec<&mut SSTableReader>,
    root_dir: PathBuf, // the director where we will store the new sstable files
    max_sstable_size: usize, // the maximum size an sstable can be before a new one is created
}

impl Compactor {
    // merges all the provided sstables, and writes a new set of sstables out.
    fn compact(&mut self) -> Vec<(KeyRange, PathBuf)> {
        let iterators = self
            .sstables_in
            .iter_mut()
            .map(|&mut reader| DBIteratorItemPeekable::new(Box::new(reader.scan("").unwrap())))
            .enumerate()
            .map(|(precedence, iter)| {
                Reverse(DBIteratorItem(iter, precedence as DBIteratorPrecedence))
            });
        let iter = DBIterator {
            iterators: iterators.collect(),
            key_prefix: "".to_string(),
        };

        SSTableWriter writer;
        for (key, value) in iter {

        }
    }
}
