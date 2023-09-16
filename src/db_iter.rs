use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    ops::Bound,
};

use crate::db::{
    sstable::{SSTableError, SSTableReader},
    Memtable,
};
use crate::db::{EntryValue, Key, Value};

// DBIteratorItem is an element in priority queue used for DB::scan().
pub(crate) struct DBIteratorItem<'a>(
    pub(crate) DBIteratorItemPeekable<'a>,
    pub(crate) DBIteratorItemPrecedence,
);
pub(crate) type DBIteratorItemPrecedence = u32; // smaller is newer. active memtable has 0, frozen memtables have 1..N, and sstables have N+1..
                                                // This is a container for a peekable iterator for iterating over memtable or sstable.
pub(crate) struct DBIteratorItemPeekable<'a> {
    next: Option<(Key, EntryValue)>, // this is the item next() and peek() will return
    iter: Box<dyn Iterator<Item = (Key, EntryValue)> + 'a>,
}

impl<'a> DBIteratorItemPeekable<'a> {
    fn new(mut iter: Box<dyn Iterator<Item = (Key, EntryValue)> + 'a>) -> DBIteratorItemPeekable {
        let next = iter.next();
        DBIteratorItemPeekable { next, iter }
    }

    pub(crate) fn from_sstable(
        reader: &'a mut SSTableReader,
        key_prefix: &str,
    ) -> Result<DBIteratorItemPeekable<'a>, SSTableError> {
        reader
            .scan(key_prefix, false)
            .map(|iter| DBIteratorItemPeekable::new(Box::new(iter)))
    }

    pub(crate) fn from_memtable(
        memtable: &'a Memtable,
        key_prefix: &str,
    ) -> DBIteratorItemPeekable<'a> {
        DBIteratorItemPeekable::new(Box::new(
            memtable
                .range((Bound::Included(key_prefix.to_string()), Bound::Unbounded))
                .map(|(key, entry_val)| (key.clone(), entry_val.clone())),
        ))
    }

    pub(crate) fn next(&mut self) -> Option<(Key, EntryValue)> {
        std::mem::replace(&mut self.next, self.iter.next())
    }

    pub(crate) fn peek(&self) -> Option<&(Key, EntryValue)> {
        self.next.as_ref()
    }
}

impl Ord for DBIteratorItem<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // is `self` Less or Greater than `other` ?
        match (&self.0.peek(), &other.0.peek()) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((self_key, _)), Some((ref other_key, _))) => {
                (self_key, self.1).cmp(&(other_key, other.1))
            }
            (None, None) => Ordering::Equal,
        }
    }
}
impl<'a> PartialOrd for DBIteratorItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}
impl<'a> PartialEq for DBIteratorItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl<'a> Eq for DBIteratorItem<'a> {}

/// An iterator used to scan over key-value pairs.
///
/// This iterator is returned by [`DB::scan()`]
pub struct DBIterator<'a> {
    // BinaryHeap is a max-heap, so items (memtable & sstable iterators) are placed with
    // Reverse() to make it a min-heap.
    pub(crate) iterators: BinaryHeap<Reverse<DBIteratorItem<'a>>>,

    // the prefix to scan
    pub(crate) key_prefix: Key,
}

// impl<'a> DBIterator<'a> {
//     pub(crate) fn new(iterators: , key_prefix: Key)
// }

impl<'a> Iterator for DBIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        'pop_key_val: loop {
            // 1. Take out the smallest iterator (we have to put it back at the end)
            let mut top_iter = self.iterators.pop();
            let top_kv = match top_iter {
                None => return None, // There are no more items in the iterator.
                Some(Reverse(DBIteratorItem(ref mut db_iter_peekable, _))) => {
                    db_iter_peekable.next()
                }
            };

            match top_kv {
                // If we hit an iterator that's empty, it implies that all iterators are empty,
                // so we can early-exit.
                None => return None,
                Some((key, entry_value)) => {
                    // 2. Early-exit if we're past the key_prefix.
                    if !key.starts_with(&self.key_prefix) {
                        return None;
                    }
                    // 3. Skip any duplicates of this key -- we already have the newest one.
                    self.skip_entries_with_key(&key);

                    // 4. Put the iterator back into the binary heap and return the entry
                    self.iterators.push(top_iter.unwrap());

                    match entry_value {
                        EntryValue::Present(value) => {
                            return Some((key, value));
                        }
                        EntryValue::Deleted => {
                            continue 'pop_key_val;
                        } // deleted -- try the next key value.
                    }
                }
            };
        }
    }
}

impl DBIterator<'_> {
    fn peek_next_key(&mut self) -> Option<&Key> {
        let next_memtable = self.iterators.peek();
        next_memtable?;
        let DBIteratorItem(ref db_peekable_iter_ref, _) = next_memtable.as_ref().unwrap().0;
        // let mut db_peekable_iter = db_peekable_iter_ref.borrow_mut();
        let next_kv = db_peekable_iter_ref.peek();
        match next_kv {
            Some((key, _)) => Some(key),
            _ => None,
        }
    }

    // Must call peek_next_key() first -- panics if there is no next key.
    fn skip_next_key(&mut self) {
        let next_memtable = self.iterators.pop().unwrap();
        let DBIteratorItem(mut db_peekable_iter_ref, next_kv_order) = next_memtable.0;
        db_peekable_iter_ref.next();
        self.iterators
            .push(Reverse(DBIteratorItem(db_peekable_iter_ref, next_kv_order)));
    }

    fn skip_entries_with_key(&mut self, key: &String) {
        loop {
            match self.peek_next_key() {
                Some(next_key) if key == next_key => self.skip_next_key(),
                _ => return,
            }
        }
    }
}
