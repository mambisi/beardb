use crate::skiplist::SkipList;

pub(crate) struct MemTable<'a> {
    table : SkipList<'a>
}

