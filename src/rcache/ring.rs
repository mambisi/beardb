pub(crate) trait Consumer {
    fn push(&self, keys: Vec<u64>) -> bool;
}
