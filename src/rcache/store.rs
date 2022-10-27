/*
   // Get returns the value associated with the key parameter.
   Get(uint64, uint64) (interface{}, bool)
   // Expiration returns the expiration time for this key.
   Expiration(uint64) time.Time
   // Set adds the key-value pair to the Map or updates the value if it's
   // already present. The key-value pair is passed as a pointer to an
   // item object.
   Set(*Item)
   // Del deletes the key-value pair from the Map.
   Del(uint64, uint64) (uint64, interface{})
   // Update attempts to update the key with a new value and returns true if
   // successful.
   Update(*Item) (interface{}, bool)
   // Cleanup removes items that have an expired TTL.
   Cleanup(policy policy, onEvict itemCallback)
   // Clear clears all contents of the store.
   Clear(onEvict itemCallback)
*/

pub(crate) struct StoreItem<V> {
    pub(crate) key: u64,
    pub(crate) conflict: u64,
    pub(crate) value: V,
}

pub(crate) trait Store<V> {
    fn get(&self, key: u64, conflict: u64) -> Option<&V>;
    fn set(&self, item: &StoreItem<V>);
    fn remove(&self, key: u64, conflict: u64) -> Option<V>;
    fn update(&self, item: &StoreItem<V>) -> Option<V>;
}
