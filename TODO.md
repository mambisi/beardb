## Tech Debt TODOs

* [ ] Define a **cost trait** for SampledLFU make cost part configurable by the user of `rcache` example is have a key
  value pair that doesn't have a static cost at the time of insertion or update.
* [ ] Remove or Change LFU Cache, or use exponential decay function to remove items from the cache, by halving the
  frequencies periodically 