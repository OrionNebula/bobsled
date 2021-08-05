# bobsled

Wrapper traits for storing and indexing tabular data in ordered maps. Intended for use with BTree-based maps like [`sled`](https://github.com/spacejam/sled) or [`std::collections::BTreeMap`](https://doc.rust-lang.org/stable/std/collections/struct.BTreeMap.html), but any lexicographically-ordered map with binary blob keys and values should work.
